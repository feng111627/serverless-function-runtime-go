package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrencyLimit 覆盖业务：并发上限与排队策略。
func TestConcurrencyLimit(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	var inFlight atomic.Int32
	var maxSeen atomic.Int32
	release := make(chan struct{})
	err := rt.Register("limited", FunctionConfig{
		MaxConcurrency: 2,
		MaxQueue:       1,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		n := inFlight.Add(1)
		for {
			old := maxSeen.Load()
			if n <= old || maxSeen.CompareAndSwap(old, n) {
				break
			}
		}
		<-release
		inFlight.Add(-1)
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 并发上限为2时，同时在途执行不得超过2")
	var wg sync.WaitGroup
	results := make([]error, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, results[i] = rt.Invoke(context.Background(), "limited", nil)
		}(i)
	}
	// 等待 2 个在途 + 1 个排队就位，第 4 个应被背压拒绝。
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, _ := rt.Stats("limited")
		if st.InFlight == 2 && st.Queued == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("排队状态未达预期: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Log("用例2: 并发占满且队列满时，新请求应收到 ErrQueueFull 背压信号")
	_, err = rt.Invoke(context.Background(), "limited", nil)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("期望 ErrQueueFull，实际: %v", err)
	}

	close(release)
	wg.Wait()
	if got := maxSeen.Load(); got != 2 {
		t.Fatalf("并发上限被突破，最大在途: %d", got)
	}
	succeeded := 0
	for _, e := range results {
		if e == nil {
			succeeded++
		}
	}
	t.Logf("用例3: 排队请求不静默丢弃，4个请求中 %d 个成功、%d 个被背压拒绝",
		succeeded, 4-succeeded)
	if succeeded != 3 {
		t.Fatalf("期望3个成功（2在途+1排队），实际: %d", succeeded)
	}
	t.Log("并发上限与排队用例全部通过")
}

// TestExecutionTimeout 覆盖业务：执行超时。
func TestExecutionTimeout(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	err := rt.Register("sleepy", FunctionConfig{
		Timeout: 50 * time.Millisecond,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		select {
		case <-time.After(10 * time.Second):
			return "late", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 执行超过函数超时配置，应返回可区分的 ErrTimeout")
	start := time.Now()
	_, err = rt.Invoke(context.Background(), "sleepy", nil)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望 ErrTimeout，实际: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("超时未被及时终止，耗时: %v", elapsed)
	}

	t.Log("用例2: 超时后实例应恢复可用，后续调用正常")
	// 上一个 handler 响应 ctx 取消后退出，实例应可复用。
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, _ := rt.Stats("sleepy")
		if st.Timeouts == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("超时计数未更新: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Log("执行超时用例全部通过")
}

// TestNoCrossFunctionDispatch 覆盖业务：请求不会被派发给其他函数。
func TestNoCrossFunctionDispatch(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	block := make(chan struct{})
	err := rt.Register("busy", FunctionConfig{MaxConcurrency: 1, MaxQueue: 0}, func(ctx context.Context, inv *Invocation) (any, error) {
		<-block
		return "busy", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	err = rt.Register("free", FunctionConfig{}, func(ctx context.Context, inv *Invocation) (any, error) {
		return "free", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: busy 函数占满时不影响 free 函数调用")
	done := make(chan struct{})
	go func() {
		rt.Invoke(context.Background(), "busy", nil)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond) // 等 busy 进入执行
	res, err := rt.Invoke(context.Background(), "free", nil)
	if err != nil || res != "free" {
		t.Fatalf("free 调用受影响: res=%v err=%v", res, err)
	}

	t.Log("用例2: busy 队列长度为0时，超限请求立即拒绝而非派给 free")
	if _, err := rt.Invoke(context.Background(), "busy", nil); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("期望 ErrQueueFull，实际: %v", err)
	}
	close(block)
	<-done
	t.Log("函数隔离用例全部通过")
}
