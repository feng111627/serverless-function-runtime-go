package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 业务场景：并发上限 —— 拒绝策略下超限请求立即失败且原因可区分。
func TestConcurrencyLimit_RejectPolicy(t *testing.T) {
	rt := New()
	defer rt.Close()
	block := make(chan struct{})
	var inFlight, maxSeen atomic.Int32
	err := rt.Register("fn", Config{
		MaxConcurrency: 2,
		Policy:         PolicyReject,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		defer inFlight.Add(-1)
		<-block
		return req, nil
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); rt.Invoke(context.Background(), "fn", nil) }()
	}
	// 等两个请求都进入执行。
	deadline := time.Now().Add(2 * time.Second)
	for inFlight.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	_, err = rt.Invoke(context.Background(), "fn", nil)
	if !errors.Is(err, ErrConcurrencyLimited) {
		t.Fatalf("期望 ErrConcurrencyLimited，实际: %v", err)
	}
	var ie *InvokeError
	if errors.As(err, &ie) && ie.Reason != ReasonConcurrencyLimited {
		t.Fatalf("期望原因 concurrency_limited，实际: %s", ie.Reason)
	}
	t.Logf("用例[并发上限-拒绝策略]：在途=%d 超限请求被拒绝，原因=%s", maxSeen.Load(), ReasonConcurrencyLimited)

	close(block)
	wg.Wait()
	if maxSeen.Load() > 2 {
		t.Fatalf("在途并发 %d 超过上限 2", maxSeen.Load())
	}
	t.Logf("用例[并发上限-拒绝策略]：峰值在途=%d，未超过上限 2 ✓", maxSeen.Load())
}

// 业务场景：并发上限 —— 排队策略下超限请求排队等待，最终全部成功。
func TestConcurrencyLimit_QueuePolicy(t *testing.T) {
	rt := New()
	defer rt.Close()
	var inFlight, maxSeen atomic.Int32
	err := rt.Register("fn", Config{
		MaxConcurrency: 2,
		Policy:         PolicyQueue,
		QueueSize:      8,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		n := inFlight.Add(1)
		for {
			m := maxSeen.Load()
			if n <= m || maxSeen.CompareAndSwap(m, n) {
				break
			}
		}
		defer inFlight.Add(-1)
		time.Sleep(20 * time.Millisecond)
		return req, nil
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	const total = 6
	var wg sync.WaitGroup
	var succeeded atomic.Int32
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := rt.Invoke(context.Background(), "fn", i)
			if err != nil {
				t.Errorf("排队请求不应失败: %v", err)
				return
			}
			if res != i {
				t.Errorf("结果错乱: 期望 %d 实际 %v", i, res)
				return
			}
			succeeded.Add(1)
		}(i)
	}
	wg.Wait()
	if succeeded.Load() != total {
		t.Fatalf("成功数 %d，期望 %d", succeeded.Load(), total)
	}
	if maxSeen.Load() > 2 {
		t.Fatalf("在途并发 %d 超过上限 2", maxSeen.Load())
	}
	t.Logf("用例[并发上限-排队策略]：%d 个请求全部成功，峰值在途=%d（上限 2）✓", succeeded.Load(), maxSeen.Load())
}

// 业务场景：执行超时 —— 超时调用被终止并返回可区分原因。
func TestInvokeTimeout(t *testing.T) {
	rt := New()
	defer rt.Close()
	err := rt.Register("slow", Config{
		MaxConcurrency: 1,
		Timeout:        50 * time.Millisecond,
	}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			return "done", nil
		}
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	start := time.Now()
	_, err = rt.Invoke(context.Background(), "slow", nil)
	elapsed := time.Since(start)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望 ErrTimeout，实际: %v", err)
	}
	var ie *InvokeError
	if errors.As(err, &ie) && ie.Reason != ReasonTimeout {
		t.Fatalf("期望原因 timeout，实际: %s", ie.Reason)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("超时未被及时终止，耗时 %v", elapsed)
	}
	t.Logf("用例[执行超时]：50ms 超时生效，%v 后终止，原因=%s ✓", elapsed.Round(time.Millisecond), ReasonTimeout)
}

// 业务场景：路由隔离 —— 请求只派给目标函数，不会串到别的函数。
func TestInvokeRoutingIsolation(t *testing.T) {
	rt := New()
	defer rt.Close()
	rt.Register("a", Config{}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		return "a", nil
	})
	rt.Register("b", Config{}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		return "b", nil
	})

	res, err := rt.Invoke(context.Background(), "a", nil)
	if err != nil || res != "a" {
		t.Fatalf("函数 a 返回 %v, %v", res, err)
	}
	if _, err := rt.Invoke(context.Background(), "missing", nil); !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("未注册函数期望 ErrFunctionNotFound，实际: %v", err)
	}
	t.Log("用例[路由隔离]：请求精确路由到目标函数，未注册函数返回可区分错误 ✓")
}
