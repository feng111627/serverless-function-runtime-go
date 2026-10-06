package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestInstanceReuse 覆盖业务：实例复用与实例级状态。
func TestInstanceReuse(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	err := rt.Register("counter", FunctionConfig{MaxConcurrency: 1}, func(ctx context.Context, inv *Invocation) (any, error) {
		n, _ := inv.State["count"].(int)
		n++
		inv.State["count"] = n
		return fmt.Sprintf("%s#%d", inv.InstanceID, n), nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 串行调用应复用同一实例，实例状态在多次调用间累积")
	first, err := rt.Invoke(context.Background(), "counter", nil)
	if err != nil {
		t.Fatalf("调用失败: %v", err)
	}
	second, err := rt.Invoke(context.Background(), "counter", nil)
	if err != nil {
		t.Fatalf("调用失败: %v", err)
	}
	third, err := rt.Invoke(context.Background(), "counter", nil)
	if err != nil {
		t.Fatalf("调用失败: %v", err)
	}
	t.Logf("三次调用结果: %v / %v / %v", first, second, third)
	if first != "counter-inst-1#1" || second != "counter-inst-1#2" || third != "counter-inst-1#3" {
		t.Fatalf("实例未被复用或状态未累积: %v %v %v", first, second, third)
	}

	st, _ := rt.Stats("counter")
	t.Logf("用例2: 串行调用只应冷启动一次，实际冷启动 %d 次", st.ColdStarts)
	if st.ColdStarts != 1 {
		t.Fatalf("期望冷启动1次，实际: %d", st.ColdStarts)
	}
	t.Log("实例复用用例全部通过")
}

// TestInstanceStateIsolation 覆盖业务：不同实例的可变状态互不污染。
func TestInstanceStateIsolation(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	var mu sync.Mutex
	seen := map[string]int{}
	err := rt.Register("iso", FunctionConfig{MaxConcurrency: 2}, func(ctx context.Context, inv *Invocation) (any, error) {
		n, _ := inv.State["hits"].(int)
		n++
		inv.State["hits"] = n
		mu.Lock()
		if n > seen[inv.InstanceID] {
			seen[inv.InstanceID] = n
		}
		mu.Unlock()
		<-release
		return nil, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 两个并发调用应落在不同实例上，各自状态独立")
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rt.Invoke(context.Background(), "iso", nil)
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, _ := rt.Stats("iso")
		if st.Instances == 2 && st.InFlight == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("未扩容到2个实例: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	wg.Wait()
	mu.Lock()
	t.Logf("各实例状态: %v", seen)
	if len(seen) != 2 {
		t.Fatalf("期望2个实例各自持有状态，实际: %v", seen)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("实例 %s 的状态被污染，计数: %d", id, n)
		}
	}
	mu.Unlock()
	t.Log("实例状态隔离用例全部通过")
}

// TestInstanceReclaim 覆盖业务：空闲实例回收与回收后不再接受请求。
func TestInstanceReclaim(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	err := rt.Register("evanescent", FunctionConfig{
		MaxConcurrency: 1,
		IdleTimeout:    60 * time.Millisecond,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		return inv.InstanceID, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 空闲超过 IdleTimeout 的实例应被回收")
	first, err := rt.Invoke(context.Background(), "evanescent", nil)
	if err != nil {
		t.Fatalf("调用失败: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	var st PoolStats
	for {
		st, _ = rt.Stats("evanescent")
		if st.Instances == 0 && st.Reclaimed >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("实例未被回收: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("回收完成: %+v", st)

	t.Log("用例2: 回收后的实例不再接受请求，新调用应冷启动新实例")
	second, err := rt.Invoke(context.Background(), "evanescent", nil)
	if err != nil {
		t.Fatalf("调用失败: %v", err)
	}
	t.Logf("回收前实例: %v，回收后实例: %v", first, second)
	if first == second {
		t.Fatalf("回收后的实例仍被使用: %v", first)
	}
	st, _ = rt.Stats("evanescent")
	if st.ColdStarts != 2 {
		t.Fatalf("期望冷启动2次，实际: %d", st.ColdStarts)
	}
	t.Log("实例回收用例全部通过")
}

// TestBusyInstanceNotReclaimed 覆盖业务：执行中的实例不会被回收。
func TestBusyInstanceNotReclaimed(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	started := make(chan struct{})
	err := rt.Register("longrun", FunctionConfig{
		MaxConcurrency: 1,
		IdleTimeout:    50 * time.Millisecond,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		close(started)
		<-release
		return inv.InstanceID, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 执行时长超过 IdleTimeout 时，执行中的实例不得被回收")
	resCh := make(chan any, 1)
	go func() {
		res, _ := rt.Invoke(context.Background(), "longrun", nil)
		resCh <- res
	}()
	<-started
	time.Sleep(150 * time.Millisecond) // 远超 IdleTimeout
	st, _ := rt.Stats("longrun")
	if st.Instances != 1 || st.Reclaimed != 0 {
		t.Fatalf("执行中的实例被回收: %+v", st)
	}
	close(release)
	if res := <-resCh; res != "longrun-inst-1" {
		t.Fatalf("调用结果异常: %v", res)
	}
	t.Log("执行中实例保护用例全部通过")
}
