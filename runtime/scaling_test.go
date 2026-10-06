package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestScaleUpAndDown 覆盖业务：压力上升扩容、压力回落缩容。
func TestScaleUpAndDown(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	err := rt.Register("elastic", FunctionConfig{
		MaxConcurrency: 4,
		MaxInstances:   4,
		IdleTimeout:    60 * time.Millisecond,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		<-release
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 4个并发请求压上来，实例数应扩容到4")
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rt.Invoke(context.Background(), "elastic", nil)
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, _ := rt.Stats("elastic")
		if st.Instances == 4 && st.InFlight == 4 {
			t.Logf("扩容到位: %+v", st)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("扩容未达预期: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Log("用例2: 压力回落后，空闲实例应缩容回收")
	close(release)
	wg.Wait()
	deadline = time.Now().Add(3 * time.Second)
	for {
		st, _ := rt.Stats("elastic")
		if st.Instances == 0 && st.Reclaimed == 4 {
			t.Logf("缩容到位: %+v", st)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("缩容未达预期: %+v", st)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Log("扩缩容用例全部通过")
}

// TestBackpressure 覆盖业务：积压队列上限与背压信号。
func TestBackpressure(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	err := rt.Register("bottleneck", FunctionConfig{
		MaxConcurrency: 1,
		MaxQueue:       2,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		<-release
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 1在途+2排队占满后，超出请求应收到 ErrQueueFull")
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rt.Invoke(context.Background(), "bottleneck", nil)
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, _ := rt.Stats("bottleneck")
		if st.InFlight == 1 && st.Queued == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("积压状态未达预期: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	_, err = rt.Invoke(context.Background(), "bottleneck", nil)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("期望 ErrQueueFull，实际: %v", err)
	}
	st, _ := rt.Stats("bottleneck")
	t.Logf("背压生效: %+v", st)
	if st.Rejected != 1 {
		t.Fatalf("期望拒绝计数为1，实际: %d", st.Rejected)
	}

	t.Log("用例2: 排队请求在压力释放后应被正常执行，不静默丢弃")
	close(release)
	wg.Wait()
	st, _ = rt.Stats("bottleneck")
	if st.Executed != 3 {
		t.Fatalf("期望3个请求全部执行，实际: %d", st.Executed)
	}
	t.Log("背压用例全部通过")
}

// TestBurstCapped 覆盖业务：突发流量下实例数不无限增长。
func TestBurstCapped(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	err := rt.Register("burst", FunctionConfig{
		MaxConcurrency: 3,
		MaxInstances:   3,
		MaxQueue:       100,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		<-release
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 30个突发请求下，实例数不得超过上限3")
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rt.Invoke(context.Background(), "burst", nil)
		}()
	}
	maxInstances := 0
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, _ := rt.Stats("burst")
		if st.Instances > maxInstances {
			maxInstances = st.Instances
		}
		if st.Queued == 27 && st.InFlight == 3 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("突发期间峰值实例数: %d", maxInstances)
	if maxInstances > 3 {
		t.Fatalf("实例数突破上限: %d", maxInstances)
	}
	close(release)
	wg.Wait()
	st, _ := rt.Stats("burst")
	t.Logf("突发结束后: %+v", st)
	if st.Executed != 30 {
		t.Fatalf("期望30个请求全部执行，实际: %d", st.Executed)
	}
	t.Log("突发上限用例全部通过")
}

// TestMinInstancesFloor 覆盖业务：常驻实例下限，缩容不跌破。
func TestMinInstancesFloor(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	err := rt.Register("warm", FunctionConfig{
		MinInstances: 2,
		MaxInstances: 4,
		IdleTimeout:  50 * time.Millisecond,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 注册后应预热到 MinInstances=2")
	st, _ := rt.Stats("warm")
	if st.Instances != 2 {
		t.Fatalf("期望预热2个实例，实际: %d", st.Instances)
	}

	t.Log("用例2: 长时间空闲后实例数不应跌破下限")
	time.Sleep(200 * time.Millisecond)
	st, _ = rt.Stats("warm")
	t.Logf("空闲后: %+v", st)
	if st.Instances != 2 {
		t.Fatalf("实例数跌破下限: %d", st.Instances)
	}
	t.Log("常驻下限用例全部通过")
}
