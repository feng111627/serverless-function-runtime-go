package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 业务场景：压力上升扩容 —— 并发请求推动实例数增长，但不超过实例上限。
func TestScaleUpUnderPressure(t *testing.T) {
	rt := New()
	defer rt.Close()
	const maxInst = 3
	release := make(chan struct{})
	var started atomic.Int32
	rt.Register("fn", Config{
		MaxConcurrency: maxInst,
		MaxInstances:   maxInst,
		Policy:         PolicyQueue,
		QueueSize:      16,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		started.Add(1)
		<-release
		return nil, nil
	})

	var wg sync.WaitGroup
	for i := 0; i < maxInst; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); rt.Invoke(context.Background(), "fn", nil) }()
	}
	deadline := time.Now().Add(2 * time.Second)
	for started.Load() < maxInst && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	s := mustStats(t, rt, "fn")
	if s.Instances != maxInst {
		t.Fatalf("压力下实例数期望扩容到 %d，实际 %d", maxInst, s.Instances)
	}
	t.Logf("用例[压力扩容]：%d 个并发请求推动实例数扩容到 %d（上限 %d）✓", maxInst, s.Instances, maxInst)
	close(release)
	wg.Wait()
}

// 业务场景：突发流量 —— 实例数有硬上限，超限请求排队，不会无限扩容。
func TestBurstCappedByMaxInstances(t *testing.T) {
	rt := New()
	defer rt.Close()
	const maxInst = 2
	var maxInstances atomic.Int32
	rt.Register("fn", Config{
		MaxConcurrency: maxInst,
		MaxInstances:   maxInst,
		Policy:         PolicyQueue,
		QueueSize:      32,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		n := mustStats(t, rt, "fn").Instances
		for {
			m := maxInstances.Load()
			if int32(n) <= m || maxInstances.CompareAndSwap(m, int32(n)) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		return nil, nil
	})

	const burst = 20
	var wg sync.WaitGroup
	var succeeded atomic.Int32
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := rt.Invoke(context.Background(), "fn", nil); err == nil {
				succeeded.Add(1)
			}
		}()
	}
	wg.Wait()
	if succeeded.Load() != burst {
		t.Fatalf("排队策略下 %d 个突发请求应全部成功，实际 %d", burst, succeeded.Load())
	}
	if maxInstances.Load() > maxInst {
		t.Fatalf("实例数峰值 %d 超过上限 %d", maxInstances.Load(), maxInst)
	}
	t.Logf("用例[突发上限]：%d 个突发请求全部成功，实例数峰值=%d（上限 %d），未无限增长 ✓",
		burst, maxInstances.Load(), maxInst)
}

// 业务场景：背压 —— 积压队列满后返回可区分的背压信号。
func TestBackpressureQueueFull(t *testing.T) {
	rt := New()
	defer rt.Close()
	release := make(chan struct{})
	rt.Register("fn", Config{
		MaxConcurrency: 1,
		Policy:         PolicyQueue,
		QueueSize:      2,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		<-release
		return nil, nil
	})

	go rt.Invoke(context.Background(), "fn", nil) // 占住唯一并发槽
	deadline := time.Now().Add(2 * time.Second)
	for mustStats(t, rt, "fn").InFlight < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	go rt.Invoke(context.Background(), "fn", nil) // 排队 1
	go rt.Invoke(context.Background(), "fn", nil) // 排队 2
	deadline = time.Now().Add(2 * time.Second)
	for mustStats(t, rt, "fn").Waiting < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	_, err := rt.Invoke(context.Background(), "fn", nil)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("队列满期望 ErrQueueFull，实际: %v", err)
	}
	var ie *InvokeError
	if errors.As(err, &ie) && ie.Reason != ReasonQueueFull {
		t.Fatalf("期望原因 queue_full，实际: %s", ie.Reason)
	}
	t.Logf("用例[背压]：在途=1 排队=2 队列满，新请求收到背压信号，原因=%s ✓", ReasonQueueFull)
	close(release)
}

// 业务场景：压力回落缩容 —— 空闲实例被回收，但执行中的调用不被打断。
func TestScaleDownDoesNotInterrupt(t *testing.T) {
	rt := New()
	defer rt.Close()
	const idle = 80 * time.Millisecond
	release := make(chan struct{})
	var done atomic.Bool
	rt.Register("fn", Config{
		MaxConcurrency: 2,
		MaxInstances:   2,
		IdleTimeout:    idle,
		Timeout:        5 * time.Second,
	}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		if req == "long" {
			<-release
			done.Store(true)
			return "long-ok", nil
		}
		return nil, nil
	})

	// 制造两个实例：一个跑长任务，一个空闲。
	longRes := make(chan any, 1)
	go func() {
		r, _ := rt.Invoke(context.Background(), "fn", "long")
		longRes <- r
	}()
	rt.Invoke(context.Background(), "fn", "short")
	deadline := time.Now().Add(2 * time.Second)
	for mustStats(t, rt, "fn").Instances < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// 等待空闲实例被回收：实例数降到 1（长任务实例仍在执行，不可回收）。
	deadline = time.Now().Add(3 * time.Second)
	for mustStats(t, rt, "fn").Instances != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := mustStats(t, rt, "fn").Instances; n != 1 {
		t.Fatalf("缩容后应剩 1 个忙碌实例，实际 %d", n)
	}
	if done.Load() {
		t.Fatal("长任务不应提前结束")
	}
	close(release)
	if r := <-longRes; r != "long-ok" {
		t.Fatalf("执行中的调用被缩容打断，结果 %v", r)
	}
	t.Log("用例[回落缩容]：空闲实例被回收（2->1），执行中的长调用未被打断并正常返回 ✓")
}
