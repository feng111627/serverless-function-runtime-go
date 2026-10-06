package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

// 业务场景：实例复用 —— 串行调用复用同一实例，实例私有状态在复用间延续。
func TestInstanceReuse(t *testing.T) {
	rt := New()
	defer rt.Close()
	var ids []int
	rt.Register("fn", Config{MaxConcurrency: 1}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		ids = append(ids, env.InstanceID)
		n, _ := env.Get("count")
		if n == nil {
			n = 0
		}
		env.Put("count", n.(int)+1)
		return n.(int) + 1, nil
	})

	for i := 1; i <= 3; i++ {
		res, err := rt.Invoke(context.Background(), "fn", nil)
		if err != nil {
			t.Fatalf("invoke: %v", err)
		}
		if res != i {
			t.Fatalf("第 %d 次调用期望计数值 %d，实际 %v", i, i, res)
		}
	}
	if ids[0] != ids[1] || ids[1] != ids[2] {
		t.Fatalf("期望复用同一实例，实际实例序列 %v", ids)
	}
	t.Logf("用例[实例复用]：3 次串行调用复用实例 #%d，私有计数器延续到 %d ✓", ids[0], 3)
}

// 业务场景：实例状态隔离 —— 不同实例的私有状态互不可见。
func TestInstanceStateIsolation(t *testing.T) {
	rt := New()
	defer rt.Close()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	rt.Register("fn", Config{MaxConcurrency: 2}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		env.Put("owner", env.InstanceID)
		started <- struct{}{}
		<-release
		v, _ := env.Get("owner")
		return v, nil
	})

	done := make(chan any, 2)
	go func() { r, _ := rt.Invoke(context.Background(), "fn", nil); done <- r }()
	go func() { r, _ := rt.Invoke(context.Background(), "fn", nil); done <- r }()
	<-started
	<-started
	close(release)
	r1, r2 := <-done, <-done
	if r1 == r2 {
		t.Fatalf("两个并发实例的私有状态不应相同: %v == %v", r1, r2)
	}
	t.Logf("用例[状态隔离]：实例私有状态互不可见（owner=%v vs owner=%v）✓", r1, r2)
}

// 业务场景：空闲回收 —— 实例空闲超时后被回收，后续调用触发冷启动新实例。
func TestInstanceIdleReclaim(t *testing.T) {
	rt := New()
	defer rt.Close()
	var ids []int
	rt.Register("fn", Config{
		MaxConcurrency: 1,
		IdleTimeout:    60 * time.Millisecond,
	}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		ids = append(ids, env.InstanceID)
		return nil, nil
	})

	rt.Invoke(context.Background(), "fn", nil)
	if n := mustStats(t, rt, "fn").Instances; n != 1 {
		t.Fatalf("调用后实例数期望 1，实际 %d", n)
	}
	// 等待空闲回收。
	deadline := time.Now().Add(3 * time.Second)
	for mustStats(t, rt, "fn").Instances != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := mustStats(t, rt, "fn").Instances; n != 0 {
		t.Fatalf("空闲实例应被回收，剩余 %d", n)
	}
	rt.Invoke(context.Background(), "fn", nil)
	if len(ids) != 2 || ids[0] == ids[1] {
		t.Fatalf("回收后应冷启动新实例，实例序列 %v", ids)
	}
	t.Logf("用例[空闲回收]：实例 #%d 空闲 60ms 后被回收，再次调用冷启动实例 #%d ✓", ids[0], ids[1])
}

// 业务场景：超时实例不复用 —— 超时后被强制回收，防止被污染状态泄漏。
func TestTimeoutInstanceNotReused(t *testing.T) {
	rt := New()
	defer rt.Close()
	// 超时的 handler 仍在后台运行，用 channel 传递实例 ID 保证无数据竞争。
	idCh := make(chan int, 4)
	rt.Register("fn", Config{
		MaxConcurrency: 1,
		Timeout:        40 * time.Millisecond,
	}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		idCh <- env.InstanceID
		if req == "slow" {
			time.Sleep(500 * time.Millisecond)
		}
		return nil, nil
	})

	_, err := rt.Invoke(context.Background(), "fn", "slow")
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望超时，实际 %v", err)
	}
	rt.Invoke(context.Background(), "fn", "fast")
	first, second := <-idCh, <-idCh
	if first == second {
		t.Fatalf("超时实例不应复用，两次调用都落在实例 #%d", first)
	}
	t.Logf("用例[超时回收]：超时实例 #%d 被强制回收，下次调用使用新实例 #%d ✓", first, second)
}

// 业务场景：冷启动 —— 新实例有冷启动延迟，复用实例没有。
func TestColdStartLatency(t *testing.T) {
	rt := New()
	defer rt.Close()
	rt.Register("fn", Config{
		MaxConcurrency: 1,
		ColdStart:      80 * time.Millisecond,
	}, func(ctx context.Context, env *ExecEnv, req any) (any, error) {
		return nil, nil
	})

	start := time.Now()
	rt.Invoke(context.Background(), "fn", nil)
	cold := time.Since(start)
	start = time.Now()
	rt.Invoke(context.Background(), "fn", nil)
	warm := time.Since(start)
	if cold < 80*time.Millisecond {
		t.Fatalf("首次调用应包含冷启动耗时，实际 %v", cold)
	}
	if warm >= cold {
		t.Fatalf("复用调用 %v 应显著快于冷启动 %v", warm, cold)
	}
	t.Logf("用例[冷启动]：首次 %v（含冷启动），复用 %v ✓", cold.Round(time.Millisecond), warm.Round(time.Millisecond))
}

func mustStats(t *testing.T, rt *Runtime, name string) Stats {
	t.Helper()
	s, err := rt.Stats(name)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	return s
}

