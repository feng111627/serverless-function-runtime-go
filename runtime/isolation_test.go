package runtime

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestQuarantineNoReuseNoCrosstalk 覆盖业务：
// 函数体完全不理会取消时，超时后其占用实例必须被隔离，
// 同一实例任何时刻都不能并发跑第二次调用，残留状态也不能影响下一次调用。
func TestQuarantineNoReuseNoCrosstalk(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	var totalRunning atomic.Int32 // 全池当前仍在跑的函数体数（含僵尸）
	zombieStarted := make(chan struct{}, 4)
	releaseZombie := make(chan struct{})

	var mu sync.Mutex
	curPerInst := map[string]int{}  // 各实例当前执行数
	maxPerInst := map[string]int{}  // 各实例观测到的并发峰值
	entryPoison := map[string]any{} // 各实例进入时看到的残留状态（不应有别人的值）

	err := rt.Register("rogue", FunctionConfig{
		MaxConcurrency: 1,
		MaxInstances:   2,
		MaxQueue:       16,
		Timeout:        80 * time.Millisecond,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		mu.Lock()
		// 记录进入时该实例 store 里上一次执行留下的“残留”。
		entryPoison[inv.InstanceID] = inv.State["last"]
		curPerInst[inv.InstanceID]++
		if curPerInst[inv.InstanceID] > maxPerInst[inv.InstanceID] {
			maxPerInst[inv.InstanceID] = curPerInst[inv.InstanceID]
		}
		mu.Unlock()

		totalRunning.Add(1)
		// 在实例私有状态上留下本次执行的残留。
		inv.State["last"] = inv.InstanceID
		zombieStarted <- struct{}{}
		// 完全不理会 ctx.Done()，直到测试显式放行。
		<-releaseZombie

		totalRunning.Add(-1)
		mu.Lock()
		curPerInst[inv.InstanceID]--
		mu.Unlock()
		return inv.InstanceID, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 第一次调用超时，返回 ErrTimeout，实例 inst-1 被隔离且僵尸仍在后台")
	if _, err := rt.Invoke(context.Background(), "rogue", nil); !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望 ErrTimeout，实际: %v", err)
	}
	<-zombieStarted
	st, _ := rt.Stats("rogue")
	t.Logf("第一次超时后快照: %+v", st)
	if st.Quarantined != 1 || st.QuarantinedNow != 1 || st.InFlight != 0 {
		t.Fatalf("隔离状态不符预期: %+v", st)
	}

	t.Log("用例2: 第二次调用超时也必须落到全新实例 inst-2，绝不能复用隔离中的 inst-1")
	if _, err := rt.Invoke(context.Background(), "rogue", nil); !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望 ErrTimeout，实际: %v", err)
	}
	<-zombieStarted
	st, _ = rt.Stats("rogue")
	t.Logf("第二次超时后快照: %+v", st)
	if st.Quarantined != 2 || st.QuarantinedNow != 2 || st.Instances != 2 || st.InFlight != 0 {
		t.Fatalf("期望两个实例均被隔离且在途为0: %+v", st)
	}
	if st.Timeouts != 2 {
		t.Fatalf("期望累计超时2次，实际: %d", st.Timeouts)
	}

	t.Log("用例3: 两个僵尸可在不同实例上并存，但同一实例并发峰值必须恒为1")
	if got := totalRunning.Load(); got != 2 {
		t.Fatalf("期望两个僵尸执行同时在后台，实际: %d", got)
	}
	mu.Lock()
	t.Logf("各实例并发峰值: %v，进入时残留状态: %v", maxPerInst, entryPoison)
	for id, n := range maxPerInst {
		if n != 1 {
			mu.Unlock()
			t.Fatalf("实例 %s 被并发执行，峰值: %d", id, n)
		}
	}
	t.Log("用例4: 新实例进入时不得看到上一个隔离实例留下的状态")
	for id, poison := range entryPoison {
		if poison != nil {
			mu.Unlock()
			t.Fatalf("实例 %s 读到了其他执行的残留状态: %v", id, poison)
		}
	}
	mu.Unlock()

	t.Log("用例5: 放行僵尸后，隔离实例应被销毁并自愈补位，当前隔离数归零")
	close(releaseZombie)
	deadline := time.Now().Add(3 * time.Second)
	for {
		st, _ = rt.Stats("rogue")
		if st.QuarantinedNow == 0 && st.Replaced == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("自愈未完成: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("自愈完成后快照: %+v", st)
	if st.Instances > 2 {
		t.Fatalf("自愈突破实例上限: %d", st.Instances)
	}

	t.Log("用例6: 自愈补回的实例可正常服务，且不再有残留状态")
	res, err := rt.Invoke(context.Background(), "rogue", nil)
	if err != nil {
		t.Fatalf("自愈后调用失败: %v", err)
	}
	t.Logf("自愈后调用结果: %v", res)
	if res != "rogue-inst-3" && res != "rogue-inst-4" {
		t.Fatalf("期望落在自愈替换实例 inst-3/inst-4，实际: %v", res)
	}
	mu.Lock()
	if poison := entryPoison[res.(string)]; poison != nil {
		mu.Unlock()
		t.Fatalf("替换实例读到残留状态: %v", poison)
	}
	mu.Unlock()

	st, _ = rt.Stats("rogue")
	if st.Executed != 1 {
		t.Fatalf("期望正常完成1次，实际: %d", st.Executed)
	}
	t.Log("隔离不复用/不串扰用例全部通过")
}

// TestQuarantineSelfHealUnderLoad 覆盖业务：
// 持续流量下连环超时导致多个实例被隔离时，排队请求不被丢弃、
// 实例总数绝不突破上限；僵尸结束后自动补回健康实例并恢复正常服务。
func TestQuarantineSelfHealUnderLoad(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	const maxInst = 3
	var rogue atomic.Bool
	rogue.Store(true)
	releaseZombies := make(chan struct{})

	err := rt.Register("storm", FunctionConfig{
		MaxConcurrency: 2,
		MaxInstances:   maxInst,
		MaxQueue:       32,
		Timeout:        60 * time.Millisecond,
		IdleTimeout:    5 * time.Second,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		if rogue.Load() {
			// 失控阶段：完全不理会取消，直到外部放行。
			<-releaseZombies
		}
		return inv.InstanceID, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	// 后台采样：任何时刻实例数不得超过 MaxInstances，在途不得超过 MaxConcurrency。
	stopSample := make(chan struct{})
	var peakInst, peakInFlight atomic.Int32
	go func() {
		for {
			select {
			case <-stopSample:
				return
			default:
				if st, ok := rt.Stats("storm"); ok {
					for {
						old := peakInst.Load()
						if int(st.Instances) <= int(old) || peakInst.CompareAndSwap(old, int32(st.Instances)) {
							break
						}
					}
					for {
						old := peakInFlight.Load()
						if st.InFlight <= int(old) || peakInFlight.CompareAndSwap(old, int32(st.InFlight)) {
							break
						}
					}
				}
				time.Sleep(2 * time.Millisecond)
			}
		}
	}()

	t.Log("用例1: 4个请求压上来（并发2+排队2），失控阶段连环超时隔离到3个实例")
	type outcome struct {
		err error
	}
	outcomes := make([]outcome, 4)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, outcomes[i].err = rt.Invoke(context.Background(), "storm", nil)
		}(i)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		st, _ := rt.Stats("storm")
		if st.QuarantinedNow == 3 && st.Queued == 1 && st.InFlight == 0 && st.Timeouts == 3 {
			t.Logf("连环隔离稳定: %+v", st)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("未达到预期隔离态势: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Log("用例2: 隔离期间排队请求仍保留，没有背压拒绝，实例数与在途数不破上限")
	st, _ := rt.Stats("storm")
	if st.Rejected != 0 {
		t.Fatalf("排队请求不应被拒绝，实际拒绝数: %d", st.Rejected)
	}
	if got := peakInst.Load(); got > maxInst {
		t.Fatalf("实例数突破上限 %d: %d", maxInst, got)
	}
	if got := peakInFlight.Load(); got > 2 {
		t.Fatalf("在途数突破并发上限: %d", got)
	}
	t.Logf("峰值实例数=%d 峰值在途=%d，均未破限", peakInst.Load(), peakInFlight.Load())

	t.Log("用例3: 函数恢复健康并放行僵尸后，排队请求被自愈实例服务，不丢弃")
	rogue.Store(false)
	close(releaseZombies)
	wg.Wait() // 排队的第4个请求必须能等到结果，否则这里会挂起
	timeouts, succeeded := 0, 0
	for _, o := range outcomes {
		if errors.Is(o.err, ErrTimeout) {
			timeouts++
		} else if o.err == nil {
			succeeded++
		} else {
			t.Fatalf("出现非预期错误: %v", o.err)
		}
	}
	t.Logf("4个请求结果: %d 个超时隔离、%d 个由自愈实例正常完成", timeouts, succeeded)
	if timeouts != 3 || succeeded != 1 {
		t.Fatalf("期望3超时1成功，实际 %d 超时 %d 成功", timeouts, succeeded)
	}

	deadline = time.Now().Add(3 * time.Second)
	for {
		st, _ = rt.Stats("storm")
		if st.QuarantinedNow == 0 && st.Replaced == 3 && st.Executed == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("自愈未完成: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stopSample)
	t.Logf("自愈完成: %+v", st)
	if st.Instances > maxInst {
		t.Fatalf("自愈后实例数仍破限: %d", st.Instances)
	}

	t.Log("用例4: 恢复健康后后续调用稳定成功，不再产生超时/隔离")
	for i := 0; i < 3; i++ {
		if _, err := rt.Invoke(context.Background(), "storm", nil); err != nil {
			t.Fatalf("恢复后第%d次调用失败: %v", i+1, err)
		}
	}
	st, _ = rt.Stats("storm")
	t.Logf("恢复后快照: %+v", st)
	if st.Executed != 4 || st.Timeouts != 3 || st.QuarantinedNow != 0 {
		t.Fatalf("恢复后计数异常: %+v", st)
	}
	if got := peakInst.Load(); got > maxInst {
		t.Fatalf("全过程实例数破限: %d", got)
	}
	t.Log("持续流量隔离与自愈加用例全部通过")
}

// TestQuarantineOnCallerCancel 覆盖业务：
// 调用方主动取消且函数体不理会取消时，实例同样被隔离，
// 但错误必须与超时可区分（返回调用方 ctx 错误，不计入 Timeouts）。
func TestQuarantineOnCallerCancel(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	started := make(chan struct{})
	err := rt.Register("stuck", FunctionConfig{
		MaxConcurrency: 1,
		MaxInstances:   2,
		Timeout:        5 * time.Second, // 远大于测试时长，确保不是超时触发
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		close(started)
		<-release // 不理会取消
		return inv.InstanceID, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, e := rt.Invoke(ctx, "stuck", nil)
		errCh <- e
	}()
	<-started
	t.Log("用例1: 调用方主动取消，应回传 context.Canceled 而非 ErrTimeout")
	cancel()
	select {
	case got := <-errCh:
		if !errors.Is(got, context.Canceled) {
			t.Fatalf("期望 context.Canceled，实际: %v", got)
		}
		if errors.Is(got, ErrTimeout) {
			t.Fatalf("主动取消不应返回 ErrTimeout")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("取消后调用未及时返回")
	}

	t.Log("用例2: 取消后实例应被隔离，且不记超时计数")
	st, _ := rt.Stats("stuck")
	t.Logf("快照: %+v", st)
	if st.Quarantined != 1 || st.QuarantinedNow != 1 {
		t.Fatalf("期望隔离1个实例，实际: %+v", st)
	}
	if st.Timeouts != 0 {
		t.Fatalf("主动取消不应计入超时，实际: %d", st.Timeouts)
	}

	t.Log("用例3: 放行僵尸后实例销毁自愈，隔离数归零")
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, _ = rt.Stats("stuck")
		if st.QuarantinedNow == 0 && st.Replaced == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("自愈未完成: %+v", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Log("主动取消隔离用例全部通过")
}

// TestQuarantineFunctionIsolation 覆盖业务：
// 一个函数发生隔离与自愈，不影响其他函数的池、实例与调用。
func TestQuarantineFunctionIsolation(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	releaseBad := make(chan struct{})
	err := rt.Register("bad", FunctionConfig{
		MaxConcurrency: 1,
		MaxInstances:   1,
		Timeout:        50 * time.Millisecond,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		<-releaseBad // 失控
		return nil, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	err = rt.Register("good", FunctionConfig{MaxConcurrency: 2}, func(ctx context.Context, inv *Invocation) (any, error) {
		return "good-" + inv.InstanceID, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: bad 函数超时隔离期间，good 函数调用完全不受影响")
	_, err = rt.Invoke(context.Background(), "bad", nil)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望 ErrTimeout，实际: %v", err)
	}
	res, err := rt.Invoke(context.Background(), "good", nil)
	if err != nil || res != "good-good-inst-1" {
		t.Fatalf("good 函数受 bad 隔离影响: res=%v err=%v", res, err)
	}
	res, err = rt.Invoke(context.Background(), "good", nil)
	if err != nil || res != "good-good-inst-1" {
		t.Fatalf("good 实例复用异常: res=%v err=%v", res, err)
	}

	t.Log("用例2: good 的统计里不出现任何隔离/替换记录")
	if st, _ := rt.Stats("good"); st.Quarantined != 0 || st.Replaced != 0 || st.Timeouts != 0 || st.Executed != 2 {
		t.Fatalf("good 统计被污染: %+v", st)
	}
	if st, _ := rt.Stats("bad"); st.Quarantined != 1 || st.QuarantinedNow != 1 {
		t.Fatalf("bad 隔离状态异常: %+v", st)
	}

	close(releaseBad)
	t.Log("函数间隔离用例全部通过")
}

// TestShutdownWithQuarantinedZombie 覆盖业务：
// 存在隔离中僵尸执行时 Shutdown 不被挂住、不复活实例；
// 僵尸随后结束只做清理，不会引起 panic 或自愈。
func TestShutdownWithQuarantinedZombie(t *testing.T) {
	rt := New()

	release := make(chan struct{})
	err := rt.Register("ghost", FunctionConfig{
		MaxConcurrency: 1,
		MaxInstances:   2,
		Timeout:        40 * time.Millisecond,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		<-release // 失控，关闭期间才放行
		return inv.InstanceID, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	if _, err := rt.Invoke(context.Background(), "ghost", nil); !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望 ErrTimeout，实际: %v", err)
	}
	if st, _ := rt.Stats("ghost"); st.QuarantinedNow != 1 {
		t.Fatalf("期望1个隔离实例，实际: %+v", st)
	}

	t.Log("用例1: 僵尸未结束时 Shutdown 也应及时返回，不被挂住")
	done := make(chan error, 1)
	go func() { done <- rt.Shutdown(context.Background()) }()
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("Shutdown 返回错误: %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown 被隔离僵尸挂住")
	}

	t.Log("用例2: 关闭期间僵尸结束只做清理，不 panic、不自愈补实例")
	before, _ := rt.Stats("ghost")
	close(release)
	time.Sleep(100 * time.Millisecond)
	after, _ := rt.Stats("ghost")
	if after.Replaced != before.Replaced {
		t.Fatalf("关闭后不应自愈替换实例: before=%d after=%d", before.Replaced, after.Replaced)
	}
	if after.QuarantinedNow != 0 {
		t.Fatalf("僵尸结束后隔离数应归零，实际: %d", after.QuarantinedNow)
	}
	t.Logf("关闭前快照: %+v", before)
	t.Logf("僵尸清理后快照: %+v", after)
	t.Log("关闭与隔离共存用例全部通过")
}
