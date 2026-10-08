package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// waitFor 轮询 cond 直到成立或超时，超时则以 msg 失败退出。
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("等待超时: %s", msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRunawayExecutionQuarantined 覆盖业务：失控执行（函数体不理会取消）的隔离。
// 验证超时/终止后未真正结束的执行占用的实例不会被并发复用、
// 残留状态不会串扰后续调用，且失控执行结束后隔离计数回落。
func TestRunawayExecutionQuarantined(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	release := make(chan struct{}) // 关闭后所有失控执行退出
	blockStarted := make(chan string, 8)

	var mu sync.Mutex
	inFlightPerInst := map[string]int{}
	maxPerInst := map[string]int{}

	err := rt.Register("runaway", FunctionConfig{
		MaxConcurrency:   1,
		MaxInstances:     1,
		MaxQueue:         8,
		Timeout:          50 * time.Millisecond,
		TerminationGrace: 30 * time.Millisecond,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		mu.Lock()
		inFlightPerInst[inv.InstanceID]++
		if inFlightPerInst[inv.InstanceID] > maxPerInst[inv.InstanceID] {
			maxPerInst[inv.InstanceID] = inFlightPerInst[inv.InstanceID]
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			inFlightPerInst[inv.InstanceID]--
			mu.Unlock()
		}()

		if string(inv.Payload) == "block" {
			blockStarted <- inv.InstanceID
			<-ctx.Done() // 收到取消信号……
			// ……但不理会取消，继续运行，并残留实例级状态
			inv.State["tainted"] = true
			<-release
			return "runaway-done", nil
		}
		// 正常调用：若实例被带病复用，会看到上一次失控执行的残留状态
		if _, tainted := inv.State["tainted"]; tainted {
			return "TAINTED:" + inv.InstanceID, nil
		}
		return inv.InstanceID, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 函数体不理会取消，超时后返回 ErrTimeout，实例被隔离")
	_, err = rt.Invoke(context.Background(), "runaway", []byte("block"))
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望 ErrTimeout，实际: %v", err)
	}
	runawayInst := <-blockStarted
	waitFor(t, 2*time.Second, "实例未被隔离", func() bool {
		st, _ := rt.Stats("runaway")
		return st.QuarantinedNow == 1
	})
	st, _ := rt.Stats("runaway")
	t.Logf("隔离生效: %+v", st)
	if st.Timeouts != 1 || st.Quarantined != 1 {
		t.Fatalf("超时/隔离计数不符: %+v", st)
	}

	t.Log("用例2: 失控执行仍在后台运行时，后续调用不得复用同一实例")
	res, err := rt.Invoke(context.Background(), "runaway", []byte("check"))
	if err != nil {
		t.Fatalf("隔离后调用失败: %v", err)
	}
	got := res.(string)
	t.Logf("失控实例: %s，后续调用落在: %s", runawayInst, got)
	if got == runawayInst {
		t.Fatalf("失控实例被并发复用: %s", got)
	}
	if len(got) > 8 && got[:8] == "TAINTED:" {
		t.Fatalf("失控执行的残留状态串扰到后续调用: %s", got)
	}

	t.Log("用例3: 隔离期间运行时已自愈补位，实例总数不突破上限")
	st, _ = rt.Stats("runaway")
	if st.Replaced != 1 {
		t.Fatalf("期望自愈补位1次，实际: %d", st.Replaced)
	}
	if st.Instances > 1 {
		t.Fatalf("实例数突破上限: %+v", st)
	}

	t.Log("用例4: 调用方主动取消的失控执行同样被隔离，且与超时可区分")
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelRes := make(chan error, 1)
	go func() {
		_, err := rt.Invoke(cancelCtx, "runaway", []byte("block"))
		cancelRes <- err
	}()
	<-blockStarted // 等失控执行开始
	cancel()
	if err := <-cancelRes; !errors.Is(err, context.Canceled) {
		t.Fatalf("期望 context.Canceled，实际: %v", err)
	}
	waitFor(t, 2*time.Second, "取消的失控执行未被隔离", func() bool {
		st, _ := rt.Stats("runaway")
		return st.QuarantinedNow == 2
	})
	st, _ = rt.Stats("runaway")
	t.Logf("取消触发隔离: %+v", st)
	if st.Timeouts != 1 {
		t.Fatalf("调用方取消不应计入超时: %+v", st)
	}
	if st.Quarantined != 2 || st.Replaced != 2 {
		t.Fatalf("隔离/补位计数不符: %+v", st)
	}
	if st.Instances > 1 {
		t.Fatalf("实例数突破上限: %+v", st)
	}

	t.Log("用例5: 失控执行真正结束后，隔离计数回落，实例被销毁")
	close(release)
	waitFor(t, 2*time.Second, "隔离计数未回落", func() bool {
		st, _ := rt.Stats("runaway")
		return st.QuarantinedNow == 0
	})
	st, _ = rt.Stats("runaway")
	t.Logf("失控执行结束: %+v", st)
	if st.Quarantined != 2 {
		t.Fatalf("累计隔离数不应回落: %+v", st)
	}

	mu.Lock()
	for id, n := range maxPerInst {
		if n > 1 {
			t.Fatalf("实例 %s 被并发复用，最大并发执行数: %d", id, n)
		}
	}
	mu.Unlock()
	t.Log("失控执行隔离用例全部通过")
}

// TestQuarantineSelfHealing 覆盖业务：隔离后的自愈——持续流量下补回可用容量，
// 实例总数不突破上限，排队请求不丢失。
func TestQuarantineSelfHealing(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	err := rt.Register("heal", FunctionConfig{
		MaxConcurrency:   2,
		MaxInstances:     2,
		MaxQueue:         16,
		Timeout:          50 * time.Millisecond,
		TerminationGrace: 20 * time.Millisecond,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		if string(inv.Payload) == "block" {
			<-release // 完全不理会 ctx 取消
			return "runaway-done", nil
		}
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	// 全程采样实例数，验证不突破上限。
	var maxInstances int
	var sampleMu sync.Mutex
	stopSampling := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopSampling:
				return
			default:
			}
			st, _ := rt.Stats("heal")
			sampleMu.Lock()
			if st.Instances > maxInstances {
				maxInstances = st.Instances
			}
			sampleMu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()
	defer close(stopSampling)

	t.Log("用例1: 2个失控调用占满全部实例并超时，两个实例都应被隔离")
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = rt.Invoke(context.Background(), "heal", []byte("block"))
		}(i)
	}
	wg.Wait()
	for i, e := range errs {
		if !errors.Is(e, ErrTimeout) {
			t.Fatalf("第%d个失控调用期望 ErrTimeout，实际: %v", i, e)
		}
	}
	waitFor(t, 2*time.Second, "两个实例未全部被隔离", func() bool {
		st, _ := rt.Stats("heal")
		return st.QuarantinedNow == 2
	})
	st, _ := rt.Stats("heal")
	t.Logf("全部隔离: %+v", st)
	if st.Quarantined != 2 || st.Replaced != 2 {
		t.Fatalf("隔离/补位计数不符: %+v", st)
	}

	t.Log("用例2: 失控实例仍卡死，持续流量下可用容量已自愈，请求不丢")
	const traffic = 6
	results := make([]any, traffic)
	for i := 0; i < traffic; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := rt.Invoke(context.Background(), "heal", []byte("ok"))
			if err != nil {
				results[i] = err
			} else {
				results[i] = res
			}
		}(i)
	}
	wg.Wait()
	for i, r := range results {
		if r != "ok" {
			t.Fatalf("第%d个正常请求未被执行（疑似丢失）: %v", i, r)
		}
	}
	st, _ = rt.Stats("heal")
	t.Logf("自愈后流量正常: %+v", st)
	if st.Executed != traffic {
		t.Fatalf("期望%d个请求全部执行完成，实际: %d", traffic, st.Executed)
	}
	if st.Rejected != 0 {
		t.Fatalf("不应出现背压拒绝: %+v", st)
	}

	t.Log("用例3: 全程实例总数不突破上限")
	sampleMu.Lock()
	t.Logf("采样期间峰值实例数: %d（上限2）", maxInstances)
	if maxInstances > 2 {
		t.Fatalf("实例数突破上限: %d", maxInstances)
	}
	sampleMu.Unlock()

	t.Log("用例4: 失控执行结束后隔离计数回落，补位实例继续服役")
	close(release)
	waitFor(t, 2*time.Second, "隔离计数未回落", func() bool {
		st, _ := rt.Stats("heal")
		return st.QuarantinedNow == 0
	})
	res, err := rt.Invoke(context.Background(), "heal", []byte("ok"))
	if err != nil || res != "ok" {
		t.Fatalf("隔离结束后调用异常: res=%v err=%v", res, err)
	}
	st, _ = rt.Stats("heal")
	t.Logf("最终状态: %+v", st)
	if st.Instances > 2 {
		t.Fatalf("实例数突破上限: %+v", st)
	}
	t.Log("隔离自愈用例全部通过")
}

// TestGracefulTerminationNotQuarantined 覆盖业务：兼容现有语义——
// 函数体响应取消并在宽限期内退出时，实例不被隔离，正常复用。
func TestGracefulTerminationNotQuarantined(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	err := rt.Register("polite", FunctionConfig{
		MaxConcurrency:   1,
		Timeout:          50 * time.Millisecond,
		TerminationGrace: 200 * time.Millisecond,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		if string(inv.Payload) != "slow" {
			return inv.InstanceID, nil
		}
		<-ctx.Done()
		time.Sleep(10 * time.Millisecond) // 清理耗时，宽限期内退出
		return inv.InstanceID, nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 超时应返回 ErrTimeout，且函数体在宽限期内退出")
	_, err = rt.Invoke(context.Background(), "polite", []byte("slow"))
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望 ErrTimeout，实际: %v", err)
	}

	t.Log("用例2: 宽限期后实例未被隔离，仍被复用，无冷启动")
	waitFor(t, 2*time.Second, "超时计数未更新", func() bool {
		st, _ := rt.Stats("polite")
		return st.Timeouts == 1
	})
	// 等宽限期过后再观察，确保不会延迟触发隔离。
	time.Sleep(300 * time.Millisecond)
	st, _ := rt.Stats("polite")
	t.Logf("超时后状态: %+v", st)
	if st.Quarantined != 0 || st.QuarantinedNow != 0 || st.Replaced != 0 {
		t.Fatalf("响应取消的函数不应触发隔离: %+v", st)
	}
	if st.ColdStarts != 1 || st.Instances != 1 {
		t.Fatalf("实例不应被替换: %+v", st)
	}

	t.Log("用例3: 后续调用复用同一实例（验证未被隔离销毁）")
	res, err := rt.Invoke(context.Background(), "polite", nil)
	if err != nil {
		t.Fatalf("后续调用失败: %v", err)
	}
	t.Logf("后续调用落在: %v", res)
	st, _ = rt.Stats("polite")
	if st.ColdStarts != 1 {
		t.Fatalf("实例未被复用，冷启动次数: %d", st.ColdStarts)
	}
	t.Log("优雅终止兼容用例全部通过")
}

// TestQuarantineDoesNotAffectOtherFunctions 覆盖业务：一个函数的失控隔离
// 不影响其他函数的容量与派发。
func TestQuarantineDoesNotAffectOtherFunctions(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	release := make(chan struct{})
	err := rt.Register("sick", FunctionConfig{
		MaxConcurrency:   1,
		MaxInstances:     1,
		Timeout:          50 * time.Millisecond,
		TerminationGrace: 20 * time.Millisecond,
	}, func(ctx context.Context, inv *Invocation) (any, error) {
		<-release
		return "sick", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	err = rt.Register("healthy", FunctionConfig{}, func(ctx context.Context, inv *Invocation) (any, error) {
		return "healthy", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: sick 函数失控超时被隔离期间，healthy 函数调用不受影响")
	_, err = rt.Invoke(context.Background(), "sick", nil)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("期望 ErrTimeout，实际: %v", err)
	}
	waitFor(t, 2*time.Second, "sick 实例未被隔离", func() bool {
		st, _ := rt.Stats("sick")
		return st.QuarantinedNow == 1
	})
	for i := 0; i < 3; i++ {
		res, err := rt.Invoke(context.Background(), "healthy", []byte(fmt.Sprint(i)))
		if err != nil || res != "healthy" {
			t.Fatalf("healthy 调用受影响: res=%v err=%v", res, err)
		}
	}
	st, _ := rt.Stats("healthy")
	t.Logf("healthy 池状态: %+v", st)
	if st.Quarantined != 0 || st.QuarantinedNow != 0 {
		t.Fatalf("隔离不应波及其他函数: %+v", st)
	}
	close(release)
	t.Log("函数间隔离用例全部通过")
}
