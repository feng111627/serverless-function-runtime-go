package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRegisterAndInvoke 覆盖业务：函数注册与基本调用。
func TestRegisterAndInvoke(t *testing.T) {
	rt := New()
	defer rt.Shutdown(context.Background())

	err := rt.Register("echo", FunctionConfig{}, func(ctx context.Context, inv *Invocation) (any, error) {
		return string(inv.Payload), nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}
	t.Log("用例1: 注册后调用，应原样返回 payload")
	res, err := rt.Invoke(context.Background(), "echo", []byte("hello"))
	if err != nil {
		t.Fatalf("调用失败: %v", err)
	}
	if res.(string) != "hello" {
		t.Fatalf("返回值不符: %v", res)
	}

	t.Log("用例2: 重复注册同名函数，应返回 ErrFunctionExists")
	if err := rt.Register("echo", FunctionConfig{}, func(ctx context.Context, inv *Invocation) (any, error) {
		return nil, nil
	}); !errors.Is(err, ErrFunctionExists) {
		t.Fatalf("期望 ErrFunctionExists，实际: %v", err)
	}

	t.Log("用例3: 调用未注册函数，应返回 ErrFunctionNotFound")
	if _, err := rt.Invoke(context.Background(), "ghost", nil); !errors.Is(err, ErrFunctionNotFound) {
		t.Fatalf("期望 ErrFunctionNotFound，实际: %v", err)
	}
	t.Log("注册与基本调用用例全部通过")
}

// TestShutdown 覆盖业务：运行时关闭语义。
func TestShutdown(t *testing.T) {
	rt := New()
	release := make(chan struct{})
	started := make(chan struct{})
	err := rt.Register("slow", FunctionConfig{Timeout: 5 * time.Second}, func(ctx context.Context, inv *Invocation) (any, error) {
		close(started)
		<-release
		return "done", nil
	})
	if err != nil {
		t.Fatalf("注册失败: %v", err)
	}

	t.Log("用例1: 关闭不打断执行中的调用")
	resCh := make(chan any, 1)
	go func() {
		res, _ := rt.Invoke(context.Background(), "slow", nil)
		resCh <- res
	}()
	<-started
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- rt.Shutdown(context.Background()) }()
	close(release)
	if res := <-resCh; res != "done" {
		t.Fatalf("执行中的调用被关闭打断，结果: %v", res)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("Shutdown 返回错误: %v", err)
	}

	t.Log("用例2: 关闭后新调用应返回 ErrRuntimeClosed")
	if _, err := rt.Invoke(context.Background(), "slow", nil); !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("期望 ErrRuntimeClosed，实际: %v", err)
	}

	t.Log("用例3: 关闭后注册应返回 ErrRuntimeClosed")
	if err := rt.Register("late", FunctionConfig{}, func(ctx context.Context, inv *Invocation) (any, error) {
		return nil, nil
	}); !errors.Is(err, ErrRuntimeClosed) {
		t.Fatalf("期望 ErrRuntimeClosed，实际: %v", err)
	}
	t.Log("关闭语义用例全部通过")
}
