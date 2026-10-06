package runtime

import (
	"context"
	"fmt"
	"sync"
)

// Runtime 是本地无服务器函数运行时，管理所有已注册函数。
// 每个函数拥有独立的实例池与队列，请求不会被派发给其他函数。
type Runtime struct {
	mu     sync.RWMutex
	pools  map[string]*pool
	closed bool
}

// New 创建一个运行时。
func New() *Runtime {
	return &Runtime{pools: make(map[string]*pool)}
}

// Register 注册函数，重名返回 ErrFunctionExists，运行时关闭后返回 ErrRuntimeClosed。
func (r *Runtime) Register(name string, cfg FunctionConfig, h Handler) error {
	if name == "" {
		return fmt.Errorf("runtime: function name must not be empty")
	}
	if h == nil {
		return fmt.Errorf("runtime: handler must not be nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRuntimeClosed
	}
	if _, ok := r.pools[name]; ok {
		return fmt.Errorf("%w: %s", ErrFunctionExists, name)
	}
	r.pools[name] = newPool(name, cfg.withDefaults(), h)
	return nil
}

// Invoke 同步调用函数，返回结果或可区分的失败原因：
// ErrFunctionNotFound / ErrQueueFull / ErrTimeout / ErrRuntimeClosed。
func (r *Runtime) Invoke(ctx context.Context, name string, payload []byte) (any, error) {
	r.mu.RLock()
	p, ok := r.pools[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrFunctionNotFound, name)
	}
	seq := p.reqSeq.Add(1)
	inv := &Invocation{
		RequestID: fmt.Sprintf("%s-req-%d", name, seq),
		FuncName:  name,
		Payload:   payload,
	}
	return p.invoke(ctx, inv)
}

// Stats 返回指定函数的池快照，函数不存在时返回 false。
func (r *Runtime) Stats(name string) (PoolStats, bool) {
	r.mu.RLock()
	p, ok := r.pools[name]
	r.mu.RUnlock()
	if !ok {
		return PoolStats{}, false
	}
	return p.stats(), true
}

// Shutdown 停止运行时：拒绝新调用，清空各函数队列，
// 空闲实例立即回收，执行中的调用等待其自然结束后销毁实例。
func (r *Runtime) Shutdown(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	pools := make([]*pool, 0, len(r.pools))
	for _, p := range r.pools {
		pools = append(pools, p)
	}
	r.mu.Unlock()

	done := make(chan struct{})
	go func() {
		for _, p := range pools {
			p.close()
		}
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
