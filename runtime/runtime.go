package runtime

import (
	"context"
	"errors"
	"sync"
)

// Runtime 是函数注册与调用的入口。
type Runtime struct {
	mu        sync.RWMutex
	functions map[string]*Function
	closed    bool
}

func New() *Runtime {
	return &Runtime{functions: make(map[string]*Function)}
}

// Register 注册函数；同名重复注册返回错误。
func (r *Runtime) Register(name string, cfg Config, h Handler) error {
	if h == nil {
		return errors.New("runtime: handler is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRuntimeClosed
	}
	if _, ok := r.functions[name]; ok {
		return errors.New("runtime: function already registered: " + name)
	}
	r.functions[name] = newFunction(name, cfg, h)
	return nil
}

// Invoke 按名字调用函数，严格路由到目标函数，不会派给别的函数。
func (r *Runtime) Invoke(ctx context.Context, name string, req any) (any, error) {
	r.mu.RLock()
	f, ok := r.functions[name]
	r.mu.RUnlock()
	if !ok {
		return nil, &InvokeError{name, ReasonCanceled, ErrFunctionNotFound}
	}
	return f.Invoke(ctx, req)
}

// Stats 返回函数的运行快照。
func (r *Runtime) Stats(name string) (Stats, error) {
	r.mu.RLock()
	f, ok := r.functions[name]
	r.mu.RUnlock()
	if !ok {
		return Stats{}, ErrFunctionNotFound
	}
	return f.Stats(), nil
}

// Close 回收所有实例并拒绝新调用；进行中的调用不受影响。
func (r *Runtime) Close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	fns := make([]*Function, 0, len(r.functions))
	for _, f := range r.functions {
		fns = append(fns, f)
	}
	r.mu.Unlock()
	for _, f := range fns {
		f.close()
	}
}
