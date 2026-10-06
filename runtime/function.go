package runtime

import (
	"context"
	"sync"
)

// Function 是已注册的函数，包含配置、并发控制与实例池。
type Function struct {
	name    string
	cfg     Config
	handler Handler

	slots chan struct{} // 容量 = MaxConcurrency 的信号量

	mu      sync.Mutex
	waiting int
	closed  bool

	pool *pool
}

func newFunction(name string, cfg Config, h Handler) *Function {
	cfg = cfg.withDefaults()
	return &Function{
		name:    name,
		cfg:     cfg,
		handler: h,
		slots:   make(chan struct{}, cfg.MaxConcurrency),
		pool:    newPool(cfg.MaxInstances, cfg.IdleTimeout, cfg.ColdStart),
	}
}

type result struct {
	res any
	err error
}

// Invoke 同步调用函数，返回结果或可区分的失败原因。
func (f *Function) Invoke(ctx context.Context, req any) (any, error) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, &InvokeError{f.name, ReasonCanceled, ErrRuntimeClosed}
	}
	f.mu.Unlock()

	// 1) 准入：占用并发槽，超限按策略排队或拒绝。
	select {
	case f.slots <- struct{}{}:
	default:
		if f.cfg.Policy == PolicyReject {
			return nil, &InvokeError{f.name, ReasonConcurrencyLimited, ErrConcurrencyLimited}
		}
		f.mu.Lock()
		if f.waiting >= f.cfg.QueueSize {
			f.mu.Unlock()
			return nil, &InvokeError{f.name, ReasonQueueFull, ErrQueueFull}
		}
		f.waiting++
		f.mu.Unlock()
		select {
		case f.slots <- struct{}{}:
		case <-ctx.Done():
			f.mu.Lock()
			f.waiting--
			f.mu.Unlock()
			return nil, &InvokeError{f.name, ReasonCanceled, ctx.Err()}
		}
		f.mu.Lock()
		f.waiting--
		f.mu.Unlock()
	}
	defer func() { <-f.slots }()

	// 2) 获取实例（复用 / 冷启动 / 等待）。
	inst, err := f.pool.acquire(ctx)
	if err != nil {
		return nil, &InvokeError{f.name, ReasonCanceled, err}
	}

	// 3) 带超时执行；超时的实例强制回收，不复用，避免被污染的状态泄漏。
	execCtx, cancel := context.WithTimeout(ctx, f.cfg.Timeout)
	defer cancel()
	env := &ExecEnv{InstanceID: inst.id, inst: inst}
	resCh := make(chan result, 1)
	go func() {
		res, err := f.handler(execCtx, env, req)
		resCh <- result{res, err}
	}()
	select {
	case r := <-resCh:
		f.pool.release(inst, false)
		if r.err != nil {
			return nil, &InvokeError{f.name, ReasonHandler, r.err}
		}
		return r.res, nil
	case <-execCtx.Done():
		f.pool.release(inst, true)
		if ctx.Err() != nil {
			return nil, &InvokeError{f.name, ReasonCanceled, ctx.Err()}
		}
		return nil, &InvokeError{f.name, ReasonTimeout, ErrTimeout}
	}
}

// Stats 是观测用快照。
type Stats struct {
	InFlight  int
	Waiting   int
	Instances int
}

func (f *Function) Stats() Stats {
	f.mu.Lock()
	waiting := f.waiting
	f.mu.Unlock()
	return Stats{InFlight: len(f.slots), Waiting: waiting, Instances: f.pool.size()}
}

func (f *Function) close() {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	f.pool.close()
}
