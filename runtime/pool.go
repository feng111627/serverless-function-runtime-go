package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// invokeResult 是一次执行的产出。
type invokeResult struct {
	res any
	err error
}

// queuedRequest 是排队中的请求，result 为容量 1 的通道，保证发送不阻塞。
type queuedRequest struct {
	ctx    context.Context
	inv    *Invocation
	result chan invokeResult
}

// PoolStats 是实例池的可观测快照。
type PoolStats struct {
	Instances   int   // 当前实例数（含执行中与回收中）
	InFlight    int   // 在途执行数
	Queued      int   // 积压队列长度
	ColdStarts  int64 // 累计冷启动次数
	Executed    int64 // 累计执行完成次数
	Timeouts    int64 // 累计超时次数
	Rejected    int64 // 累计因背压被拒绝次数
	Reclaimed   int64 // 累计回收实例数
}

// pool 是单个函数的实例池，负责排队、派发、扩缩容与回收。
type pool struct {
	name    string
	cfg     FunctionConfig
	handler Handler

	mu        sync.Mutex
	instances map[*instance]struct{}
	queue     []queuedRequest
	inFlight  int
	closed    bool

	reqSeq  atomic.Uint64
	instSeq atomic.Uint64

	coldStarts atomic.Int64
	executed   atomic.Int64
	timeouts   atomic.Int64
	rejected   atomic.Int64
	reclaimed  atomic.Int64

	stopReaper chan struct{}
	reaperDone chan struct{}
}

func newPool(name string, cfg FunctionConfig, h Handler) *pool {
	p := &pool{
		name:       name,
		cfg:        cfg,
		handler:    h,
		instances:  make(map[*instance]struct{}),
		stopReaper: make(chan struct{}),
		reaperDone: make(chan struct{}),
	}
	// 常驻下限：预热 MinInstances 个实例。
	for i := 0; i < cfg.MinInstances; i++ {
		inst := p.newInstanceLocked()
		inst.warm()
	}
	go p.reapLoop()
	return p
}

// newInstanceLocked 创建实例并计入池，调用方需持有 p.mu。
// 新实例处于 cold-starting 状态，store 独立分配，不与其他实例共享。
func (p *pool) newInstanceLocked() *instance {
	id := fmt.Sprintf("%s-inst-%d", p.name, p.instSeq.Add(1))
	inst := newInstance(id)
	p.instances[inst] = struct{}{}
	p.coldStarts.Add(1)
	return inst
}

// acquireLocked 获取一个可用实例：优先复用空闲实例，
// 无空闲且未达 MaxInstances 时冷启动新实例（扩容），
// 全部繁忙且已达上限时返回 nil（请求继续排队）。
func (p *pool) acquireLocked() *instance {
	for inst := range p.instances {
		if inst.tryAcquire() {
			return inst
		}
	}
	if len(p.instances) < p.cfg.MaxInstances {
		inst := p.newInstanceLocked()
		inst.warm()
		if inst.tryAcquire() {
			return inst
		}
	}
	return nil
}

// invoke 提交一次调用：有空闲容量时立即派发，否则入队，
// 队列满时返回 ErrQueueFull 背压信号，绝不静默丢弃或派给其他函数。
func (p *pool) invoke(ctx context.Context, inv *Invocation) (any, error) {
	req := queuedRequest{ctx: ctx, inv: inv, result: make(chan invokeResult, 1)}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrRuntimeClosed
	}
	// 快路径：队列无积压且并发未满时直接派发。
	if len(p.queue) == 0 && p.inFlight < p.cfg.MaxConcurrency {
		if inst := p.acquireLocked(); inst != nil {
			p.inFlight++
			p.mu.Unlock()
			go p.execute(inst, req)
			r := <-req.result
			return r.res, r.err
		}
	}
	// 慢路径：入队等待，队列满则拒绝（背压）。
	if len(p.queue) >= p.cfg.MaxQueue {
		p.mu.Unlock()
		p.rejected.Add(1)
		return nil, ErrQueueFull
	}
	p.queue = append(p.queue, req)
	p.mu.Unlock()

	r := <-req.result
	return r.res, r.err
}

// dispatchLocked 尽量把队列中的请求派发给实例，调用方需持有 p.mu。
func (p *pool) dispatchLocked() {
	for len(p.queue) > 0 && p.inFlight < p.cfg.MaxConcurrency {
		inst := p.acquireLocked()
		if inst == nil {
			return // 实例全部繁忙且已达上限，请求留在队列中
		}
		req := p.queue[0]
		p.queue = p.queue[1:]
		// 排队期间调用方已取消的请求直接跳过，不占用实例。
		if req.ctx.Err() != nil {
			inst.release()
			req.result <- invokeResult{err: req.ctx.Err()}
			continue
		}
		p.inFlight++
		go p.execute(inst, req)
	}
}

// execute 在独立 goroutine 中运行函数体，施加执行超时。
func (p *pool) execute(inst *instance, req queuedRequest) {
	ctx, cancel := context.WithTimeout(req.ctx, p.cfg.Timeout)
	defer cancel()

	req.inv.InstanceID = inst.id
	req.inv.State = inst.store

	resCh := make(chan invokeResult, 1)
	go func() {
		res, err := p.handler(ctx, req.inv)
		resCh <- invokeResult{res: res, err: err}
	}()

	select {
	case r := <-resCh:
		p.finish(inst)
		p.executed.Add(1)
		req.result <- r
	case <-ctx.Done():
		p.finish(inst)
		// 区分“函数执行超时”与“调用方主动取消”。
		if errors.Is(ctx.Err(), context.DeadlineExceeded) && req.ctx.Err() == nil {
			p.timeouts.Add(1)
			req.result <- invokeResult{err: ErrTimeout}
		} else {
			req.result <- invokeResult{err: req.ctx.Err()}
		}
	}
}

// finish 回收一次执行的容量并触发后续派发。
func (p *pool) finish(inst *instance) {
	inst.release()
	p.mu.Lock()
	p.inFlight--
	p.dispatchLocked()
	p.mu.Unlock()
}

// reapLoop 周期性回收空闲超时的实例（缩容），
// 正在执行的实例不会被回收，实例数不会低于 MinInstances。
func (p *pool) reapLoop() {
	defer close(p.reaperDone)
	interval := p.cfg.IdleTimeout / 2
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopReaper:
			return
		case <-ticker.C:
			p.reap()
		}
	}
}

func (p *pool) reap() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for inst := range p.instances {
		if len(p.instances) <= p.cfg.MinInstances {
			break
		}
		if inst.tryReclaim(p.cfg.IdleTimeout) {
			delete(p.instances, inst)
			p.reclaimed.Add(1)
		}
	}
}

// stats 返回池的当前快照。
func (p *pool) stats() PoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return PoolStats{
		Instances:  len(p.instances),
		InFlight:   p.inFlight,
		Queued:     len(p.queue),
		ColdStarts: p.coldStarts.Load(),
		Executed:   p.executed.Load(),
		Timeouts:   p.timeouts.Load(),
		Rejected:   p.rejected.Load(),
		Reclaimed:  p.reclaimed.Load(),
	}
}

// close 停止池：拒绝新调用与排队请求，空闲实例立即回收，
// 执行中的实例标记为 draining，调用结束后自行销毁，不打断在途调用。
func (p *pool) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	queued := p.queue
	p.queue = nil
	for inst := range p.instances {
		switch instanceState(inst.state.Load()) {
		case stateIdle:
			if inst.tryReclaim(0) {
				delete(p.instances, inst)
				p.reclaimed.Add(1)
			}
		case stateBusy:
			inst.markDraining()
		}
	}
	p.mu.Unlock()

	for _, req := range queued {
		req.result <- invokeResult{err: ErrRuntimeClosed}
	}
	close(p.stopReaper)
	<-p.reaperDone
}
