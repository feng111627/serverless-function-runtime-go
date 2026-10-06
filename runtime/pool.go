package runtime

import (
	"context"
	"sync"
	"time"
)

// pool 管理单个函数的实例集合：冷启动、复用、空闲回收。
type pool struct {
	mu          sync.Mutex
	instances   []*instance
	nextID      int
	max         int
	idleTimeout time.Duration
	coldStart   time.Duration
	notify      chan struct{} // 每次状态变化时关闭并重建，用于唤醒等待者
	stopCh      chan struct{}
	closed      bool
}

func newPool(max int, idleTimeout, coldStart time.Duration) *pool {
	p := &pool{
		max:         max,
		idleTimeout: idleTimeout,
		coldStart:   coldStart,
		notify:      make(chan struct{}),
		stopCh:      make(chan struct{}),
	}
	go p.reapLoop()
	return p
}

// acquire 取一个空闲实例，或在未达上限时冷启动新实例；达到上限则等待。
func (p *pool) acquire(ctx context.Context) (*instance, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrRuntimeClosed
		}
		// 1) 优先复用空闲实例。
		for _, inst := range p.instances {
			if inst.tryAcquire() {
				p.mu.Unlock()
				return inst, nil
			}
		}
		// 2) 未达上限：创建新实例（冷启动），先占位再模拟启动耗时。
		if len(p.instances) < p.max {
			inst := newInstance(p.nextID)
			p.nextID++
			inst.tryAcquire()
			p.instances = append(p.instances, inst)
			p.mu.Unlock()
			if p.coldStart > 0 {
				select {
				case <-time.After(p.coldStart):
				case <-ctx.Done():
					p.release(inst, true)
					return nil, ctx.Err()
				}
			}
			return inst, nil
		}
		// 3) 全部忙碌且已达上限：等待释放或 ctx 取消。
		ch := p.notify
		p.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// release 归还实例；retire 为 true 时直接回收（如超时后不复用，避免状态污染）。
func (p *pool) release(inst *instance, retire bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if retire {
		inst.forceRetire()
		p.removeLocked(inst)
	} else {
		inst.markIdle()
	}
	p.broadcastLocked()
}

func (p *pool) removeLocked(inst *instance) {
	for i, x := range p.instances {
		if x == inst {
			p.instances = append(p.instances[:i], p.instances[i+1:]...)
			return
		}
	}
}

func (p *pool) broadcastLocked() {
	close(p.notify)
	p.notify = make(chan struct{})
}

// size 返回当前存活实例数。
func (p *pool) size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.instances)
}

// reapLoop 周期性回收空闲超时的实例（缩容）；忙碌实例不受影响。
func (p *pool) reapLoop() {
	interval := p.idleTimeout / 2
	if interval <= 0 || interval > 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	if interval < 5*time.Millisecond {
		interval = 5 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case now := <-t.C:
			p.mu.Lock()
			var kept []*instance
			for _, inst := range p.instances {
				if inst.idleFor(now) >= p.idleTimeout && inst.retire() {
					continue
				}
				kept = append(kept, inst)
			}
			p.instances = kept
			p.mu.Unlock()
		}
	}
}

// close 停止回收循环并回收全部实例；等待中的 acquire 会收到错误。
func (p *pool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.stopCh)
	for _, inst := range p.instances {
		inst.forceRetire()
	}
	p.instances = nil
	p.broadcastLocked()
}
