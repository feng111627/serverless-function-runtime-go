package runtime

import (
	"context"
	"sync"
	"time"
)

type instanceState int

const (
	stateIdle instanceState = iota
	stateBusy
	stateRetired
)

// ExecEnv 是每次调用注入 handler 的执行环境，携带实例身份与实例私有状态。
type ExecEnv struct {
	InstanceID int
	inst       *instance
}

// Get 读取实例私有状态。
func (e *ExecEnv) Get(key string) (any, bool) {
	e.inst.mu.Lock()
	defer e.inst.mu.Unlock()
	v, ok := e.inst.store[key]
	return v, ok
}

// Put 写入实例私有状态（仅当前实例可见，不跨实例共享）。
func (e *ExecEnv) Put(key string, v any) {
	e.inst.mu.Lock()
	defer e.inst.mu.Unlock()
	e.inst.store[key] = v
}

// Handler 是用户函数。
type Handler func(ctx context.Context, env *ExecEnv, req any) (any, error)

type instance struct {
	id       int
	mu       sync.Mutex
	state    instanceState
	lastUsed time.Time
	store    map[string]any
}

func newInstance(id int) *instance {
	return &instance{id: id, state: stateIdle, lastUsed: time.Now(), store: make(map[string]any)}
}

// tryAcquire 仅当实例空闲时将其置为忙碌；已回收或忙碌则失败。
func (i *instance) tryAcquire() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.state != stateIdle {
		return false
	}
	i.state = stateBusy
	return true
}

// markIdle 将实例置回空闲；已回收的实例保持回收态，不再接受请求。
func (i *instance) markIdle() {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.state == stateBusy {
		i.state = stateIdle
		i.lastUsed = time.Now()
	}
}

// retire 回收实例；忙碌中的实例返回 false（不打断执行中的调用）。
func (i *instance) retire() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.state == stateBusy || i.state == stateRetired {
		return false
	}
	i.state = stateRetired
	return true
}

// forceRetire 强制回收（用于超时后不复用，防止被污染的状态泄漏）。
func (i *instance) forceRetire() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.state = stateRetired
}

func (i *instance) isIdle() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.state == stateIdle
}

func (i *instance) idleFor(now time.Time) time.Duration {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.state != stateIdle {
		return 0
	}
	return now.Sub(i.lastUsed)
}
