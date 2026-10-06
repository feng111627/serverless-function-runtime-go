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
}

// Get 读取实例私有状态。
func (e *ExecEnv) Get(key string) (any, bool) { return nil, false } // TODO

// Put 写入实例私有状态（仅当前实例可见）。
func (e *ExecEnv) Put(key string, v any) {} // TODO

// Handler 是用户函数。
type Handler func(ctx context.Context, env *ExecEnv, req any) (any, error)

type instance struct {
	id       int
	mu       sync.Mutex
	state    instanceState
	lastUsed time.Time
	store    map[string]any
}
