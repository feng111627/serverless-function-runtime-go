package runtime

import (
	"context"
	"sync"
)

// Runtime 是函数注册与调用的入口。
type Runtime struct {
	mu        sync.RWMutex
	functions map[string]*Function
	closed    bool
}

func New() *Runtime { return nil } // TODO

// Register 注册函数；同名重复注册返回错误。
func (r *Runtime) Register(name string, cfg Config, h Handler) error { return nil } // TODO

// Invoke 按名字调用函数，不会把请求派给别的函数。
func (r *Runtime) Invoke(ctx context.Context, name string, req any) (any, error) { return nil, nil } // TODO

// Stats 返回函数的运行快照。
func (r *Runtime) Stats(name string) (Stats, error) { return Stats{}, nil } // TODO

// Close 回收所有实例并拒绝新调用。
func (r *Runtime) Close() {} // TODO
