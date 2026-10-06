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
	stopCh      chan struct{}
}

func newPool(max int, idleTimeout, coldStart time.Duration) *pool { return nil } // TODO

// acquire 取一个空闲实例，或在未达上限时冷启动新实例。
func (p *pool) acquire(ctx context.Context) (*instance, error) { return nil, nil } // TODO

// release 归还实例；retire 为 true 时直接回收（如超时后不复用）。
func (p *pool) release(inst *instance, retire bool) {} // TODO

// size 返回当前存活实例数。
func (p *pool) size() int { return 0 } // TODO

// close 停止回收循环并回收全部实例。
func (p *pool) close() {} // TODO
