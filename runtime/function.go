package runtime

import (
	"context"
	"sync"
)

// Function 是已注册的函数，包含配置、并发控制与实例池。
type Function struct {
	name   string
	cfg    Config
	handler Handler

	mu       sync.Mutex
	inFlight int
	waiting  int

	pool *pool
}

// Invoke 同步调用函数，返回结果或可区分的失败原因。
func (f *Function) Invoke(ctx context.Context, req any) (any, error) { return nil, nil } // TODO

// Stats 是观测用快照。
type Stats struct {
	InFlight  int
	Waiting   int
	Instances int
}

func (f *Function) Stats() Stats { return Stats{} } // TODO
