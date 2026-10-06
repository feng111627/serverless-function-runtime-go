package runtime

import (
	"context"
	"time"
)

// Invocation 是一次函数调用的上下文。
// State 为实例级可变状态：同一实例的多次调用可见，不同实例之间隔离，
// 实例回收后随之销毁，不会泄漏到新实例。
type Invocation struct {
	RequestID  string
	FuncName   string
	Payload    []byte
	InstanceID string
	State      map[string]any
}

// Handler 是函数体。ctx 在执行超时时被取消，实现应响应取消；
// 即使实现不响应取消，运行时也会向调用方返回 ErrTimeout。
type Handler func(ctx context.Context, inv *Invocation) (any, error)

// FunctionConfig 描述一个已注册函数的并发、超时与伸缩语义。
type FunctionConfig struct {
	// MaxConcurrency 该函数允许的最大在途执行数，超过后请求进入队列。
	// <= 0 时取默认值 4。
	MaxConcurrency int
	// Timeout 单次执行超时，超时调用被终止并返回 ErrTimeout。
	// <= 0 时取默认值 5s。
	Timeout time.Duration
	// MaxQueue 积压队列上限，满时新请求以 ErrQueueFull 拒绝（背压）。
	// < 0 时取默认值 64；= 0 表示不排队，超出并发即拒绝。
	MaxQueue int
	// MinInstances 常驻实例数下限，缩容不会低于该值。<= 0 时取 0。
	MinInstances int
	// MaxInstances 实例数上限，突发流量不会突破该值。<= 0 时取默认值 16。
	MaxInstances int
	// IdleTimeout 实例空闲超过该时长后被回收（缩容）。
	// <= 0 时取默认值 30s。
	IdleTimeout time.Duration
}

const (
	defaultMaxConcurrency = 4
	defaultTimeout        = 5 * time.Second
	defaultMaxQueue       = 64
	defaultMaxInstances   = 16
	defaultIdleTimeout    = 30 * time.Second
)

// withDefaults 返回填充默认值后的配置副本。
func (c *FunctionConfig) withDefaults() FunctionConfig {
	out := *c
	if out.MaxConcurrency <= 0 {
		out.MaxConcurrency = defaultMaxConcurrency
	}
	if out.Timeout <= 0 {
		out.Timeout = defaultTimeout
	}
	if out.MaxQueue < 0 {
		out.MaxQueue = defaultMaxQueue
	}
	if out.MinInstances < 0 {
		out.MinInstances = 0
	}
	if out.MaxInstances <= 0 {
		out.MaxInstances = defaultMaxInstances
	}
	if out.IdleTimeout <= 0 {
		out.IdleTimeout = defaultIdleTimeout
	}
	// 实例上限至少要能容纳并发上限，否则并发上限永远无法达到。
	if out.MaxInstances < out.MaxConcurrency {
		out.MaxInstances = out.MaxConcurrency
	}
	if out.MinInstances > out.MaxInstances {
		out.MinInstances = out.MaxInstances
	}
	return out
}
