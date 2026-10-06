package runtime

import "errors"

// 可被 errors.Is 区分的哨兵错误。
var (
	ErrTimeout            = errors.New("runtime: invocation timed out")
	ErrConcurrencyLimited = errors.New("runtime: concurrency limit reached")
	ErrQueueFull          = errors.New("runtime: queue full (backpressure)")
	ErrFunctionNotFound   = errors.New("runtime: function not found")
	ErrInstanceRetired    = errors.New("runtime: instance retired")
	ErrRuntimeClosed      = errors.New("runtime: closed")
)

// Reason 是可解释的失败原因分类。
type Reason string

const (
	ReasonTimeout            Reason = "timeout"
	ReasonConcurrencyLimited Reason = "concurrency_limited"
	ReasonQueueFull          Reason = "queue_full"
	ReasonHandler            Reason = "handler_error"
	ReasonCanceled           Reason = "canceled"
)

// InvokeError 携带函数名与失败原因，便于调用方区分。
type InvokeError struct {
	Function string
	Reason   Reason
	Err      error
}

func (e *InvokeError) Error() string { return "" } // TODO
func (e *InvokeError) Unwrap() error { return nil } // TODO
