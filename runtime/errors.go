package runtime

import "errors"

// 可区分的失败原因，调用方可用 errors.Is 判定。
var (
	// ErrFunctionNotFound 调用了未注册的函数。
	ErrFunctionNotFound = errors.New("runtime: function not found")
	// ErrFunctionExists 重复注册同名函数。
	ErrFunctionExists = errors.New("runtime: function already registered")
	// ErrQueueFull 积压队列已满，背压信号，请求被拒绝。
	ErrQueueFull = errors.New("runtime: queue full, backpressure")
	// ErrTimeout 函数执行超过其超时配置，调用被终止。
	ErrTimeout = errors.New("runtime: execution timeout")
	// ErrInstanceUnavailable 实例正在执行或已被回收，无法接受请求。
	ErrInstanceUnavailable = errors.New("runtime: instance unavailable")
	// ErrRuntimeClosed 运行时已关闭，不再接受新调用。
	ErrRuntimeClosed = errors.New("runtime: runtime closed")
)
