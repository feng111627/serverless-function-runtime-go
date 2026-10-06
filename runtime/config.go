package runtime

import "time"

// Policy 决定并发已满时的行为：排队或拒绝。
type Policy int

const (
	PolicyQueue Policy = iota
	PolicyReject
)

// Config 是单个函数的运行时配置。
type Config struct {
	MaxConcurrency int
	Timeout        time.Duration
	QueueSize      int
	Policy         Policy
	MaxInstances   int
	IdleTimeout    time.Duration
	ColdStart      time.Duration
}

func (c Config) withDefaults() Config { return c } // TODO
