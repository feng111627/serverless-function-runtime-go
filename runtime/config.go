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

func (c Config) withDefaults() Config {
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = 1
	}
	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}
	if c.QueueSize < 0 {
		c.QueueSize = 0
	}
	if c.MaxInstances <= 0 {
		// 默认实例上限与并发上限一致：一个实例同一时刻只处理一个请求。
		c.MaxInstances = c.MaxConcurrency
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 60 * time.Second
	}
	return c
}
