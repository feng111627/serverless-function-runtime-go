# serverless-function-runtime-go

一套本地无服务器函数运行时：支持函数注册与调用、每函数独立的并发上限与执行超时、
执行实例的冷启动/复用/回收，以及压力下的自动伸缩与背压。

## 快速开始

```go
rt := runtime.New()
defer rt.Close()

rt.Register("hello", runtime.Config{
    MaxConcurrency: 4,               // 最多 4 个在途调用
    Timeout:        2 * time.Second, // 单次调用执行超时
    Policy:         runtime.PolicyQueue,
    QueueSize:      16,              // 积压队列上限
    MaxInstances:   4,               // 实例数量硬上限
    IdleTimeout:    30 * time.Second,
}, func(ctx context.Context, env *runtime.ExecEnv, req any) (any, error) {
    return "hello " + req.(string), nil
})

res, err := rt.Invoke(ctx, "hello", "world")
```

## 并发与超时语义

- **并发上限（MaxConcurrency）**：每个函数独立的在途调用上限。超限请求按 `Policy` 处理：
  - `PolicyReject`：立即失败，返回 `ErrConcurrencyLimited`（原因 `concurrency_limited`）。
  - `PolicyQueue`：进入有界积压队列等待空位；调用方 context 取消时以原因 `canceled` 退出队列。
- **执行超时（Timeout）**：单次调用在超时后被终止，返回 `ErrTimeout`（原因 `timeout`），
  与 handler 自身错误（原因 `handler_error`）、调用方取消（原因 `canceled`）明确区分。
- **路由隔离**：`Invoke` 严格按名字路由到目标函数，不会把请求派给别的函数；
  未注册的函数返回 `ErrFunctionNotFound`。
- 所有失败都包装为 `*InvokeError`，携带 `Function` 与 `Reason` 字段，
  可用 `errors.Is`（哨兵错误）或 `errors.As`（读取原因）区分。

## 实例生命周期

- **冷启动**：无空闲实例且未达 `MaxInstances` 时创建新实例，可配置 `ColdStart` 模拟启动耗时。
- **复用**：调用结束后实例回到空闲态，优先被后续调用复用；实例私有状态
  （`ExecEnv.Put/Get`）在复用间延续，且**仅当前实例可见**，不同实例互不共享。
- **回收**：实例空闲超过 `IdleTimeout` 后被后台回收（缩容）；已回收或执行中的实例
  不会接受新请求。
- **超时强制回收**：执行超时的实例可能被 handler 遗留的 goroutine 污染，因此不复用，
  直接强制回收，防止状态泄漏到后续调用。

## 伸缩与背压规则

- **扩容**：压力上升时按需创建实例，实例数硬上限为 `MaxInstances`（默认等于
  `MaxConcurrency`），突发流量不会导致实例无限增长。
- **缩容**：空闲实例超时回收；**执行中的调用不会被打断**——忙碌实例不参与回收。
- **背压**：积压队列容量为 `QueueSize`，队列满时新请求立即失败，
  返回 `ErrQueueFull`（原因 `queue_full`），不会静默丢弃。

## 本地验证

```bash
# 运行全部单测（verbose 日志可看到各业务场景与用例结果）
go test ./runtime/ -v

# 带数据竞争检测
go test ./... -race
```

测试覆盖的业务场景（日志中以 `用例[...]` 标注）：

| 场景 | 用例 |
| --- | --- |
| 并发上限 | 拒绝策略超限即失败、排队策略全部成功且峰值不超上限 |
| 执行超时 | 超时调用被及时终止并返回 `timeout` 原因 |
| 路由隔离 | 请求精确路由、未注册函数报可区分错误 |
| 实例复用 | 串行调用复用同一实例、私有状态延续 |
| 状态隔离 | 并发实例私有状态互不可见 |
| 空闲回收 | 空闲超时回收、再次调用冷启动新实例 |
| 超时回收 | 超时实例不复用，防止状态污染 |
| 冷启动 | 首次调用含冷启动耗时，复用无 |
| 压力扩容 | 并发压力推动实例数增长至上限 |
| 突发上限 | 突发流量下实例数不超硬上限、排队请求全部成功 |
| 背压 | 队列满返回 `queue_full` 背压信号 |
| 回落缩容 | 空闲实例被回收，执行中的调用不被打断 |
