# serverless-function-runtime-go

一个本地无服务器（Serverless）函数运行时，提供函数注册与调用、并发与超时控制、
执行实例生命周期管理，以及压力下的自动伸缩与背压。所有入口的语义一致且失败原因可区分。

## 快速开始

```go
rt := runtime.New()
defer rt.Shutdown(context.Background())

err := rt.Register("hello", runtime.FunctionConfig{
    MaxConcurrency:   4,               // 最大在途执行数
    Timeout:          2 * time.Second, // 单次执行超时
    MaxQueue:         64,              // 积压队列上限
    MinInstances:     0,               // 常驻实例下限
    MaxInstances:     8,               // 实例数上限
    IdleTimeout:      30 * time.Second,// 空闲回收时长
    TerminationGrace: 100 * time.Millisecond, // 终止后等待函数体退出的宽限期
}, func(ctx context.Context, inv *runtime.Invocation) (any, error) {
    return string(inv.Payload), nil
})

res, err := rt.Invoke(ctx, "hello", []byte("world"))
```

## 并发与超时语义

- 每个函数有独立的并发上限 `MaxConcurrency`，在途执行数绝不超过该值。
- 超出并发上限的请求进入该函数自己的有界队列（长度上限 `MaxQueue`）等待派发；
  队列满时新请求**同步拒绝**并返回 `ErrQueueFull`（背压信号）。
  `MaxQueue = 0` 表示不排队，超出并发即拒绝。
- 请求**不会静默丢弃**，也**不会派发给其他函数**——每个函数有独立的实例池与队列。
- 单次执行超过 `Timeout` 时调用被终止，返回 `ErrTimeout`；
  函数体收到的 `ctx` 会被取消，实现应响应取消以便及时释放资源。
  **即使函数体不理会取消**，运行时也会保证其占用的实例不被复用（见下节「失控执行的隔离与自愈」）。
- 调用方主动取消（`ctx.Done`）与函数超时是**可区分**的：前者返回调用方的 `ctx.Err()`，
  后者返回 `ErrTimeout`。排队期间被取消的请求会被跳过，不占用实例。

所有失败原因均可用 `errors.Is` 判定：

| 错误 | 含义 |
|---|---|
| `ErrFunctionNotFound` | 调用了未注册的函数 |
| `ErrFunctionExists` | 重复注册同名函数 |
| `ErrQueueFull` | 并发占满且积压队列已满（背压） |
| `ErrTimeout` | 函数执行超时，调用被终止 |
| `ErrInstanceUnavailable` | 实例正在执行或已回收，不可接受请求 |
| `ErrRuntimeClosed` | 运行时已关闭 |

## 实例生命周期

实例状态机：`cold-starting → idle → busy → idle … → (draining | quarantined) → stopped`

- **冷启动**：无空闲实例且未达 `MaxInstances` 时创建新实例，状态为 `cold-starting`，
  预热完成后转为 `idle`。
- **复用**：`idle` 实例可被后续调用复用；每次执行独占实例（`busy`），
  执行结束回到 `idle`。
- **回收**：`idle` 时长超过 `IdleTimeout` 的实例被回收为 `stopped`，
  但实例数不会低于 `MinInstances`。
- **保护**：正在执行（`busy`）的实例不会被回收；已回收（`stopped`）的实例
  不再接受任何请求；`Shutdown` 时执行中的实例标记为 `draining`，
  调用自然结束后销毁，不打断在途调用。
- **状态隔离**：每个实例持有独立的 `Invocation.State`（实例创建时分配），
  同一实例的多次调用可累积状态，不同实例之间不共享，实例回收后状态随之销毁。

## 失控执行的隔离与自愈

函数体可能不理会 `ctx` 取消：调用已被判定超时/终止，但函数体仍在后台继续运行。
运行时对这类「失控执行」提供隔离与自愈保障：

- **终止判定即答复**：调用被判定超时（`ErrTimeout`）或被调用方取消（`ctx.Err()`）后，
  调用方立即拿到结果，并发配额立即释放，不等函数体真正退出。
- **宽限期**：终止判定后等待 `TerminationGrace`（默认 100ms）。
  函数体在宽限期内真正退出，实例干净，正常回到 `idle` 被复用——
  响应取消的函数不受任何影响。
- **隔离**：宽限期过后函数体仍未退出，判定为失控执行，实例被标记为
  `quarantined` 并从可用容量中移除。**隔离实例绝不会再被派发给任何调用**，
  因此任何时刻都不会出现同一实例同时跑两次调用；
  失控执行残留的 `State` 随实例销毁，不会串扰后续调用。
  失控执行真正结束后，实例转为 `stopped` 销毁，隔离计数回落。
- **自愈**：隔离实例不再算作可用容量，运行时立即在 `MaxInstances` 上限内
  补位一个健康实例（计入 `Replaced`），并继续派发排队请求。
  个别卡死的调用不会永久拖垮整体容量，**实例总数仍不突破 `MaxInstances`**，
  排队中的请求也不会因此被丢弃。
- **兼容关系**：隔离与自愈只作用于「被终止但函数体未退出」的实例，
  不改变既有语义——并发上限、有界队列与背压拒绝、超时与取消可区分、
  函数间互不干扰、空闲回收与常驻下限等规则全部照旧；
  隔离中的实例不参与复用与回收扫描，也不计入 `Instances`。

## 伸缩与背压规则

- **扩容**：有请求待派发且无空闲实例时，若实例数 < `MaxInstances` 则冷启动新实例；
  突发流量下实例数**绝不突破** `MaxInstances`。
- **缩容**：后台回收器周期性扫描，回收空闲超时的实例，实例数不跌破 `MinInstances`；
  缩容只针对 `idle` 实例，不打断执行中的调用。
- **背压**：并发占满且队列达到 `MaxQueue` 时，新请求立即以 `ErrQueueFull` 拒绝，
  拒绝计数体现在 `Stats` 的 `Rejected` 字段。
- 配置兜底：`MaxInstances` 至少为 `MaxConcurrency`（保证并发上限可达），
  `MinInstances` 不超过 `MaxInstances`，未显式设置的字段取默认值
  （并发 4、超时 5s、队列 64、实例上限 16、空闲回收 30s、终止宽限 100ms）。

## 可观测性

`rt.Stats(name)` 返回函数实例池快照：

```go
type PoolStats struct {
    Instances      int   // 当前实例数（不含已隔离）
    InFlight       int   // 在途执行数
    Queued         int   // 积压队列长度
    QuarantinedNow int   // 当前隔离中（失控执行未真正结束）的实例数
    ColdStarts     int64 // 累计冷启动次数
    Executed       int64 // 累计执行完成次数
    Timeouts       int64 // 累计超时次数
    Rejected       int64 // 累计背压拒绝次数
    Reclaimed      int64 // 累计回收实例数
    Quarantined    int64 // 累计被隔离的实例数
    Replaced       int64 // 累计自愈补位的实例数
}
```

通过 `Timeouts` / `Quarantined` / `Replaced` / `QuarantinedNow` 可一眼区分
正常完成与异常隔离：`QuarantinedNow > 0` 表示当前仍有失控执行未退出，
`Quarantined` 与 `Replaced` 的差值反映隔离自愈的历史规模。

## 本地验证

```bash
# 构建与静态检查
go build ./... && go vet ./...

# 运行全部单测（-v 日志可看到各业务覆盖的用例与断言过程）
go test ./... -v

# 竞态检测
go test ./... -race -count=1
```

单测按业务组织，日志可直接看出覆盖范围：

| 测试文件 | 覆盖业务 |
|---|---|
| `runtime/runtime_test.go` | 函数注册/调用、重复注册、未注册调用、关闭语义 |
| `runtime/concurrency_test.go` | 并发上限、排队、背压拒绝、执行超时、函数间隔离 |
| `runtime/instance_test.go` | 实例复用、状态隔离、空闲回收、执行中实例保护 |
| `runtime/scaling_test.go` | 压力扩容、回落缩容、队列上限背压、突发上限、常驻下限 |
| `runtime/isolation_test.go` | 失控执行隔离（不理会取消）、防并发复用与状态串扰、自愈补位与实例上限、优雅终止兼容、函数间隔离 |

### 复现失控执行场景

`runtime/isolation_test.go` 中的 `TestRunawayExecutionQuarantined` 即最小复现：
函数体在收到 `ctx` 取消后仍继续运行（不理会取消），断言超时后该实例
不会被并发复用、残留状态不串扰、实例被隔离且运行时自愈补位。
直接运行 `go test ./runtime/ -run TestRunawayExecutionQuarantined -v`
即可在日志中看到隔离与自愈的完整过程（`QuarantinedNow` 升降、
`Quarantined`/`Replaced` 累计计数）。
