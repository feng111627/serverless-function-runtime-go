# serverless-function-runtime-go

一个本地无服务器（Serverless）函数运行时，提供函数注册与调用、并发与超时控制、
执行实例生命周期管理，以及压力下的自动伸缩与背压。所有入口的语义一致且失败原因可区分。

在上述能力之上，运行时还提供一层**失控执行的隔离与自愈保险**：即使函数体完全不理会
取消信号、超时后仍在后台继续运行，它占用的实例也绝不会再被复用，运行时会隔离该实例、
在其真正结束后销毁，并在持续流量下自动补回健康实例，使个别卡死的调用无法搞乱运行时语义
或永久拖垮整体容量。

## 快速开始

```go
rt := runtime.New()
defer rt.Shutdown(context.Background())

err := rt.Register("hello", runtime.FunctionConfig{
    MaxConcurrency: 4,               // 最大在途执行数
    Timeout:        2 * time.Second, // 单次执行超时
    MaxQueue:       64,              // 积压队列上限
    MinInstances:   0,               // 常驻实例下限
    MaxInstances:   8,               // 实例数上限
    IdleTimeout:    30 * time.Second,// 空闲回收时长
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

## 失控执行的隔离与自愈

**问题背景**：Go 无法安全地强制终止一个 goroutine。超时或调用方取消时，运行时只能
撤销 `ctx`，若函数体不理会取消，该执行会作为“僵尸”在后台继续。若此时把实例过早
释放回空闲池，就可能出现同一个实例同时跑两次调用、上一次执行的残留状态污染下一次
调用的问题。运行时为此提供以下保证（函数体**完全不理会取消也成立**）：

- **判定即隔离（无并发复用）**：某次调用一旦被判定为超时（`ErrTimeout`）或被终止
  （调用方取消，回传其 `ctx.Err()`），而函数体尚未真正返回，其占用的实例立即从
  `busy` 转为新增的 `quarantined`（隔离）状态，**永远不会回到 `idle`**。
  因此同一实例任何时刻都不可能同时承载两次调用。
- **无状态串扰**：被隔离实例的私有 `Invocation.State` 随实例一起封存并在僵尸结束后
  随实例删除而可被 GC，后续请求只能落到新建的替换实例上，看不到任何残留状态。
- **不计入可用容量，但不占并发额度**：隔离判定时该调用占的**并发额度立即释放**（
  `InFlight` 递减），排队请求可立刻继续派发；但被隔离的实例仍计入实例总数并占位，
  直到僵尸真正结束才被删除。
- **自愈补位**：僵尸执行真正结束的瞬间（运行时通过无缓冲结果通道精确感知，不靠
  轮询），实例转为 `stopped` 并移出实例池；只要池未关闭且当前实例数小于
  `MaxInstances`，立即预热一个健康的替换实例（计入 `Replaced`）并尝试消化队列。
- **排队请求不丢弃**：若隔离发生时全部实例槽位都被僵尸占满（例如 `MaxInstances=1`），
  后续请求按既有规则在该函数的有界队列中等待，**绝不丢请求、绝不跨函数派发**；
  僵尸结束、槽位释放后自动得到服务。队列满时仍按既有规则返回 `ErrQueueFull` 背压。
- **失败原因可区分**：超时隔离回传 `ErrTimeout`，调用方主动取消隔离回传调用方的
  `ctx.Err()`（如 `context.Canceled`），后者**不**计入 `Timeouts` 计数。

### 与现有规则的兼容关系

| 既有规则 | 隔离/自愈下的行为 |
|---|---|
| 并发上限 `MaxConcurrency` | 不变。隔离判定即释放并发额度，在途数任何时刻不超过上限。 |
| 排队与背压 | 不变。隔离不丢排队请求；槽位全被僵尸占满时请求继续排队，队列满仍 `ErrQueueFull`。 |
| 超时可区分 | 不变。超时→`ErrTimeout`，调用方取消→其 `ctx.Err()`，二者仍可区分。 |
| 伸缩 | 扩容只基于健康实例；自愈替换与扩容都计入实例总数，**绝不突破 `MaxInstances`**；空闲回收只动 `idle` 实例，不碰隔离实例。 |
| 函数间隔离 | 不变。隔离与自愈统计、实例池都按函数独立，A 函数隔离不影响 B 函数。 |
| `Shutdown` | 不被隔离僵尸挂住：关闭拒绝新调用与排队请求，隔离实例保持隔离；僵尸结束后仅销毁、**不自愈**，不会在关闭后复活实例。 |

## 实例生命周期

实例状态机：`cold-starting → idle → busy → idle … → (draining) → stopped`

失控执行引入一条独立的终态分支：

```
busy ──(调用超时/取消且函数体尚未返回)──> quarantined ──(僵尸真正结束)──> stopped
                                              │
                                  (Shutdown 期间保持隔离，结束后只销毁不自愈)
```

- **冷启动**：无空闲实例且未达 `MaxInstances` 时创建新实例，状态为 `cold-starting`，
  预热完成后转为 `idle`。
- **复用**：`idle` 实例可被后续调用复用；每次执行独占实例（`busy`），
  执行结束回到 `idle`。
- **回收**：`idle` 时长超过 `IdleTimeout` 的实例被回收为 `stopped`，
  但实例数不会低于 `MinInstances`。
- **保护**：正在执行（`busy`）的实例不会被回收；已回收（`stopped`）的实例
  不再接受任何请求；`Shutdown` 时执行中的实例标记为 `draining`，
  调用自然结束后销毁，不打断在途调用。
- **隔离**：调用被判超时/终止但执行未结束时，实例转为 `quarantined`，永不再复用，
  僵尸结束后销毁并由自愈逻辑补位（详见上一节）。
- **状态隔离**：每个实例持有独立的 `Invocation.State`（实例创建时分配），
  同一实例的多次调用可累积状态，不同实例之间不共享，实例回收/隔离销毁后状态随之消失。

## 伸缩与背压规则

- **扩容**：有请求待派发且无空闲实例时，若实例数 < `MaxInstances` 则冷启动新实例；
  突发流量下实例数**绝不突破** `MaxInstances`。
- **缩容**：后台回收器周期性扫描，回收空闲超时的实例，实例数不跌破 `MinInstances`；
  缩容只针对 `idle` 实例，不打断执行中的调用。
- **背压**：并发占满且队列达到 `MaxQueue` 时，新请求立即以 `ErrQueueFull` 拒绝，
  拒绝计数体现在 `Stats` 的 `Rejected` 字段。
- 配置兜底：`MaxInstances` 至少为 `MaxConcurrency`（保证并发上限可达），
  `MinInstances` 不超过 `MaxInstances`，未显式设置的字段取默认值
  （并发 4、超时 5s、队列 64、实例上限 16、空闲回收 30s）。

## 可观测性

`rt.Stats(name)` 返回函数实例池快照：

```go
type PoolStats struct {
    Instances      int   // 当前实例数（含执行中、隔离中与回收中）
    InFlight       int   // 在途执行数
    Queued         int   // 积压队列长度
    ColdStarts     int64 // 累计冷启动次数
    Executed       int64 // 累计正常执行完成次数
    Timeouts       int64 // 累计超时次数
    Rejected       int64 // 累计背压拒绝次数
    Reclaimed      int64 // 累计回收实例数
    Quarantined    int64 // 累计被隔离的实例数（超时/终止时执行仍未结束）
    Replaced       int64 // 累计为自愈隔离槽位而新建的替换实例数
    QuarantinedNow int   // 当前仍处于隔离中的实例数（僵尸执行尚未结束）
}
```

通过 `Timeouts` / `Quarantined` / `Replaced` / `QuarantinedNow` 可以一眼分清
“正常完成（`Executed`）”与“异常隔离自愈”：`QuarantinedNow > 0` 说明此刻还有
卡死执行在后台占位；`Quarantined == Replaced` 且 `QuarantinedNow == 0` 说明
已发生的隔离均已自愈完毕。

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
| `runtime/isolation_test.go` | 不理会取消时的隔离（零并发复用/无状态串扰）、持续流量下自愈且不破上限、主动取消可区分、函数间互不干扰、与关闭共存 |

## 本地复现失控执行与隔离自愈

下面的函数完全不理会取消：超时后运行时仍能保证其实例不会被复用。

```go
rt := runtime.New()
defer rt.Shutdown(context.Background())

rt.Register("rogue", runtime.FunctionConfig{
    MaxConcurrency: 1,
    MaxInstances:   2,
    Timeout:        80 * time.Millisecond,
}, func(ctx context.Context, inv *runtime.Invocation) (any, error) {
    inv.State["last"] = inv.InstanceID // 留下“残留状态”
    <-time.After(10 * time.Second)     // 故意不理会 ctx.Done()
    return inv.InstanceID, nil
})

// 第 1 次：80ms 后返回 ErrTimeout，inst-1 进入隔离，僵尸在后台继续。
rt.Invoke(ctx, "rogue", nil)
// 第 2 次：落在全新的 inst-2 上，绝不会复用 inst-1，也看不到 inst-1 的残留状态。
rt.Invoke(ctx, "rogue", nil)

st, _ := rt.Stats("rogue")
// st.Quarantined == 2，st.QuarantinedNow == 2，st.Instances == 2（未破上限）
// 僵尸最终结束后：st.QuarantinedNow == 0，st.Replaced == 2（自动补回健康实例）
```

复现与验证命令：

```bash
# 只跑隔离/自愈加测，观察每个用例的断言过程
go test ./... -run TestQuarantine -count=1 -v

# 全量回归（含原有并发/超时/复用回收/伸缩/背压用例）+ 竞态检测，重复多轮
go test ./... -race -count=5
```
