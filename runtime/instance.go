package runtime

import (
	"sync/atomic"
	"time"
)

// instanceState 实例生命周期状态。
type instanceState int32

const (
	stateColdStarting instanceState = iota // 冷启动中
	stateIdle                              // 空闲，可接受请求
	stateBusy                              // 执行中，不可接受新请求
	stateDraining                          // 回收中，等待当前调用结束后销毁
	stateQuarantined                       // 隔离中：调用已超时/取消但执行未结束，实例永不再复用
	stateStopped                           // 已回收，不再接受任何请求
)

func (s instanceState) String() string {
	switch s {
	case stateColdStarting:
		return "cold-starting"
	case stateIdle:
		return "idle"
	case stateBusy:
		return "busy"
	case stateDraining:
		return "draining"
	case stateQuarantined:
		return "quarantined"
	case stateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// instance 是一个函数执行实例。
// store 是实例私有可变状态，仅在创建时分配一次，
// 不同实例之间不共享，杜绝跨实例污染。
type instance struct {
	id    string
	state atomic.Int32
	store map[string]any

	lastUsed atomic.Int64 // 最近一次转为空闲的 unix 纳秒时间
}

func newInstance(id string) *instance {
	inst := &instance{
		id:    id,
		store: make(map[string]any),
	}
	inst.state.Store(int32(stateColdStarting))
	return inst
}

// tryAcquire 仅当实例空闲时将其置为 busy，成功返回 true。
// 执行中、回收中或已回收的实例一律返回 false，不会接受新请求。
func (inst *instance) tryAcquire() bool {
	return inst.state.CompareAndSwap(int32(stateIdle), int32(stateBusy))
}

// release 在执行结束后调用：若处于 draining 则转为 stopped，
// 否则回到 idle 并刷新空闲时间戳。
func (inst *instance) release() {
	for {
		s := instanceState(inst.state.Load())
		switch s {
		case stateBusy:
			inst.lastUsed.Store(time.Now().UnixNano())
			if inst.state.CompareAndSwap(int32(stateBusy), int32(stateIdle)) {
				return
			}
		case stateDraining:
			if inst.state.CompareAndSwap(int32(stateDraining), int32(stateStopped)) {
				return
			}
		default:
			return
		}
	}
}

// warm 冷启动完成，转为空闲。
func (inst *instance) warm() {
	inst.lastUsed.Store(time.Now().UnixNano())
	inst.state.CompareAndSwap(int32(stateColdStarting), int32(stateIdle))
}

// tryReclaim 仅当实例空闲且空闲时长超过 idleTimeout 时将其回收。
// 正在执行的实例（busy）不会被回收，缩容不会打断在途调用。
func (inst *instance) tryReclaim(idleTimeout time.Duration) bool {
	if instanceState(inst.state.Load()) != stateIdle {
		return false
	}
	idleFor := time.Since(time.Unix(0, inst.lastUsed.Load()))
	if idleFor < idleTimeout {
		return false
	}
	return inst.state.CompareAndSwap(int32(stateIdle), int32(stateStopped))
}

// markDraining 把 busy 实例标记为回收中，调用结束后由 release 完成销毁。
func (inst *instance) markDraining() {
	inst.state.CompareAndSwap(int32(stateBusy), int32(stateDraining))
}

// markQuarantined 把 busy 实例标记为隔离：调用已被判定超时或终止，
// 但函数体可能仍在后台执行。隔离实例永远不会回到 idle，
// 因此同一实例不可能同时跑两次调用，也不会有残留状态污染后续请求。
// 若实例已进入 draining（运行时关闭流程），则保持 draining，不覆盖关闭语义。
func (inst *instance) markQuarantined() bool {
	return inst.state.CompareAndSwap(int32(stateBusy), int32(stateQuarantined))
}

// quarantineToStopped 隔离实例对应的“僵尸执行”真正结束后，将其销毁。
// 只有处于 quarantined 状态时生效，返回是否转换成功。
func (inst *instance) quarantineToStopped() bool {
	return inst.state.CompareAndSwap(int32(stateQuarantined), int32(stateStopped))
}

func (inst *instance) isStopped() bool {
	return instanceState(inst.state.Load()) == stateStopped
}

func (inst *instance) isQuarantined() bool {
	return instanceState(inst.state.Load()) == stateQuarantined
}
