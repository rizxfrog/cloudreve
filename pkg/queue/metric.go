package queue

import "sync/atomic"

// Metric interface
type Metric interface {
	IncBusyWorker()
	DecBusyWorker()
	BusyWorkers() uint64
	SuccessTasks() uint64
	FailureTasks() uint64
	SubmittedTasks() uint64
	IncSuccessTask()
	IncFailureTask()
	IncSubmittedTask()
	// IncSuspendingTask/DscSuspendingTask track tasks that are deferred to a
	// future resume time. The decrement is clamped at zero because a claimed
	// task may still be suspended when it is first handed to a worker, in which
	// case no matching increment was ever recorded by this process.
	IncSuspendingTask()
	DscSuspendingTask()
}

var _ Metric = (*metric)(nil)

type metric struct {
	busyWorkers     uint64
	successTasks    uint64
	failureTasks    uint64
	submittedTasks  uint64
	suspendingTasks uint64
}

// NewMetric for default metric structure
func NewMetric() Metric {
	return &metric{}
}

func (m *metric) IncBusyWorker() {
	atomic.AddUint64(&m.busyWorkers, 1)
}

func (m *metric) DecBusyWorker() {
	atomic.AddUint64(&m.busyWorkers, ^uint64(0))
}

func (m *metric) BusyWorkers() uint64 {
	return atomic.LoadUint64(&m.busyWorkers)
}

func (m *metric) IncSuccessTask() {
	atomic.AddUint64(&m.successTasks, 1)
}

func (m *metric) IncFailureTask() {
	atomic.AddUint64(&m.failureTasks, 1)
}

func (m *metric) IncSubmittedTask() {
	atomic.AddUint64(&m.submittedTasks, 1)
}

func (m *metric) SuccessTasks() uint64 {
	return atomic.LoadUint64(&m.successTasks)
}

func (m *metric) FailureTasks() uint64 {
	return atomic.LoadUint64(&m.failureTasks)
}

func (m *metric) SubmittedTasks() uint64 {
	return atomic.LoadUint64(&m.submittedTasks)
}

func (m *metric) SuspendingTasks() uint64 {
	return atomic.LoadUint64(&m.suspendingTasks)
}

func (m *metric) IncSuspendingTask() {
	atomic.AddUint64(&m.suspendingTasks, 1)
}

// DscSuspendingTask decrements the suspending counter without underflowing it.
// A remote queue backend can hand a still-suspended task straight to a worker
// (for example after a crash placed it back on the stream), so the suspended ->
// processing transition may run without a preceding local increment.
func (m *metric) DscSuspendingTask() {
	for {
		current := atomic.LoadUint64(&m.suspendingTasks)
		if current == 0 {
			return
		}

		if atomic.CompareAndSwapUint64(&m.suspendingTasks, current, current-1) {
			return
		}
	}
}
