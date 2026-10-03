package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
)

var (
	// ErrQueueShutdown the queue is released and closed.
	ErrQueueShutdown = errors.New("queue has been closed and released")
	// ErrMaxCapacity Maximum size limit reached
	ErrMaxCapacity = errors.New("golang-queue: maximum size limit reached")
	// ErrNoTaskInQueue there is nothing in the queue
	ErrNoTaskInQueue = errors.New("golang-queue: no Task in queue")
)

// fifoCompactThreshold bounds how many consumed slots may accumulate at the head
// of the ready deque before the live region is compacted into the front.
const fifoCompactThreshold = 64

type (
	Scheduler interface {
		// Queue add a new Task into the queue. The context bounds any blocking
		// network operation performed by a remote queue backend.
		Queue(ctx context.Context, task Task) error
		// Request get a new Task from the queue. The returned Task must be
		// released with Ack once the worker is done with it, otherwise a remote
		// backend may redeliver it.
		Request(ctx context.Context) (Task, error)
		// Ack releases a Task previously returned by Request. It is called
		// exactly once per successful Request, including when the worker panics,
		// so backends may use it to settle their delivery bookkeeping.
		Ack(ctx context.Context, task Task) error
		// Shutdown stop all worker
		Shutdown() error
	}

	// fifoScheduler keeps tasks in-process. It backs queues whose tasks cannot be
	// serialised for a remote backend, either because they carry live references
	// (closures, result channels) or because they were handed to this process
	// directly by another node.
	fifoScheduler struct {
		sync.Mutex
		ready       []Task
		readyHead   int
		pending     []Task
		pendingHead int
		capacity    int
		logger      logging.Logger
		stopOnce    sync.Once
		stopFlag    int32
	}
)

// Queue appends a task, deferring it when its resume time lies in the future.
func (s *fifoScheduler) Queue(_ context.Context, task Task) error {
	if atomic.LoadInt32(&s.stopFlag) == 1 {
		return ErrQueueShutdown
	}

	s.Lock()
	defer s.Unlock()

	if s.capacity > 0 && s.lenLocked() >= s.capacity {
		return ErrMaxCapacity
	}

	if task.ResumeTime() > time.Now().Unix() {
		s.pending = append(s.pending, task)
		return nil
	}

	s.ready = append(s.ready, task)
	return nil
}

// Request pops the oldest ready task, promoting any deferred task whose resume
// time has arrived.
func (s *fifoScheduler) Request(_ context.Context) (Task, error) {
	if atomic.LoadInt32(&s.stopFlag) == 1 {
		return nil, ErrQueueShutdown
	}

	s.Lock()
	defer s.Unlock()

	s.promoteLocked(time.Now().Unix())

	if s.readyHead >= len(s.ready) {
		return nil, ErrNoTaskInQueue
	}

	task := s.ready[s.readyHead]
	s.ready[s.readyHead] = nil
	s.readyHead++
	s.compactLocked()

	return task, nil
}

// Ack is a no-op: fifoScheduler owns its tasks outright and never redelivers.
func (s *fifoScheduler) Ack(_ context.Context, _ Task) error {
	return nil
}

// Shutdown the worker
func (s *fifoScheduler) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&s.stopFlag, 0, 1) {
		return ErrQueueShutdown
	}

	return nil
}

// promoteLocked moves deferred tasks whose resume time has arrived into the ready
// deque. The whole deferred set is examined rather than just its head, so a task
// suspended far in the future cannot hide the tasks queued behind it.
func (s *fifoScheduler) promoteLocked(now int64) {
	if s.pendingHead >= len(s.pending) {
		s.pending = s.pending[:0]
		s.pendingHead = 0
		return
	}

	live := s.pending[s.pendingHead:]
	kept := live[:0]
	for _, task := range live {
		if task.ResumeTime() > now {
			kept = append(kept, task)
			continue
		}

		s.ready = append(s.ready, task)
	}

	for i := len(kept); i < len(live); i++ {
		live[i] = nil
	}

	s.pending = kept
	s.pendingHead = 0
}

// compactLocked reclaims consumed slots once they dominate the deque, keeping
// repeated pops from growing the backing array without bound.
func (s *fifoScheduler) compactLocked() {
	if s.readyHead == 0 {
		return
	}

	if s.readyHead >= len(s.ready) {
		s.ready = s.ready[:0]
		s.readyHead = 0
		return
	}

	if s.readyHead >= fifoCompactThreshold && s.readyHead*2 >= len(s.ready) {
		s.ready = append(s.ready[:0], s.ready[s.readyHead:]...)
		s.readyHead = 0
	}
}

func (s *fifoScheduler) lenLocked() int {
	return (len(s.ready) - s.readyHead) + (len(s.pending) - s.pendingHead)
}

// NewFifoScheduler for create new Scheduler instance
func NewFifoScheduler(queueSize int, logger logging.Logger) Scheduler {
	return &fifoScheduler{
		capacity: queueSize,
		logger:   logger,
	}
}
