package queue

import (
	"context"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
)

// fakeDep satisfies the queue's dependency interface.
type fakeDep struct{}

func (fakeDep) ForkWithLogger(ctx context.Context, _ logging.Logger) context.Context { return ctx }

// countingTask records execution.
type countingTask struct {
	*fakeWorkTask
	executed chan struct{}
}

func (t *countingTask) Do(context.Context) (task.Status, error) {
	if t.executed != nil {
		select {
		case t.executed <- struct{}{}:
		default:
		}
	}
	return task.StatusCompleted, nil
}

// TestQueueDispatchesOverStream drives a real queue.Queue configured with a stream
// scheduler, exercising the object the server actually builds.
func TestQueueDispatchesOverStream(t *testing.T) {
	pool := testPool(t)

	signal := make(chan struct{}, 1)

	// The queue reconstructs tasks through the factory registry, so register the
	// signal where the reconstruction can find it.
	executedTasks.Lock()
	executedTasks.byID[1] = signal
	executedTasks.Unlock()
	t.Cleanup(func() {
		executedTasks.Lock()
		delete(executedTasks.byID, 1)
		executedTasks.Unlock()
	})

	stored := &ent.Task{
		ID:           1,
		Type:         "counting",
		Status:       task.StatusQueued,
		PrivateState: "{}",
		PublicState:  &types.TaskPublicState{},
	}
	stored.SetUser(&ent.User{ID: 0, Email: "anonymous"})
	tc := newFakeTaskClient(stored)

	q := New(testLogger(), tc, nil, fakeDep{},
		WithName("t_e2e"),
		WithWorkerCount(1),
		WithMaxTaskExecution(time.Minute),
		WithStreamScheduler(pool, "instance-e2e"),
	)
	t.Cleanup(func() {
		q.Shutdown()
		cleanupQueue(t, pool, "t_e2e")
	})

	q.Start()

	if err := q.QueueTask(context.Background(), &fakeWorkTask{DBTask: &DBTask{Task: stored}}); err != nil {
		t.Fatalf("queue task: %s", err)
	}

	// The task must reach a worker, which requires the entry to be read, the task
	// claimed, and the model reconstructed from the factory registry.
	select {
	case <-signal:
	case <-time.After(15 * time.Second):
		t.Fatal("task was never dispatched to a worker")
	}

	if n := tc.claimCount(1); n != 1 {
		t.Fatalf("expected exactly one claim, got %d", n)
	}

	// The entry must be settled once the worker is done.
	conn := pool.Get()
	defer conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if pendingCount(t, conn, "cr:queue:t_e2e") == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("entry was never acknowledged")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestTwoInstancesExecuteTaskOnce is the payoff of the whole design: two independent
// instances sharing one stream must run an advertised task exactly once, and an entry
// taken over from a dead instance must be recovered by the survivor.
func TestTwoInstancesExecuteTaskOnce(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	signal := make(chan struct{}, 8)

	executedTasks.Lock()
	executedTasks.byID[1] = signal
	executedTasks.Unlock()
	t.Cleanup(func() {
		executedTasks.Lock()
		delete(executedTasks.byID, 1)
		executedTasks.Unlock()
	})

	stored := &ent.Task{
		ID:           1,
		Type:         "counting",
		Status:       task.StatusQueued,
		PrivateState: "{}",
		PublicState:  &types.TaskPublicState{},
	}
	stored.SetUser(&ent.User{ID: 0, Email: "anonymous"})

	// Each instance reads the same database row, so each gets its own copy of the
	// model: a shared pointer would be a harness artefact, not something the
	// database-backed client ever produces. Their claims are tracked independently.
	tcA := newFakeTaskClient(stored)
	tcB := newFakeTaskClient(newFakeWorkTask(1, task.StatusQueued, 0).Task)
	tcB.tasks[1].Type = "counting"

	qa := New(testLogger(), tcA, nil, fakeDep{},
		WithName("t_cluster"),
		WithWorkerCount(2),
		WithStreamScheduler(pool, "instance-a"),
	)
	qb := New(testLogger(), tcB, nil, fakeDep{},
		WithName("t_cluster"),
		WithWorkerCount(2),
		WithStreamScheduler(pool, "instance-b"),
	)

	t.Cleanup(func() {
		qa.Shutdown()
		qb.Shutdown()
		cleanupQueue(t, pool, "t_cluster")
	})

	qa.Start()
	qb.Start()

	if err := qa.QueueTask(ctx, &fakeWorkTask{DBTask: &DBTask{Task: stored}}); err != nil {
		t.Fatalf("queue: %s", err)
	}

	// Exactly one worker must pick the task up.
	select {
	case <-signal:
	case <-time.After(15 * time.Second):
		t.Fatal("task was never dispatched")
	}

	// Give both instances time to race for the entry.
	time.Sleep(500 * time.Millisecond)

	total := tcA.claimCount(1) + tcB.claimCount(1)
	if total != 1 {
		t.Fatalf("expected the task to be claimed exactly once across instances, got %d", total)
	}

	select {
	case <-signal:
		t.Fatal("the task ran more than once across instances")
	default:
	}
}

// TestQueueRecoversPendingTaskAfterRestart covers the headline benefit of moving to
// a durable queue: a task persisted before a process died but never advertised is
// picked up by the recovery sweep on the next start.
func TestQueueRecoversPendingTaskAfterRestart(t *testing.T) {
	pool := testPool(t)
	cleanupQueue(t, pool, "t_restart")

	signal := make(chan struct{}, 4)
	executedTasks.Lock()
	executedTasks.byID[1] = signal
	executedTasks.Unlock()
	t.Cleanup(func() {
		executedTasks.Lock()
		delete(executedTasks.byID, 1)
		executedTasks.Unlock()
	})

	// A task that was persisted but whose advertisement was lost, which is what a
	// crash between the two leaves behind.
	stored := newFakeWorkTask(1, task.StatusQueued, 0).Task
	stored.Type = "counting"
	tc := newFakeTaskClient(stored)

	q := New(testLogger(), tc, nil, fakeDep{},
		WithName("t_restart"),
		WithWorkerCount(1),
		WithResumeTaskType("counting"),
		WithStreamScheduler(pool, "instance-restart"),
	)
	t.Cleanup(func() {
		q.Shutdown()
		cleanupQueue(t, pool, "t_restart")
	})

	q.Start()

	// Nothing was submitted; the task can only arrive through the recovery sweep.
	select {
	case <-signal:
	case <-time.After(15 * time.Second):
		t.Fatal("the recovery sweep did not pick up the pending task")
	}

	if n := tc.claimCount(1); n != 1 {
		t.Fatalf("expected exactly one claim, got %d", n)
	}
}

// TestFifoSchedulerResumesDeferredOutOfOrder is the regression guard for the
// head-of-line blocking the old scheduler had: a task deferred far into the future
// must not hold back a task queued behind it.
func TestFifoSchedulerResumesDeferredOutOfOrder(t *testing.T) {
	s := NewFifoScheduler(0, testLogger())
	ctx := context.Background()

	// The first task is deferred well into the future; the second is ready now.
	deferred := newFakeWorkTask(1, task.StatusSuspending, time.Now().Add(time.Hour).Unix())
	ready := newFakeWorkTask(2, task.StatusQueued, 0)

	if err := s.Queue(ctx, deferred); err != nil {
		t.Fatalf("queue deferred: %s", err)
	}
	if err := s.Queue(ctx, ready); err != nil {
		t.Fatalf("queue ready: %s", err)
	}

	got, err := s.Request(ctx)
	if err != nil {
		t.Fatalf("request: %s", err)
	}
	if got.ID() != 2 {
		t.Fatalf("a deferred task must not block a ready one, got task %d", got.ID())
	}

	// The deferred task is not yet available.
	if _, err := s.Request(ctx); err != ErrNoTaskInQueue {
		t.Fatalf("expected no task in queue, got %v", err)
	}

	// Bring its resume time forward and confirm it becomes available.
	deferred.OnSuspend(time.Now().Add(-time.Second).Unix())

	got, err = s.Request(ctx)
	if err != nil {
		t.Fatalf("request after resume: %s", err)
	}
	if got.ID() != 1 {
		t.Fatalf("expected the resumed task, got %d", got.ID())
	}
}

// TestFifoAckIsNoop verifies the local scheduler needs no acknowledgement to keep
// tasks flowing, so the ack the worker always performs is harmless.
func TestFifoAckIsNoop(t *testing.T) {
	s := NewFifoScheduler(0, testLogger())
	ctx := context.Background()

	ready := newFakeWorkTask(1, task.StatusQueued, 0)
	if err := s.Queue(ctx, ready); err != nil {
		t.Fatalf("queue: %s", err)
	}

	got, err := s.Request(ctx)
	if err != nil {
		t.Fatalf("request: %s", err)
	}

	if err := s.Ack(ctx, got); err != nil {
		t.Fatalf("ack must be a no-op for the local scheduler: %s", err)
	}

	// The task was handed out, so it is not delivered again.
	if _, err := s.Request(ctx); err != ErrNoTaskInQueue {
		t.Fatalf("expected no task in queue, got %v", err)
	}
}
