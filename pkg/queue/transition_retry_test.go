package queue

import (
	"context"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
)

func init() {
	RegisterResumableTaskFactory("suspend_once", func(model *ent.Task) Task {
		return &suspendingTask{fakeWorkTask: &fakeWorkTask{DBTask: &DBTask{Task: model}}}
	})
}

// TestQueueLeavesSuspendedTaskRunnableWhenPersistFails covers the failure that
// strands a task in "processing" forever.
//
// A bulk mail task suspends between batches and is expected back. The transition
// that records the suspension is the only thing that writes its cursor, and its
// error is discarded by the worker loop; the stream entry is acknowledged
// regardless. So when that write fails, the task is left with a resume time in a
// non-terminal status while nothing advertises it any more: the work completed
// but the row never settles, and the admin task list shows "processing" for good.
//
// It must stay runnable instead, so the next iteration can carry the suspension
// through.
func TestQueueLeavesSuspendedTaskRunnableWhenPersistFails(t *testing.T) {
	pool := testPool(t)

	stored := &ent.Task{
		ID:           21,
		Type:         "suspend_once",
		Status:       task.StatusQueued,
		PrivateState: "{}",
		PublicState:  &types.TaskPublicState{},
	}
	stored.SetUser(&ent.User{ID: 0, Email: "anonymous"})

	tc := newFakeTaskClient(stored)
	// The suspension is what the bulk mail task persists between batches; fail
	// that write a few times, as a briefly unhealthy database would.
	tc.UpdateFailTimes(task.StatusSuspending, 2)

	q := New(testLogger(), tc, nil, fakeDep{},
		WithName("t_persist_fail"),
		WithWorkerCount(1),
		WithMaxTaskExecution(time.Minute),
		WithStreamScheduler(pool, "instance-persist"),
	)
	t.Cleanup(func() {
		q.Shutdown()
		cleanupQueue(t, pool, "t_persist_fail")
	})

	q.Start()

	// Queue the task: the worker takes it and suspends.
	if err := q.QueueTask(context.Background(), &fakeWorkTask{DBTask: &DBTask{Task: stored}}); err != nil {
		t.Fatalf("queue task: %s", err)
	}

	// The task must survive the failed writes and then be recorded normally:
	// were it left stranded, nothing would ever advertise it again and the row
	// would say "processing" forever.
	deadline := time.Now().Add(60 * time.Second)
	var finalStatus task.Status
	for time.Now().Before(deadline) {
		tc.mu.Lock()
		finalStatus = tc.tasks[21].Status
		tc.mu.Unlock()

		if finalStatus == task.StatusSuspending {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if finalStatus != task.StatusSuspending {
		t.Fatalf("task 21 was not resumed after its suspension failed to persist "+
			"(final status=%q, claims=%d)", finalStatus, tc.claimCount(21))
	}
}
