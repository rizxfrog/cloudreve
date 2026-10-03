package inventory

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/enttest"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/boolset"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
)

// taskHarness wires a task client to a real (in-memory) database.
type taskHarness struct {
	client *ent.Client
	tasks  TaskClient
}

func newTaskHarness(t *testing.T) *taskHarness {
	t.Helper()

	client := enttest.Open(t, "sqlite3", filepath.Join(t.TempDir(), "tasks.db"))
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	// Claiming a task with no owner substitutes the anonymous user, whose group has a
	// fixed id. The real migration creates the admin, user and anonymous groups in
	// that order, so reproduce it to get the same ids.
	ctx := context.Background()
	for _, name := range []string{"Admin", "User", "Anonymous"} {
		_, err := client.Group.Create().
			SetName(name).
			SetPermissions(&boolset.BooleanSet{}).
			Save(ctx)
		require.NoError(t, err)
	}

	return &taskHarness{
		client: client,
		tasks:  NewTaskClient(client, "sqlite3", nil),
	}
}

// seed inserts a task in the given status and returns its id.
func (h *taskHarness) seed(t *testing.T, status task.Status) int {
	t.Helper()

	ctx := context.Background()
	created, err := h.client.Task.Create().
		SetType("fake").
		SetStatus(status).
		SetPublicState(&types.TaskPublicState{}).
		SetPrivateState(`{"phase":"one"}`).
		SetCorrelationID(uuid.Must(uuid.NewV4())).
		Save(ctx)
	require.NoError(t, err)

	return created.ID
}

func (h *taskHarness) statusOf(t *testing.T, id int) task.Status {
	t.Helper()

	row, err := h.client.Task.Get(context.Background(), id)
	require.NoError(t, err)
	return row.Status
}

// TestClaimPendingTaskWinsExactlyOnce is the core guarantee the distributed queue
// rests on: only one caller may take a pending task.
func TestClaimPendingTaskWinsExactlyOnce(t *testing.T) {
	ctx := context.Background()
	h := newTaskHarness(t)
	id := h.seed(t, task.StatusQueued)

	model, claimed, err := h.tasks.ClaimPendingTask(ctx, id, false)
	require.NoError(t, err)
	require.True(t, claimed)
	require.NotNil(t, model)

	// The caller must see the status the task had before the claim, so the state
	// machine takes the pending -> processing transition.
	require.Equal(t, task.StatusQueued, model.Status)
	require.Equal(t, task.StatusProcessing, h.statusOf(t, id))

	// The private state must come along, or a resumed task could not continue.
	require.Equal(t, `{"phase":"one"}`, model.PrivateState)

	// A second caller must lose.
	_, claimed, err = h.tasks.ClaimPendingTask(ctx, id, false)
	require.NoError(t, err)
	require.False(t, claimed, "a claimed task must not be claimable again")
}

// TestClaimPendingTaskRecoversSuspendedTask verifies a task that was suspended is
// claimable and reports its suspension, which is what drives the queue's suspending
// counter down again.
func TestClaimPendingTaskRecoversSuspendedTask(t *testing.T) {
	ctx := context.Background()
	h := newTaskHarness(t)
	id := h.seed(t, task.StatusSuspending)

	model, claimed, err := h.tasks.ClaimPendingTask(ctx, id, false)
	require.NoError(t, err)
	require.True(t, claimed)
	require.Equal(t, task.StatusSuspending, model.Status)
	require.Equal(t, task.StatusProcessing, h.statusOf(t, id))
}

// TestClaimPendingTaskProcessingRequiresEvidence verifies the boundary that keeps a
// duplicate advertisement from starting a second copy of a running task: a task
// already processing is only claimable when the caller has evidence it was abandoned.
func TestClaimPendingTaskProcessingRequiresEvidence(t *testing.T) {
	ctx := context.Background()
	h := newTaskHarness(t)
	id := h.seed(t, task.StatusProcessing)

	_, claimed, err := h.tasks.ClaimPendingTask(ctx, id, false)
	require.NoError(t, err)
	require.False(t, claimed, "an ordinary claim must not take over a running task")

	model, claimed, err := h.tasks.ClaimPendingTask(ctx, id, true)
	require.NoError(t, err)
	require.True(t, claimed, "a redelivery must recover a stranded running task")
	require.Equal(t, task.StatusProcessing, model.Status)
	require.Equal(t, task.StatusProcessing, h.statusOf(t, id))
}

// TestClaimPendingTaskRejectsFinishedTasks verifies terminal tasks are never
// resurrected, so a stale stream entry cannot re-run completed work.
func TestClaimPendingTaskRejectsFinishedTasks(t *testing.T) {
	ctx := context.Background()

	for _, status := range []task.Status{
		task.StatusCompleted,
		task.StatusError,
		task.StatusCanceled,
	} {
		t.Run(string(status), func(t *testing.T) {
			h := newTaskHarness(t)
			id := h.seed(t, status)

			for _, allowProcessing := range []bool{false, true} {
				_, claimed, err := h.tasks.ClaimPendingTask(ctx, id, allowProcessing)
				require.NoError(t, err)
				require.False(t, claimed, "a finished task must never be claimed")
			}

			require.Equal(t, status, h.statusOf(t, id))
		})
	}
}

// TestClaimPendingTaskMissingTask verifies a deleted task is reported as not claimed
// rather than as an error, so a stale stream entry is discarded quietly.
func TestClaimPendingTaskMissingTask(t *testing.T) {
	ctx := context.Background()
	h := newTaskHarness(t)

	_, claimed, err := h.tasks.ClaimPendingTask(ctx, 9999, true)
	require.NoError(t, err)
	require.False(t, claimed)
}

// TestClaimPendingTaskLoadsOwnerForSystemTasks verifies the claim substitutes an
// owner for a task with no user edge. Persisting a state transition reads the owner,
// so without this a cron-spawned task would panic the worker that claimed it.
func TestClaimPendingTaskLoadsOwnerForSystemTasks(t *testing.T) {
	ctx := context.Background()
	h := newTaskHarness(t)
	id := h.seed(t, task.StatusQueued)

	model, claimed, err := h.tasks.ClaimPendingTask(ctx, id, false)
	require.NoError(t, err)
	require.True(t, claimed)

	require.NotNil(t, model.Edges.User, "a claimed task must carry an owner")
	require.NotNil(t, model.Edges.User.Edges.Group, "the owner's group must be loaded too")
}

// TestClaimPendingTaskLoadsOwningUser verifies a user-owned task is claimed with its
// real owner, not a substitute.
func TestClaimPendingTaskLoadsOwningUser(t *testing.T) {
	ctx := context.Background()
	h := newTaskHarness(t)

	group, err := h.client.Group.Create().SetName("members").SetPermissions(&boolset.BooleanSet{}).Save(ctx)
	require.NoError(t, err)
	user, err := h.client.User.Create().
		SetEmail("owner@example.com").
		SetNick("owner").
		SetGroupID(group.ID).
		Save(ctx)
	require.NoError(t, err)

	created, err := h.client.Task.Create().
		SetType("fake").
		SetStatus(task.StatusQueued).
		SetPublicState(&types.TaskPublicState{}).
		SetCorrelationID(uuid.Must(uuid.NewV4())).
		SetUserID(user.ID).
		Save(ctx)
	require.NoError(t, err)

	model, claimed, err := h.tasks.ClaimPendingTask(ctx, created.ID, false)
	require.NoError(t, err)
	require.True(t, claimed)

	require.NotNil(t, model.Edges.User)
	require.Equal(t, user.ID, model.Edges.User.ID)
	require.Equal(t, "owner@example.com", model.Edges.User.Email)
}
