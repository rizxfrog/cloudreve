package modelscope

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
)

// ModelScopeCommitTaskType names the queue task that carries repository
// commits. It labels the task in queue logs and metrics.
const ModelScopeCommitTaskType = "modelscope_commit"

// newQueuedCommitRunner builds the commit runner used when a policy enables
// queued commits. Every commit is submitted to q and awaited, so the caller
// observes the same synchronous success or failure it sees on the direct path,
// while the queue supplies the ordering and the pacing.
func newQueuedCommitRunner(c *Client, q queue.Queue, pacer *commitPacer, minInterval, maxInterval time.Duration) *queuedCommitRunner {
	return &queuedCommitRunner{
		client:      c,
		q:           q,
		pacer:       pacer,
		minInterval: minInterval,
		maxInterval: maxInterval,
	}
}

type queuedCommitRunner struct {
	client                   *Client
	q                        queue.Queue
	pacer                    *commitPacer
	minInterval, maxInterval time.Duration
}

// run submits the commit and blocks until the task reports its outcome.
func (r *queuedCommitRunner) run(ctx context.Context, actions []map[string]any) error {
	t := newCommitTask(ctx, r, actions)

	if err := r.q.QueueTask(ctx, t); err != nil {
		return fmt.Errorf("failed to queue ModelScope commit: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-t.result:
		return err
	}
}

// commitTask carries one repository commit through the queue.
//
// It is an InMemoryTask: a commit is a short, awaited unit of work tied to the
// upload that produced it, so persisting it would only add a database round
// trip and leave rows that must be swept. The queue is still the ordering
// authority, which is what the feature is for.
type commitTask struct {
	*queue.InMemoryTask

	runner  *queuedCommitRunner
	actions []map[string]any
	// callerCtx is the context of the upload that requested this commit. It
	// bounds the write so a commit for an abandoned upload is not performed:
	// without queuing the write would have been aborted by the same
	// cancellation, and the queue must not change that.
	callerCtx context.Context

	// result receives the commit outcome exactly once. It is buffered so the
	// worker never blocks when the caller has already given up.
	result chan error
}

func newCommitTask(ctx context.Context, runner *queuedCommitRunner, actions []map[string]any) *commitTask {
	return &commitTask{
		InMemoryTask: &queue.InMemoryTask{
			DBTask: &queue.DBTask{
				Task: &ent.Task{
					CorrelationID: logging.CorrelationID(ctx),
					PublicState:   &types.TaskPublicState{},
				},
			},
		},
		runner:    runner,
		actions:   actions,
		callerCtx: ctx,
		result:    make(chan error, 1),
	}
}

func (t *commitTask) Type() string {
	return ModelScopeCommitTaskType
}

// Do runs the commit through the repository pacer, so consecutive queued
// commits stay a randomized interval apart even if the queue runs more than one
// worker.
//
// The outcome is delivered to the awaiting caller and the task always reports
// completion: the retry policy belongs to the upload that is waiting, and a
// task-level retry would re-run a commit whose caller already saw the error.
func (t *commitTask) Do(queueCtx context.Context) (task.Status, error) {
	ctx, cancel := context.WithCancel(queueCtx)
	defer cancel()

	// Mirror the caller's cancellation onto this context. AfterFunc ties the
	// two without leaking a goroutine when the queue context ends first.
	stop := context.AfterFunc(t.callerCtx, cancel)
	defer stop()

	err := t.runner.pacer.run(ctx, t.runner.minInterval, t.runner.maxInterval, func() error {
		return t.runner.client.commit(ctx, t.actions)
	})

	t.result <- err
	return task.StatusCompleted, nil
}
