package modelscope

import (
	"context"
	"sync"
	"time"

	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
)

const (
	// DefaultBatchWindowMinSeconds and DefaultBatchWindowMaxSeconds are the
	// randomized merge window, in seconds, applied when windowed batch commits
	// are enabled without an explicit range.
	DefaultBatchWindowMinSeconds = 3
	DefaultBatchWindowMaxSeconds = 5

	// maxBatchActionsPerCommit bounds how many actions one merged commit
	// carries. It matches the largest batch the individual path already
	// produces (the recycle sweep commits 100 objects at once), so a merge never
	// composes a request with more actions than one that is known to work.
	maxBatchActionsPerCommit = 100

	// maxBatchBytesPerCommit bounds the payload of one merged commit. The action
	// count alone is not a bound on the request: a merged commit can carry inline
	// objects, and 100 inline objects would build a body of hundreds of
	// megabytes in memory. A single inline object is at most inlineLimit raw
	// (~6.7 MiB once base64 encoded), so this allows several of them per commit
	// while keeping the built body within the same order of magnitude as one
	// upload already holds.
	maxBatchBytesPerCommit = 32 << 20

	// mergedCommitTimeout bounds a merged commit. Several uploads wait on it, so
	// it cannot inherit any single caller's deadline; it is generous enough for
	// a full batch of inline objects.
	mergedCommitTimeout = 10 * time.Minute
)

// commitBatcher merges the commits of one repository into periodic combined
// commits.
//
// When a policy selects windowed commits, a commit is not sent on its own. The
// first one opens a random window, every commit arriving during that window is
// collected, and when the window elapses the collected actions are written as
// one repository commit. A burst of uploads therefore costs a fraction of the
// requests that individual commits would need.
//
// Unlike the queued mode, merging hides the commit from its caller until the
// window closes. The caller's success still means its action is part of a
// commit that was actually written, so the durability contract is unchanged;
// only the latency is.
type commitBatcher struct {
	// ops carries submitted actions to the single merge loop. It is unbuffered,
	// so a submitter blocks until the loop has taken its action: that is what
	// keeps a burst from accumulating without bound.
	ops chan batchOp

	// mu guards write and the merge window. Both are refreshed on every driver
	// construction so a policy edit (rotated token, changed revision, resized
	// window) takes effect even though the merge loop outlives any single
	// client.
	mu        sync.Mutex
	write     func(context.Context, []map[string]any) error
	minWindow time.Duration
	maxWindow time.Duration

	// The fields below are owned by the merge loop and are never touched by a
	// submitter, which is why the pending set needs no lock of its own.
	pending  []map[string]any
	pendingB int
	seen     map[string]struct{}
}

type batchOp struct {
	actions []map[string]any
	result  chan error
}

// newCommitBatcher starts a merge loop. write is the current commit writer and
// the window is the current merge window; both may be replaced later.
func newCommitBatcher(minWindow, maxWindow time.Duration, write func(context.Context, []map[string]any) error, l logging.Logger) *commitBatcher {
	b := &commitBatcher{
		ops:       make(chan batchOp),
		write:     write,
		minWindow: minWindow,
		maxWindow: maxWindow,
		seen:      make(map[string]struct{}),
	}

	go b.loop(l)
	return b
}

// setWriter replaces the commit writer, so a rebuilt client is the one that
// performs subsequent merged commits.
func (b *commitBatcher) setWriter(write func(context.Context, []map[string]any) error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.write = write
}

// setWindow replaces the merge window. The loop reads it when it opens a
// window, so a resized policy takes effect on the next window rather than
// requiring a process restart. The window already open keeps the size it was
// drawn with, which is what its waiting callers were told to expect.
func (b *commitBatcher) setWindow(minWindow, maxWindow time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.minWindow = minWindow
	b.maxWindow = maxWindow
}

func (b *commitBatcher) currentWriter() func(context.Context, []map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.write
}

// currentWindow returns the merge window to draw the next commit window from.
func (b *commitBatcher) currentWindow() (time.Duration, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.minWindow, b.maxWindow
}

// submit hands actions to the merge loop and waits for their outcome. The error
// reflects the merged commit the actions landed in, so a caller never proceeds
// on a commit that has not been written.
func (b *commitBatcher) submit(ctx context.Context, actions []map[string]any) error {
	result := make(chan error, 1)

	select {
	case b.ops <- batchOp{actions: actions, result: result}:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		// The loop still owns the action and reports into the buffered channel;
		// the caller simply stops waiting. Dropping the action would be wrong,
		// because other callers may be merged into the same commit.
		return ctx.Err()
	}
}

// loop is the single owner of the pending set. It collects actions until the
// current window elapses or the batch is full, then writes one commit.
//
// The window is read from the batcher each time a new window opens, so a policy
// edit that resizes it takes effect without a restart.
func (b *commitBatcher) loop(l logging.Logger) {
	var (
		timer   *time.Timer
		timeout <-chan time.Time
		waiters []chan error
	)

	flush := func() {
		if len(b.pending) == 0 {
			return
		}

		// Detached from any single caller: several uploads wait on this commit,
		// so one of them being cancelled must not cancel the write the others
		// still need. The batch is copied out first, because the loop keeps
		// running while the write is in flight and will start a fresh batch.
		actions := b.pending
		b.pending = nil
		b.pendingB = 0
		clear(b.seen)

		ctx, cancel := context.WithTimeout(context.Background(), mergedCommitTimeout)
		err := b.currentWriter()(ctx, actions)
		cancel()

		for _, w := range waiters {
			w <- err
		}
		waiters = nil

		if err != nil {
			l.Error("ModelScope merged commit of %d action(s) failed: %s", len(actions), err)
		} else {
			l.Debug("ModelScope merged commit wrote %d action(s)", len(actions))
		}
	}

	for {
		select {
		case op := <-b.ops:
			if timeout == nil {
				// This action opens a new window. Its duration is drawn once and
				// applies to the whole batch, so every caller merged into it
				// waits the same bounded time. The range is read per window, so
				// a resized policy applies from the next one on.
				minWindow, maxWindow := b.currentWindow()
				timer = time.NewTimer(randomInterval(minWindow, maxWindow))
				timeout = timer.C
			}

			fresh := actionsNotIn(b.seen, op.actions)
			b.pending = append(b.pending, fresh...)
			for _, action := range fresh {
				b.pendingB += actionBytes(action)
			}
			waiters = append(waiters, op.result)

			if len(b.pending) >= maxBatchActionsPerCommit || b.pendingB >= maxBatchBytesPerCommit {
				timer.Stop()
				flush()
				timer = nil
				timeout = nil
			}

		case <-timeout:
			flush()
			timer = nil
			timeout = nil
		}
	}
}

// actionsNotIn returns the actions whose object path is not already part of the
// batch, so an object committed twice inside one window is written once.
func actionsNotIn(seen map[string]struct{}, actions []map[string]any) []map[string]any {
	fresh := make([]map[string]any, 0, len(actions))
	for _, action := range actions {
		if path, ok := action["path"].(string); ok && path != "" {
			if _, duplicate := seen[path]; duplicate {
				continue
			}
			seen[path] = struct{}{}
		}

		fresh = append(fresh, action)
	}

	return fresh
}

// actionBytes estimates the request payload an action contributes. Only the
// inline content matters: a blob action carries no body, and the fixed fields
// are a few dozen bytes. The estimate is what the batch keeps under budget.
func actionBytes(action map[string]any) int {
	if content, ok := action["content"].(string); ok {
		return len(content)
	}

	return 0
}

// normalizeBatchWindow turns the configured second values into the duration
// range the batcher enforces, applying the same "zero means default" and
// reversed-range repair as the commit interval.
func normalizeBatchWindow(minWindow, maxWindow time.Duration, l logging.Logger) (time.Duration, time.Duration) {
	if minWindow <= 0 {
		minWindow = DefaultBatchWindowMinSeconds * time.Second
	}

	if maxWindow <= 0 {
		maxWindow = DefaultBatchWindowMaxSeconds * time.Second
	}

	if maxWindow < minWindow {
		l.Warning("ModelScope batch window max %s is below min %s, using the min for both", maxWindow, minWindow)
		maxWindow = minWindow
	}

	return minWindow, maxWindow
}

// batchBatcherRegistry holds one merge loop per repository.
//
// Sharing is what makes merging possible at all: the concurrent commits that
// should be combined come from concurrent requests, each with its own driver,
// so a batcher owned by a driver would merge nothing and only add the window
// delay.
type batchBatcherRegistry struct {
	mu       sync.Mutex
	batchers map[string]*commitBatcher
}

var sharedCommitBatchers = &batchBatcherRegistry{batchers: make(map[string]*commitBatcher)}

// forRepository returns the merge loop of key, creating and starting it with
// the given writer and window on first use. An existing loop adopts both, so a
// policy edit takes effect without a restart while the loop keeps its pending
// batch and its open window.
func (r *batchBatcherRegistry) forRepository(key string, minWindow, maxWindow time.Duration,
	write func(context.Context, []map[string]any) error, l logging.Logger) *commitBatcher {
	r.mu.Lock()
	defer r.mu.Unlock()

	batcher, ok := r.batchers[key]
	if !ok {
		batcher = newCommitBatcher(minWindow, maxWindow, write, l)
		r.batchers[key] = batcher
		return batcher
	}

	batcher.setWriter(write)
	batcher.setWindow(minWindow, maxWindow)
	return batcher
}
