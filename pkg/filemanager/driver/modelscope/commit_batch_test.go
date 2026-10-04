package modelscope

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/stretchr/testify/require"
)

// The tests here cover windowed batch commits: commits that arrive close
// together must leave as one repository commit, and every caller must observe
// the outcome of the commit its action landed in. Each test uses its own
// endpoint, because the batcher registry is keyed by repository and shared.
const (
	testWindow      = 250 * time.Millisecond
	testWindowSlack = 60 * time.Millisecond
)

// raiseWindow makes the merge window close promptly while still leaving room
// for the concurrent submitters to reach the loop.
func testWindowRange() (time.Duration, time.Duration) { return testWindow, testWindow }

// commitRecorder.actionCounts returns how many actions each observed commit
// carried, in arrival order.
func (r *commitRecorder) actionCounts() []int {
	r.mu.Lock()
	defer r.mu.Unlock()

	counts := make([]int, 0, len(r.body))
	for _, payload := range r.body {
		actions, _ := payload["actions"].([]any)
		counts = append(counts, len(actions))
	}
	return counts
}

// commitConcurrently submits n commits for n distinct objects from separate
// goroutines and returns their errors. The goroutines are released together so
// the commits reach the merge loop inside the same window. Distinct objects are
// used because a repeated object path is deduplicated by design.
func commitConcurrently(t *testing.T, client *Client, n int) []error {
	t.Helper()

	paths := make([]string, n)
	for i := range n {
		paths[i] = ObjectPath("00", shaOf(t, []byte(fmt.Sprintf("blob-%d", i))))
	}

	return commitPaths(t, client, paths)
}

// commitPaths submits one commit per given object path, released together.
func commitPaths(t *testing.T, client *Client, paths []string) []error {
	t.Helper()

	errs := make([]error, len(paths))
	start := make(chan struct{})
	var wg sync.WaitGroup

	for i, path := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = client.Commit(context.Background(), []map[string]any{
				blobAction(path, HashFromObjectPath(path), 1024),
			})
		}()
	}

	close(start)
	wg.Wait()
	return errs
}

// Commits arriving inside one window must be written as a single repository
// commit: that is the entire point of the mode, since request count is what
// ModelScope limits.
func TestBatchMergesCommitsInsideWindow(t *testing.T) {
	recorder := &commitRecorder{}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)
	client.SetCommitBatch(testWindowRange())

	errs := commitConcurrently(t, client, 4)

	for i, err := range errs {
		require.NoError(t, err, "commit %d failed", i)
	}

	starts, _, maxInFlight := recorder.spans()
	require.Len(t, starts, 1, "the window must produce exactly one upstream commit")
	require.Equal(t, []int{4}, recorder.actionCounts(), "the merged commit must carry every action")
	require.Equal(t, 1, maxInFlight, "merged commits must never overlap")
}

// Merging must not turn a failed commit into a success for the callers waiting
// on it: content that was never written cannot be reported as stored.
func TestBatchPropagatesFailureToEveryCaller(t *testing.T) {
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = roundTripper(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusForbidden, `{"Code":403,"Message":"no write permission to repository"}`), nil
	})
	client.SetCommitBatch(testWindowRange())

	errs := commitConcurrently(t, client, 3)

	for i, err := range errs {
		require.Error(t, err, "commit %d must report the failed merge", i)
		require.ErrorIs(t, err, ErrUpstream)
		require.Contains(t, err.Error(), "no write permission")
	}
}

// A caller waiting on a merged commit must not be released before the commit it
// was merged into has been written; otherwise success would no longer mean the
// content is stored.
func TestBatchDelaysCallerUntilWindowCloses(t *testing.T) {
	recorder := &commitRecorder{}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)

	window := 150 * time.Millisecond
	client.SetCommitBatch(window, window)

	started := time.Now()
	errs := commitConcurrently(t, client, 1)
	require.NoError(t, errs[0])

	require.GreaterOrEqual(t, time.Since(started), window-testWindowSlack,
		"the caller must wait for the merge window to close")
	require.Len(t, recorder.starts, 1)
}

// Once a window has closed, a later commit must open a new one rather than be
// appended to a batch that has already been written.
func TestBatchSeparatesCommitsAcrossWindows(t *testing.T) {
	recorder := &commitRecorder{}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)
	client.SetCommitBatch(60*time.Millisecond, 60*time.Millisecond)

	require.NoError(t, client.Commit(context.Background(),
		[]map[string]any{blobAction(ObjectPath("00", testHash), testHash, 1024)}))
	require.NoError(t, client.Commit(context.Background(),
		[]map[string]any{blobAction(ObjectPath("00", testHash), testHash, 1024)}))

	starts, _, _ := recorder.spans()
	require.Len(t, starts, 2, "each window must produce its own commit")
	require.Equal(t, []int{1, 1}, recorder.actionCounts())
}

// The same object committed twice inside one window must be referenced once:
// a merged commit carries create actions, and creating one path twice is
// exactly what ModelScope rejects.
func TestBatchDeduplicatesObjectPathsInsideWindow(t *testing.T) {
	recorder := &commitRecorder{}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)
	client.SetCommitBatch(testWindowRange())

	// Three commits for the same object, released together.
	path := ObjectPath("00", shaOf(t, []byte("duplicate")))
	errs := commitPaths(t, client, []string{path, path, path})
	for i, err := range errs {
		require.NoError(t, err, "commit %d failed", i)
	}

	require.Len(t, recorder.starts, 1)
	require.Equal(t, []int{1}, recorder.actionCounts(),
		"three commits for one object must collapse into one action")
}

// A batch is capped so a merge never composes a request larger than the ones
// the individual path already produces.
func TestBatchSplitsOversizedBatch(t *testing.T) {
	recorder := &commitRecorder{}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)

	// The window stays open long enough that only the cap can close a batch.
	client.SetCommitBatch(30*time.Second, 30*time.Second)

	total := maxBatchActionsPerCommit + 5
	paths := make([]string, total)
	for i := range total {
		paths[i] = ObjectPath("00", shaOf(t, []byte(fmt.Sprintf("sized-%d", i))))
	}
	errs := commitPaths(t, client, paths)
	for i, err := range errs {
		require.NoError(t, err, "commit %d failed", i)
	}

	counts := recorder.actionCounts()
	require.GreaterOrEqual(t, len(counts), 2, "an oversized batch must be split")

	sum := 0
	for _, c := range counts {
		require.LessOrEqual(t, c, maxBatchActionsPerCommit, "a merged commit exceeded the cap")
		sum += c
	}
	require.Equal(t, total, sum, "every action must be written exactly once")
}

// One caller giving up must not drop the actions it contributed, because the
// batch it opened may already carry writes other callers are waiting on.
func TestBatchSurvivesCancelledCaller(t *testing.T) {
	recorder := &commitRecorder{}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)
	client.SetCommitBatch(testWindowRange())

	ctx, cancel := context.WithCancel(context.Background())
	abandonedPath := ObjectPath("00", shaOf(t, []byte("abandoned")))
	survivorPath := ObjectPath("00", shaOf(t, []byte("survivor")))
	abandoned := make(chan error, 1)
	go func() {
		abandoned <- client.Commit(ctx, []map[string]any{blobAction(abandonedPath, testHash, 1)})
	}()

	// Give the abandoned commit time to enter the batch, then cancel it.
	time.Sleep(20 * time.Millisecond)
	cancel()
	require.ErrorIs(t, <-abandoned, context.Canceled)

	errs := commitPaths(t, client, []string{survivorPath})
	require.NoError(t, errs[0], "the surviving caller must still succeed")

	require.Len(t, recorder.starts, 1)
	require.Equal(t, []int{2}, recorder.actionCounts(),
		"the cancelled caller's action must still be written")
}

// The merge loop is shared per repository, so two clients built for the same
// repository — one per concurrent request in production — must merge together.
// A per-client batcher would merge nothing and only add latency.
func TestBatchIsSharedAcrossClientsOfOneRepository(t *testing.T) {
	recorder := &commitRecorder{}
	endpoint := testEndpoint(t)

	first := newTestClient(t, endpoint)
	second := newTestClient(t, endpoint)
	first.api.Transport = recorder.transport(t)
	second.api.Transport = recorder.transport(t)
	first.SetCommitBatch(testWindowRange())
	second.SetCommitBatch(testWindowRange())

	// Distinct objects, so the shared batch is observable rather than
	// deduplicated down to a single action.
	paths := []string{
		ObjectPath("00", shaOf(t, []byte("shared-a"))),
		ObjectPath("00", shaOf(t, []byte("shared-b"))),
	}
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, client := range []*Client{first, second} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = client.Commit(context.Background(),
				[]map[string]any{blobAction(paths[i], HashFromObjectPath(paths[i]), 1024)})
		}()
	}
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Len(t, recorder.starts, 1, "clients of one repository must share a merge window")
	require.Equal(t, []int{2}, recorder.actionCounts())
}

// The policy switch has to actually connect the batcher; a setting parsed but
// never wired is the failure this guards against.
func TestNewRoutesCommitsThroughBatchWhenEnabled(t *testing.T) {
	recorder := &commitRecorder{}
	policy := newPolicy(t, &types.PolicySetting{
		ModelScopeBatchCommit:    true,
		ModelScopeBatchWindowMin: 1,
		ModelScopeBatchWindowMax: 1,
	})

	d, err := New(context.Background(), policy, nil, nil, nil, logging.NewConsoleLogger(logging.LevelError))
	require.NoError(t, err)
	require.NotNil(t, d.client.commitRunner, "windowed commits must install a commit runner")

	d.client.api.Transport = recorder.transport(t)
	require.NoError(t, d.client.Commit(context.Background(),
		[]map[string]any{blobAction(ObjectPath("00", testHash), testHash, 1024)}))

	require.Len(t, recorder.starts, 1, "the merged commit must still reach the repository")
}

// The two modes are mutually exclusive, and a policy row that predates the
// validation could still carry both. Queued commits must win, because spacing a
// commit is the more conservative failure.
func TestNewPrefersQueueWhenBothModesAreSet(t *testing.T) {
	recorder := &commitRecorder{}
	q := newTestQueue(t)

	policy := newPolicy(t, &types.PolicySetting{
		ModelScopeQueueCommit:       true,
		ModelScopeCommitIntervalMin: 1,
		ModelScopeCommitIntervalMax: 1,
		ModelScopeBatchCommit:       true,
		ModelScopeBatchWindowMin:    1,
		ModelScopeBatchWindowMax:    1,
	})

	d, err := New(context.Background(), policy, nil, nil, q, logging.NewConsoleLogger(logging.LevelError))
	require.NoError(t, err)

	d.client.api.Transport = recorder.transport(t)
	require.NoError(t, d.client.Commit(context.Background(),
		[]map[string]any{blobAction(ObjectPath("00", testHash), testHash, 1024)}))

	require.Equal(t, 1, q.SubmittedTasks(), "the queued mode must take precedence")
	require.Len(t, recorder.starts, 1)
}

// The merge must hold for real uploads, not just for direct Commit calls: two
// concurrent Put operations, each following its own upload path, must leave the
// repository with a single commit carrying both objects.
func TestBatchMergesConcurrentUploads(t *testing.T) {
	recorder := &commitRecorder{}
	policy := newPolicy(t, &types.PolicySetting{
		ModelScopeBatchCommit:    true,
		ModelScopeBatchWindowMin: 1,
		ModelScopeBatchWindowMax: 1,
	})

	d, err := New(context.Background(), policy, nil, nil, nil, logging.NewConsoleLogger(logging.LevelError))
	require.NoError(t, err)
	d.client.api.Transport = recorder.transport(t)

	contents := [][]byte{[]byte("first upload body"), []byte("second upload body")}
	start := make(chan struct{})
	errs := make([]error, len(contents))
	var wg sync.WaitGroup

	for i, content := range contents {
		wg.Add(1)
		go func() {
			defer wg.Done()

			props := &fs.UploadProps{Size: int64(len(content)), ClientHash: shaOf(t, content)}
			req := newUploadRequest(io.NopCloser(bytes.NewReader(content)), props)
			<-start
			errs[i] = d.Put(context.Background(), req)
		}()
	}

	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "upload %d failed", i)
	}

	starts, _, _ := recorder.spans()
	require.Len(t, starts, 1, "concurrent uploads inside one window must share a commit")
	require.Equal(t, []int{2}, recorder.actionCounts(),
		"the merged commit must carry one action per uploaded object")
}

// Inline objects are the reason the action count alone cannot bound a batch:
// 100 of them would build a body of hundreds of megabytes. The byte budget must
// close a batch before that happens.
func TestBatchSplitsOnByteBudget(t *testing.T) {
	recorder := &commitRecorder{}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)

	// A window long enough that only the byte budget can close the batches.
	client.SetCommitBatch(30*time.Second, 30*time.Second)

	// Each inline action carries its content, so the encoded size is roughly
	// what the budget counts. Six of these exceed the budget while staying well
	// under the action cap.
	content := make([]byte, maxBatchBytesPerCommit/4)
	paths := make([]string, 6)
	for i := range paths {
		paths[i] = ObjectPath("00", shaOf(t, []byte(fmt.Sprintf("inline-%d", i))))
	}

	start := make(chan struct{})
	errs := make([]error, len(paths))
	var wg sync.WaitGroup
	for i, path := range paths {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = client.Commit(context.Background(),
				[]map[string]any{inlineAction(path, content)})
		}()
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "commit %d failed", i)
	}

	counts := recorder.actionCounts()
	require.GreaterOrEqual(t, len(counts), 2, "the byte budget must split the batch")

	sum := 0
	for _, c := range counts {
		require.Less(t, c, len(paths), "a batch must not exceed the byte budget")
		sum += c
	}
	require.Equal(t, len(paths), sum, "every action must be written exactly once")
}

func TestNormalizeBatchWindowFillsDefaults(t *testing.T) {
	logger := logging.NewConsoleLogger(logging.LevelError)

	minWindow, maxWindow := normalizeBatchWindow(0, 0, logger)
	require.Equal(t, DefaultBatchWindowMinSeconds*time.Second, minWindow)
	require.Equal(t, DefaultBatchWindowMaxSeconds*time.Second, maxWindow)

	// A reversed range is repaired rather than left to invert the window.
	minWindow, maxWindow = normalizeBatchWindow(9*time.Second, 2*time.Second, logger)
	require.Equal(t, 9*time.Second, minWindow)
	require.Equal(t, 9*time.Second, maxWindow)

	// An explicit range is kept as configured.
	minWindow, maxWindow = normalizeBatchWindow(3*time.Second, 5*time.Second, logger)
	require.Equal(t, 3*time.Second, minWindow)
	require.Equal(t, 5*time.Second, maxWindow)
}

// A merge must not depend on a caller's deadline: several uploads wait on one
// commit, so a short deadline on one of them cannot abort the write the others
// need. Only the batch's own timeout bounds it.
func TestBatchWriteOutlivesCallerDeadline(t *testing.T) {
	recorder := &commitRecorder{hold: 80 * time.Millisecond}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)
	client.SetCommitBatch(testWindowRange())

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	// The window closes before the caller's deadline, so the caller observes its
	// own timeout while the write proceeds.
	err := client.Commit(ctx, []map[string]any{blobAction(ObjectPath("00", testHash), testHash, 1024)})
	require.True(t, err == nil || errors.Is(err, context.DeadlineExceeded))

	// Whichever the caller saw, the merged commit must have been written.
	require.Eventually(t, func() bool {
		starts, _, _ := recorder.spans()
		return len(starts) == 1
	}, 2*time.Second, 10*time.Millisecond, "the merged commit must be written regardless of the caller")
}
