package modelscope

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/stretchr/testify/require"
)

// pacerDep satisfies queue.Dep; the queue only uses it to derive a per-task
// context, which these tests do not observe.
type pacerDep struct{}

func (pacerDep) ForkWithLogger(ctx context.Context, _ logging.Logger) context.Context { return ctx }

// testEndpoint returns a unique HTTPS origin per test.
//
// The commit pacer is shared per repository, so two tests that used the same
// endpoint and repository would queue behind each other's interval and make the
// timing assertions depend on test order. A distinct origin keeps each test on
// its own pacer.
func testEndpoint(t *testing.T) string {
	t.Helper()
	// t.Name() is unique per test; case and slashes are not valid in a host
	// label, so it is reduced to a safe token.
	name := strings.NewReplacer("/", "-", "_", "-", " ", "-").Replace(t.Name())
	return "https://" + strings.ToLower(name) + ".modelscope.test"
}

// newTestQueue builds a real single-worker queue so the commit path under test
// is the one production uses, not a stand-in.
func newTestQueue(t *testing.T) queue.Queue {
	t.Helper()

	q := queue.New(logging.NewConsoleLogger(logging.LevelError), nil, nil, pacerDep{},
		queue.WithName("TestModelScopeCommitQueue"),
		queue.WithWorkerCount(1),
		queue.WithTaskPullInterval(5*time.Millisecond),
	)
	q.Start()
	t.Cleanup(q.Shutdown)
	return q
}

// commitRecorder records when each commit request started and finished, and the
// greatest number of commits that were ever in flight at once.
type commitRecorder struct {
	mu        sync.Mutex
	starts    []time.Time
	ends      []time.Time
	inflight  int
	maxInFlgt int
	body      []map[string]any
	// hold is how long a commit request is kept open, so overlap is observable.
	hold time.Duration
}

func (r *commitRecorder) transport(t *testing.T) roundTripper {
	t.Helper()

	return roundTripper(func(req *http.Request) (*http.Response, error) {
		r.mu.Lock()
		r.starts = append(r.starts, time.Now())
		r.inflight++
		if r.inflight > r.maxInFlgt {
			r.maxInFlgt = r.inflight
		}
		r.mu.Unlock()

		var payload map[string]any
		_ = json.NewDecoder(req.Body).Decode(&payload)

		time.Sleep(r.hold)

		r.mu.Lock()
		r.ends = append(r.ends, time.Now())
		r.inflight--
		r.body = append(r.body, payload)
		r.mu.Unlock()

		return jsonResponse(http.StatusOK, `{"Code":200}`), nil
	})
}

func (r *commitRecorder) spans() ([]time.Time, []time.Time, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.starts...), append([]time.Time(nil), r.ends...), r.maxInFlgt
}

// commitThroughQueue runs the given number of commits concurrently through the
// queued client and waits for all of them.
func commitThroughQueue(t *testing.T, client *Client, n int) {
	t.Helper()

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = client.Commit(context.Background(), []map[string]any{
				blobAction(ObjectPath("00", testHash), testHash, 1024),
			})
		}()
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "commit %d failed", i)
	}
}

// Queued commits must not run at the same time: a ModelScope commit is an
// unguarded whole-revision mutation, so overlapping writes can lose one another.
func TestQueuedCommitsAreSerialized(t *testing.T) {
	recorder := &commitRecorder{hold: 30 * time.Millisecond}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)

	// A fixed interval keeps the assertion independent of the random draw.
	client.SetCommitQueue(newTestQueue(t), 20*time.Millisecond, 20*time.Millisecond)

	commitThroughQueue(t, client, 4)

	starts, ends, maxInFlight := recorder.spans()
	require.Len(t, starts, 4)
	require.Equal(t, 1, maxInFlight, "queued commits must never overlap")
	require.Len(t, ends, 4)
}

// Consecutive commits must be spaced by the configured interval, counted from
// the end of the previous commit, because ModelScope rejects commits that follow
// each other too closely.
func TestQueuedCommitsHonorInterval(t *testing.T) {
	const interval = 80 * time.Millisecond

	recorder := &commitRecorder{}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)
	client.SetCommitQueue(newTestQueue(t), interval, interval)

	commitThroughQueue(t, client, 3)

	starts, ends, _ := recorder.spans()
	require.Len(t, starts, 3)

	// A timer can only fire at or after its deadline, so each start must be at
	// least the interval after the previous commit finished. A small tolerance
	// absorbs scheduler jitter without hiding a missing wait.
	const tolerance = 10 * time.Millisecond
	for i := 1; i < len(starts); i++ {
		gap := starts[i].Sub(ends[i-1])
		require.GreaterOrEqual(t, gap, interval-tolerance,
			"commit %d started %s after the previous one finished, expected at least %s", i, gap, interval)
	}
}

// With queuing off, commits go straight out: the feature must not silently slow
// down every existing ModelScope policy.
func TestCommitsRunDirectlyWithoutQueue(t *testing.T) {
	recorder := &commitRecorder{hold: 30 * time.Millisecond}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)

	commitThroughQueue(t, client, 3)

	starts, _, maxInFlight := recorder.spans()
	require.Len(t, starts, 3)
	require.Greater(t, maxInFlight, 1,
		"without queuing, independent commits must be allowed to overlap")
}

// The driver has to actually turn the queue on; the setting is worthless if it
// is parsed but never connected.
func TestSetCommitQueueSubmitsTasks(t *testing.T) {
	recorder := &commitRecorder{}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = recorder.transport(t)

	q := newTestQueue(t)
	client.SetCommitQueue(q, 10*time.Millisecond, 10*time.Millisecond)

	commitThroughQueue(t, client, 3)

	require.Equal(t, 3, q.SubmittedTasks(), "every commit must be submitted to the queue")
	require.GreaterOrEqual(t, q.SuccessTasks(), 1)
	starts, _, _ := recorder.spans()
	require.Len(t, starts, 3, "the queued task must still perform the commit")
}

// A commit routed through the queue must report the upstream failure to its
// waiting caller, not swallow it.
func TestQueuedCommitPropagatesFailure(t *testing.T) {
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = roundTripper(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusForbidden, `{"Code":403,"Message":"no write permission to repository"}`), nil
	})
	client.SetCommitQueue(newTestQueue(t), 10*time.Millisecond, 10*time.Millisecond)

	err := client.Commit(context.Background(), []map[string]any{blobAction("00/ab/cd", testHash, 1)})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUpstream)
	require.Contains(t, err.Error(), "no write permission")
}

// Deletes are commits too, so they must also pass through the queue; otherwise
// a delete could still collide with an upload's commit.
func TestQueuedDeleteIsRoutedThroughQueue(t *testing.T) {
	recorder := &commitRecorder{}
	client := newTestClient(t, testEndpoint(t))
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/commit/") {
			return recorder.transport(t)(r)
		}
		// The object is present, so the delete commits.
		return jsonResponse(http.StatusOK, "present"), nil
	})

	q := newTestQueue(t)
	client.SetCommitQueue(q, 10*time.Millisecond, 10*time.Millisecond)

	require.NoError(t, client.Delete(context.Background(), ObjectPath("00", testHash)))
	require.Equal(t, 1, q.SubmittedTasks(), "a delete must be queued like any other commit")
}

func TestNormalizeCommitIntervalFillsDefaults(t *testing.T) {
	logger := logging.NewConsoleLogger(logging.LevelError)

	minInterval, maxInterval := normalizeCommitInterval(0, 0, logger)
	require.Equal(t, DefaultCommitIntervalMinSeconds*time.Second, minInterval)
	require.Equal(t, DefaultCommitIntervalMaxSeconds*time.Second, maxInterval)

	// A reversed range is repaired rather than left to invert the delay.
	minInterval, maxInterval = normalizeCommitInterval(9*time.Second, 2*time.Second, logger)
	require.Equal(t, 9*time.Second, minInterval)
	require.Equal(t, 9*time.Second, maxInterval)
}

func TestRandomIntervalStaysInRange(t *testing.T) {
	minInterval := 3 * time.Second
	maxInterval := 5 * time.Second

	for range 200 {
		got := randomInterval(minInterval, maxInterval)
		require.GreaterOrEqual(t, got, minInterval)
		require.LessOrEqual(t, got, maxInterval)
	}

	// A single-valued range must not be exceeded by the inclusive draw.
	require.Equal(t, minInterval, randomInterval(minInterval, minInterval))
}
