package modelscope

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
)

const (
	// DefaultCommitIntervalMinSeconds and DefaultCommitIntervalMaxSeconds are
	// the randomized commit spacing, in seconds, applied when queued commits are
	// enabled without an explicit range.
	DefaultCommitIntervalMinSeconds = 3
	DefaultCommitIntervalMaxSeconds = 5
)

// commitPacer serializes the commits of one repository and keeps a randomized
// quiet period between them.
//
// ModelScope rejects commits submitted too close together. When a policy
// enables queued commits, every commit of that repository passes through its
// pacer: one commit runs at a time, and the next one waits a random interval
// drawn from the configured range after the previous one finished.
//
// Commits are serialized rather than merely spaced because a ModelScope commit
// is an unguarded whole-revision mutation: two in-flight commits can interleave
// and an older one can overwrite a newer one. Admitting them one at a time is
// what makes the outcome deterministic.
type commitPacer struct {
	// gate holds a single token, so only one commit runs at a time. It is a
	// buffered channel rather than a mutex so a waiting commit can abandon the
	// wait when its upload is cancelled.
	gate chan struct{}
	// next is the earliest instant the next commit may start.
	next time.Time
}

func newCommitPacer() *commitPacer {
	return &commitPacer{gate: make(chan struct{}, 1)}
}

// run executes commit once this repository is free and the quiet period since
// the previous commit has elapsed. Waiting is bounded by ctx, so a cancelled
// upload releases its turn instead of holding the repository.
func (p *commitPacer) run(ctx context.Context, minInterval, maxInterval time.Duration, commit func() error) error {
	select {
	case p.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.gate }()

	// Only the token holder reaches here, so next is not shared.
	if wait := time.Until(p.next); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}

	err := commit()

	// The quiet period is counted from the end of this commit, so a slow commit
	// cannot make the next one start immediately after it returns.
	p.next = time.Now().Add(randomInterval(minInterval, maxInterval))
	return err
}

// randomInterval picks a duration in [minInterval, maxInterval].
func randomInterval(minInterval, maxInterval time.Duration) time.Duration {
	if maxInterval <= minInterval {
		return minInterval
	}

	return minInterval + time.Duration(rand.Int64N(int64(maxInterval-minInterval)+1))
}

// commitPacerRegistry holds one pacer per repository.
//
// A driver is built for each operation and a queued commit is dispatched from
// whichever request produced it, so pacing state cannot live on a driver or a
// task: two uploads running at the same time would each pace their own commits
// and never hold the interval between them. The registry is what makes the
// ordering hold across concurrent operations in this process.
type commitPacerRegistry struct {
	mu     sync.Mutex
	pacers map[string]*commitPacer
}

var sharedCommitPacers = &commitPacerRegistry{pacers: make(map[string]*commitPacer)}

// forRepository returns the pacer of key, creating it on first use.
func (r *commitPacerRegistry) forRepository(key string) *commitPacer {
	r.mu.Lock()
	defer r.mu.Unlock()

	pacer, ok := r.pacers[key]
	if !ok {
		pacer = newCommitPacer()
		r.pacers[key] = pacer
	}

	return pacer
}

// commitQueueKey identifies the commit target of a client. Only commits written
// to the same repository and revision need to be ordered, so keying the pacer
// by that target lets unrelated repositories commit in parallel.
func commitQueueKey(endpoint, repoType, repo, revision string) string {
	return endpoint + "\x00" + repoType + "\x00" + repo + "\x00" + revision
}

// normalizeCommitInterval turns the configured second values into the duration
// range the pacer enforces. Missing values fall back to the defaults and a
// reversed range is repaired, so a policy written outside the admin API cannot
// invert the interval.
func normalizeCommitInterval(minInterval, maxInterval time.Duration, l logging.Logger) (time.Duration, time.Duration) {
	if minInterval <= 0 {
		minInterval = DefaultCommitIntervalMinSeconds * time.Second
	}

	if maxInterval <= 0 {
		maxInterval = DefaultCommitIntervalMaxSeconds * time.Second
	}

	if maxInterval < minInterval {
		l.Warning("ModelScope commit interval max %s is below min %s, using the min for both", maxInterval, minInterval)
		maxInterval = minInterval
	}

	return minInterval, maxInterval
}
