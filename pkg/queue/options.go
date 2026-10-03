package queue

import (
	"runtime"
	"time"

	"github.com/gomodule/redigo/redis"
)

// An Option configures a mutex.
type Option interface {
	apply(*options)
}

// OptionFunc is a function that configures a queue.
type OptionFunc func(*options)

// Apply calls f(option)
func (f OptionFunc) apply(option *options) {
	f(option)
}

type options struct {
	maxTaskExecution   time.Duration // Maximum execution time for a Task.
	retryDelay         time.Duration
	taskPullInterval   time.Duration
	backoffFactor      float64
	backoffMaxDuration time.Duration
	maxRetry           int
	resumeTaskType     []string
	workerCount        int
	name               string

	// streamPool enables a distributed scheduler backed by Redis Streams. It is
	// nil for queues whose tasks cannot be serialised, or when Redis is not
	// configured, in which case the queue stays process-local.
	streamPool     *redis.Pool
	streamConsumer string
}

func newDefaultOptions() *options {
	return &options{
		workerCount:        runtime.NumCPU(),
		maxTaskExecution:   60 * time.Hour,
		backoffFactor:      2,
		backoffMaxDuration: 60 * time.Second,
		resumeTaskType:     []string{},
		taskPullInterval:   1 * time.Second,
		name:               "default",
	}
}

// WithMaxTaskExecution set maximum execution time for a Task.
func WithMaxTaskExecution(d time.Duration) Option {
	return OptionFunc(func(q *options) {
		q.maxTaskExecution = d
	})
}

// WithRetryDelay set retry delay
func WithRetryDelay(d time.Duration) Option {
	return OptionFunc(func(q *options) {
		q.retryDelay = d
	})
}

// WithBackoffFactor set backoff factor
func WithBackoffFactor(f float64) Option {
	return OptionFunc(func(q *options) {
		q.backoffFactor = f
	})
}

// WithBackoffMaxDuration set backoff max duration
func WithBackoffMaxDuration(d time.Duration) Option {
	return OptionFunc(func(q *options) {
		q.backoffMaxDuration = d
	})
}

// WithMaxRetry set max retry
func WithMaxRetry(n int) Option {
	return OptionFunc(func(q *options) {
		q.maxRetry = n
	})
}

// WithResumeTaskType set resume Task type
func WithResumeTaskType(types ...string) Option {
	return OptionFunc(func(q *options) {
		q.resumeTaskType = types
	})
}

// WithWorkerCount set worker count
func WithWorkerCount(num int) Option {
	return OptionFunc(func(q *options) {
		if num <= 0 {
			num = runtime.NumCPU()
		}
		q.workerCount = num
	})
}

// WithName set queue name
func WithName(name string) Option {
	return OptionFunc(func(q *options) {
		q.name = name
	})
}

// WithStreamScheduler distributes this queue's tasks over Redis Streams.
//
// pool is shared across queues. consumer identifies the running process and must
// be stable for its lifetime, so that entries it holds can be distinguished from
// those abandoned by a crashed instance.
//
// The caller is responsible for only enabling this on queues whose tasks can be
// reconstructed from the database, and for ensuring the backing store is actually
// reachable: a queue that cannot reach Redis cannot dispatch anything.
func WithStreamScheduler(pool *redis.Pool, consumer string) Option {
	return OptionFunc(func(q *options) {
		q.streamPool = pool
		q.streamConsumer = consumer
	})
}

// WithTaskPullInterval set task pull interval
func WithTaskPullInterval(d time.Duration) Option {
	return OptionFunc(func(q *options) {
		q.taskPullInterval = d
	})
}
