package queue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/gomodule/redigo/redis"
)

const (
	// streamGroupName is the single consumer group shared by every instance of the
	// cluster, so a task is delivered to exactly one of them.
	streamGroupName = "workers"

	// streamBlockTimeout bounds a single XREADGROUP call. It is the interval at
	// which the read loop re-checks for shutdown and for reclaimed entries.
	streamBlockTimeout = 5 * time.Second

	// streamHeartbeatInterval is how often an instance refreshes the idle time of
	// the entries it still owns. It must stay comfortably below
	// streamClaimMinIdle, otherwise a healthy instance would have its long-running
	// task stolen mid-execution.
	streamHeartbeatInterval = 20 * time.Second

	// streamClaimMinIdle is how long an entry must sit unattended in a consumer's
	// pending list before another instance may take it over. It must exceed the
	// heartbeat interval by enough margin to survive a stalled heartbeat, and must
	// be short enough that a crash is recovered promptly.
	streamClaimMinIdle = 90 * time.Second

	// streamClaimBatch bounds how many abandoned entries are reclaimed per pass.
	streamClaimBatch = 20

	// streamMaxLen caps the stream length. Trimming only discards entries that have
	// already been acknowledged, and the bound is deliberately generous: an entry
	// trimmed before it is acknowledged would strand the task it refers to until
	// the next repair pass.
	streamMaxLen = 100_000

	// streamPromoteInterval is how often deferred tasks are scanned for promotion.
	streamPromoteInterval = 1 * time.Second

	// streamPromoteBatch bounds how many deferred tasks are promoted per pass.
	streamPromoteBatch = 100

	// streamInflightTTL bounds the lifetime of the advertisement marker. The marker
	// is an optimisation that keeps repeated submissions from piling up duplicate
	// references, not a correctness mechanism: a reference that is not claimed by
	// exactly one worker is discarded by the conditional claim, so a marker leaked
	// by a crashed instance only costs a harmless extra reference until it expires.
	streamInflightTTL = time.Hour

	// streamRepairLockTTL bounds the startup repair pass. It is released on expiry
	// rather than explicitly, so an instance that dies mid-repair does not block
	// recovery until it is restarted.
	streamRepairLockTTL = 300

	// streamKeyPrefix namespaces every Redis key owned by a queue.
	streamKeyPrefix = "cr:queue:"
)

// repairCtx marks an advertisement as part of the single-instance recovery sweep.
//
// The sweep's job is to restore advertisements that were lost. It runs under a
// cluster-wide lock after a restart, and it bypasses the deduplication that ordinary
// submissions use: a marker left behind by a process that died between publishing and
// claiming would otherwise suppress exactly the recovery it exists to perform.
//
// It deliberately does not claim the ability to recover a task left processing. The
// database alone cannot distinguish such a task from one still running on another
// instance, so recovery is left to redelivery, which carries that evidence.
type repairCtx struct{}

// streamScheduler distributes tasks over a Redis Stream.
//
// The database remains the source of truth for task state; the stream only carries
// the identity of tasks that are ready to run. Two properties follow from that
// split and are load-bearing:
//
//   - The stream entry is a hint, not the task. If an entry is lost, the repair
//     pass re-advertises anything the database still reports as pending.
//   - Claiming happens in the database, not in Redis. The stream guarantees that an
//     entry is offered to one consumer at a time; the conditional status update
//     guarantees that the task runs only once even when an entry is redelivered
//     after a crash. Every task reaching this scheduler is therefore required to be
//     resumable, which is already true of everything backed by a database row.
//
// A task may be advertised through two channels, and exactly one of them holds it
// at any time: the stream, for tasks that are ready to run now, and the delayed
// sorted set, for tasks waiting on a resume time.
type streamScheduler struct {
	name     string // Queue name, used for logging and for deriving Redis keys.
	stream   string // Stream holding ready task references.
	delayed  string // Sorted set holding tasks deferred to a future resume time.
	repair   string // Lock key serialising the startup repair pass.
	consumer string // Stable identity of this process within the consumer group.

	pool       *redis.Pool
	taskClient inventory.TaskClient
	logger     logging.Logger

	// reclaimed carries entries taken over from other instances, so the read path
	// does not have to interleave XAUTOCLAIM with its blocking read.
	reclaimed chan streamEntry

	// inflight maps a task onto the stream entry currently representing it.
	mu       sync.Mutex
	inflight map[int]string

	quit     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
	stopFlag int32
}

// streamEntry is a reference to a task advertised on the stream.
//
// recover records that this reference is allowed to take over a task the database
// still reports as processing. It is set only when the entry was taken over from a
// consumer that stopped proving it was alive, which is the sole evidence available
// that a task left mid-execution was actually abandoned rather than still running.
// A reference without it may only claim a task that is queued or suspended, which is
// what stops a duplicate advertisement from starting a second copy of a running task.
type streamEntry struct {
	entryID  string
	taskID   int
	taskType string
	recover  bool
}

func NewStreamScheduler(l logging.Logger, pool *redis.Pool, consumer, name string, taskClient inventory.TaskClient) *streamScheduler {
	return &streamScheduler{
		name:       name,
		stream:     streamKeyPrefix + name,
		delayed:    streamKeyPrefix + name + ":delayed",
		repair:     streamKeyPrefix + name + ":repair",
		consumer:   consumer,
		pool:       pool,
		taskClient: taskClient,
		logger:     l,
		reclaimed:  make(chan streamEntry, streamClaimBatch),
		inflight:   make(map[int]string),
		quit:       make(chan struct{}),
	}
}

// Init verifies the backing store is usable and creates the consumer group. It must
// succeed before the queue is started: a queue that believes it is distributed but
// cannot reach Redis would accept tasks and never run them.
func (s *streamScheduler) Init(_ context.Context) error {
	conn := s.pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis for queue %q: %w", s.name, err)
	}

	// The group starts at "0" so that entries published before this instance
	// started are still deliverable. BUSYGROUP means another instance won the race
	// and the group already exists, which is success.
	if _, err := conn.Do("XGROUP", "CREATE", s.stream, streamGroupName, "0", "MKSTREAM"); err != nil {
		if !strings.Contains(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("failed to create consumer group for queue %q: %w", s.name, err)
		}
	}

	// Verify the commands the scheduler depends on actually exist. Redis gained
	// consumer groups in 4.0, XAUTOCLAIM in 6.2, and XADD's recovery would silently
	// stop working on an older server, leaving tasks advertised but never dispatched.
	if err := s.verifyCapabilities(conn); err != nil {
		return err
	}

	return nil
}

// verifyCapabilities fails fast on a server too old to run a distributed queue.
func (s *streamScheduler) verifyCapabilities(conn redis.Conn) error {
	raw, err := redis.String(conn.Do("XADD", s.stream, "MAXLEN", "~", 1, "*", "probe", "1"))
	if err != nil {
		return fmt.Errorf("Redis does not support streams, queue %q cannot be distributed: %w", s.name, err)
	}

	if _, err := conn.Do("XACK", s.stream, streamGroupName, raw); err != nil {
		return fmt.Errorf("Redis does not support consumer group acknowledgement: %w", err)
	}

	// A claiming probe: the reply shape changed in Redis 7.0, so it is also the
	// check that the reply parser and the server agree.
	if _, err := conn.Do("XAUTOCLAIM", s.stream, streamGroupName, s.consumer, 0, "0-0", "COUNT", 1); err != nil {
		return fmt.Errorf("Redis 6.2 or newer is required for queue %q, unsupported command XAUTOCLAIM: %w", s.name, err)
	}

	// Keep the probe entry from lingering in the pending list of the group.
	if _, err := conn.Do("XDEL", s.stream, raw); err != nil {
		return fmt.Errorf("failed to clean up capability probe: %w", err)
	}

	return nil
}

// Start launches the background loops that keep the stream healthy. It must be
// called after Init.
func (s *streamScheduler) Start() {
	s.wg.Add(3)
	go s.heartbeatLoop()
	go s.reclaimLoop()
	go s.promoteLoop()
}

// Queue advertises a task for execution.
func (s *streamScheduler) Queue(ctx context.Context, t Task) error {
	if atomic.LoadInt32(&s.stopFlag) == 1 {
		return ErrQueueShutdown
	}

	if !t.ShouldPersist() {
		return fmt.Errorf("task %d of type %q cannot be distributed", t.ID(), t.Type())
	}

	conn := s.pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis for queue %q: %w", s.name, err)
	}

	// A task deferred to the future belongs in the sorted set, not on the stream:
	// it may wait for hours, and letting it occupy a delivery slot would only
	// produce a busy retry loop.
	if t.ResumeTime() > time.Now().Unix() {
		if _, err := conn.Do("ZADD", s.delayed, t.ResumeTime(), t.ID()); err != nil {
			return fmt.Errorf("failed to defer task %d: %w", t.ID(), err)
		}
		return nil
	}

	// Work submitted by the recovery sweep is ordinary work: it cannot know whether a
	// task left processing was abandoned or is still running elsewhere, so it
	// advertises like any other submission. Recovering such a task is left to
	// redelivery, which is the only thing that carries evidence of abandonment.
	//
	// The sweep does bypass the deduplication below. It exists to restore
	// advertisements that were lost, and an advertisement marker left behind by a
	// process that died between publishing and claiming would otherwise suppress
	// exactly the recovery it is meant to perform.
	if _, recovering := ctx.Value(repairCtx{}).(bool); recovering {
		return s.publish(conn, t.ID(), t.Type())
	}

	// A duplicate reference is discarded by the conditional claim, so this is only
	// an optimisation that keeps repeated submissions from accumulating on the
	// stream. It has to be taken before publishing: a worker that finishes while
	// the entry is being published would release a marker that does not exist yet,
	// and the marker left behind afterwards would suppress the task's next
	// submission until it expired.
	advertised, err := s.lockInflight(conn, t.ID())
	if err != nil {
		return err
	}
	if !advertised {
		return nil
	}

	if err := s.publish(conn, t.ID(), t.Type()); err != nil {
		// The task was not advertised, so the marker must not be held.
		s.releaseInflight(conn, t.ID())
		return err
	}

	return nil
}

// Request hands out the next task to run.
//
// It blocks until a task is available rather than reporting an empty stream upward:
// the underlying read already blocks for a bounded interval, so returning early
// would only add an artificial delay between polls.
func (s *streamScheduler) Request(ctx context.Context) (Task, error) {
	for {
		if atomic.LoadInt32(&s.stopFlag) == 1 {
			return nil, ErrQueueShutdown
		}

		entry, err := s.nextEntry(ctx)
		if err != nil {
			if errors.Is(err, ErrNoTaskInQueue) {
				if ctx.Err() != nil {
					return nil, ErrQueueShutdown
				}
				continue
			}

			return nil, err
		}

		// Winning the conditional claim is what actually reserves the task. Losing
		// it means the task was taken by another instance or already finished, in
		// which case this entry is settled and the next one is considered.
		model, claimed, err := s.taskClient.ClaimPendingTask(ctx, entry.taskID, entry.recover)
		if err != nil {
			// Leave the entry unacknowledged so it can be reclaimed rather than lost.
			return nil, fmt.Errorf("failed to claim task %d: %w", entry.taskID, err)
		}

		if !claimed {
			s.ackEntry(entry.entryID)
			continue
		}

		t, err := NewTaskFromModel(model)
		if err != nil {
			// An unknown task type cannot be reconstructed on redelivery either, so
			// the entry would only be reclaimed forever.
			s.logger.Error("Dropping task %d of unsupported type %q: %s", entry.taskID, model.Type, err)
			s.ackEntry(entry.entryID)
			return nil, err
		}

		s.mu.Lock()
		s.inflight[entry.taskID] = entry.entryID
		s.mu.Unlock()

		return t, nil
	}
}

// Ack settles the entry that carried the finished task and guarantees that a task
// which is still pending stays advertised.
//
// The second half matters: a worker that suspends a task re-queues it while its
// entry is still held, so that re-queue is necessarily suppressed by the marker
// this very entry owns. Releasing the marker without re-advertising would strand
// the task until the next repair pass.
//
// Not acknowledging at all is a safe failure: the entry stays pending and another
// instance takes it over, which is the intended crash-recovery path.
func (s *streamScheduler) Ack(ctx context.Context, t Task) error {
	s.mu.Lock()
	entryID, tracked := s.inflight[t.ID()]
	delete(s.inflight, t.ID())
	s.mu.Unlock()

	conn := s.pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis for queue %q: %w", s.name, err)
	}

	// Only the marker this process set may be released: in a cluster another
	// instance may already have taken over the task and hold its own.
	s.releaseInflight(conn, t.ID())

	if t.Status() == task.StatusQueued || t.Status() == task.StatusSuspending {
		if err := s.ensureAdvertised(conn, t); err != nil {
			return err
		}
	}

	if !tracked {
		s.logger.Warning("Task %d finished without a tracked stream entry in queue %q, its entry may be redelivered", t.ID(), s.name)
		return nil
	}

	if _, err := conn.Do("XACK", s.stream, streamGroupName, entryID); err != nil {
		return fmt.Errorf("failed to acknowledge task %d: %w", t.ID(), err)
	}

	return nil
}

// ensureAdvertised puts a still-pending task back into whichever channel is
// responsible for it.
//
// It runs on the worker's own completion path, where the task is known to be left
// unfinished, so it is authoritative: it is the only thing standing between a
// suspended task and being stranded until the next restart. It therefore does not
// defer to the advertisement marker, which this very task's entry may still own.
func (s *streamScheduler) ensureAdvertised(conn redis.Conn, t Task) error {
	// A task with a future resume time is held by the delayed set, which the
	// promotion loop drains once the time arrives.
	if t.ResumeTime() > time.Now().Unix() {
		if _, err := conn.Do("ZADD", s.delayed, t.ResumeTime(), t.ID()); err != nil {
			return fmt.Errorf("failed to defer task %d: %w", t.ID(), err)
		}
		return nil
	}

	// The task is due now, so any deferred entry is stale and is removed to keep the
	// promotion loop from advertising it a second time.
	if _, err := conn.Do("ZREM", s.delayed, t.ID()); err != nil {
		return fmt.Errorf("failed to clear deferred entry for task %d: %w", t.ID(), err)
	}

	if err := s.publish(conn, t.ID(), t.Type()); err != nil {
		return err
	}

	return nil
}

// Shutdown stops the background loops. Entries still owned by this instance are
// deliberately left unacknowledged so that they are picked up elsewhere.
func (s *streamScheduler) Shutdown() error {
	if !atomic.CompareAndSwapInt32(&s.stopFlag, 0, 1) {
		return ErrQueueShutdown
	}

	s.stopOnce.Do(func() {
		close(s.quit)
		s.wg.Wait()
	})

	return nil
}

// tryAcquireRepairLock serialises the startup repair pass across instances.
func (s *streamScheduler) tryAcquireRepairLock() (bool, error) {
	conn := s.pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return false, fmt.Errorf("failed to connect to Redis for queue %q: %w", s.name, err)
	}

	acquired, err := redis.String(conn.Do("SET", s.repair, s.consumer, "NX", "EX", streamRepairLockTTL))
	if errors.Is(err, redis.ErrNil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	return acquired == "OK", nil
}

// nextEntry returns the next advertised task, preferring entries reclaimed from
// other instances.
func (s *streamScheduler) nextEntry(ctx context.Context) (streamEntry, error) {
	select {
	case entry := <-s.reclaimed:
		return entry, nil
	default:
	}

	return s.readNew(ctx)
}

// readNew reads a never-delivered entry, blocking up to streamBlockTimeout.
func (s *streamScheduler) readNew(ctx context.Context) (streamEntry, error) {
	conn := s.pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return streamEntry{}, fmt.Errorf("failed to connect to Redis for queue %q: %w", s.name, err)
	}

	// A blocking read would otherwise hold the goroutine for the whole block timeout
	// even while the queue is shutting down, so it is issued against the caller's
	// context.
	rc, ok := conn.(redis.ConnWithContext)
	if !ok {
		return streamEntry{}, fmt.Errorf("redis connection for queue %q does not support context cancellation", s.name)
	}

	reply, err := rc.DoContext(ctx, "XREADGROUP", "GROUP", streamGroupName, s.consumer,
		"COUNT", 1, "BLOCK", int(streamBlockTimeout.Milliseconds()),
		"STREAMS", s.stream, ">")
	if err != nil {
		return streamEntry{}, fmt.Errorf("failed to read from stream %q: %w", s.stream, err)
	}

	entries, err := parseXRead(reply)
	if err != nil {
		return streamEntry{}, err
	}
	if len(entries) == 0 {
		return streamEntry{}, ErrNoTaskInQueue
	}

	return entries[0], nil
}

// heartbeatLoop keeps long-running tasks owned by this instance from being
// reclaimed by another one.
func (s *streamScheduler) heartbeatLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(streamHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.heartbeat()
		}
	}
}

func (s *streamScheduler) heartbeat() {
	s.mu.Lock()
	ids := make([]string, 0, len(s.inflight))
	for _, entryID := range s.inflight {
		ids = append(ids, entryID)
	}
	s.mu.Unlock()

	if len(ids) == 0 {
		return
	}

	conn := s.pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		s.logger.Warning("Failed to refresh stream ownership in queue %q: %s", s.name, err)
		return
	}

	for _, entryID := range ids {
		// Claiming an entry for itself with a zero minimum idle time resets its idle
		// clock, which is what keeps it out of reach of the reclaim pass. JUSTID
		// avoids incrementing the delivery counter for what is not a redelivery.
		if _, err := conn.Do("XCLAIM", s.stream, streamGroupName, s.consumer, 0, entryID, "JUSTID", "IDLE", 0); err != nil {
			s.logger.Warning("Failed to refresh ownership of entry %q in queue %q: %s", entryID, s.name, err)
		}
	}
}

// reclaimLoop takes over entries abandoned by instances that stopped heartbeating.
func (s *streamScheduler) reclaimLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(streamClaimMinIdle)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.reclaim()
		}
	}
}

func (s *streamScheduler) reclaim() {
	conn := s.pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		s.logger.Warning("Failed to reclaim abandoned tasks in queue %q: %s", s.name, err)
		return
	}

	reply, err := conn.Do("XAUTOCLAIM", s.stream, streamGroupName, s.consumer,
		int(streamClaimMinIdle.Milliseconds()), "0-0", "COUNT", streamClaimBatch)
	if err != nil {
		s.logger.Warning("Failed to reclaim abandoned tasks in queue %q: %s", s.name, err)
		return
	}

	for _, entry := range parseXAutoClaim(reply) {
		// Ownership was taken from a consumer that stopped refreshing the entry, so
		// whatever task it holds may have been left behind mid-execution.
		entry.recover = true

		select {
		case s.reclaimed <- entry:
		default:
			// The local buffer is full; the remainder stays available for the next
			// pass, because ownership only transfers for what was returned.
			return
		}
	}
}

// promoteLoop moves deferred tasks whose resume time has arrived back onto the
// stream.
func (s *streamScheduler) promoteLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(streamPromoteInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-ticker.C:
			s.promote()
		}
	}
}

func (s *streamScheduler) promote() {
	conn := s.pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		s.logger.Warning("Failed to promote deferred tasks in queue %q: %s", s.name, err)
		return
	}

	ids, err := redis.Ints(conn.Do("ZRANGEBYSCORE", s.delayed, "-inf", time.Now().Unix(),
		"LIMIT", 0, streamPromoteBatch))
	if err != nil {
		s.logger.Warning("Failed to list deferred tasks in queue %q: %s", s.name, err)
		return
	}

	for _, id := range ids {
		// Removing from the sorted set is the promotion's critical section: only the
		// instance that succeeds may advertise the task, so concurrent passes cannot
		// produce two entries for it.
		removed, err := redis.Int(conn.Do("ZREM", s.delayed, id))
		if err != nil {
			s.logger.Warning("Failed to promote task %d in queue %q: %s", id, s.name, err)
			continue
		}
		if removed != 1 {
			continue
		}

		// Promotion is authoritative, like the other recovery paths: the task is known
		// to be due and unfinished, and deferring to the advertisement marker would let
		// a marker leaked by a crashed process strand it.
		if err := s.publish(conn, id, ""); err != nil {
			s.logger.Warning("Failed to advertise promoted task %d in queue %q: %s", id, s.name, err)
		}
	}
}

// publish appends a reference to the task onto the stream.
//
// Whether the reference may recover a task left processing is not carried here: it
// is decided when the entry is read, and only an entry taken over from a consumer
// that stopped proving it was alive qualifies.
func (s *streamScheduler) publish(conn redis.Conn, taskID int, taskType string) error {
	_, err := conn.Do("XADD", s.stream, "MAXLEN", "~", streamMaxLen,
		"*", "id", taskID, "type", taskType)
	if err != nil {
		return fmt.Errorf("failed to advertise task %d: %w", taskID, err)
	}

	return nil
}

// lockInflight records that a task is now advertised, reporting whether this caller
// is the one that advertised it.
func (s *streamScheduler) lockInflight(conn redis.Conn, taskID int) (bool, error) {
	held, err := redis.String(conn.Do("SET", s.inflightKey(taskID), s.consumer, "NX", "EX", int(streamInflightTTL.Seconds())))
	if errors.Is(err, redis.ErrNil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to mark task %d as advertised: %w", taskID, err)
	}

	return held == "OK", nil
}

// releaseInflight clears the advertisement marker, but only when this process owns
// it. A task taken over by another instance keeps its marker, so this process cannot
// suppress the advertisement that instance is relying on.
func (s *streamScheduler) releaseInflight(conn redis.Conn, taskID int) {
	key := s.inflightKey(taskID)

	owner, err := redis.String(conn.Do("GET", key))
	if errors.Is(err, redis.ErrNil) {
		return
	}
	if err != nil {
		s.logger.Warning("Failed to inspect advertisement marker for task %d: %s", taskID, err)
		return
	}
	if owner != s.consumer {
		return
	}

	if _, err := conn.Do("DEL", key); err != nil {
		s.logger.Warning("Failed to release advertisement marker for task %d: %s", taskID, err)
	}
}

func (s *streamScheduler) inflightKey(taskID int) string {
	return streamKeyPrefix + s.name + ":inflight:" + strconv.Itoa(taskID)
}

// ackEntry settles an entry whose task this instance did not end up owning.
func (s *streamScheduler) ackEntry(entryID string) {
	conn := s.pool.Get()
	defer conn.Close()
	if err := conn.Err(); err != nil {
		return
	}

	if _, err := conn.Do("XACK", s.stream, streamGroupName, entryID); err != nil {
		s.logger.Warning("Failed to acknowledge entry %q in queue %q: %s", entryID, s.name, err)
	}
}
