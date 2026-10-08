package queue

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/gomodule/redigo/redis"
)

func init() {
	RegisterResumableTaskFactory("fake", func(model *ent.Task) Task {
		return &fakeWorkTask{DBTask: &DBTask{Task: model}}
	})
}

// executedTasks carries the execution signal for a model reconstructed from the
// database, which is how the queue hands a task to a worker.
var executedTasks = struct {
	sync.Mutex
	byID map[int]chan struct{}
}{byID: make(map[int]chan struct{})}

func init() {
	RegisterResumableTaskFactory("counting", func(model *ent.Task) Task {
		executedTasks.Lock()
		signal := executedTasks.byID[model.ID]
		executedTasks.Unlock()

		return &countingTask{
			fakeWorkTask: &fakeWorkTask{DBTask: &DBTask{Task: model}},
			executed:     signal,
		}
	})
}

// fakeTaskClient stands in for the database-backed task client. It records which
// tasks were claimed and models the conditional-claim semantics the real client
// provides, including the distinction between an ordinary claim and one that may
// take over a task left processing.
type fakeTaskClient struct {
	inventory.TaskClient

	mu      sync.Mutex
	tasks   map[int]*ent.Task
	claimed map[int]int
	// failOn, when set, makes Update reject that many transitions to that status.
	failOn        task.Status
	failRemaining int
}

func newFakeTaskClient(tasks ...*ent.Task) *fakeTaskClient {
	f := &fakeTaskClient{
		tasks:   make(map[int]*ent.Task),
		claimed: make(map[int]int),
	}
	for _, t := range tasks {
		f.tasks[t.ID] = copyTaskModel(t)
	}
	return f
}

// copyTaskModel returns a copy of the row, the way reading it from the database
// would. Without it, the model a worker mutates would be the very object the caller
// queued, which production never does.
func copyTaskModel(model *ent.Task) *ent.Task {
	copied := *model
	if model.PublicState != nil {
		state := *model.PublicState
		copied.PublicState = &state
	}
	// The user edge is immutable for the harness, so sharing it is safe.
	return &copied
}

func (f *fakeTaskClient) ClaimPendingTask(_ context.Context, taskID int, allowProcessing bool) (*ent.Task, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	model, ok := f.tasks[taskID]
	if !ok {
		return nil, false, nil
	}

	allowed := model.Status == task.StatusQueued || model.Status == task.StatusSuspending
	if allowProcessing {
		allowed = allowed || model.Status == task.StatusProcessing
	}
	if !allowed {
		return nil, false, nil
	}

	f.claimed[taskID]++
	model.Status = task.StatusProcessing
	return model, true, nil
}

func (f *fakeTaskClient) claimCount(taskID int) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.claimed[taskID]
}

// New mirrors the database client's contract of returning the stored model. The
// harness always registers the task up front, so the model it is persisting is
// already the one held here.
func (f *fakeTaskClient) New(_ context.Context, args *inventory.TaskArgs) (*ent.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, existing := range f.tasks {
		if existing.Type == args.Type {
			f.applyArgs(existing, args)
			return existing, nil
		}
	}

	return nil, fmt.Errorf("no task registered for type %q", args.Type)
}

// Update mirrors the database client: it applies the new state to the stored model
// and returns it.
func (f *fakeTaskClient) Update(_ context.Context, model *ent.Task, args *inventory.TaskArgs) (*ent.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failOn != "" && args.Status == f.failOn && f.failRemaining > 0 {
		f.failRemaining--
		return nil, fmt.Errorf("simulated failure persisting transition to %q", args.Status)
	}

	f.applyArgs(model, args)
	return model, nil
}

func (f *fakeTaskClient) applyArgs(model *ent.Task, args *inventory.TaskArgs) {
	if args.PublicState != nil {
		model.PublicState = args.PublicState
	}
	if args.Status != "" {
		model.Status = args.Status
	}
	if args.Type != "" {
		model.Type = args.Type
	}
}

// GetPendingTasks returns the tasks the database would report as unfinished, which
// is what the startup recovery sweep scans.
func (f *fakeTaskClient) GetPendingTasks(_ context.Context, taskType ...string) ([]*ent.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	wanted := make(map[string]bool, len(taskType))
	for _, t := range taskType {
		wanted[t] = true
	}

	var found []*ent.Task
	for _, model := range f.tasks {
		if len(wanted) > 0 && !wanted[model.Type] {
			continue
		}

		switch model.Status {
		case task.StatusQueued, task.StatusProcessing, task.StatusSuspending:
			found = append(found, copyTaskModel(model))
		}
	}

	return found, nil
}

// fakeWorkTask is a minimal resumable task.
type fakeWorkTask struct {
	*DBTask
}

func newFakeWorkTask(id int, status task.Status, resumeAt int64) *fakeWorkTask {
	t := &ent.Task{
		ID:           id,
		Type:         "fake",
		Status:       status,
		PrivateState: "{}",
		PublicState:  &types.TaskPublicState{ResumeTime: resumeAt},
	}
	// The queue reads a task's owner when it persists a state transition, and the
	// database client always supplies one, substituting the anonymous user for tasks
	// with no user edge. The harness does the same up front.
	t.SetUser(&ent.User{ID: 0, Email: "anonymous"})

	return &fakeWorkTask{DBTask: &DBTask{Task: t}}
}

func (t *fakeWorkTask) Do(context.Context) (task.Status, error) {
	return task.StatusCompleted, nil
}

func (t *fakeWorkTask) ShouldPersist() bool { return true }

func testLogger() logging.Logger {
	return logging.NewConsoleLogger(logging.LevelError)
}

// testPool returns a pool against a dedicated Redis database, cleared once for the
// whole test binary so that tests running in sequence cannot clobber each other
// mid-flight.
var poolOnce sync.Once

func testPool(t *testing.T) *redis.Pool {
	t.Helper()

	password := os.Getenv("TEST_REDIS_PASSWORD")
	if password == "" {
		t.Skip("TEST_REDIS_PASSWORD not set")
	}

	pool := redis.Pool{
		MaxIdle:     8,
		IdleTimeout: 30 * time.Second,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", "127.0.0.1:6379", redis.DialPassword(password), redis.DialDatabase(9))
		},
	}

	poolOnce.Do(func() {
		conn := pool.Get()
		defer conn.Close()
		if _, err := conn.Do("FLUSHDB"); err != nil {
			t.Fatalf("cannot reach redis: %s", err)
		}
	})

	return &pool
}

// cleanupQueue removes every Redis key a queue owns, so repeated runs of the suite
// cannot observe each other's advertisements.
func cleanupQueue(t *testing.T, pool *redis.Pool, name string) {
	t.Helper()

	conn := pool.Get()
	defer conn.Close()

	prefix := streamKeyPrefix + name
	keys, err := redis.Strings(conn.Do("KEYS", prefix+"*"))
	if err != nil {
		return
	}

	if len(keys) == 0 {
		return
	}

	if _, err := conn.Do("DEL", redis.Args{}.AddFlat(keys)...); err != nil {
		t.Logf("failed to clean up keys for %q: %s", name, err)
	}
}

// testScheduler returns a scheduler bound to a queue name unique to the test.
func testScheduler(t *testing.T, name string, tc *fakeTaskClient) (*streamScheduler, *redis.Pool) {
	t.Helper()

	pool := testPool(t)
	cleanupQueue(t, pool, name)

	s := NewStreamScheduler(testLogger(), pool, "instance-a", name, tc)
	if err := s.Init(context.Background()); err != nil {
		t.Fatalf("init: %s", err)
	}
	s.Start()

	t.Cleanup(func() {
		_ = s.Shutdown()
		cleanupQueue(t, pool, name)
	})

	return s, pool
}

// TestStreamDeduplicatesAdvertisements verifies repeated submissions produce a
// single stream entry.
func TestStreamDeduplicatesAdvertisements(t *testing.T) {
	work := newFakeWorkTask(1, task.StatusQueued, 0)
	tc := newFakeTaskClient(work.Task)
	s, pool := testScheduler(t, "t_dedupe", tc)

	ctx := context.Background()

	for range 5 {
		if err := s.Queue(ctx, work); err != nil {
			t.Fatalf("queue: %s", err)
		}
	}

	conn := pool.Get()
	defer conn.Close()

	length, err := redis.Int(conn.Do("XLEN", s.stream))
	if err != nil {
		t.Fatalf("xlen: %s", err)
	}
	if length != 1 {
		t.Fatalf("expected 1 advertisement, got %d", length)
	}
}

// TestStreamDeliversToSingleConsumer verifies a task advertised once is claimed
// once, and that the entry is settled afterwards.
func TestStreamDeliversToSingleConsumer(t *testing.T) {
	work := newFakeWorkTask(1, task.StatusQueued, 0)
	tc := newFakeTaskClient(work.Task)
	s, pool := testScheduler(t, "t_single", tc)

	ctx := context.Background()
	if err := s.Queue(ctx, work); err != nil {
		t.Fatalf("queue: %s", err)
	}

	got, err := s.Request(ctx)
	if err != nil {
		t.Fatalf("request: %s", err)
	}
	if got.ID() != 1 {
		t.Fatalf("expected task 1, got %d", got.ID())
	}
	if n := tc.claimCount(1); n != 1 {
		t.Fatalf("expected 1 claim, got %d", n)
	}

	if err := s.Ack(ctx, got); err != nil {
		t.Fatalf("ack: %s", err)
	}

	conn := pool.Get()
	defer conn.Close()
	if pending := pendingCount(t, conn, s.stream); pending != 0 {
		t.Fatalf("expected 0 pending entries after ack, got %d", pending)
	}
}

// TestStreamRedeliversAbandonedEntry verifies that an entry whose owner stopped
// heartbeating is taken over, and that the takeover is the only thing permitted to
// recover a task the database still reports as processing.
func TestStreamRedeliversAbandonedEntry(t *testing.T) {
	stranded := newFakeWorkTask(1, task.StatusProcessing, 0)
	tc := newFakeTaskClient(stranded.Task)

	pool := testPool(t)
	cleanupQueue(t, pool, "t_reclaim")

	s := NewStreamScheduler(testLogger(), pool, "instance-b", "t_reclaim", tc)
	if err := s.Init(context.Background()); err != nil {
		t.Fatalf("init: %s", err)
	}
	t.Cleanup(func() {
		_ = s.Shutdown()
		cleanupQueue(t, pool, "t_reclaim")
	})

	// A previous instance advertised the task and took the entry, then stopped
	// proving it was alive while the task was still marked processing.
	dead := pool.Get()
	defer dead.Close()

	if _, err := dead.Do("XADD", s.stream, "*", "id", 1, "type", "fake"); err != nil {
		t.Fatalf("xadd: %s", err)
	}
	reply, err := dead.Do("XREADGROUP", "GROUP", streamGroupName, "dead-instance", "COUNT", 1, "STREAMS", s.stream, ">")
	if err != nil {
		t.Fatalf("xreadgroup: %s", err)
	}
	entries, err := parseXRead(reply)
	if err != nil {
		t.Fatalf("parse: %s", err)
	}
	if len(entries) != 1 || entries[0].taskID != 1 {
		t.Fatalf("unexpected entries: %+v", entries)
	}

	// Age the entry past the reclaim window without waiting for it.
	if _, err := dead.Do("XCLAIM", s.stream, streamGroupName, "dead-instance", 0, entries[0].entryID, "JUSTID", "IDLE", 999999); err != nil {
		t.Fatalf("xclaim: %s", err)
	}

	// Run the reclaim pass directly rather than waiting for its ticker.
	s.reclaim()

	select {
	case entry := <-s.reclaimed:
		if entry.taskID != 1 {
			t.Fatalf("expected task 1, got %d", entry.taskID)
		}
		if !entry.recover {
			t.Fatal("a reclaimed entry must be allowed to recover a processing task")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("entry was not reclaimed")
	}

	// A fresh advertisement must not claim a task that may be running elsewhere.
	_, claimed, err := tc.ClaimPendingTask(context.Background(), 1, false)
	if err != nil {
		t.Fatalf("claim: %s", err)
	}
	if claimed {
		t.Fatal("a fresh advertisement must not claim a processing task")
	}

	// A reclaimed delivery recovers it.
	_, claimed, err = tc.ClaimPendingTask(context.Background(), 1, true)
	if err != nil {
		t.Fatalf("claim: %s", err)
	}
	if !claimed {
		t.Fatal("a reclaimed entry must recover a stranded processing task")
	}
}

// TestStreamPromotesDeferredTask verifies a task suspended to a future time is held
// out of the stream until that time, then advertised.
func TestStreamPromotesDeferredTask(t *testing.T) {
	far := newFakeWorkTask(1, task.StatusSuspending, time.Now().Add(time.Hour).Unix())
	tc := newFakeTaskClient(far.Task)
	s, pool := testScheduler(t, "t_deferred", tc)

	ctx := context.Background()

	// Deferred well into the future: must not reach the stream.
	if err := s.Queue(ctx, far); err != nil {
		t.Fatalf("queue: %s", err)
	}

	conn := pool.Get()
	defer conn.Close()

	length, err := redis.Int(conn.Do("XLEN", s.stream))
	if err != nil {
		t.Fatalf("xlen: %s", err)
	}
	if length != 0 {
		t.Fatalf("a deferred task must not be advertised, got %d entries", length)
	}

	// A promotion pass must not pick it up early.
	s.promote()

	length, err = redis.Int(conn.Do("XLEN", s.stream))
	if err != nil {
		t.Fatalf("xlen: %s", err)
	}
	if length != 0 {
		t.Fatalf("a task that is not yet due must not be advertised, got %d entries", length)
	}

	// Bring it forward and confirm the promotion loop advertises it.
	if _, err := conn.Do("ZADD", s.delayed, time.Now().Add(-time.Second).Unix(), 1); err != nil {
		t.Fatalf("zadd: %s", err)
	}
	s.promote()

	length, err = redis.Int(conn.Do("XLEN", s.stream))
	if err != nil {
		t.Fatalf("xlen: %s", err)
	}
	if length != 1 {
		t.Fatalf("expected the due task to be advertised, got %d entries", length)
	}
}

// TestStreamReAdvertisesAfterSuspend is the regression guard for the stranding bug:
// a worker that suspends a task re-queues it while its entry is still held, so the
// acknowledgement has to leave the task advertised rather than swallowing it.
func TestStreamReAdvertisesAfterSuspend(t *testing.T) {
	work := newFakeWorkTask(1, task.StatusQueued, 0)
	tc := newFakeTaskClient(work.Task)
	s, pool := testScheduler(t, "t_suspend", tc)

	ctx := context.Background()
	if err := s.Queue(ctx, work); err != nil {
		t.Fatalf("queue: %s", err)
	}

	got, err := s.Request(ctx)
	if err != nil {
		t.Fatalf("request: %s", err)
	}

	// The worker gives up for now and defers the task into the future, which is what
	// the retry path does. It operates on the task it was handed, and the state
	// machine persists the suspension before the entry is acknowledged.
	got.ResumeAfter(time.Hour)
	got.Model().Status = task.StatusSuspending
	if err := s.Queue(ctx, got); err != nil {
		t.Fatalf("re-queue: %s", err)
	}

	if err := s.Ack(ctx, got); err != nil {
		t.Fatalf("ack: %s", err)
	}

	conn := pool.Get()
	defer conn.Close()

	// The deferred task is now recorded for later.
	score, err := redis.Int64(conn.Do("ZSCORE", s.delayed, 1))
	if err != nil {
		t.Fatalf("the deferred task was not recorded: %s", err)
	}
	if score <= time.Now().Unix() {
		t.Fatalf("expected a future resume time, got %d", score)
	}

	// Nothing is left unacknowledged, so a crash cannot resurrect a task that was
	// deliberately deferred.
	if pending := pendingCount(t, conn, s.stream); pending != 0 {
		t.Fatalf("expected the entry to be settled, got %d pending", pending)
	}

	// Bring the resume time forward and confirm the task comes back around.
	before, err := redis.Int(conn.Do("XLEN", s.stream))
	if err != nil {
		t.Fatalf("xlen: %s", err)
	}

	if _, err := conn.Do("ZADD", s.delayed, time.Now().Add(-time.Second).Unix(), 1); err != nil {
		t.Fatalf("zadd: %s", err)
	}
	s.promote()

	after, err := redis.Int(conn.Do("XLEN", s.stream))
	if err != nil {
		t.Fatalf("xlen: %s", err)
	}
	if after != before+1 {
		t.Fatalf("expected exactly one new advertisement, got %d new", after-before)
	}

	// The task must be runnable again, not merely advertised.
	again, err := s.Request(ctx)
	if err != nil {
		t.Fatalf("request: %s", err)
	}
	if again.ID() != 1 {
		t.Fatalf("expected task 1 to be delivered again, got %d", again.ID())
	}
	if n := tc.claimCount(1); n != 2 {
		t.Fatalf("expected the task to be claimed twice in total, got %d", n)
	}
}

// TestStreamReAdvertisesTaskWhoseEntryWasLost covers the complement: a task left
// queued with no entry at all, which is what a crash between persisting the task and
// advertising it leaves behind, must still be recoverable.
func TestStreamReAdvertisesTaskWhoseEntryWasLost(t *testing.T) {
	tc := newFakeTaskClient()
	s, pool := testScheduler(t, "t_lost", tc)

	conn := pool.Get()
	defer conn.Close()

	// An advertisement marker leaked by a process that died before publishing must
	// not be able to block the recovery sweep.
	if _, err := conn.Do("SET", s.inflightKey(1), "dead-instance", "EX", 3600); err != nil {
		t.Fatalf("set: %s", err)
	}

	// The repair sweep publishes unconditionally.
	repairCtx := context.WithValue(context.Background(), repairCtx{}, true)
	if err := s.Queue(repairCtx, newFakeWorkTask(1, task.StatusQueued, 0)); err != nil {
		t.Fatalf("queue: %s", err)
	}

	length, err := redis.Int(conn.Do("XLEN", s.stream))
	if err != nil {
		t.Fatalf("xlen: %s", err)
	}
	if length != 1 {
		t.Fatalf("expected the recovery sweep to advertise the task, got %d entries", length)
	}
}

// TestParseRealReplies exercises the hand-written reply parsers against real Redis
// output, which is the part a fake cannot vouch for.
func TestParseRealReplies(t *testing.T) {
	pool := testPool(t)
	s := NewStreamScheduler(testLogger(), pool, "parser", "t_parse", newFakeTaskClient())
	if err := s.Init(context.Background()); err != nil {
		t.Fatalf("init: %s", err)
	}

	conn := pool.Get()
	defer conn.Close()

	if _, err := conn.Do("XADD", s.stream, "*", "id", 42, "type", "fake"); err != nil {
		t.Fatalf("xadd: %s", err)
	}

	reply, err := conn.Do("XREADGROUP", "GROUP", streamGroupName, "parser", "COUNT", 1, "STREAMS", s.stream, ">")
	if err != nil {
		t.Fatalf("xreadgroup: %s", err)
	}
	entries, err := parseXRead(reply)
	if err != nil {
		t.Fatalf("parse: %s", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].taskID != 42 || entries[0].taskType != "fake" {
		t.Fatalf("entry mis-parsed: %+v", entries[0])
	}
	if entries[0].entryID == "" {
		t.Fatal("entry has no id")
	}

	// A drained stream must yield no entries rather than an error.
	reply, err = conn.Do("XREADGROUP", "GROUP", streamGroupName, "parser", "COUNT", 1, "STREAMS", s.stream, ">")
	if err != nil {
		t.Fatalf("xreadgroup: %s", err)
	}
	entries, err = parseXRead(reply)
	if err != nil {
		t.Fatalf("parse of empty read: %s", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no entries, got %d", len(entries))
	}

	// Age the delivered entry so it becomes reclaimable, then verify the takeover
	// reply is parsed too.
	if _, err := conn.Do("XCLAIM", s.stream, streamGroupName, "parser", 0, entries0EntryID(t, conn, s.stream), "JUSTID", "IDLE", 999999); err != nil {
		t.Fatalf("xclaim: %s", err)
	}

	reply, err = conn.Do("XAUTOCLAIM", s.stream, streamGroupName, "other", 1000, "0-0", "COUNT", 10)
	if err != nil {
		t.Fatalf("xautoclaim: %s", err)
	}
	reclaimed := parseXAutoClaim(reply)
	if len(reclaimed) != 1 {
		t.Fatalf("expected 1 reclaimed entry, got %d (%v)", len(reclaimed), reply)
	}
	if reclaimed[0].taskID != 42 || reclaimed[0].entryID == "" {
		t.Fatalf("reclaimed entry mis-parsed: %+v", reclaimed[0])
	}
}

// entries0EntryID returns the id of the first entry currently on the stream.
func entries0EntryID(t *testing.T, conn redis.Conn, stream string) string {
	t.Helper()

	reply, err := conn.Do("XRANGE", stream, "-", "+")
	if err != nil {
		t.Fatalf("xrange: %s", err)
	}

	rows, ok := reply.([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("unexpected xrange reply: %v", reply)
	}

	first, ok := rows[0].([]any)
	if !ok || len(first) == 0 {
		t.Fatalf("unexpected xrange row: %v", rows[0])
	}

	return string(redisBytes(first[0]))
}

// pendingCount reports how many entries are awaiting acknowledgement. XPENDING
// without a range returns a summary whose first element is the total, so it is read
// positionally rather than as a bare integer.
func pendingCount(t *testing.T, conn redis.Conn, stream string) int {
	t.Helper()

	reply, err := conn.Do("XPENDING", stream, streamGroupName)
	if err != nil {
		t.Fatalf("xpending: %s", err)
	}

	summary, ok := reply.([]any)
	if !ok || len(summary) == 0 {
		t.Fatalf("unexpected xpending reply: %v", reply)
	}

	count, err := redis.Int(summary[0], nil)
	if err != nil {
		t.Fatalf("unexpected xpending count: %s", err)
	}

	return count
}

// UpdateFailTimes makes Update reject the next n transitions to the given status,
// standing in for a transient database failure on the persist path.
func (f *fakeTaskClient) UpdateFailTimes(status task.Status, n int) {
	f.mu.Lock()
	f.failOn = status
	f.failRemaining = n
	f.mu.Unlock()
}
