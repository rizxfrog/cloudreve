package workflows

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/pkg/email"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
	"github.com/stretchr/testify/require"
)

// bulkMailTestUserClient serves a fixed user set through the cursor interface the
// task walks, so the walk itself is what is under test.
type bulkMailTestUserClient struct {
	inventory.UserClient

	users []*ent.User
	// afterIDs records the cursor each page was requested with, which is how the
	// test observes that a resumed task continues rather than restarts.
	afterIDs []int
}

func (c *bulkMailTestUserClient) CountUsers(context.Context, inventory.UserFilter) (int, error) {
	return len(c.users), nil
}

func (c *bulkMailTestUserClient) ListUsersAfterID(_ context.Context, args *inventory.ListUserAfterIDParameters) ([]*ent.User, error) {
	c.afterIDs = append(c.afterIDs, args.AfterID)

	var page []*ent.User
	for _, u := range c.users {
		if u.ID <= args.AfterID {
			continue
		}
		page = append(page, u)
		if len(page) == args.Limit {
			break
		}
	}

	return page, nil
}

// bulkMailTestSender records accepted messages, and can be made to refuse them to
// model an unreachable SMTP server.
type bulkMailTestSender struct {
	email.Driver

	recipients []string
	titles     []string
	bodies     []string
	failWith   error
}

func (s *bulkMailTestSender) Send(_ context.Context, to, title, body string) error {
	if s.failWith != nil {
		return s.failWith
	}
	s.recipients = append(s.recipients, to)
	s.titles = append(s.titles, title)
	s.bodies = append(s.bodies, body)
	return nil
}

func (s *bulkMailTestSender) Close() {}

// bulkMailTestSettings supplies only the site fields a render reads.
type bulkMailTestSettings struct {
	setting.Provider
}

func (bulkMailTestSettings) SiteBasic(context.Context) *setting.SiteBasic {
	return &setting.SiteBasic{Name: "Example Cloud"}
}

func (bulkMailTestSettings) Logo(context.Context) *setting.Logo { return &setting.Logo{} }

func (bulkMailTestSettings) SiteURL(context.Context) *url.URL {
	siteURL, _ := url.Parse("https://cloud.example.com")
	return siteURL
}

// bulkMailTestContext wires a dependency containing only the collaborators the
// task touches.
func bulkMailTestContext(t *testing.T, users *bulkMailTestUserClient, sender *bulkMailTestSender, settings setting.Provider) context.Context {
	t.Helper()

	dep := dependency.NewDependency(
		dependency.WithLogger(logging.NewConsoleLogger(logging.LevelError)),
		dependency.WithUserClient(users),
		dependency.WithEmailClient(sender),
		dependency.WithSettingProvider(settings),
	)

	return context.WithValue(context.Background(), dependency.DepCtx{}, dep)
}

func seededRecipient(id int, emailAddr string) *ent.User {
	return &ent.User{ID: id, Email: emailAddr, Nick: "user", Status: "active"}
}

var errSenderUnavailable = errors.New("smtp pool is closed")

// TestBulkMailWalksEveryRecipientOnceAcrossBatches is the task's core property:
// a send to more users than one batch covers must reach each of them exactly
// once, resuming from the persisted cursor rather than starting over.
func TestBulkMailWalksEveryRecipientOnceAcrossBatches(t *testing.T) {
	users := &bulkMailTestUserClient{}
	for i := 1; i <= BulkMailBatchSize*2+3; i++ {
		users.users = append(users.users, seededRecipient(i, addrFor(i)))
	}
	sender := &bulkMailTestSender{}
	ctx := bulkMailTestContext(t, users, sender, bulkMailTestSettings{})

	bulk, err := NewBulkMailTask(ctx, inventory.UserFilter{}, "Subject", "Body")
	require.NoError(t, err)

	// Drive the state machine the way the queue does, re-entering Do on suspend.
	iterations := 0
	for {
		iterations++
		require.Less(t, iterations, 20, "task did not converge")

		status, err := bulk.Do(ctx)
		require.NoError(t, err)
		if status == task.StatusCompleted {
			break
		}
		require.Equal(t, task.StatusSuspending, status)
	}

	require.Len(t, sender.recipients, len(users.users))
	for i, blacklisted := range users.users {
		require.Equal(t, blacklisted.Email, sender.recipients[i])
	}

	// The second page must have been requested from where the first stopped.
	require.Equal(t, []int{0}, users.afterIDs[:1])
	require.NotContains(t, users.afterIDs, users.users[0].ID)
}

// TestBulkMailSkipsPlaceholderAddresses ensures an account that cannot receive
// mail is neither counted as delivered nor abort the send.
func TestBulkMailSkipsPlaceholderAddresses(t *testing.T) {
	users := &bulkMailTestUserClient{users: []*ent.User{
		seededRecipient(1, "real@example.com"),
		seededRecipient(2, "12345@login.qq.com"),
		seededRecipient(3, "other@example.com"),
	}}
	sender := &bulkMailTestSender{}
	ctx := bulkMailTestContext(t, users, sender, bulkMailTestSettings{})

	bulk, err := NewBulkMailTask(ctx, inventory.UserFilter{}, "Subject", "Body")
	require.NoError(t, err)

	status, err := bulk.Do(ctx)
	require.NoError(t, err)
	require.Equal(t, task.StatusSuspending, status)

	require.Equal(t, []string{"real@example.com", "other@example.com"}, sender.recipients)

	summary := bulk.Summarize(nil)
	require.EqualValues(t, 2, summary.Props["queued"])
	require.EqualValues(t, 1, summary.Props["skipped"])
}

// TestBulkMailStopsAtRefusalWithoutAdvancingCursor proves a driver that refuses
// messages aborts the run, and leaves the cursor behind the refused recipient so
// a retry starts from them instead of from the beginning.
func TestBulkMailStopsAtRefusalWithoutAdvancingCursor(t *testing.T) {
	users := &bulkMailTestUserClient{users: []*ent.User{
		seededRecipient(1, "first@example.com"),
		seededRecipient(2, "second@example.com"),
		seededRecipient(3, "third@example.com"),
	}}
	sender := &bulkMailTestSender{}
	ctx := bulkMailTestContext(t, users, sender, bulkMailTestSettings{})

	bulk, err := NewBulkMailTask(ctx, inventory.UserFilter{}, "Subject", "Body")
	require.NoError(t, err)

	sender.failWith = errSenderUnavailable
	_, err = bulk.Do(ctx)
	require.ErrorIs(t, err, errSenderUnavailable)

	// Nothing may be reported as queued, and the refused recipient must be
	// retried rather than skipped.
	require.Empty(t, sender.recipients)
	summary := bulk.Summarize(nil)
	require.EqualValues(t, 0, summary.Props["queued"])

	sender.failWith = nil
	status, err := bulk.Do(ctx)
	require.NoError(t, err)
	require.Equal(t, task.StatusSuspending, status)
	require.Equal(t, []string{"first@example.com", "second@example.com", "third@example.com"}, sender.recipients)
}

// TestBulkMailRendersPerRecipient proves the delivered message is personalized
// rather than identical for everyone.
func TestBulkMailRendersPerRecipient(t *testing.T) {
	users := &bulkMailTestUserClient{users: []*ent.User{
		seededRecipient(1, "alice@example.com"),
		seededRecipient(2, "bob@example.com"),
	}}
	users.users[0].Nick = "Alice"
	users.users[1].Nick = "Bob"
	sender := &bulkMailTestSender{}
	ctx := bulkMailTestContext(t, users, sender, bulkMailTestSettings{})

	bulk, err := NewBulkMailTask(ctx, inventory.UserFilter{}, "Hi {{ .User.Nick }}", "For {{ .User.Email }}")
	require.NoError(t, err)

	_, err = bulk.Do(ctx)
	require.NoError(t, err)

	require.Equal(t, []string{"Hi Alice", "Hi Bob"}, sender.titles)
	require.Equal(t, []string{"For alice@example.com", "For bob@example.com"}, sender.bodies)
}

// TestBulkMailCompletesWhenNoRecipientMatches keeps an empty audience from
// producing a suspending task that never ends.
func TestBulkMailCompletesWhenNoRecipientMatches(t *testing.T) {
	users := &bulkMailTestUserClient{}
	sender := &bulkMailTestSender{}
	ctx := bulkMailTestContext(t, users, sender, bulkMailTestSettings{})

	bulk, err := NewBulkMailTask(ctx, inventory.UserFilter{}, "Subject", "Body")
	require.NoError(t, err)

	status, err := bulk.Do(ctx)
	require.NoError(t, err)
	require.Equal(t, task.StatusCompleted, status)
	require.Empty(t, sender.recipients)
}

func addrFor(i int) string {
	return "user" + string(rune('a'+i%26)) + "@example.com"
}

// TestBulkMailSummarizeOnPersistedTask covers the shape a task has when it is
// reloaded from the database to be listed: state is nil, so Summarize has to
// unmarshal it. State() takes the task's own lock, so summarizing while holding
// that lock deadlocks the caller -- which stalls the whole admin task list,
// since every listed row is summarized.
func TestBulkMailSummarizeOnPersistedTask(t *testing.T) {
	bulk, err := NewBulkMailTask(context.Background(), inventory.UserFilter{}, "Subject", "Body")
	require.NoError(t, err)

	// Rebuild the task the way the queue does when reading a row back, so the
	// in-memory state is empty and only PrivateState carries it.
	persisted := NewBulkMailTaskFromModel(&ent.Task{
		ID:           1,
		Type:         queue.BulkMailTaskType,
		Status:       task.StatusCompleted,
		PrivateState: bulk.State(),
	})

	done := make(chan *queue.Summary, 1)
	go func() {
		done <- persisted.Summarize(nil)
	}()

	select {
	case summary := <-done:
		require.NotNil(t, summary)
		require.Equal(t, "Subject", summary.Props[summaryKeyBulkMailTitle])
		require.EqualValues(t, 0, summary.Props[summaryKeyBulkMailTotal])
	case <-time.After(5 * time.Second):
		t.Fatal("Summarize deadlocked on a persisted task")
	}
}
