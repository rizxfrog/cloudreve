package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/task"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/email"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/queue"
)

type (
	// BulkMailTask delivers one administrator authored message to every user
	// matching a filter.
	BulkMailTask struct {
		*queue.DBTask

		l     logging.Logger
		state *BulkMailTaskState
	}
	BulkMailTaskState struct {
		Filter inventory.UserFilter `json:"filter"`
		Title  string               `json:"title"`
		Body   string               `json:"body"`

		// LastUserID is the cursor: every user at or below it has been accounted
		// for. A page based walk would drift over a set that is being written to,
		// so the walk is keyed on the one column that never moves.
		LastUserID int `json:"last_user_id"`
		Total      int `json:"total"`
		// Queued is how many recipients were handed to the mail queue, Skipped
		// how many have no deliverable address. Delivery is asynchronous, so a
		// refusal to accept a message aborts the batch instead of being counted
		// per recipient: it means the driver is unreachable, not that this
		// particular address was rejected.
		Queued  int `json:"queued"`
		Skipped int `json:"skipped"`
	}
)

const (
	// BulkMailBatchSize bounds how many recipients one iteration handles before
	// suspending, so progress is checkpointed regularly and a large list does not
	// have to fit inside a single task execution window.
	BulkMailBatchSize = 50

	// BulkMailProgressKey identifies this task's progress entry.
	BulkMailProgressKey = "bulk_mail"

	summaryKeyBulkMailTotal   = "total"
	summaryKeyBulkMailQueued  = "queued"
	summaryKeyBulkMailSkipped = "skipped"
	summaryKeyBulkMailTitle   = "title"
)

func init() {
	queue.RegisterResumableTaskFactory(queue.BulkMailTaskType, NewBulkMailTaskFromModel)
}

// NewBulkMailTask creates a task that sends a message to the users matching the
// given filter.
//
// The title and body are captured now rather than referenced: the message the
// administrator reviewed must be the message recipients get, even if the task
// waits in a queue behind other work.
func NewBulkMailTask(ctx context.Context, filter inventory.UserFilter, title, body string) (*BulkMailTask, error) {
	state := &BulkMailTaskState{
		Filter: filter,
		Title:  title,
		Body:   body,
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal state: %w", err)
	}

	return &BulkMailTask{
		DBTask: &queue.DBTask{
			Task: &ent.Task{
				Type:          queue.BulkMailTaskType,
				CorrelationID: logging.CorrelationID(ctx),
				PrivateState:  string(stateBytes),
				PublicState:   &types.TaskPublicState{},
			},
			DirectOwner: inventory.UserFromContext(ctx),
		},
	}, nil
}

func NewBulkMailTaskFromModel(model *ent.Task) queue.Task {
	return &BulkMailTask{
		DBTask: &queue.DBTask{
			Task: model,
		},
	}
}

func (m *BulkMailTask) Do(ctx context.Context) (task.Status, error) {
	dep := dependency.FromContext(ctx)
	m.l = dep.Logger()

	state := &BulkMailTaskState{}
	if err := json.Unmarshal([]byte(m.State()), state); err != nil {
		return task.StatusError, fmt.Errorf("failed to unmarshal state: %s (%w)", err, queue.CriticalErr)
	}
	m.state = state

	next, err := m.send(ctx, dep)

	// Persist progress even when this iteration failed, so a retry resumes where
	// the failure left off instead of re-messaging everyone already reached.
	newStateStr, marshalErr := json.Marshal(m.state)
	if marshalErr != nil {
		return task.StatusError, fmt.Errorf("failed to marshal state: %w", marshalErr)
	}

	m.Lock()
	m.Task.PrivateState = string(newStateStr)
	m.Unlock()

	if err != nil {
		return task.StatusError, err
	}

	if next == task.StatusCompleted {
		m.l.Info("Bulk mail finished: %d queued, %d skipped out of %d recipients.",
			m.state.Queued, m.state.Skipped, m.state.Total)
	}

	return next, nil
}

// send walks one batch of recipients and reports whether more remain.
//
// Delivery itself is asynchronous: the SMTP driver accepts a message into its
// own retry queue, so an error here means the message was not accepted, not that
// the server rejected the recipient. Per-recipient delivery failures are only
// visible in the mail server log.
func (m *BulkMailTask) send(ctx context.Context, dep dependency.Dep) (task.Status, error) {
	userClient := dep.UserClient()
	ctx = context.WithValue(ctx, inventory.LoadUserGroup{}, true)

	if m.state.Total == 0 {
		total, err := userClient.CountUsers(ctx, m.state.Filter)
		if err != nil {
			return task.StatusError, fmt.Errorf("failed to count recipients: %w", err)
		}
		m.state.Total = total
	}

	if m.state.Total == 0 {
		m.l.Info("Bulk mail matched no recipients, nothing to send.")
		return task.StatusCompleted, nil
	}

	sender := dep.EmailClient(ctx)

	users, err := userClient.ListUsersAfterID(ctx, &inventory.ListUserAfterIDParameters{
		AfterID:    m.state.LastUserID,
		Limit:      BulkMailBatchSize,
		UserFilter: m.state.Filter,
	})
	if err != nil {
		return task.StatusError, fmt.Errorf("failed to list recipients: %w", err)
	}

	if len(users) == 0 {
		return task.StatusCompleted, nil
	}

	for _, u := range users {
		// A placeholder address is not a mailbox, so it is neither a delivery nor
		// a failure.
		if !email.Undeliverable(u.Email) {
			// Render per recipient: the message may reference the recipient, and
			// the title and body must not be shared across deliveries.
			title, body, err := email.RenderBulkMail(dep.SettingProvider(), ctx, u, m.state.Title, m.state.Body)
			if err != nil {
				m.l.Warning("Failed to render bulk mail for user %d: %s", u.ID, err)
				return task.StatusError, fmt.Errorf("failed to render bulk mail: %w", err)
			}

			if err := sender.Send(ctx, u.Email, title, body); err != nil {
				// A refusal means the driver is not accepting messages at all, so
				// every remaining recipient would fail too. The cursor stays
				// behind this user so a later retry starts from them.
				m.l.Warning("Failed to queue bulk mail for user %d: %s", u.ID, err)
				return task.StatusError, fmt.Errorf("failed to queue bulk mail: %w", err)
			}
			m.state.Queued++
		} else {
			m.state.Skipped++
		}

		m.state.LastUserID = u.ID
	}

	m.l.Info("Bulk mail progress: %d/%d recipients accounted for.",
		m.state.Queued+m.state.Skipped, m.state.Total)

	// More recipients may remain. Suspending re-enters Do with persisted state,
	// which also lets the task yield between batches instead of holding a worker
	// for the whole list.
	m.ResumeAfter(0)
	return task.StatusSuspending, nil
}

func (m *BulkMailTask) Progress(ctx context.Context) queue.Progresses {
	m.Lock()
	defer m.Unlock()

	if m.state == nil {
		return queue.Progresses{}
	}

	return queue.Progresses{
		BulkMailProgressKey: {
			Total:      int64(m.state.Total),
			Current:    int64(m.state.Queued + m.state.Skipped),
			Identifier: BulkMailProgressKey,
		},
	}
}

func (m *BulkMailTask) Summarize(hasher hashid.Encoder) *queue.Summary {
	m.Lock()
	state := m.state
	m.Unlock()

	if state == nil {
		// State() takes the same lock this method uses, so it must not be called
		// while holding it.
		state = &BulkMailTaskState{}
		if err := json.Unmarshal([]byte(m.State()), state); err != nil {
			return nil
		}

		m.Lock()
		m.state = state
		m.Unlock()
	}

	return &queue.Summary{
		Props: map[string]any{
			summaryKeyBulkMailTitle:   state.Title,
			summaryKeyBulkMailTotal:   state.Total,
			summaryKeyBulkMailQueued:  state.Queued,
			summaryKeyBulkMailSkipped: state.Skipped,
		},
	}
}

// NormalizeRecipientSuffixes cleans the free form suffix list an administrator
// types into the UI: surrounding space and case are noise, and a repeated domain
// adds nothing to an OR.
func NormalizeRecipientSuffixes(raw []string) []string {
	suffixes := make([]string, 0, len(raw))
	seen := make(map[string]struct{}, len(raw))
	for _, s := range raw {
		s = strings.TrimSpace(strings.ToLower(s))
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		suffixes = append(suffixes, s)
	}

	return suffixes
}
