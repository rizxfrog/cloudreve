package admin

import (
	"context"
	"strings"

	"github.com/cloudreve/Cloudreve/v4/application/dependency"
	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/user"
	"github.com/cloudreve/Cloudreve/v4/inventory"
	"github.com/cloudreve/Cloudreve/v4/pkg/email"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/workflows"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/serializer"
	"github.com/gin-gonic/gin"
	"github.com/samber/lo"
)

type (
	// BulkMailRecipientService previews the audience a message would reach.
	BulkMailRecipientService struct {
		Groups        []int    `json:"groups"`
		Statuses      []string `json:"statuses"`
		Nick          string   `json:"nick"`
		Email         string   `json:"email"`
		EmailSuffixes []string `json:"email_suffixes"`
	}
	BulkMailRecipientParamCtx struct{}

	BulkMailRecipientPreview struct {
		// Count is the total number of accounts matching the filter.
		Count int `json:"count"`
		// Undeliverable is how many of them have no mailbox and will be skipped.
		Undeliverable int `json:"undeliverable"`
		// Sample lists a few matching recipients so an administrator can sanity
		// check the filter against real accounts.
		Sample []BulkMailRecipient `json:"sample"`
	}

	BulkMailRecipient struct {
		ID          int    `json:"id"`
		Nick        string `json:"nick"`
		Email       string `json:"email"`
		Status      string `json:"status"`
		Group       string `json:"group"`
		HashID      string `json:"hash_id"`
		Deliverable bool   `json:"deliverable"`
	}

	// SendBulkMailService queues a message for delivery to a filtered audience.
	SendBulkMailService struct {
		Groups        []int    `json:"groups"`
		Statuses      []string `json:"statuses"`
		Nick          string   `json:"nick"`
		Email         string   `json:"email"`
		EmailSuffixes []string `json:"email_suffixes"`

		Title string `json:"title" binding:"required,max=255"`
		Body  string `json:"body" binding:"required,max=1048576"`

		// ConfirmRecipients is the count the administrator was shown. Delivery is
		// refused if the audience has since grown beyond it, so a message cannot
		// reach more people than the sender reviewed.
		ConfirmRecipients int `json:"confirm_recipients" binding:"required,min=1"`
	}
	SendBulkMailParamCtx struct{}

	SendBulkMailResponse struct {
		TaskID string `json:"task_id"`
		// Recipients is how many addresses the task will actually deliver to. It
		// excludes accounts with no usable address, matching the count the sender
		// confirmed.
		Recipients int `json:"recipients"`
	}
)

// sampleRecipientLimit bounds how many recipients a preview returns.
const sampleRecipientLimit = 10

// bulkMailFilter validates the shared filter fields and converts them.
func bulkMailFilter(groups []int, statuses []string, nick, emailAddr string, suffixes []string) (inventory.UserFilter, error) {
	filter := inventory.UserFilter{
		GroupIDs:      groups,
		Nick:          nick,
		Email:         emailAddr,
		EmailSuffixes: workflows.NormalizeRecipientSuffixes(suffixes),
	}

	for _, s := range statuses {
		if s == "" {
			continue
		}
		status := user.Status(s)
		if err := user.StatusValidator(status); err != nil {
			return filter, serializer.NewError(serializer.CodeParamErr, "Invalid user status: "+s, err)
		}
		filter.Statuses = append(filter.Statuses, status)
	}

	return filter, nil
}

// withUndeliverableExcluded narrows a filter to the recipients that have a real
// mailbox.
//
// Suffix filters on a filter are OR'ed, so "only the placeholder ones" cannot be
// expressed by adding a suffix. Subtracting the deliverable count from the total
// answers the same question, and stays correct for any future placeholder
// domain added to Undeliverable.
func withUndeliverableExcluded(filter inventory.UserFilter) inventory.UserFilter {
	filter.NotEmailSuffixes = append(filter.NotEmailSuffixes, email.QQPlaceholderSuffix)
	return filter
}

// previewContext requests the eager loading a preview renders.
func previewContext(c *gin.Context) context.Context {
	return context.WithValue(c, inventory.LoadUserGroup{}, true)
}

func (s *BulkMailRecipientService) Preview(c *gin.Context) (*BulkMailRecipientPreview, error) {
	dep := dependency.FromContext(c)
	hasher := dep.HashIDEncoder()
	userClient := dep.UserClient()

	filter, err := bulkMailFilter(s.Groups, s.Statuses, s.Nick, s.Email, s.EmailSuffixes)
	if err != nil {
		return nil, err
	}

	ctx := previewContext(c)

	count, err := userClient.CountUsers(ctx, filter)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to count recipients", err)
	}

	users, err := userClient.ListUsersAfterID(ctx, &inventory.ListUserAfterIDParameters{
		Limit:      sampleRecipientLimit,
		UserFilter: filter,
	})
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to list recipients", err)
	}

	undeliverable := 0
	if count > 0 {
		deliverable, err := userClient.CountUsers(ctx, withUndeliverableExcluded(filter))
		if err != nil {
			return nil, serializer.NewError(serializer.CodeDBError, "Failed to count recipients", err)
		}
		undeliverable = count - deliverable
	}

	return &BulkMailRecipientPreview{
		Count:         count,
		Undeliverable: undeliverable,
		Sample: lo.Map(users, func(u *ent.User, _ int) BulkMailRecipient {
			group := ""
			if u.Edges.Group != nil {
				group = u.Edges.Group.Name
			}
			return BulkMailRecipient{
				ID:          u.ID,
				Nick:        u.Nick,
				Email:       u.Email,
				Status:      string(u.Status),
				Group:       group,
				HashID:      hashid.EncodeUserID(hasher, u.ID),
				Deliverable: !email.Undeliverable(u.Email),
			}
		}),
	}, nil
}

func (s *SendBulkMailService) Send(c *gin.Context) (*SendBulkMailResponse, error) {
	dep := dependency.FromContext(c)
	userClient := dep.UserClient()

	// A bulk send with no working SMTP configuration would be accepted and then
	// fail recipient by recipient. Refusing up front says so once.
	if !dep.SettingProvider().SMTP(c).Configured() {
		return nil, serializer.NewError(serializer.CodeInternalSetting, "SMTP is not configured", nil)
	}

	filter, err := bulkMailFilter(s.Groups, s.Statuses, s.Nick, s.Email, s.EmailSuffixes)
	if err != nil {
		return nil, err
	}

	ctx := previewContext(c)

	count, err := userClient.CountUsers(ctx, filter)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to count recipients", err)
	}

	if count == 0 {
		return nil, serializer.NewError(serializer.CodeParamErr, "No recipients match the filter", nil)
	}

	// The audience is recomputed rather than trusted from the request, and a
	// growth in it since the preview aborts the send: a filter is a query, not a
	// list, so its meaning can change between confirming and sending.
	if count > s.ConfirmRecipients {
		return nil, serializer.NewError(serializer.CodeParamErr,
			"Recipient count changed since it was confirmed, please review and send again", nil)
	}

	// The count the sender reviewed and this response both describe deliveries,
	// so addresses the send will skip must not be counted here. The guard above
	// deliberately uses the wider matched count, since that is what may grow.
	deliverable, err := userClient.CountUsers(ctx, withUndeliverableExcluded(filter))
	if err != nil {
		return nil, serializer.NewError(serializer.CodeDBError, "Failed to count recipients", err)
	}

	if deliverable == 0 {
		return nil, serializer.NewError(serializer.CodeParamErr, "No recipients can receive mail", nil)
	}

	task, err := workflows.NewBulkMailTask(c, filter, strings.TrimSpace(s.Title), s.Body)
	if err != nil {
		return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to create task", err)
	}

	if err := dep.IoIntenseQueue(c).QueueTask(c, task); err != nil {
		return nil, serializer.NewError(serializer.CodeCreateTaskError, "Failed to queue task", err)
	}

	return &SendBulkMailResponse{
		TaskID:     hashid.EncodeTaskID(dep.HashIDEncoder(), task.ID()),
		Recipients: deliverable,
	}, nil
}
