package email

import (
	"context"
	"fmt"
	"html/template"
	"strings"
	texttemplate "text/template"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
)

// BulkMailContext is the data a bulk mail message is rendered against.
//
// It mirrors the context of the transactional templates: the same site fields
// are available, plus the recipient, so an administrator can address a user by
// name in the same way the built in emails do.
type BulkMailContext struct {
	*CommonContext
	User *ent.User
}

// BulkMailVariables lists the placeholders a bulk mail message may use, for
// display in the administrator UI.
var BulkMailVariables = []struct {
	Name string
	Des  string
}{
	{".SiteBasic.Name", "Site name"},
	{".SiteBasic.Title", "Site title"},
	{".SiteBasic.Description", "Site description"},
	{".SiteUrl", "Site URL"},
	{".Logo.Normal", "Logo URL for light theme"},
	{".Logo.Light", "Logo URL for dark theme"},
	{".User.Nick", "Recipient nickname"},
	{".User.Email", "Recipient email address"},
}

// NewBulkMailContext builds the render context for one recipient.
func NewBulkMailContext(settings setting.Provider, ctx context.Context, user *ent.User) *BulkMailContext {
	return &BulkMailContext{
		CommonContext: commonContext(ctx, settings),
		User:          user,
	}
}

func (c *BulkMailContext) templateData() map[string]any {
	return map[string]any{
		"SiteBasic": map[string]string{
			"Name":        c.SiteBasic.Name,
			"Title":       c.SiteBasic.Title,
			"ID":          c.SiteBasic.ID,
			"Description": c.SiteBasic.Description,
			"Script":      c.SiteBasic.Script,
		},
		"Logo": map[string]string{
			"Normal": c.Logo.Normal,
			"Light":  c.Logo.Light,
		},
		"SiteUrl": c.SiteUrl,
		"User": map[string]any{
			"Nick":  c.User.Nick,
			"Email": c.User.Email,
		},
	}
}

// RenderBulkMail renders the subject and body for one recipient.
//
// The subject is plain text while the body is HTML, which is why they use
// different template engines: escaping an interpolated value in a subject line
// would put "&amp;" in front of the recipient, whereas the body must escape it to
// keep a nickname containing markup from becoming markup.
func RenderBulkMail(settings setting.Provider, ctx context.Context, user *ent.User, title, body string) (string, string, error) {
	renderCtx := NewBulkMailContext(settings, ctx, user)
	data := renderCtx.templateData()

	titleTmpl, err := texttemplate.New("bulkMailTitle").Parse(title)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse mail title: %w", err)
	}

	var resTitle strings.Builder
	if err := titleTmpl.Execute(&resTitle, data); err != nil {
		return "", "", fmt.Errorf("failed to execute mail title: %w", err)
	}

	bodyTmpl, err := template.New("bulkMailBody").Parse(body)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse mail body: %w", err)
	}

	var resBody strings.Builder
	if err := bodyTmpl.Execute(&resBody, data); err != nil {
		return "", "", fmt.Errorf("failed to execute mail body: %w", err)
	}

	return resTitle.String(), resBody.String(), nil
}
