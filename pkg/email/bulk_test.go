package email

import (
	"context"
	"net/url"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
	"github.com/stretchr/testify/require"
)

// bulkMailTestSettings supplies only the site fields a bulk mail render reads.
type bulkMailTestSettings struct {
	setting.Provider
}

func (bulkMailTestSettings) SiteBasic(context.Context) *setting.SiteBasic {
	return &setting.SiteBasic{Name: "Example Cloud", Title: "Files"}
}

func (bulkMailTestSettings) Logo(context.Context) *setting.Logo {
	return &setting.Logo{Normal: "/logo.svg", Light: "/logo_light.svg"}
}

func (bulkMailTestSettings) SiteURL(context.Context) *url.URL {
	siteURL, _ := url.Parse("https://cloud.example.com")
	return siteURL
}

// TestRenderBulkMailSubstitutesRecipient proves a message can address the person
// receiving it, and that the same template renders differently per recipient.
func TestRenderBulkMailSubstitutesRecipient(t *testing.T) {
	ctx := context.Background()
	settings := bulkMailTestSettings{}

	title, body, err := RenderBulkMail(settings, ctx,
		&ent.User{Nick: "Alice", Email: "alice@example.com"},
		"Hello {{ .User.Nick }}", "<p>{{ .SiteBasic.Name }} welcomes {{ .User.Email }}</p>")
	require.NoError(t, err)
	require.Equal(t, "Hello Alice", title)
	require.Equal(t, "<p>Example Cloud welcomes alice@example.com</p>", body)

	otherTitle, _, err := RenderBulkMail(settings, ctx,
		&ent.User{Nick: "Bob", Email: "bob@example.com"},
		"Hello {{ .User.Nick }}", "")
	require.NoError(t, err)
	require.Equal(t, "Hello Bob", otherTitle)
}

// TestRenderBulkMailEscapesBodyButNotTitle guards the separation between the two
// template engines: a nickname containing markup must stay text in the HTML body,
// while a subject line must not gain entity escapes.
func TestRenderBulkMailEscapesBodyButNotTitle(t *testing.T) {
	ctx := context.Background()
	user := &ent.User{Nick: `A & B <script>`, Email: "x@example.com"}

	title, body, err := RenderBulkMail(bulkMailTestSettings{}, ctx, user, "{{ .User.Nick }}", "{{ .User.Nick }}")
	require.NoError(t, err)

	require.Equal(t, "A & B <script>", title)
	require.Equal(t, "A &amp; B &lt;script&gt;", body)
	require.NotContains(t, body, "<script>")
}

// TestRenderBulkMailReportsTemplateErrors ensures a malformed template surfaces as
// an error rather than being delivered literally.
func TestRenderBulkMailReportsTemplateErrors(t *testing.T) {
	_, _, err := RenderBulkMail(bulkMailTestSettings{}, context.Background(),
		&ent.User{Nick: "Alice"}, "{{ .User.Nick ", "body")
	require.Error(t, err)

	_, _, err = RenderBulkMail(bulkMailTestSettings{}, context.Background(),
		&ent.User{Nick: "Alice"}, "title", "{{ .User.Nick ")
	require.Error(t, err)
}

func TestUndeliverableRecognizesQQPlaceholder(t *testing.T) {
	require.True(t, Undeliverable("12345@login.qq.com"))
	require.False(t, Undeliverable("user@qq.com"))
	require.False(t, Undeliverable("user@example.com"))
	require.False(t, Undeliverable(""))
}
