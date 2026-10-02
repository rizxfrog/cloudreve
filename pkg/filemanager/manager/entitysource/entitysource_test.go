package entitysource

import (
	"context"
	"io"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/boolset"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/driver"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/request"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
	"github.com/stretchr/testify/require"
)

// The tests below pin the proxy/direct decision for downloads, which is what
// decides whether a client fetches object bytes from upstream or through this
// server. Uploads are governed separately by driver.UploadProxyRequired.

type stubAuth struct{}

func (stubAuth) Sign(body string, expires int64) string { return "sig" }
func (stubAuth) Check(body, sign string) error          { return nil }

type stubSettings struct {
	setting.Provider
	site string
}

func (s stubSettings) SiteURL(context.Context) *url.URL {
	u, _ := url.Parse(s.site)
	return u
}

type stubMime struct{}

func (stubMime) TypeByName(string) string { return "application/octet-stream" }

type stubRequestClient struct{}

func (stubRequestClient) Apply(...request.Option) {}
func (stubRequestClient) Request(string, string, io.Reader, ...request.Option) *request.Response {
	return nil
}

// handlerBase carries the Handler surface that no test under here exercises.
// The methods are spelled out rather than embedded so the interface assertions
// below keep catching a change to the handler contract.
type handlerBase struct{}

func (handlerBase) Put(context.Context, *fs.UploadRequest) error { panic("unused") }
func (handlerBase) Delete(context.Context, ...string) ([]string, error) {
	panic("unused")
}
func (handlerBase) Open(context.Context, string) (*os.File, error) { panic("unused") }
func (handlerBase) LocalPath(context.Context, string) string       { return "" }
func (handlerBase) Token(context.Context, *fs.UploadSession, *fs.UploadRequest) (*fs.UploadCredential, error) {
	panic("unused")
}
func (handlerBase) CancelToken(context.Context, *fs.UploadSession) error { return nil }
func (handlerBase) CompleteUpload(context.Context, *fs.UploadSession) error {
	return nil
}
func (handlerBase) List(context.Context, string, driver.ListProgressFunc, bool) ([]fs.PhysicalObject, error) {
	panic("unused")
}
func (handlerBase) MediaMeta(context.Context, string, string, string) ([]driver.MediaMeta, error) {
	panic("unused")
}

// stubHandler is a resolvable, non-streaming handler: it decides per object
// whether a directly fetchable URL exists.
type stubHandler struct {
	handlerBase
	features     *boolset.BooleanSet
	sourceURL    string
	sourceCalls  int
	hasPublicSrc func(fs.Entity) bool
}

func (h *stubHandler) Capabilities() *driver.Capabilities {
	return &driver.Capabilities{StaticFeatures: h.features}
}

func (h *stubHandler) Source(context.Context, fs.Entity, *driver.GetSourceArgs) (string, error) {
	h.sourceCalls++
	if h.sourceURL == "" {
		return "", driver.ErrNoPublicUrl
	}
	return h.sourceURL, nil
}

func (h *stubHandler) Thumb(context.Context, *time.Time, string, fs.Entity) (string, error) {
	return "", driver.ErrNoPublicUrl
}

func (h *stubHandler) HasPublicSource(e fs.Entity) bool {
	if h.hasPublicSrc == nil {
		// A handler that does not implement the resolver contract has uniformly
		// addressable objects.
		return true
	}
	return h.hasPublicSrc(e)
}

// streamOnlyHandler serves every object itself and offers no URL at all.
type streamOnlyHandler struct {
	handlerBase
	features *boolset.BooleanSet
}

func (h *streamOnlyHandler) Capabilities() *driver.Capabilities {
	return &driver.Capabilities{StaticFeatures: h.features}
}

func (h *streamOnlyHandler) OpenStream(context.Context, fs.Entity, int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("payload")), nil
}

// Source must exist to satisfy the base handler contract, but a streamer never
// reaches it because every object is served through OpenStream.
func (h *streamOnlyHandler) Source(context.Context, fs.Entity, *driver.GetSourceArgs) (string, error) {
	panic("a streamer must not be asked for a source URL")
}

func (h *streamOnlyHandler) Thumb(context.Context, *time.Time, string, fs.Entity) (string, error) {
	panic("a streamer must not be asked for a thumb URL")
}

// As a guard against silently regressing the interface contract, every stub
// must keep satisfying driver.Handler.
var (
	_ driver.Handler  = (*stubHandler)(nil)
	_ driver.Handler  = (*streamOnlyHandler)(nil)
	_ driver.Streamer = (*streamOnlyHandler)(nil)
)

func newTestEntity(t *testing.T, size int64) fs.Entity {
	t.Helper()
	return fs.NewEntity(&ent.Entity{
		ID:                    7,
		Size:                  size,
		Source:                "00/ab/cdef0123456789",
		StoragePolicyEntities: 1,
	})
}

func newTestSource(t *testing.T, handler driver.Handler, internalProxy bool) EntitySource {
	t.Helper()

	hasher, err := hashid.New("test-salt")
	require.NoError(t, err)

	policy := &ent.StoragePolicy{Settings: &types.PolicySetting{InternalProxy: internalProxy}}

	return NewEntitySource(
		newTestEntity(t, 1024),
		handler,
		policy,
		stubAuth{},
		stubSettings{site: "https://cloud.example.com"},
		hasher,
		stubRequestClient{},
		nil,
		nil,
		stubMime{},
		nil,
	)
}

func featuresWith(caps ...driver.HandlerCapability) *boolset.BooleanSet {
	features := &boolset.BooleanSet{}
	set := map[driver.HandlerCapability]bool{}
	for _, c := range caps {
		set[c] = true
	}
	boolset.Sets(set, features)
	return features
}

func isInternalProxyUrl(t *testing.T, u *EntityUrl) bool {
	t.Helper()
	parsed, err := url.Parse(u.Url)
	require.NoError(t, err)
	return strings.Contains(parsed.Path, "/file/content/") && parsed.Query().Get("sign") != ""
}

func TestUrlUsesInternalProxyWhenDownloadProxyRequired(t *testing.T) {
	handler := &stubHandler{
		features:  featuresWith(driver.HandlerCapabilityDownloadProxyRequired),
		sourceURL: "https://cdn.example.com/signed-blob",
	}
	source := newTestSource(t, handler, false)

	u, err := source.Url(context.Background())
	require.NoError(t, err)

	require.True(t, isInternalProxyUrl(t, u),
		"a handler declaring a required download proxy must be served by this server, got %q", u.Url)
	require.Zero(t, handler.sourceCalls,
		"a mandatory proxy must not resolve an upstream URL at all")
}

func TestUrlUsesInternalProxyWhenPolicyEnablesIt(t *testing.T) {
	handler := &stubHandler{
		features:  featuresWith(),
		sourceURL: "https://cdn.example.com/signed-blob",
	}
	source := newTestSource(t, handler, true)

	u, err := source.Url(context.Background())
	require.NoError(t, err)

	require.True(t, isInternalProxyUrl(t, u),
		"the policy internal proxy setting must relay the download, got %q", u.Url)
	require.Zero(t, handler.sourceCalls)
}

func TestUrlUsesDirectSourceByDefault(t *testing.T) {
	handler := &stubHandler{
		features:  featuresWith(),
		sourceURL: "https://cdn.example.com/signed-blob",
	}
	source := newTestSource(t, handler, false)

	u, err := source.Url(context.Background())
	require.NoError(t, err)

	require.Equal(t, "https://cdn.example.com/signed-blob", u.Url,
		"a plain handler must hand the client a directly fetchable URL")
	require.Equal(t, 1, handler.sourceCalls)
}

func TestUrlFallsBackToRelayForObjectWithoutPublicSource(t *testing.T) {
	// A handler that reports the object has no fetchable URL, for example an
	// object stored inline rather than as a separately addressable blob.
	handler := &stubHandler{
		features:     featuresWith(driver.HandlerCapabilityContentAddressed),
		sourceURL:    "",
		hasPublicSrc: func(fs.Entity) bool { return false },
	}
	source := newTestSource(t, handler, false)

	u, err := source.Url(context.Background())
	require.NoError(t, err)

	require.True(t, isInternalProxyUrl(t, u),
		"an object with no public URL must be relayed, got %q", u.Url)
	require.Zero(t, handler.sourceCalls,
		"the relay must be chosen without attempting a source URL")
}

func TestUrlFallsBackToRelayWhenSourceHasNoUrl(t *testing.T) {
	// The handler believed the object was fetchable but upstream said otherwise.
	handler := &stubHandler{
		features:     featuresWith(driver.HandlerCapabilityContentAddressed),
		sourceURL:    "",
		hasPublicSrc: func(fs.Entity) bool { return true },
	}
	source := newTestSource(t, handler, false)

	u, err := source.Url(context.Background())
	require.NoError(t, err)

	require.True(t, isInternalProxyUrl(t, u),
		"a source URL failure must degrade to the relay, got %q", u.Url)
	require.Equal(t, 1, handler.sourceCalls)
}

func TestUrlKeepsDirectSourceWhenPublicSourceExists(t *testing.T) {
	// A large object of the same handler is stored as a fetchable blob, so the
	// client may still be given a direct URL.
	handler := &stubHandler{
		features:     featuresWith(driver.HandlerCapabilityContentAddressed),
		sourceURL:    "https://cdn.example.com/large-blob",
		hasPublicSrc: func(fs.Entity) bool { return true },
	}
	source := newTestSource(t, handler, false)

	u, err := source.Url(context.Background())
	require.NoError(t, err)

	require.Equal(t, "https://cdn.example.com/large-blob", u.Url)
	require.Equal(t, 1, handler.sourceCalls)
}

func TestShouldInternalProxyHonoursNoInternalProxyOption(t *testing.T) {
	handler := &stubHandler{features: featuresWith(), sourceURL: "https://cdn.example.com/blob"}
	source := newTestSource(t, handler, true)

	// An explicit opt-out must reach the direct source even when the policy
	// enables the relay, which is how internal callers read metadata.
	u, err := source.Url(context.Background(), WithNoInternalProxy())
	require.NoError(t, err)

	require.Equal(t, "https://cdn.example.com/blob", u.Url)
}

func TestCanGeneratePublicUrlTracksResolvability(t *testing.T) {
	noSource := &stubHandler{
		features:     featuresWith(driver.HandlerCapabilityContentAddressed),
		hasPublicSrc: func(fs.Entity) bool { return false },
	}
	require.False(t, newTestSource(t, noSource, false).CanGeneratePublicUrl(),
		"a handler that must relay must report no public URL")

	withSource := &stubHandler{
		features:     featuresWith(driver.HandlerCapabilityContentAddressed),
		sourceURL:    "https://cdn.example.com/blob",
		hasPublicSrc: func(fs.Entity) bool { return true },
	}
	require.True(t, newTestSource(t, withSource, false).CanGeneratePublicUrl())
}

func TestStreamingHandlerHasNoPublicSource(t *testing.T) {
	// A streamer serves content itself, so it never has a fetchable URL even
	// without declaring a required download proxy.
	handler := &streamOnlyHandler{features: featuresWith()}
	require.False(t, newTestSource(t, handler, false).CanGeneratePublicUrl())
}
