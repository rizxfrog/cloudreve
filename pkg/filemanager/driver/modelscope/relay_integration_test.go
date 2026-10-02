package modelscope

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/driver"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/manager/entitysource"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/cloudreve/Cloudreve/v4/pkg/request"
	"github.com/cloudreve/Cloudreve/v4/pkg/setting"
	"github.com/stretchr/testify/require"
)

// This test wires the real driver into the real entity-source read path, which
// is where the download proxy/direct decision is made. It pins two things that
// unit tests on the handler alone cannot: that relay mode serves the bytes
// through this server instead of proxying to its own route, and that direct
// mode hands the client a storage URL it can fetch itself.

type relayAuth struct{}

func (relayAuth) Sign(body string, expires int64) string { return "test-sign" }
func (relayAuth) Check(body, sign string) error          { return nil }

type relaySettings struct {
	setting.Provider
	site *url.URL
}

func (s relaySettings) SiteURL(context.Context) *url.URL { return s.site }

type relayMime struct{}

func (relayMime) TypeByName(string) string { return "application/octet-stream" }

type relayRequestClient struct{}

func (relayRequestClient) Apply(...request.Option) {}
func (relayRequestClient) Request(string, string, io.Reader, ...request.Option) *request.Response {
	return nil
}

// storageServer records whether the storage host was asked for bytes. Direct
// mode is only a URL, so a well behaved implementation never contacts it here.
type storageServer struct {
	bytes    []byte
	requests int
	server   *httptest.Server
}

func newStorageServer(t *testing.T, payload []byte) *storageServer {
	t.Helper()
	s := &storageServer{bytes: payload}
	s.server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.requests++
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(s.bytes)
	}))
	t.Cleanup(s.server.Close)
	return s
}

func newRelaySource(t *testing.T, handler driver.Handler, policy *ent.StoragePolicy, entity fs.Entity) entitysource.EntitySource {
	t.Helper()

	hasher, err := hashid.New("test-salt")
	require.NoError(t, err)

	site, err := url.Parse("https://cloud.example.com")
	require.NoError(t, err)

	// The read path resolves its context from the options, so a real caller
	// always supplies one; without it a streaming read cannot start.
	return entitysource.NewEntitySource(
		entity,
		handler,
		policy,
		relayAuth{},
		relaySettings{site: site},
		hasher,
		relayRequestClient{},
		logging.NewConsoleLogger(logging.LevelError),
		nil,
		relayMime{},
		nil,
		entitysource.WithContext(context.Background()),
	)
}

// liveRepoStub serves the repository file request: inline objects stream their
// bytes, blobs are answered with a redirect to an allowlisted storage host the
// way ModelScope serves an LFS object.
func liveRepoStub(t *testing.T, storage *storageServer, objectPath string, inline bool) *Client {
	t.Helper()

	payload := storage.bytes
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/info/lfs/objects/batch") {
			_, _ = w.Write([]byte(`{"Code":200,"Data":{"objects":[]}}`))
			return
		}
		if strings.HasSuffix(r.URL.Path, "/repo") {
			if inline {
				w.Header().Set("Content-Type", "application/octet-stream")
				_, _ = w.Write(payload)
				return
			}
			// The storage host must be on the allowlist, so the signed URL
			// points at a real storage host name rather than the test server.
			http.Redirect(w, r, "https://oss.aliyuncs.com/bucket/"+HashFromObjectPath(objectPath),
				http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(server.URL, "relay-token", "owner/repo", "datasets", "master",
		logging.NewConsoleLogger(logging.LevelError))
	require.NoError(t, err)
	client.api.Transport = server.Client().Transport
	// Bytes for the relay path are served by the test server; the guarded dial
	// is covered separately and is not what this test is about.
	client.storage.Transport = server.Client().Transport

	return client
}

func newDriverWithClient(t *testing.T, client *Client) *Driver {
	t.Helper()
	return &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}
}

func modelScopePolicy(internalProxy bool) *ent.StoragePolicy {
	return &ent.StoragePolicy{
		Type:     types.PolicyTypeModelScope,
		Settings: &types.PolicySetting{InternalProxy: internalProxy},
	}
}

func TestRelayDownloadServesInlineObjectThroughServer(t *testing.T) {
	payload := []byte("inline object content")
	storage := newStorageServer(t, payload)
	objectPath := ObjectPath("00", shaOf(t, payload))
	client := liveRepoStub(t, storage, objectPath, true)

	// The policy relays downloads, so the bytes must come through this server.
	source := newRelaySource(t, newDriverWithClient(t, client), modelScopePolicy(true),
		newSizedFakeEntity(objectPath, int64(len(payload))))

	rec := httptest.NewRecorder()
	source.Serve(rec, httptest.NewRequest(http.MethodGet, "/api/v1/file/content/x/0/y", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, string(payload), rec.Body.String(),
		"relay mode must stream the object through this server")
}

func TestRelayDownloadDoesNotLoopBackToItsOwnRoute(t *testing.T) {
	payload := []byte("inline object content")
	storage := newStorageServer(t, payload)
	objectPath := ObjectPath("00", shaOf(t, payload))
	client := liveRepoStub(t, storage, objectPath, true)

	source := newRelaySource(t, newDriverWithClient(t, client), modelScopePolicy(true),
		newSizedFakeEntity(objectPath, int64(len(payload))))

	// Serve reverse-proxies to whatever Url returns when a source URL exists.
	// This server's own content route must therefore never be treated as one,
	// or resolving it would recurse into this handler forever.
	u, err := source.Url(context.Background())
	require.NoError(t, err)
	require.Contains(t, u.Url, "/file/content/")

	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		source.Serve(rec, httptest.NewRequest(http.MethodGet, "/api/v1/file/content/x/0/y", nil))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return: it proxied to its own route and recursed")
	}
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, string(payload), rec.Body.String())
}

func TestDirectDownloadHandsClientStorageUrl(t *testing.T) {
	payload := []byte("blob content that is large")
	storage := newStorageServer(t, payload)
	objectPath := ObjectPath("00", shaOf(t, payload))
	client := liveRepoStub(t, storage, objectPath, false)

	// The policy leaves the relay off, so a blob must resolve to a direct URL.
	source := newRelaySource(t, newDriverWithClient(t, client), modelScopePolicy(false),
		newSizedFakeEntity(objectPath, inlineLimit+1))

	u, err := source.Url(context.Background())
	require.NoError(t, err)

	require.True(t, strings.HasPrefix(u.Url, "https://oss.aliyuncs.com/"),
		"direct mode must return the storage URL, got %q", u.Url)
	require.NotContains(t, u.Url, "/file/content/",
		"direct mode must not route through this server")
	require.Zero(t, storage.requests,
		"generating the URL must not itself download the object")
}

func TestDirectDownloadFallsBackToRelayForInlineObject(t *testing.T) {
	payload := []byte("small inline object")
	storage := newStorageServer(t, payload)
	objectPath := ObjectPath("00", shaOf(t, payload))
	client := liveRepoStub(t, storage, objectPath, true)

	// At or below the inline limit the object has no fetchable URL even though
	// the policy asked for direct downloads, so it must degrade to the relay.
	source := newRelaySource(t, newDriverWithClient(t, client), modelScopePolicy(false),
		newSizedFakeEntity(objectPath, int64(len(payload))))

	u, err := source.Url(context.Background())
	require.NoError(t, err)
	require.Contains(t, u.Url, "/file/content/",
		"an object with no fetchable URL must fall back to the relay, got %q", u.Url)

	rec := httptest.NewRecorder()
	source.Serve(rec, httptest.NewRequest(http.MethodGet, "/api/v1/file/content/x/0/y", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, string(payload), rec.Body.String(),
		"the fallback must still serve the object through this server")
}

func TestRelayDownloadHonorsRangeRequest(t *testing.T) {
	payload := []byte("0123456789abcdef")
	storage := newStorageServer(t, payload)
	objectPath := ObjectPath("00", shaOf(t, payload))
	client := liveRepoStub(t, storage, objectPath, true)

	source := newRelaySource(t, newDriverWithClient(t, client), modelScopePolicy(true),
		newSizedFakeEntity(objectPath, int64(len(payload))))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/file/content/x/0/y", nil)
	req.Header.Set("Range", "bytes=4-7")
	rec := httptest.NewRecorder()
	source.Serve(rec, req)

	require.Equal(t, http.StatusPartialContent, rec.Code)
	require.Equal(t, "4567", rec.Body.String(),
		"a range request must be served from the relayed stream")
}
