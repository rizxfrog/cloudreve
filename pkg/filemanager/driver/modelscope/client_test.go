package modelscope

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/stretchr/testify/require"
)

const testHash = "24780644a95f759a9aeeb228c3d852028f2fd40ce0b74d68134246ec4a959547"

// roundTripper adapts a function into an http.RoundTripper so a client's
// transports can be swapped without a live server.
type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func newTestClient(t *testing.T, endpoint string) *Client {
	t.Helper()

	client, err := NewClient(endpoint, "test-token", "owner/repo", "datasets", "master",
		logging.NewConsoleLogger(logging.LevelError))
	require.NoError(t, err)
	return client
}

func TestObjectPathRoundTrip(t *testing.T) {
	// The physical path is the namespace, then the digest split into two segments.
	objectPath := ObjectPath("00", testHash)
	require.Equal(t, "00/24/780644a95f759a9aeeb228c3d852028f2fd40ce0b74d68134246ec4a959547", objectPath)

	require.Equal(t, testHash, HashFromObjectPath(objectPath))
	require.Empty(t, HashFromObjectPath("00/short"))
	require.Empty(t, HashFromObjectPath("00/zz/nothex"))
}

func TestNewClientRejectsInvalidConfiguration(t *testing.T) {
	logger := logging.NewConsoleLogger(logging.LevelError)

	cases := map[string]struct {
		endpoint string
		token    string
		repo     string
		repoType string
		revision string
	}{
		"plain http endpoint": {endpoint: "http://www.modelscope.cn", token: "t", repo: "a/b", repoType: "datasets", revision: "master"},
		"endpoint with path":  {endpoint: "https://www.modelscope.cn/api", token: "t", repo: "a/b", repoType: "datasets", revision: "master"},
		"missing token":       {endpoint: "https://www.modelscope.cn", token: "", repo: "a/b", repoType: "datasets", revision: "master"},
		"header injecting token": {
			endpoint: "https://www.modelscope.cn", token: "t\r\nX-Evil: 1", repo: "a/b", repoType: "datasets", revision: "master",
		},
		"repo without owner": {endpoint: "https://www.modelscope.cn", token: "t", repo: "repo", repoType: "datasets", revision: "master"},
		"unknown repo type":  {endpoint: "https://www.modelscope.cn", token: "t", repo: "a/b", repoType: "spaces", revision: "master"},
		"path traversing revision": {
			endpoint: "https://www.modelscope.cn", token: "t", repo: "a/b", repoType: "datasets", revision: "..",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := NewClient(tc.endpoint, tc.token, tc.repo, tc.repoType, tc.revision, logger)
			require.Error(t, err)
		})
	}
}

func TestSafeStorageURL(t *testing.T) {
	allowed := []string{
		"https://lfs.modelscope.cn/api/v1/repos/datasets/owner/repo/blobs/" + testHash + "/10",
		"https://oss.aliyuncs.com/bucket/object",
		"https://s3.amazonaws.com/bucket/object",
	}
	for _, raw := range allowed {
		require.True(t, safeStorageURL(raw), "expected %q to be allowed", raw)
	}

	rejected := []string{
		"http://lfs.modelscope.cn/blob",            // not https
		"https://evil.com/blob",                    // not allowlisted
		"https://modelscope.cn.evil.com/blob",      // suffix must be a real label boundary
		"https://user:pass@lfs.modelscope.cn/blob", // embedded credentials
		"https://lfs.modelscope.cn:8443/blob",      // unexpected port
		"https://127.0.0.1/blob",                   // literal address
		"https://lfs.modelscope.cn/blob#fragment",  // fragment
		"https://fake-modelscope.cn/blob",          // lookalike host
	}
	for _, raw := range rejected {
		require.False(t, safeStorageURL(raw), "expected %q to be rejected", raw)
	}
}

func TestValidateReturnsUploadTarget(t *testing.T) {
	target := "https://lfs.modelscope.cn/api/v1/repos/datasets/owner/repo/blobs/" + testHash + "/1024"

	client := newTestClient(t, "https://www.modelscope.cn")
	var capturedAuth string
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		capturedAuth = r.Header.Get("Authorization")
		body := `{"Code":200,"Data":{"objects":[{"oid":"` + testHash + `","size":1024,"actions":{"upload":{"href":"` + target + `"}}}]}}`
		return jsonResponse(http.StatusOK, body), nil
	})

	got, err := client.Validate(context.Background(), testHash, 1024)
	require.NoError(t, err)
	require.Equal(t, target, got)
	require.Equal(t, "Bearer test-token", capturedAuth)
}

func TestValidateReportsExistingBlobAsNoTarget(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	// An oid absent from objects means the blob already exists upstream.
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"Code":200,"Data":{"objects":[]}}`), nil
	})

	got, err := client.Validate(context.Background(), testHash, 1024)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestValidateRejectsRewrittenUploadAction(t *testing.T) {
	// A target on the API origin would receive the credential, so it must be refused.
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		body := `{"Code":200,"Data":{"objects":[{"oid":"` + testHash + `","size":1024,"actions":{"upload":{"href":"https://www.modelscope.cn/evil"}}}]}}`
		return jsonResponse(http.StatusOK, body), nil
	})

	_, err := client.Validate(context.Background(), testHash, 1024)
	require.ErrorIs(t, err, ErrUpstream)
}

func TestValidateRejectsExtraUploadHeaders(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	target := "https://lfs.modelscope.cn/api/v1/repos/datasets/owner/repo/blobs/" + testHash + "/1024"
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		body := `{"Code":200,"Data":{"objects":[{"oid":"` + testHash + `","size":1024,"actions":{"upload":{"href":"` + target + `","upload_header":{"Range":"bytes=0-","X-Extra":"1"}}}}]}}`
		return jsonResponse(http.StatusOK, body), nil
	})

	_, err := client.Validate(context.Background(), testHash, 1024)
	require.ErrorIs(t, err, ErrUpstream)
}

func TestValidateRejectsFailedEnvelope(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"Code":500,"Message":"boom"}`), nil
	})

	_, err := client.Validate(context.Background(), testHash, 1024)
	require.ErrorIs(t, err, ErrUpstream)
}

func TestPutSendsCredentialOnlyToCanonicalRoute(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	var (
		gotAuth string
		gotBody string
	)
	client.storage.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		gotAuth = r.Header.Get("Authorization")
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody = string(raw)
		return jsonResponse(http.StatusOK, `{"Code":200}`), nil
	})

	// A storage host outside the modelscope.cn origin never receives the token.
	target := "https://oss.aliyuncs.com/bucket/object"
	require.NoError(t, client.Put(context.Background(), target, testHash, strings.NewReader("payload"), int64(len("payload"))))
	require.Empty(t, gotAuth)
	require.Equal(t, "payload", gotBody)
}

func TestPutSendsCredentialToLFSRoute(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	var gotAuth string
	client.storage.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		gotAuth = r.Header.Get("Authorization")
		return jsonResponse(http.StatusOK, `{"Code":200}`), nil
	})

	target := client.lfsUploadRoute(testHash, 7)
	require.NoError(t, client.Put(context.Background(), target, testHash, strings.NewReader("payload"), 7))
	require.Equal(t, "Bearer test-token", gotAuth)
}

func TestCommitSendsInlineContentBase64(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	var payload map[string]any
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		return jsonResponse(http.StatusOK, `{"Code":200}`), nil
	})

	content := []byte("small document")
	require.NoError(t, client.Commit(context.Background(), []map[string]any{inlineAction("00/ab/cd", content)}))

	actions, ok := payload["actions"].([]any)
	require.True(t, ok)
	require.Len(t, actions, 1)

	action := actions[0].(map[string]any)
	require.Equal(t, "normal", action["type"])
	require.Equal(t, base64.StdEncoding.EncodeToString(content), action["content"])
}

func TestDeleteSkipsAbsentPaths(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	var commitCalls int
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/commit/") {
			commitCalls++
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}

		// Both probe reads report the object as absent.
		return jsonResponse(http.StatusNotFound, ""), nil
	})

	require.NoError(t, client.Delete(context.Background(), "00/ab/cd", "00/ef/01"))
	// Nothing to delete means nothing is committed: ModelScope rejects a commit
	// whose actions reference a path missing from the revision.
	require.Zero(t, commitCalls)
}

func TestDeleteCommitsOnlyExistingPaths(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	var committedPayload map[string]any
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/commit/") {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&committedPayload))
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}

		if strings.Contains(r.URL.RawQuery, "FilePath=00%2Fab%2Fcd") {
			return jsonResponse(http.StatusOK, "present"), nil
		}
		return jsonResponse(http.StatusNotFound, ""), nil
	})

	require.NoError(t, client.Delete(context.Background(), "00/ab/cd", "00/ef/01"))

	actions, ok := committedPayload["actions"].([]any)
	require.True(t, ok)
	require.Len(t, actions, 1)

	action := actions[0].(map[string]any)
	require.Equal(t, "delete", action["action"])
	require.Equal(t, "00/ab/cd", action["path"])
}

func TestOpenReadFollowsRedirectToStorageWithoutCredential(t *testing.T) {
	// Serve the object from a separate allowlisted host so the redirect is genuine.
	var storageAuth string
	storage := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		storageAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte("object-bytes"))
	}))
	defer storage.Close()

	client := newTestClient(t, "https://www.modelscope.cn")
	// The test storage host is not on the production allowlist, so the check is
	// exercised against a host that is.
	client.storage.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		storageAuth = r.Header.Get("Authorization")
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("object-bytes")),
		}, nil
	})

	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusFound,
			Header: http.Header{
				"Location": []string{"https://oss.aliyuncs.com/bucket/" + testHash},
			},
			Body: io.NopCloser(strings.NewReader("")),
		}, nil
	})

	reader, err := client.OpenRead(context.Background(), "00/ab/cd", 0)
	require.NoError(t, err)
	defer reader.Close()

	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "object-bytes", string(raw))
	require.Empty(t, storageAuth, "storage must never receive the repository credential")
}

func TestOpenReadRejectsRedirectToUnlistedHost(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{"https://evil.com/steal"}},
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})

	_, err := client.OpenRead(context.Background(), "00/ab/cd", 0)
	require.ErrorIs(t, err, ErrUpstream)
}

func TestOpenReadServesInlineBodyDirectly(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("inline-content")),
		}, nil
	})

	reader, err := client.OpenRead(context.Background(), "00/ab/cd", 0)
	require.NoError(t, err)
	defer reader.Close()

	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "inline-content", string(raw))
}

func TestDownloadTargetRejectsMismatchedHash(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		// The signed URL points at a different object than the one requested.
		return &http.Response{
			StatusCode: http.StatusFound,
			Header: http.Header{
				"Location": []string{"https://oss.aliyuncs.com/bucket/" + strings.Repeat("a", 64)},
			},
			Body: io.NopCloser(strings.NewReader("")),
		}, nil
	})

	_, _, err := client.DownloadTarget(context.Background(), "00/ab/cd", testHash, "name.bin")
	require.ErrorIs(t, err, ErrUpstream)
}

func TestDownloadTargetStripsLayoutProbesAndSanitizesFilename(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		location := "https://oss.aliyuncs.com/bucket/89/6f/" + testHash +
			"?namespace=owner&repository=repo&revision=master&tag=v1&Signature=abc"
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{location}},
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})

	got, signed, err := client.DownloadTarget(context.Background(), "00/ab/cd", testHash, "my file.bin")
	require.NoError(t, err)
	require.True(t, signed)

	parsed, err := url.Parse(got)
	require.NoError(t, err)

	query := parsed.Query()
	for _, key := range []string{"namespace", "repository", "revision", "tag"} {
		require.Empty(t, query.Get(key), "repository layout probe %q must be stripped", key)
	}
	require.Equal(t, "abc", query.Get("Signature"))
	require.Equal(t, "my file.bin", query.Get("filename"))
}

func TestDownloadTargetSanitizesHeaderInjectingFilename(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		location := "https://oss.aliyuncs.com/bucket/" + testHash + "?Signature=abc"
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{location}},
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})

	got, signed, err := client.DownloadTarget(context.Background(), "00/ab/cd", testHash, "bad\r\nX-Evil: 1")
	require.NoError(t, err)
	require.True(t, signed)

	parsed, err := url.Parse(got)
	require.NoError(t, err)
	// A name that could break out of the quoted header falls back to the hash.
	require.Equal(t, testHash, parsed.Query().Get("filename"))
}

func TestDriverPutResolvesContentAddressedPath(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	var (
		probedPath string
		commitBody map[string]any
	)
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/commit/") {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&commitBody))
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}

		probedPath = r.URL.Query().Get("FilePath")
		return jsonResponse(http.StatusNotFound, ""), nil
	})

	driver := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	// Small payloads are stored inline rather than as LFS blobs, but the object
	// path is still content addressed.
	content := []byte("hello modelscope")
	temp := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(temp, content, 0o644))
	file, err := os.Open(temp)
	require.NoError(t, err)

	props := newUploadProps(int64(len(content)))
	req := newUploadRequest(file, props)

	require.NoError(t, driver.Put(context.Background(), req))

	// The object path must be derived from the SHA-256 of the content that was
	// actually uploaded, and written back so completion can persist it.
	wantPath := "00/" + shaOf(t, content)[:2] + "/" + shaOf(t, content)[2:]
	require.Equal(t, wantPath, props.SavePath)
	require.Equal(t, wantPath, probedPath)

	actions, ok := commitBody["actions"].([]any)
	require.True(t, ok)
	require.Len(t, actions, 1)

	action := actions[0].(map[string]any)
	require.Equal(t, "normal", action["type"])
	require.EqualValues(t, len(content), action["size"])
	require.Equal(t, base64.StdEncoding.EncodeToString(content), action["content"])
}

func TestDriverPutCommitsLFSBlobForLargeContent(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	// Content larger than the inline limit goes through the LFS batch API.
	content := make([]byte, inlineLimit+1)

	var commitBody map[string]any
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/info/lfs/objects/batch") {
			// Report the blob as already stored so no upload target is needed.
			return jsonResponse(http.StatusOK, `{"Code":200,"Data":{"objects":[]}}`), nil
		}

		if strings.Contains(r.URL.Path, "/commit/") {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&commitBody))
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}

		return jsonResponse(http.StatusNotFound, ""), nil
	})

	driver := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	temp := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(temp, content, 0o644))
	file, err := os.Open(temp)
	require.NoError(t, err)

	props := newUploadProps(int64(len(content)))
	req := newUploadRequest(file, props)

	require.NoError(t, driver.Put(context.Background(), req))

	actions, ok := commitBody["actions"].([]any)
	require.True(t, ok)
	require.Len(t, actions, 1)

	action := actions[0].(map[string]any)
	require.Equal(t, "lfs", action["type"])
	require.Equal(t, shaOf(t, content), action["sha256"])
	require.EqualValues(t, len(content), action["size"])
}

func TestDriverPutRejectsSizeMismatch(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusNotFound, ""), nil
	})

	driver := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	temp := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(temp, []byte("short"), 0o644))
	file, err := os.Open(temp)
	require.NoError(t, err)

	// The declared size exceeds the actual content, which would otherwise let a
	// truncated stream be stored under a digest it does not match.
	props := newUploadProps(9999)
	req := newUploadRequest(file, props)

	require.Error(t, driver.Put(context.Background(), req))
}

// TestStorageTransportIsPinnedToHTTP11 guards the reason the storage transport
// is configured the way it is.
//
// The storage host uploads large objects roughly three times slower over HTTP/2
// than over HTTP/1.1, so the transport serving blob transfer must not negotiate
// HTTP/2. A silent revert to the default would restore the slow path while every
// other test kept passing.
func TestStorageTransportIsPinnedToHTTP11(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	transport, ok := client.storage.Transport.(*http.Transport)
	require.True(t, ok, "storage transport should be an *http.Transport")

	require.False(t, transport.ForceAttemptHTTP2,
		"storage transport must not attempt HTTP/2")
	require.NotNil(t, transport.TLSClientConfig,
		"storage transport needs an explicit TLS config to advertise HTTP/1.1 only")
	require.Equal(t, []string{"http/1.1"}, transport.TLSClientConfig.NextProtos,
		"storage transport must advertise HTTP/1.1 only")
	require.NotNil(t, transport.TLSNextProto,
		"an empty TLSNextProto map is what disables the HTTP/2 upgrade")
	require.Empty(t, transport.TLSNextProto,
		"the HTTP/2 upgrade hook must be disabled on the storage transport")

	// The API transport is untouched: metadata calls are small and stay on the
	// default configuration.
	apiTransport, ok := client.api.Transport.(*http.Transport)
	require.True(t, ok)
	require.True(t, apiTransport.ForceAttemptHTTP2,
		"api transport should keep the default HTTP/2 behaviour")
}
