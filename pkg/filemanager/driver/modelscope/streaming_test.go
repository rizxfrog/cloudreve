package modelscope

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/stretchr/testify/require"
)

// When the client declares the content digest, the driver addresses the object
// before reading the stream and never buffers it. The digest still decides the
// physical path, so it must be verified rather than trusted: content-addressed
// objects are shared between entities, and a wrong path would file content
// under a name that describes other bytes.

// newStreamedRequest builds an upload request carrying a client declared hash.
func newStreamedRequest(t *testing.T, payload []byte, declared string) *fs.UploadRequest {
	t.Helper()

	temp := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(temp, payload, 0o644))
	file, err := os.Open(temp)
	require.NoError(t, err)

	props := newUploadProps(int64(len(payload)))
	props.ClientHash = declared
	return newUploadRequest(file, props)
}

func TestStreamedUploadSkipsExistenceProbeAndBuffering(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	payload := []byte("small streamed payload")
	hash := shaOf(t, payload)

	var (
		batchSeen  bool
		commitBody map[string]any
	)
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/info/lfs/") {
			batchSeen = true
			return jsonResponse(http.StatusOK, `{"Code":200,"Data":{"objects":[]}}`), nil
		}
		if strings.Contains(r.URL.Path, "/commit/") {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&commitBody))
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}
		// An existence probe must not happen when the digest is already known.
		t.Errorf("unexpected repository read: %s", r.URL.Path)
		return jsonResponse(http.StatusInternalServerError, ""), nil
	})

	d := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}
	req := newStreamedRequest(t, payload, hash)

	require.NoError(t, d.Put(context.Background(), req))
	require.False(t, batchSeen, "an inline object needs no LFS batch call")
	require.Equal(t, ObjectPath("00", hash), req.Props.SavePath)

	actions := commitBody["actions"].([]any)
	require.Len(t, actions, 1)
}

func TestStreamedUploadRejectsMismatchedDigest(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	payload := []byte("the real content")
	wrong := shaOf(t, []byte("some other content"))

	committed := false
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/commit/") {
			committed = true
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}
		return jsonResponse(http.StatusNotFound, ""), nil
	})

	d := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}
	req := newStreamedRequest(t, payload, wrong)

	err := d.Put(context.Background(), req)

	require.ErrorIs(t, err, ErrDigestMismatch,
		"content filed under a digest it does not hash to must be refused")
	require.False(t, committed, "nothing may be committed for a mismatched digest")
	require.Empty(t, req.Props.SavePath)
}

func TestStreamedUploadRejectsShortStream(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	payload := []byte("declared longer than what arrives")
	// Declare a size larger than the content so the stream ends early.
	hash := shaOf(t, payload)

	committed := false
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/commit/") {
			committed = true
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}
		return jsonResponse(http.StatusNotFound, ""), nil
	})

	d := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	temp := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(temp, payload, 0o644))
	file, err := os.Open(temp)
	require.NoError(t, err)

	props := newUploadProps(int64(len(payload)) + 100)
	props.ClientHash = hash
	req := newUploadRequest(file, props)

	err = d.Put(context.Background(), req)

	require.Error(t, err)
	require.False(t, committed, "a truncated stream must not be committed")
}

func TestStreamedLargeUploadUsesBlobPath(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	payload := make([]byte, inlineLimit+1)
	hash := shaOf(t, payload)
	targetRoute := client.lfsUploadRoute(hash, int64(len(payload)))

	var (
		putSeen    bool
		commitBody map[string]any
	)
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(r.URL.Path, "/info/lfs/"):
			return jsonResponse(http.StatusOK, `{"Code":200,"Data":{"objects":[{"oid":"`+hash+
				`","size":`+itoa(len(payload))+`,"actions":{"upload":{"href":"`+targetRoute+`"}}}]}}`), nil
		case strings.Contains(r.URL.Path, "/blobs/"):
			putSeen = true
			// A real transport streams the request body; consuming it here is
			// what keeps the relay goroutine advancing.
			_, err := io.Copy(io.Discard, r.Body)
			require.NoError(t, err)
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		case strings.Contains(r.URL.Path, "/commit/"):
			require.NoError(t, json.NewDecoder(r.Body).Decode(&commitBody))
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}
		// The deduplication probe is expected and answered as "absent".
		return jsonResponse(http.StatusNotFound, ""), nil
	})
	client.storage.Transport = client.api.Transport

	d := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}
	req := newStreamedRequest(t, payload, hash)

	require.NoError(t, d.Put(context.Background(), req))
	require.True(t, putSeen, "the blob must still be uploaded")

	actions := commitBody["actions"].([]any)
	action := actions[0].(map[string]any)
	require.Equal(t, "lfs", action["type"])
	require.Equal(t, hash, action["sha256"])
	require.Equal(t, ObjectPath("00", hash), req.Props.SavePath)
}

func TestUploadWithoutClientHashStillWorks(t *testing.T) {
	// A client that cannot hash must keep working: the server falls back to
	// buffering the stream to learn the digest.
	client := newTestClient(t, "https://www.modelscope.cn")

	payload := []byte("no client hash supplied")
	var commitBody map[string]any
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/commit/") {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&commitBody))
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}
		return jsonResponse(http.StatusNotFound, ""), nil
	})

	d := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	temp := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(temp, payload, 0o644))
	file, err := os.Open(temp)
	require.NoError(t, err)

	props := newUploadProps(int64(len(payload)))
	req := newUploadRequest(file, props)

	require.NoError(t, d.Put(context.Background(), req))
	require.Equal(t, ObjectPath("00", shaOf(t, payload)), req.Props.SavePath)
	require.NotNil(t, commitBody)
}
