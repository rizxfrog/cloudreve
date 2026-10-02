package modelscope

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/stretchr/testify/require"
)

// The existence probe is only a deduplication shortcut. Validate and Commit
// already report an already-stored object correctly, so a probe that fails must
// not abort an upload that could otherwise succeed. A network can legitimately
// answer one of the two operations and refuse the other, so making the probe
// fatal turns a working upload into a reported failure.

func TestUploadProceedsWhenExistenceProbeFails(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	var committed map[string]any
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/commit/") {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&committed))
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}
		// The object probe is rejected, as a filtered network would do.
		return jsonResponse(http.StatusMisdirectedRequest,
			`{"message":"mirror self-forwarded loop detected"}`), nil
	})

	d := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	content := []byte("payload that must still be stored")
	temp := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(temp, content, 0o644))
	file, err := os.Open(temp)
	require.NoError(t, err)

	props := newUploadProps(int64(len(content)))
	require.NoError(t, d.Put(context.Background(), newUploadRequest(file, props)),
		"a failed probe must not fail the upload")

	wantPath := ObjectPath("00", shaOf(t, content))
	require.Equal(t, wantPath, props.SavePath, "the object must still be recorded")

	actions, ok := committed["actions"].([]any)
	require.True(t, ok, "the object must still be committed")
	require.Len(t, actions, 1)
}

func TestUploadStillSkipsWhenObjectAlreadyStored(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	commitSeen := false
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/commit/") {
			commitSeen = true
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}
		// A 2xx probe answer means the object is already present.
		return jsonResponse(http.StatusOK, `{}`), nil
	})

	d := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	content := []byte("already stored content")
	temp := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(temp, content, 0o644))
	file, err := os.Open(temp)
	require.NoError(t, err)

	props := newUploadProps(int64(len(content)))
	require.NoError(t, d.Put(context.Background(), newUploadRequest(file, props)))

	require.False(t, commitSeen,
		"content already present upstream must not be committed again")
	require.Equal(t, ObjectPath("00", shaOf(t, content)), props.SavePath)
}
