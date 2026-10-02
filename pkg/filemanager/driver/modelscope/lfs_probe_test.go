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

// A large object takes the LFS path: the batch call decides whether a blob must
// be uploaded. The existence probe that runs first is only a deduplication
// shortcut and must not block this path, because a network can answer the API
// calls while refusing the repository read used by the probe.

func TestLargeUploadProceedsWhenProbeIsRefused(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	content := make([]byte, inlineLimit+1)
	hash := shaOf(t, content)
	targetRoute := client.lfsUploadRoute(hash, int64(len(content)))

	var (
		batchSeen  bool
		putSeen    bool
		commitBody map[string]any
	)
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(r.URL.Path, "/info/lfs/objects/batch"):
			batchSeen = true
			return jsonResponse(http.StatusOK, `{"Code":200,"Data":{"objects":[{"oid":"`+hash+
				`","size":`+itoa(len(content))+`,"actions":{"upload":{"href":"`+targetRoute+`"}}}]}}`), nil
		case strings.Contains(r.URL.Path, "/commit/"):
			require.NoError(t, json.NewDecoder(r.Body).Decode(&commitBody))
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		case strings.Contains(r.URL.Path, "/blobs/"):
			putSeen = true
			return jsonResponse(http.StatusOK, `{"Code":200}`), nil
		}
		// The repository read used by the existence probe is refused, exactly as
		// a filtering network answers it.
		return jsonResponse(http.StatusMisdirectedRequest,
			`{"message":"mirror self-forwarded loop detected"}`), nil
	})
	client.storage.Transport = client.api.Transport

	d := &Driver{client: client, ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	temp := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(temp, content, 0o644))
	file, err := os.Open(temp)
	require.NoError(t, err)

	props := newUploadProps(int64(len(content)))
	require.NoError(t, d.Put(context.Background(), newUploadRequest(file, props)),
		"a refused probe must not block a large upload")

	require.True(t, batchSeen, "the LFS batch call must still be made")
	require.True(t, putSeen, "the blob must still be uploaded")
	require.Equal(t, ObjectPath("00", hash), props.SavePath)

	actions, ok := commitBody["actions"].([]any)
	require.True(t, ok)
	require.Len(t, actions, 1)

	action := actions[0].(map[string]any)
	require.Equal(t, "lfs", action["type"])
	require.Equal(t, hash, action["sha256"])
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
