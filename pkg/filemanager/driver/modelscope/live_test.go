package modelscope

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/stretchr/testify/require"
)

// fakeRepo is a minimal in-memory ModelScope repository that speaks the real
// HTTP surface: a file probe, an LFS batch endpoint and a commit endpoint.
type fakeRepo struct {
	// files is the revision contents, keyed by repository path.
	files map[string][]byte

	commits   []map[string]any
	batchSeen []map[string]any
	// existingBlobs forces the batch endpoint to report a stored blob.
	existingBlobs bool
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{files: map[string][]byte{}}
}

func (f *fakeRepo) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		switch {
		case strings.HasSuffix(path, "/info/lfs/objects/batch"):
			var payload map[string]any
			_ = json.NewDecoder(r.Body).Decode(&payload)
			f.batchSeen = append(f.batchSeen, payload)

			if f.existingBlobs {
				// An oid absent from objects means the blob is already stored.
				writeJSON(w, `{"Code":200,"Data":{"objects":[]}}`)
				return
			}
			writeJSON(w, `{"Code":200,"Data":{"objects":[]}}`)

		case strings.Contains(path, "/commit/"):
			var payload map[string]any
			_ = json.NewDecoder(r.Body).Decode(&payload)
			f.commits = append(f.commits, payload)

			actions, _ := payload["actions"].([]any)
			for _, raw := range actions {
				action, _ := raw.(map[string]any)
				name, _ := action["path"].(string)
				switch action["action"] {
				case "create":
					if content, ok := action["content"].(string); ok {
						decoded, err := base64.StdEncoding.DecodeString(content)
						if err != nil {
							writeJSON(w, `{"Code":400}`)
							return
						}
						f.files[name] = decoded
					} else {
						f.files[name] = []byte("blob")
					}
				case "delete":
					delete(f.files, name)
				}
			}
			writeJSON(w, `{"Code":200}`)

		case strings.Contains(path, "/repo"):
			name := r.URL.Query().Get("FilePath")
			content, ok := f.files[name]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = w.Write(content)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func writeJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

// newLiveClient builds a client pointed at a real test server rather than one
// with swapped transports, so the HTTP wiring itself is exercised.
func newLiveClient(t *testing.T, server *httptest.Server) *Client {
	t.Helper()

	client, err := NewClient(server.URL, "live-token", "owner/repo", "datasets", "master",
		logging.NewConsoleLogger(logging.LevelError))
	require.NoError(t, err)

	// The test server presents its own certificate.
	client.api.Transport = server.Client().Transport
	return client
}

func TestLiveUploadStoresInlineObjectAndReportsPath(t *testing.T) {
	repo := newFakeRepo()
	server := httptest.NewTLSServer(repo.handler())
	defer server.Close()

	driver := &Driver{client: newLiveClient(t, server), ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	content := []byte("a small document stored inline")
	temp := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.WriteFile(temp, content, 0o644))
	file, err := os.Open(temp)
	require.NoError(t, err)

	props := newUploadProps(int64(len(content)))
	require.NoError(t, driver.Put(context.Background(), newUploadRequest(file, props)))

	// The object must land in the repository under its content addressed path.
	require.Len(t, repo.commits, 1)

	wantPath := "00/" + shaOf(t, content)[:2] + "/" + shaOf(t, content)[2:]
	require.Equal(t, wantPath, props.SavePath)
	require.Equal(t, content, repo.files[wantPath])
}

func TestLiveUploadIsIdempotentForIdenticalContent(t *testing.T) {
	repo := newFakeRepo()
	server := httptest.NewTLSServer(repo.handler())
	defer server.Close()

	driver := &Driver{client: newLiveClient(t, server), ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}
	content := []byte("same bytes twice")

	upload := func() string {
		temp := filepath.Join(t.TempDir(), "payload")
		require.NoError(t, os.WriteFile(temp, content, 0o644))
		file, err := os.Open(temp)
		require.NoError(t, err)

		props := newUploadProps(int64(len(content)))
		require.NoError(t, driver.Put(context.Background(), newUploadRequest(file, props)))
		return props.SavePath
	}

	first := upload()
	second := upload()

	// Content addressing maps identical content to one object; the second upload
	// must not rewrite it.
	require.Equal(t, first, second)
	require.Len(t, repo.commits, 1, "a duplicate upload must not re-commit the object")
}

func TestLiveDownloadReadsBackInlineObject(t *testing.T) {
	repo := newFakeRepo()
	server := httptest.NewTLSServer(repo.handler())
	defer server.Close()

	driver := &Driver{client: newLiveClient(t, server), ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	content := []byte("round trip through the relay")
	objectPath := ObjectPath("00", shaOf(t, content))
	repo.files[objectPath] = content

	reader, err := driver.OpenStream(context.Background(), newFakeEntity(objectPath), 0)
	require.NoError(t, err)
	defer reader.Close()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, content, got)
}

func TestLiveDownloadHonorsRangeOffset(t *testing.T) {
	repo := newFakeRepo()
	server := httptest.NewTLSServer(repo.handler())
	defer server.Close()

	driver := &Driver{client: newLiveClient(t, server), ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	content := []byte("0123456789")
	objectPath := ObjectPath("00", shaOf(t, content))
	repo.files[objectPath] = content

	// Seeking past the start must yield the remaining bytes, which is what range
	// requests and the reader path rely on.
	reader, err := driver.OpenStream(context.Background(), newFakeEntity(objectPath), 4)
	require.NoError(t, err)
	defer reader.Close()

	got, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "456789", string(got))
}

func TestLiveDeleteRemovesObjectAndSkipsAbsent(t *testing.T) {
	repo := newFakeRepo()
	server := httptest.NewTLSServer(repo.handler())
	defer server.Close()

	driver := &Driver{client: newLiveClient(t, server), ns: "00", l: logging.NewConsoleLogger(logging.LevelError)}

	present := "00/ab/" + strings.Repeat("a", 64)
	repo.files[present] = []byte("blob")

	failed, err := driver.Delete(context.Background(), present, "00/cd/"+strings.Repeat("b", 64))
	require.NoError(t, err)
	require.Empty(t, failed)

	require.NotContains(t, repo.files, present)

	// Only the object that actually existed is committed for deletion.
	var deleted []string
	for _, commit := range repo.commits {
		actions, _ := commit["actions"].([]any)
		for _, raw := range actions {
			action, _ := raw.(map[string]any)
			if action["action"] == "delete" {
				deleted = append(deleted, action["path"].(string))
			}
		}
	}
	require.Equal(t, []string{present}, deleted)
}

func TestLivePutRejectsUploadTargetOnAPIOrigin(t *testing.T) {
	// A hostile upstream that points the upload at the credentialed API origin
	// must not receive the token. This exercises the guard through real HTTP.
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/info/lfs/objects/batch") {
			host := r.Host
			body, _ := json.Marshal(map[string]any{
				"Code": 200,
				"Data": map[string]any{"objects": []any{map[string]any{
					"oid":  testHash,
					"size": inlineLimit + 1,
					"actions": map[string]any{"upload": map[string]any{
						"href": "https://" + host + "/api/v1/repos/datasets/owner/repo/blobs/" + testHash,
					}},
				}}},
			})
			writeJSON(w, string(body))
			return
		}
		writeJSON(w, `{"Code":200}`)
	}))
	defer server.Close()

	client := newLiveClient(t, server)

	_, err := client.Validate(context.Background(), testHash, inlineLimit+1)
	require.ErrorIs(t, err, ErrUpstream)
}

// newFakeEntity wraps a physical source into a real entity so the driver paths
// that read e.Source() can be exercised without a database.
func newFakeEntity(source string) fs.Entity {
	return fs.NewEntity(&ent.Entity{ID: 1, Type: int(types.EntityTypeVersion), Source: source, Size: 0})
}
