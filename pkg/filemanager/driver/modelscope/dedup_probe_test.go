package modelscope

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/stretchr/testify/require"
)

// These tests pin the session-time dedup probe: a client that supplies a content
// hash must be able to skip sending content this store already holds, and must
// still send it when the content is not held anywhere.

// probeClient builds a driver whose API transport answers from a set of routes.
// routes is ordered, and the first suffix that matches the request path wins, so
// a test states exactly which call it is answering. The batch route contains the
// repository id and would otherwise be matched by the file-read route.
func probeClient(t *testing.T, routes []probeRoute) *Driver {
	t.Helper()
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		for _, route := range routes {
			if strings.HasSuffix(r.URL.Path, route.match) {
				return route.answer()
			}
		}
		return jsonResponse(http.StatusNotFound, `{"Code":404}`), nil
	})
	return &Driver{client: client, ns: "00"}
}

// probeRoute answers requests whose path ends in match.
type probeRoute struct {
	match  string
	answer func() (*http.Response, error)
}

const dedupSize int64 = 8 << 20

// TestObjectExistsByDigestSeesRepositoryPointer covers the case where this
// repository already references the object: the digest is what the client said,
// the pointer is present, and nothing needs to be transferred or committed for
// the lookup to succeed.
func TestObjectExistsByDigestSeesRepositoryPointer(t *testing.T) {
	d := probeClient(t, []probeRoute{
		{"/repo", func() (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}},
	})

	got, err := d.ObjectExistsByDigest(context.Background(), testHash, dedupSize)
	require.NoError(t, err)
	require.True(t, got, "an object already referenced here needs no transfer")
}

// TestObjectExistsByDigestSeesUpstreamBlob covers the case where the content is
// stored globally but not referenced here. The LFS batch reply omits the oid for
// an object it already holds, which is the same signal Validate reports.
func TestObjectExistsByDigestSeesUpstreamBlob(t *testing.T) {
	d := probeClient(t, []probeRoute{
		{"/repo", func() (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusNotFound, Body: http.NoBody}, nil
		}},
		{"/objects/batch", func() (*http.Response, error) {
			return jsonResponse(http.StatusOK, `{"Code":200,"Data":{"objects":[],"reused-objects":[]}}`), nil
		}},
	})

	got, err := d.ObjectExistsByDigest(context.Background(), testHash, dedupSize)
	require.NoError(t, err)
	require.True(t, got, "an oid omitted from the batch reply means the blob exists")
}

// TestObjectExistsByDigestReportsAbsentContent is the case that must keep working
// exactly as before: when nothing holds the content, the client has to send it.
func TestObjectExistsByDigestReportsAbsentContent(t *testing.T) {
	d := probeClient(t, []probeRoute{
		{"/repo", func() (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusNotFound, Body: http.NoBody}, nil
		}},
		{"/objects/batch", func() (*http.Response, error) {
			return jsonResponse(http.StatusOK,
				`{"Code":200,"Data":{"objects":[{"oid":"`+testHash+`","size":8388608,`+
					`"actions":{"upload":{"href":"https://lfs.modelscope.cn/api/v1/repos/datasets/owner/repo/blobs/`+testHash+`/8388608",`+
					`"offset":0,"upload_header":{"Range":"bytes=0-"},"upload_parameters":{}}}}]}}`), nil
		}},
	})

	got, err := d.ObjectExistsByDigest(context.Background(), testHash, dedupSize)
	require.NoError(t, err)
	require.False(t, got, "content absent everywhere must still be sent by the client")
}

// TestObjectExistsByDigestSkipsInlineSize pins that small objects are never
// prevalidated: their content travels inside the commit itself, so there is
// nothing to look up and nothing to skip.
func TestObjectExistsByDigestSkipsInlineSize(t *testing.T) {
	d := probeClient(t, []probeRoute{
		{"/repo", func() (*http.Response, error) {
			t.Error("an inline-sized object must not be probed upstream")
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		}},
	})

	got, err := d.ObjectExistsByDigest(context.Background(), testHash, inlineLimit)
	require.NoError(t, err)
	require.False(t, got, "an object committed inline has nothing to skip")
}

// TestObjectExistsByDigestRejectsMalformedDigest keeps a non-digest from being
// turned into a repository path.
func TestObjectExistsByDigestRejectsMalformedDigest(t *testing.T) {
	d := probeClient(t, nil)
	d.client.api.Transport = roundTripper(func(*http.Request) (*http.Response, error) {
		t.Error("a malformed digest must not reach upstream")
		return nil, nil
	})

	got, err := d.ObjectExistsByDigest(context.Background(), "not-a-digest", dedupSize)
	require.NoError(t, err)
	require.False(t, got)
}

// TestCommitReferenceWritesBlobPointer checks that skipping the content transfer
// still records the object under its content-addressed path.
func TestCommitReferenceWritesBlobPointer(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	var payload map[string]any
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		return jsonResponse(http.StatusOK, `{"Code":200}`), nil
	})

	d := &Driver{client: client, ns: "00"}
	props := &fs.UploadProps{Size: dedupSize, ClientHash: testHash}
	require.NoError(t, d.CommitReference(context.Background(), &fs.UploadRequest{Props: props}))

	require.Equal(t, ObjectPath("00", testHash), props.SavePath,
		"the resolved path must be written back so completion persists it")

	actions, ok := payload["actions"].([]any)
	require.True(t, ok)
	require.Len(t, actions, 1)
	action := actions[0].(map[string]any)
	require.Equal(t, "lfs", action["type"])
	require.Equal(t, ObjectPath("00", testHash), action["path"])
	require.Equal(t, testHash, action["sha256"])
}

// TestCommitReferenceRefusesInlineSize keeps the reference path from being used
// for content that is supposed to travel inside the commit.
func TestCommitReferenceRefusesInlineSize(t *testing.T) {
	d := &Driver{client: newTestClient(t, "https://www.modelscope.cn"), ns: "00"}

	err := d.CommitReference(context.Background(), &fs.UploadRequest{
		Props: &fs.UploadProps{Size: inlineLimit, ClientHash: testHash},
	})
	require.Error(t, err, "an inline object cannot be referenced without its content")
}
