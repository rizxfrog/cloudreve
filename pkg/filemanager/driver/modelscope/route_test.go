package modelscope

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// ModelScope exposes two repository URL shapes and they are not interchangeable:
//
//	file read (GET)   /api/v1/<repoType>/<repo>/repo?FilePath=...
//	LFS + commit (POST) /api/v1/repos/<repoType>/<repo>/...
//
// Requesting the file read through the "repos/" form is answered by an edge
// mirror with 421 "mirror self-forwarded loop detected" instead of by the API,
// which makes every read and every existence probe fail. These tests pin the
// correct shape for each route so the two cannot be conflated again.

func TestFileReadUsesRepoTypeRouteWithoutReposSegment(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	var gotPath string
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.Path
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{"https://oss.aliyuncs.com/b/" + testHash}},
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})

	_, err := client.Exists(context.Background(), ObjectPath("00", testHash))
	require.NoError(t, err)

	require.Equal(t, "/api/v1/datasets/owner/repo/repo", gotPath,
		"the file read route must not carry the repos/ segment")
	require.NotContains(t, gotPath, "/repos/",
		"the repos/ form is answered by a mirror with 421, not by the API")
}

func TestLFSAndCommitKeepReposSegment(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")

	var paths []string
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.Path)
		if strings.Contains(r.URL.Path, "/info/lfs/") {
			return jsonResponse(http.StatusOK, `{"Code":200,"Data":{"objects":[]}}`), nil
		}
		return jsonResponse(http.StatusOK, `{"Code":200}`), nil
	})

	_, err := client.Validate(context.Background(), testHash, 11)
	require.NoError(t, err)

	err = client.Commit(context.Background(), []map[string]any{blobAction("00/ab/cd", testHash, 11)})
	require.NoError(t, err)

	require.Len(t, paths, 2)
	for _, p := range paths {
		require.True(t, strings.HasPrefix(p, "/api/v1/repos/datasets/owner/repo/"),
			"the LFS and commit routes do require the repos/ segment, got %q", p)
	}
}

func TestProbeReportsMirror421Precisely(t *testing.T) {
	// The mirror answer must survive into the error verbatim: it is the only
	// signal that distinguishes a filtered network from a rejected credential.
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusMisdirectedRequest,
			`{"message":"mirror self-forwarded loop detected"}`), nil
	})

	_, err := client.Exists(context.Background(), ObjectPath("00", testHash))

	require.ErrorIs(t, err, ErrUpstream)
	require.Contains(t, err.Error(), "421")
	require.Contains(t, err.Error(), "mirror self-forwarded loop detected")
}
