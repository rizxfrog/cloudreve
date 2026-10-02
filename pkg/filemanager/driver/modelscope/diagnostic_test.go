package modelscope

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests pin that a failing upload says *why* it failed. The upload route
// surfaces the driver's error verbatim, so an opaque message here becomes an
// unactionable one in the admin panel and in the log.

func TestExistsReportsUpstreamStatusAndBody(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusForbidden, `{"Code":403,"Message":"no write permission to repository"}`), nil
	})

	_, err := client.Exists(context.Background(), ObjectPath("00", testHash))

	require.ErrorIs(t, err, ErrUpstream, "callers still classify it as an upstream failure")
	require.Contains(t, err.Error(), "403", "the status must be reported")
	require.Contains(t, err.Error(), "no write permission",
		"the upstream explanation is what makes the failure actionable")
}

func TestExistsTreatsNotFoundAsAbsent(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusNotFound, ""), nil
	})

	exists, err := client.Exists(context.Background(), ObjectPath("00", testHash))

	require.NoError(t, err)
	require.False(t, exists, "a missing object is a normal answer, not a failure")
}

func TestExistsDoesNotReportRedirectAsFailure(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusFound,
			Header:     http.Header{"Location": []string{"https://oss.aliyuncs.com/b/" + testHash}},
			Body:       io.NopCloser(strings.NewReader("")),
		}, nil
	})

	exists, err := client.Exists(context.Background(), ObjectPath("00", testHash))

	require.NoError(t, err)
	require.True(t, exists, "a redirect means the object is stored")
}

func TestBatchRejectionNamesTheReason(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK,
			`{"Code":200,"Data":{"objects":[{"oid":"`+testHash+`","size":11,"error":{"code":404,"message":"repository not found"}}]}}`), nil
	})

	_, err := client.Validate(context.Background(), testHash, 11)

	require.ErrorIs(t, err, ErrUpstream)
	require.Contains(t, err.Error(), "repository not found",
		"the reason the batch was rejected must reach the caller")
}

func TestBatchSizeMismatchIsReported(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK,
			`{"Code":200,"Data":{"objects":[{"oid":"`+testHash+`","size":999}]}}`), nil
	})

	_, err := client.Validate(context.Background(), testHash, 11)

	require.ErrorIs(t, err, ErrUpstream)
	require.Contains(t, err.Error(), "999")
	require.Contains(t, err.Error(), "11")
}

func TestBlobUploadFailureReportsStatusAndBody(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.storage.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusRequestEntityTooLarge, `{"Code":413,"Message":"quota exceeded"}`), nil
	})

	target := client.lfsUploadRoute(testHash, 11)
	err := client.Put(context.Background(), target, testHash, strings.NewReader("hello world"), 11)

	require.ErrorIs(t, err, ErrUpstream)
	require.Contains(t, err.Error(), "413")
	require.Contains(t, err.Error(), "quota exceeded")
}

func TestBlobUploadTransportFailureNamesTheObject(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.storage.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})

	target := client.lfsUploadRoute(testHash, 11)
	err := client.Put(context.Background(), target, testHash, strings.NewReader("hello world"), 11)

	require.ErrorIs(t, err, ErrUpstream)
	require.Contains(t, err.Error(), testHash,
		"the failing object must be identifiable from the error")
}

func TestFailureMessageIsBounded(t *testing.T) {
	// A hostile or broken upstream must not be able to inflate the error, which
	// is echoed into an API response and the log.
	huge := strings.Repeat("x", 100000)
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusBadRequest, huge), nil
	})

	_, err := client.Exists(context.Background(), ObjectPath("00", testHash))

	require.ErrorIs(t, err, ErrUpstream)
	require.Less(t, len(err.Error()), 1000, "an error must not echo an unbounded body")
}
