package modelscope

import (
	"context"
	"io"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/driver"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/stretchr/testify/require"
)

// The tests here pin how the driver reports download addressability. Uploads
// always relay through this server, while a download may be served by a signed
// storage URL, but only for objects stored as blobs rather than inline.

// newSizedFakeEntity wraps a physical source into a real entity of a given size.
func newSizedFakeEntity(source string, size int64) fs.Entity {
	return fs.NewEntity(&ent.Entity{ID: 1, Type: int(types.EntityTypeVersion), Source: source, Size: size})
}

func TestHasPublicSourceOnlyForBlobsAboveInlineLimit(t *testing.T) {
	d := &Driver{}

	require.False(t, d.HasPublicSource(newSizedFakeEntity("00/ab/cd", inlineLimit)),
		"an object at the inline limit is stored inside the repository and has no URL")
	require.False(t, d.HasPublicSource(newSizedFakeEntity("00/ab/cd", inlineLimit-1)))
	require.True(t, d.HasPublicSource(newSizedFakeEntity("00/ab/cd", inlineLimit+1)),
		"a blob has a signed URL a client can fetch itself")
}

func TestSourceReportsNoPublicUrlForUnsignedObject(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	// A 2xx answer means the object is served inline, so no URL exists.
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("inline-bytes")),
		}, nil
	})

	d := &Driver{client: client, ns: "00"}

	_, err := d.Source(context.Background(), newFakeEntity(ObjectPath("00", testHash)),
		&driver.GetSourceArgs{DisplayName: "blob.bin"})

	require.ErrorIs(t, err, driver.ErrNoPublicUrl,
		"an object served inline must ask the caller to relay it instead")
}

func TestSourceReturnsSignedUrlForBlob(t *testing.T) {
	client := newTestClient(t, "https://www.modelscope.cn")
	client.api.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusFound,
			Header: http.Header{
				"Location": []string{"https://oss.aliyuncs.com/bucket/" + testHash + "?Signature=abc"},
			},
			Body: io.NopCloser(strings.NewReader("")),
		}, nil
	})

	d := &Driver{client: client, ns: "00"}

	got, err := d.Source(context.Background(), newFakeEntity(ObjectPath("00", testHash)),
		&driver.GetSourceArgs{DisplayName: "blob.bin"})

	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "https://oss.aliyuncs.com/"),
		"a blob must resolve to a URL the client can fetch itself, got %q", got)
	require.Contains(t, got, "Signature=abc",
		"the upstream signature must be preserved")
}

func TestThumbAsksForRelay(t *testing.T) {
	d := &Driver{}

	_, err := d.Thumb(context.Background(), nil, ".jpg", newFakeEntity("00/ab/cd"))

	require.ErrorIs(t, err, driver.ErrNoPublicUrl,
		"thumbnails are generated locally, so the caller must serve them through this server")
}

func TestCapabilitiesKeepUploadsRelayed(t *testing.T) {
	d := &Driver{}

	require.True(t, driver.UploadProxyRequired(d),
		"the object path is only known after the content is read, so uploads must relay")

	// Downloads are governed by the policy setting instead: the driver supports
	// both modes, so it must not hard-require the relay.
	require.False(t, driver.DownloadProxyRequired(d),
		"downloads must stay configurable per policy")
}

func TestCapabilitiesReportContentAddressing(t *testing.T) {
	d := &Driver{}
	features := d.Capabilities().StaticFeatures

	require.True(t, features.Enabled(int(driver.HandlerCapabilityContentAddressed)))
	require.True(t, features.Enabled(int(driver.HandlerCapabilitySourceDeferred)))
}

func TestObjectPathIsStableForSameContent(t *testing.T) {
	// The physical path drives deduplication, so it must be a pure function of
	// the digest and namespace.
	require.Equal(t, ObjectPath("00", testHash), ObjectPath("00", testHash))
	require.Regexp(t, regexp.MustCompile(`^00/[0-9a-f]{2}/[0-9a-f]{62}$`), ObjectPath("00", testHash))
}
