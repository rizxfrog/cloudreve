package modelscope

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/stretchr/testify/require"
)

// shaOf returns the lowercase hex SHA-256 of b.
func shaOf(t *testing.T, b []byte) string {
	t.Helper()
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// newUploadProps builds the minimal upload properties a driver Put observes.
func newUploadProps(size int64) *fs.UploadProps {
	return &fs.UploadProps{Size: size}
}

// newUploadRequest wraps a readable file into the upload request shape the
// manager passes to a handler.
func newUploadRequest(f io.ReadCloser, props *fs.UploadProps) *fs.UploadRequest {
	seeker, _ := f.(io.Seeker)
	return &fs.UploadRequest{
		Props:  props,
		File:   f,
		Seeker: seeker,
	}
}

func TestNewClientAcceptsValidConfiguration(t *testing.T) {
	client, err := NewClient("https://www.modelscope.cn/", "token", "owner/repo", "models", "v1.0",
		nil)
	require.NoError(t, err)
	require.Equal(t, "https://www.modelscope.cn", client.endpoint)
	require.Equal(t, "models", client.repoType)
	require.Equal(t, "v1.0", client.revision)
}
