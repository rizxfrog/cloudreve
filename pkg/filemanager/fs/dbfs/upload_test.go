package dbfs

import (
	"testing"

	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/stretchr/testify/require"
)

// An encrypted upload must not declare a client digest. The digest describes the
// file the client holds, while storage receives ciphertext, so a content
// addressed driver would either refuse the stream or file the pointer against
// content that does not match the digest.
func TestClientHashForUpload(t *testing.T) {
	const digest = "24/780644a95f759a9aeeb228c3d852028f2fd40ce0b74d68134246ec4a959547"

	cases := []struct {
		name       string
		clientHash string
		encrypt    *types.EncryptMetadata
		expected   string
	}{
		{
			name:       "unencrypted upload keeps the digest",
			clientHash: digest,
			encrypt:    nil,
			expected:   digest,
		},
		{
			name:       "encrypted upload drops the digest",
			clientHash: digest,
			encrypt:    &types.EncryptMetadata{Algorithm: types.CipherAES256CTR},
			expected:   "",
		},
		{
			name:       "no digest stays empty",
			clientHash: "",
			encrypt:    &types.EncryptMetadata{Algorithm: types.CipherAES256CTR},
			expected:   "",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.expected, clientHashForUpload(c.clientHash, c.encrypt))
		})
	}
}
