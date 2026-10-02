package local

import (
	"testing"

	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/driver"
	"github.com/stretchr/testify/require"
)

// The local handler must relay downloads but must not be forced into the upload
// relay: its credential call is what creates and preallocates the placeholder
// file, so skipping it would leave an upload with nowhere to write.

func TestLocalDeclaresDownloadProxyOnly(t *testing.T) {
	d := &Driver{}

	require.True(t, driver.DownloadProxyRequired(d),
		"local content is only reachable through this server")

	require.False(t, driver.UploadProxyRequired(d),
		"local uploads are not relayed, so the handler must still receive its credential call")
}

func TestLocalReportsInboundGet(t *testing.T) {
	d := &Driver{}

	require.True(t, d.Capabilities().StaticFeatures.Enabled(int(driver.HandlerCapabilityInboundGet)),
		"content lives in this machine's filesystem and is opened directly")
}
