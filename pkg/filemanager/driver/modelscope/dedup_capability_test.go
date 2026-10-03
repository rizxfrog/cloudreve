package modelscope

import (
	"context"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/driver"
	"github.com/stretchr/testify/require"
)

// TestCapabilitiesAdvertiseDigestDedup pins that the driver tells the manager it
// can answer an existence question from a digest alone. Without the capability
// the manager never asks, and a client that already has the content would be
// forced to send it again.
func TestCapabilitiesAdvertiseDigestDedup(t *testing.T) {
	d := &Driver{}

	require.True(t,
		d.Capabilities().StaticFeatures.Enabled(int(driver.HandlerCapabilityDigestDedup)),
		"uploads must be able to skip content the store already holds")

	if _, ok := interface{}(d).(driver.DigestResolver); !ok {
		t.Fatal("advertising the capability without implementing DigestResolver leaves the manager with nothing to call")
	}
}

// TestDigestProbeIsNotUsedForInlineContent keeps the reference-only path away
// from objects whose content must travel inside the commit.
func TestDigestProbeIsNotUsedForInlineContent(t *testing.T) {
	d := &Driver{}

	exists, err := d.ObjectExistsByDigest(context.Background(), testHash, inlineLimit)
	require.NoError(t, err)
	require.False(t, exists)
}
