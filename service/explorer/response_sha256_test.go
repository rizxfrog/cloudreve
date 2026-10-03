package explorer

import (
	"context"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/driver/modelscope"
	"github.com/cloudreve/Cloudreve/v4/pkg/filemanager/fs"
	"github.com/cloudreve/Cloudreve/v4/pkg/hashid"
	"github.com/stretchr/testify/require"
)

// Verifies the digest is recovered for content-addressed policies and omitted for
// policies that name objects independently of their content. The entity response
// is what the sidebar renders, so a regression here would blank the value the UI
// shows or fabricate a digest that is not one.
func TestBuildEntitySha256(t *testing.T) {
	const digest = "8b49ca5bb6877d0830b644a436c0167a51b5b1bca92e55391eb289738edbaa00"
	msSource := modelscope.ObjectPath("00", digest)

	// A ModelScope entity: the source path embeds the digest.
	msEntity := fs.NewEntity(&ent.Entity{Source: msSource, StoragePolicyEntities: 2})
	localEntity := fs.NewEntity(&ent.Entity{Source: "/tmp/x.bin", StoragePolicyEntities: 1})

	ext := &fs.FileExtendedInfo{
		EntityStoragePolicies: map[int]*ent.StoragePolicy{
			1: {Type: types.PolicyTypeLocal, Settings: &types.PolicySetting{}},
			2: {Type: types.PolicyTypeModelScope, Settings: &types.PolicySetting{}},
		},
	}

	got := BuildEntity(context.Background(), ext, msEntity, mustHashid(t))
	require.Equal(t, digest, got.Sha256, "content-addressed entity must surface the digest")

	got = BuildEntity(context.Background(), ext, localEntity, mustHashid(t))
	require.Equal(t, "", got.Sha256, "content-independent entity must not fabricate a digest")
}

func mustHashid(t *testing.T) hashid.Encoder {
	t.Helper()
	h, err := hashid.New("test-salt")
	require.NoError(t, err)
	return h
}
