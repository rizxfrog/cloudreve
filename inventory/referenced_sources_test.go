package inventory

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/ent/enttest"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/boolset"
	"github.com/cloudreve/Cloudreve/v4/pkg/conf"
	"github.com/stretchr/testify/require"
)

// newTestFileClient builds a file client over a throwaway in-memory database.
func newTestFileClient(t *testing.T) (*ent.Client, FileClient) {
	t.Helper()

	client := enttest.Open(t, "sqlite3", filepath.Join(t.TempDir(), "referenced.db"))
	t.Cleanup(func() { _ = client.Close() })

	return client, NewFileClient(client, conf.SQLite3DB, nil)
}

// entitySeq gives each created entity a distinct owner, so helper calls do not
// collide on the unique user email.
var entitySeq int

// createReferencingEntity stores one entity holding the given physical source,
// linked to a file the way a completed upload leaves it.
func createReferencingEntity(t *testing.T, client *ent.Client, ctx context.Context,
	policyID int, source string, referenceCount int, entityType types.EntityType) int {
	t.Helper()

	entitySeq++
	suffix := strconv.Itoa(entitySeq)

	// A user must belong to a group; the relation is required by the schema.
	group, err := client.Group.Create().SetName("g-" + suffix).SetPermissions(&boolset.BooleanSet{}).Save(ctx)
	require.NoError(t, err)

	owner, err := client.User.Create().
		SetEmail("owner" + suffix + "@example.com").
		SetNick("owner").
		SetPassword("x").
		SetGroup(group).
		Save(ctx)
	require.NoError(t, err)

	file, err := client.File.Create().
		SetType(int(types.FileTypeFile)).
		SetName("f").
		SetOwnerID(owner.ID).
		SetStoragePolicyFiles(policyID).
		Save(ctx)
	require.NoError(t, err)

	entity, err := client.Entity.Create().
		SetType(int(entityType)).
		SetSource(source).
		SetSize(10).
		SetStoragePolicyEntities(policyID).
		SetReferenceCount(referenceCount).
		Save(ctx)
	require.NoError(t, err)

	require.NoError(t, client.Entity.UpdateOne(entity).AddFile(file).Exec(ctx))
	return entity.ID
}

func createPolicy(t *testing.T, client *ent.Client, ctx context.Context) int {
	t.Helper()

	policy, err := client.StoragePolicy.Create().
		SetName("modelscope").
		SetType(types.PolicyTypeModelScope).
		Save(ctx)
	require.NoError(t, err)

	return policy.ID
}

func TestReferencedSourcesSeparatesSharedAndSoleObjects(t *testing.T) {
	client, fc := newTestFileClient(t)
	ctx := context.Background()
	policyID := createPolicy(t, client, ctx)

	// Two entities share one physical object, which is exactly what content
	// addressing produces when identical content is uploaded twice. A third
	// entity owns an object of its own.
	shared := "00/24/" + "780644a95f759a9aeeb228c3d852028f2fd40ce0b74d68134246ec4a959547"
	sole := "00/ab/" + "1111111111111111111111111111111111111111111111111111111111111111"

	// Both are being recycled; a second entity keeps the shared object alive.
	recycledShared := createReferencingEntity(t, client, ctx, policyID, shared, 1, types.EntityTypeVersion)
	createReferencingEntity(t, client, ctx, policyID, shared, 1, types.EntityTypeVersion)
	recycledSole := createReferencingEntity(t, client, ctx, policyID, sole, 1, types.EntityTypeVersion)

	referenced, err := fc.ReferencedSources(ctx, []string{shared, sole}, []int{recycledShared, recycledSole})
	require.NoError(t, err)

	// The shared object must survive: another live entity still points at it.
	require.Contains(t, referenced, shared)
	// Nothing else references the sole object, so it may be removed.
	require.NotContains(t, referenced, sole)
}

func TestReferencedSourcesIgnoresUnreferencedEntities(t *testing.T) {
	client, fc := newTestFileClient(t)
	ctx := context.Background()
	policyID := createPolicy(t, client, ctx)

	source := "00/24/" + "780644a95f759a9aeeb228c3d852028f2fd40ce0b74d68134246ec4a959547"

	// A stale entity that has lost every reference does not keep the object alive.
	createReferencingEntity(t, client, ctx, policyID, source, 0, types.EntityTypeVersion)

	referenced, err := fc.ReferencedSources(ctx, []string{source}, nil)
	require.NoError(t, err)
	require.NotContains(t, referenced, source)
}

func TestReferencedSourcesExcludesTheWholeRecycleBatch(t *testing.T) {
	client, fc := newTestFileClient(t)
	ctx := context.Background()
	policyID := createPolicy(t, client, ctx)

	source := "00/24/" + "780644a95f759a9aeeb228c3d852028f2fd40ce0b74d68134246ec4a959547"

	// Both entities are in the same recycle batch, so neither counts as a
	// surviving reference for the other.
	first := createReferencingEntity(t, client, ctx, policyID, source, 1, types.EntityTypeVersion)
	second := createReferencingEntity(t, client, ctx, policyID, source, 1, types.EntityTypeVersion)

	referenced, err := fc.ReferencedSources(ctx, []string{source}, []int{first, second})
	require.NoError(t, err)
	require.NotContains(t, referenced, source)
}

func TestReferencedSourcesReturnsEmptySetForNoInput(t *testing.T) {
	_, fc := newTestFileClient(t)

	referenced, err := fc.ReferencedSources(context.Background(), nil, nil)
	require.NoError(t, err)
	require.Empty(t, referenced)
}
