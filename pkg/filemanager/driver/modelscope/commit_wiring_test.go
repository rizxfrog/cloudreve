package modelscope

import (
	"context"
	"testing"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/cloudreve/Cloudreve/v4/pkg/logging"
	"github.com/stretchr/testify/require"
)

// newPolicy builds a ModelScope policy with the given commit-queue settings.
func newPolicy(t *testing.T, settings *types.PolicySetting) *ent.StoragePolicy {
	t.Helper()

	base := &types.PolicySetting{
		ModelScopeRepoType:  "datasets",
		ModelScopeRevision:  "master",
		ModelScopeNamespace: "00",
	}
	if settings != nil {
		*base = *settings
		base.ModelScopeRepoType = "datasets"
		base.ModelScopeRevision = "master"
		base.ModelScopeNamespace = "00"
	}

	return &ent.StoragePolicy{
		Type:       types.PolicyTypeModelScope,
		Server:     testEndpoint(t),
		BucketName: "owner/repo",
		SecretKey:  "token",
		Settings:   base,
	}
}

// New must actually connect the queue when the policy asks for queued commits;
// a setting parsed but never wired is the failure this guards against.
func TestNewRoutesCommitsThroughQueueWhenEnabled(t *testing.T) {
	recorder := &commitRecorder{}
	q := newTestQueue(t)

	policy := newPolicy(t, &types.PolicySetting{
		ModelScopeQueueCommit:       true,
		ModelScopeCommitIntervalMin: 1,
		ModelScopeCommitIntervalMax: 1,
	})

	d, err := New(context.Background(), policy, nil, nil, q, logging.NewConsoleLogger(logging.LevelError))
	require.NoError(t, err)
	require.NotNil(t, d.client.commitRunner, "queued commits must install a commit runner")

	// Drive a real commit through the driver's client and observe the queue.
	d.client.api.Transport = recorder.transport(t)

	require.NoError(t, d.client.Commit(context.Background(),
		[]map[string]any{blobAction(ObjectPath("00", testHash), testHash, 1024)}))

	require.Equal(t, 1, q.SubmittedTasks(), "the commit must be submitted to the commit queue")
	require.Len(t, recorder.starts, 1, "the queued task must still perform the commit")
}

// With the setting off, commits stay on the direct path so existing ModelScope
// policies keep their current behavior.
func TestNewKeepsCommitsDirectWhenDisabled(t *testing.T) {
	recorder := &commitRecorder{}
	q := newTestQueue(t)

	d, err := New(context.Background(), newPolicy(t, nil), nil, nil, q, logging.NewConsoleLogger(logging.LevelError))
	require.NoError(t, err)
	require.Nil(t, d.client.commitRunner, "no commit runner may be installed when queuing is off")

	d.client.api.Transport = recorder.transport(t)
	require.NoError(t, d.client.Commit(context.Background(),
		[]map[string]any{blobAction(ObjectPath("00", testHash), testHash, 1024)}))

	require.Zero(t, q.SubmittedTasks(), "the commit queue must stay unused when the policy is off")
	require.Len(t, recorder.starts, 1, "the commit must still reach the repository")
}

// A stateless node has no queue and must still pace its commits inline rather
// than submit them to a queue that never runs.
func TestNewWithoutQueueStillPacesCommits(t *testing.T) {
	recorder := &commitRecorder{}
	policy := newPolicy(t, &types.PolicySetting{
		ModelScopeQueueCommit:       true,
		ModelScopeCommitIntervalMin: 1,
		ModelScopeCommitIntervalMax: 1,
	})

	d, err := New(context.Background(), policy, nil, nil, nil, logging.NewConsoleLogger(logging.LevelError))
	require.NoError(t, err)
	require.NotNil(t, d.client.commitRunner, "pacing must remain active without a queue")

	d.client.api.Transport = recorder.transport(t)
	require.NoError(t, d.client.Commit(context.Background(),
		[]map[string]any{blobAction(ObjectPath("00", testHash), testHash, 1024)}))

	require.Len(t, recorder.starts, 1)
}
