package admin

import (
	"testing"

	"github.com/cloudreve/Cloudreve/v4/ent"
	"github.com/cloudreve/Cloudreve/v4/inventory/types"
	"github.com/stretchr/testify/require"
)

func TestNormalizePolicyIgnoresOtherTypes(t *testing.T) {
	policy := &ent.StoragePolicy{Type: types.PolicyTypeS3, Server: "", Settings: &types.PolicySetting{}}

	require.NoError(t, normalizePolicy(policy))
	// Non ModelScope policies keep whatever the administrator submitted.
	require.Empty(t, policy.Server)
	require.False(t, policy.Settings.Relay)
}

func TestNormalizePolicyAppliesModelScopeDefaults(t *testing.T) {
	policy := &ent.StoragePolicy{
		Type:       types.PolicyTypeModelScope,
		BucketName: "owner/repo",
		SecretKey:  "token",
	}

	require.NoError(t, normalizePolicy(policy))

	// Objects are content addressed, so the target cannot be known before the
	// content is read and uploads must relay through the server.
	require.True(t, policy.Settings.Relay)
	require.Equal(t, "https://www.modelscope.cn", policy.Server)
	require.Equal(t, "datasets", policy.Settings.ModelScopeRepoType)
	require.Equal(t, "master", policy.Settings.ModelScopeRevision)
	require.Equal(t, "00", policy.Settings.ModelScopeNamespace)
}

func TestNormalizePolicyRejectsInvalidModelScopeSettings(t *testing.T) {
	cases := map[string]*ent.StoragePolicy{
		"missing repository": {
			Type:      types.PolicyTypeModelScope,
			SecretKey: "token",
		},
		"missing token": {
			Type:       types.PolicyTypeModelScope,
			BucketName: "owner/repo",
		},
		"unknown repository type": {
			Type:       types.PolicyTypeModelScope,
			BucketName: "owner/repo",
			SecretKey:  "token",
			Settings:   &types.PolicySetting{ModelScopeRepoType: "spaces"},
		},
		"namespace not two digits": {
			Type:       types.PolicyTypeModelScope,
			BucketName: "owner/repo",
			SecretKey:  "token",
			Settings:   &types.PolicySetting{ModelScopeNamespace: "abc"},
		},
		"namespace too long": {
			Type:       types.PolicyTypeModelScope,
			BucketName: "owner/repo",
			SecretKey:  "token",
			Settings:   &types.PolicySetting{ModelScopeNamespace: "001"},
		},
		"negative commit interval": {
			Type:       types.PolicyTypeModelScope,
			BucketName: "owner/repo",
			SecretKey:  "token",
			Settings: &types.PolicySetting{
				ModelScopeQueueCommit:       true,
				ModelScopeCommitIntervalMin: -1,
			},
		},
		"inverted commit interval": {
			Type:       types.PolicyTypeModelScope,
			BucketName: "owner/repo",
			SecretKey:  "token",
			Settings: &types.PolicySetting{
				ModelScopeQueueCommit:       true,
				ModelScopeCommitIntervalMin: 9,
				ModelScopeCommitIntervalMax: 3,
			},
		},
		"both commit modes": {
			Type:       types.PolicyTypeModelScope,
			BucketName: "owner/repo",
			SecretKey:  "token",
			Settings: &types.PolicySetting{
				ModelScopeQueueCommit: true,
				ModelScopeBatchCommit: true,
			},
		},
		"negative batch window": {
			Type:       types.PolicyTypeModelScope,
			BucketName: "owner/repo",
			SecretKey:  "token",
			Settings: &types.PolicySetting{
				ModelScopeBatchCommit:    true,
				ModelScopeBatchWindowMin: -1,
			},
		},
		"inverted batch window": {
			Type:       types.PolicyTypeModelScope,
			BucketName: "owner/repo",
			SecretKey:  "token",
			Settings: &types.PolicySetting{
				ModelScopeBatchCommit:    true,
				ModelScopeBatchWindowMin: 9,
				ModelScopeBatchWindowMax: 3,
			},
		},
	}

	for name, policy := range cases {
		t.Run(name, func(t *testing.T) {
			require.Error(t, normalizePolicy(policy))
		})
	}
}

func TestNormalizePolicyForcesRelayOnExistingModelScopePolicy(t *testing.T) {
	// An update may clear the flag; it is reasserted so a policy can never end
	// up configured for direct uploads, which content addressing cannot serve.
	policy := &ent.StoragePolicy{
		Type:       types.PolicyTypeModelScope,
		Server:     "https://modelscope.cn/",
		BucketName: "owner/repo",
		SecretKey:  "token",
		Settings: &types.PolicySetting{
			Relay:               false,
			ModelScopeRepoType:  "models",
			ModelScopeRevision:  "v1.0",
			ModelScopeNamespace: "42",
		},
	}

	require.NoError(t, normalizePolicy(policy))
	require.True(t, policy.Settings.Relay)

	// Explicit valid values are preserved rather than overwritten by defaults.
	require.Equal(t, "models", policy.Settings.ModelScopeRepoType)
	require.Equal(t, "v1.0", policy.Settings.ModelScopeRevision)
	require.Equal(t, "42", policy.Settings.ModelScopeNamespace)
}

func TestNormalizePolicyAcceptsModelScopeCommitQueue(t *testing.T) {
	// A zero bound means "use the default", so it must not be rejected just
	// because the other bound is set.
	policy := &ent.StoragePolicy{
		Type:       types.PolicyTypeModelScope,
		BucketName: "owner/repo",
		SecretKey:  "token",
		Settings: &types.PolicySetting{
			ModelScopeQueueCommit:       true,
			ModelScopeCommitIntervalMin: 3,
			ModelScopeCommitIntervalMax: 5,
		},
	}

	require.NoError(t, normalizePolicy(policy))
	require.Equal(t, 3, policy.Settings.ModelScopeCommitIntervalMin)
	require.Equal(t, 5, policy.Settings.ModelScopeCommitIntervalMax)
}

func TestNormalizePolicyIgnoresCommitIntervalWhenQueueDisabled(t *testing.T) {
	// The range is only meaningful with queuing on, so an unused inverted range
	// must not block an otherwise valid policy.
	policy := &ent.StoragePolicy{
		Type:       types.PolicyTypeModelScope,
		BucketName: "owner/repo",
		SecretKey:  "token",
		Settings: &types.PolicySetting{
			ModelScopeCommitIntervalMin: 9,
			ModelScopeCommitIntervalMax: 3,
		},
	}

	require.NoError(t, normalizePolicy(policy))
}
