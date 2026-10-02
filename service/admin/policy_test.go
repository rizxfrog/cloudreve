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
