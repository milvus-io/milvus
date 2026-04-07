package model

import (
	"strings"
	"testing"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/stretchr/testify/assert"
)

func TestRLSPolicyValidate(t *testing.T) {
	tests := []struct {
		name    string
		policy  *RLSPolicy
		wantErr bool
	}{
		{
			name: "valid permissive policy",
			policy: &RLSPolicy{
				PolicyName:   "test_policy",
				CollectionID: 1,
				DBID:         1,
				PolicyType:   RLSPolicyTypePermissive,
				Actions:      []string{"query", "search"},
				Roles:        []string{"employee"},
				UsingExpr:    "owner_id == $current_user_name",
				CreatedAt:    1000,
			},
			wantErr: false,
		},
		{
			name: "empty policy_name",
			policy: &RLSPolicy{
				CollectionID: 1,
				PolicyType:   RLSPolicyTypePermissive,
				Actions:      []string{"query"},
				Roles:        []string{"PUBLIC"},
				UsingExpr:    "true",
			},
			wantErr: true,
		},
		{
			name: "invalid collection_id",
			policy: &RLSPolicy{
				PolicyName:   "test",
				CollectionID: -1,
				PolicyType:   RLSPolicyTypePermissive,
				Actions:      []string{"query"},
				Roles:        []string{"PUBLIC"},
				UsingExpr:    "true",
			},
			wantErr: true,
		},
		{
			name: "empty actions",
			policy: &RLSPolicy{
				PolicyName:   "test",
				CollectionID: 1,
				PolicyType:   RLSPolicyTypePermissive,
				Actions:      []string{},
				Roles:        []string{"PUBLIC"},
				UsingExpr:    "true",
			},
			wantErr: true,
		},
		{
			name: "empty roles",
			policy: &RLSPolicy{
				PolicyName:   "test",
				CollectionID: 1,
				PolicyType:   RLSPolicyTypePermissive,
				Actions:      []string{"query"},
				Roles:        []string{},
				UsingExpr:    "true",
			},
			wantErr: true,
		},
		{
			name: "missing expressions",
			policy: &RLSPolicy{
				PolicyName:   "test",
				CollectionID: 1,
				PolicyType:   RLSPolicyTypePermissive,
				Actions:      []string{"query"},
				Roles:        []string{"PUBLIC"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.policy.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestRLSPolicyAppliesTo(t *testing.T) {
	policy := &RLSPolicy{
		Actions: []string{"query", "search"},
	}

	assert.True(t, policy.AppliesTo("query"))
	assert.True(t, policy.AppliesTo("search"))
	assert.False(t, policy.AppliesTo("insert"))
	assert.False(t, policy.AppliesTo("delete"))
}

func TestRLSPolicyIsPublicRole(t *testing.T) {
	publicPolicy := &RLSPolicy{
		Roles: []string{"PUBLIC"},
	}
	assert.True(t, publicPolicy.IsPublicRole())

	namedPolicy := &RLSPolicy{
		Roles: []string{"employee"},
	}
	assert.False(t, namedPolicy.IsPublicRole())

	mixedPolicy := &RLSPolicy{
		Roles: []string{"employee", "PUBLIC"},
	}
	assert.True(t, mixedPolicy.IsPublicRole())
}

func TestNewRLSPolicy(t *testing.T) {
	policy := NewRLSPolicy(
		"test_policy",
		123,
		456,
		RLSPolicyTypePermissive,
		[]string{"query"},
		[]string{"PUBLIC"},
		"true",
		"",
		"test description",
	)

	assert.NotNil(t, policy)
	assert.Equal(t, "test_policy", policy.PolicyName)
	assert.Equal(t, int64(123), policy.CollectionID)
	assert.Equal(t, int64(456), policy.DBID)
	assert.True(t, policy.CreatedAt > 0)
}

func TestIsSupportedAction(t *testing.T) {
	tests := []struct {
		action string
		want   bool
	}{
		{"query", true},
		{"search", true},
		{"insert", true},
		{"delete", true},
		{"upsert", true},
		{"invalid", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			assert.Equal(t, tt.want, IsSupportedAction(tt.action))
		})
	}
}

func TestRLSPolicyValidateUsesConfiguredExpressionLength(t *testing.T) {
	params := paramtable.Get()
	params.Save(params.ProxyCfg.RLSMaxExpressionLength.Key, "8")
	defer params.Reset(params.ProxyCfg.RLSMaxExpressionLength.Key)

	policy := &RLSPolicy{
		PolicyName:   "len_policy",
		CollectionID: 1,
		DBID:         1,
		PolicyType:   RLSPolicyTypePermissive,
		Actions:      []string{"query"},
		Roles:        []string{"PUBLIC"},
		UsingExpr:    strings.Repeat("a", 9),
	}

	err := policy.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "max 8")
}
