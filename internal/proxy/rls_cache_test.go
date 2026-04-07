package proxy

import (
	"testing"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/stretchr/testify/assert"
)

func TestRLSCacheUpdateAndGetPolicies(t *testing.T) {
	cache := NewRLSCache()

	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"policy1",
			123,
			456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"true",
			"",
			"test policy",
		),
	}

	cache.UpdatePolicies(456, 123, policies)

	retrieved := cache.GetPoliciesForCollection(456, 123)
	assert.Len(t, retrieved, 1)
	assert.Equal(t, "policy1", retrieved[0].PolicyName)
}

func TestRLSCacheUpdateAndGetUserTags(t *testing.T) {
	cache := NewRLSCache()

	tags := map[string]string{
		"department": "engineering",
		"region":     "us-west",
	}

	cache.UpdateUserTags("alice", tags)

	retrieved := cache.GetUserTags("alice")
	assert.Equal(t, "engineering", retrieved["department"])
	assert.Equal(t, "us-west", retrieved["region"])
}

func TestRLSCacheUpdateUserTag(t *testing.T) {
	cache := NewRLSCache()

	cache.UpdateUserTag("alice", "department", "engineering")
	retrieved := cache.GetUserTags("alice")
	assert.Equal(t, "engineering", retrieved["department"])

	cache.UpdateUserTag("alice", "department", "sales")
	retrieved = cache.GetUserTags("alice")
	assert.Equal(t, "sales", retrieved["department"])
}

func TestRLSCacheDeleteUserTag(t *testing.T) {
	cache := NewRLSCache()

	cache.UpdateUserTag("alice", "department", "engineering")
	cache.UpdateUserTag("alice", "region", "us-west")

	cache.DeleteUserTag("alice", "department")
	retrieved := cache.GetUserTags("alice")
	assert.Empty(t, retrieved["department"])
	assert.Equal(t, "us-west", retrieved["region"])
}

func TestRLSCacheUpdateCollectionConfig(t *testing.T) {
	cache := NewRLSCache()

	cache.UpdateCollectionConfig(456, 123, true, false)

	config := cache.GetCollectionConfig(456, 123)
	assert.NotNil(t, config)
	assert.True(t, config.Enabled)
	assert.False(t, config.Force)
}

func TestRLSCacheInvalidateCollection(t *testing.T) {
	cache := NewRLSCache()

	tags := map[string]string{"department": "engineering"}
	cache.UpdateUserTags("alice", tags)

	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"policy1",
			123,
			456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"true",
			"",
			"test policy",
		),
	}
	cache.UpdatePolicies(456, 123, policies)

	// Invalidate collection cache
	cache.InvalidateCollectionCache(456, 123)

	// Policies should be gone
	retrieved := cache.GetPoliciesForCollection(456, 123)
	assert.Len(t, retrieved, 0)

	// User tags should remain
	userTags := cache.GetUserTags("alice")
	assert.Equal(t, "engineering", userTags["department"])
}

func TestRLSCacheInvalidateAll(t *testing.T) {
	cache := NewRLSCache()

	cache.UpdateUserTag("alice", "department", "engineering")
	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"policy1",
			123,
			456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"true",
			"",
			"test policy",
		),
	}
	cache.UpdatePolicies(456, 123, policies)

	cache.InvalidateAllCache()

	assert.Len(t, cache.GetPoliciesForCollection(456, 123), 0)
	assert.Empty(t, cache.GetUserTags("alice"))
}

func TestRLSCacheMultiplePolicies(t *testing.T) {
	cache := NewRLSCache()

	policies := []*model.RLSPolicy{
		model.NewRLSPolicy(
			"policy1",
			123,
			456,
			model.RLSPolicyTypePermissive,
			[]string{"query"},
			[]string{"PUBLIC"},
			"true",
			"",
			"policy 1",
		),
		model.NewRLSPolicy(
			"policy2",
			123,
			456,
			model.RLSPolicyTypeRestrictive,
			[]string{"search"},
			[]string{"PUBLIC"},
			"false",
			"",
			"policy 2",
		),
	}

	cache.UpdatePolicies(456, 123, policies)

	retrieved := cache.GetPoliciesForCollection(456, 123)
	assert.Len(t, retrieved, 2)
}

func TestRLSCacheGetUserTagsCopy(t *testing.T) {
	cache := NewRLSCache()

	tags := map[string]string{
		"department": "engineering",
	}
	cache.UpdateUserTags("alice", tags)

	// Get tags and modify the returned copy
	retrieved := cache.GetUserTags("alice")
	retrieved["department"] = "sales"

	// Original should be unchanged
	retrieved2 := cache.GetUserTags("alice")
	assert.Equal(t, "engineering", retrieved2["department"])
}
