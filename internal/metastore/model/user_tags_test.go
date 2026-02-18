package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRLSUserTagsSetGet(t *testing.T) {
	ut := NewRLSUserTags("alice", nil)

	err := ut.SetTag("department", "engineering")
	assert.NoError(t, err)
	assert.Equal(t, "engineering", ut.GetTag("department"))
}

func TestRLSUserTagsSetEmptyKey(t *testing.T) {
	ut := NewRLSUserTags("alice", nil)

	err := ut.SetTag("", "value")
	assert.Error(t, err)
}

func TestRLSUserTagsDeleteTag(t *testing.T) {
	ut := NewRLSUserTags("alice", map[string]string{
		"department": "engineering",
		"region":     "us-west",
	})

	assert.True(t, ut.HasTag("department"))
	ut.DeleteTag("department")
	assert.False(t, ut.HasTag("department"))
	assert.Equal(t, "us-west", ut.GetTag("region"))
}

func TestRLSUserTagsMerge(t *testing.T) {
	ut := NewRLSUserTags("alice", map[string]string{
		"department": "engineering",
	})

	ut.Merge(map[string]string{
		"region":     "us-west",
		"department": "sales", // override
	})

	assert.Equal(t, "sales", ut.GetTag("department"))
	assert.Equal(t, "us-west", ut.GetTag("region"))
}

func TestRLSUserTagsCopy(t *testing.T) {
	original := NewRLSUserTags("alice", map[string]string{
		"department": "engineering",
		"region":     "us-west",
	})

	copied := original.Copy()

	// Verify copy has same data
	assert.Equal(t, original.UserName, copied.UserName)
	assert.Equal(t, "engineering", copied.GetTag("department"))

	// Modify copy and verify original unchanged
	copied.SetTag("department", "sales")
	assert.Equal(t, "engineering", original.GetTag("department"))
	assert.Equal(t, "sales", copied.GetTag("department"))
}

func TestRLSUserTagsClear(t *testing.T) {
	ut := NewRLSUserTags("alice", map[string]string{
		"department": "engineering",
		"region":     "us-west",
	})

	assert.Equal(t, 2, len(ut.Tags))
	ut.Clear()
	assert.Equal(t, 0, len(ut.Tags))
}

func TestRLSUserTagsGetNonExistent(t *testing.T) {
	ut := NewRLSUserTags("alice", map[string]string{
		"department": "engineering",
	})

	assert.Equal(t, "", ut.GetTag("nonexistent"))
}

func TestNewRLSUserTagsNilMap(t *testing.T) {
	ut := NewRLSUserTags("bob", nil)

	assert.NotNil(t, ut)
	assert.Equal(t, "bob", ut.UserName)
	assert.NotNil(t, ut.Tags)
	assert.Equal(t, 0, len(ut.Tags))
}
