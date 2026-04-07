package model

import (
	"fmt"
)

// RLSCollectionConfig represents RLS configuration for a collection
type RLSCollectionConfig struct {
	CollectionID int64
	Enabled      bool
	Force        bool
}

// RLSUserTags represents dynamic metadata tags for a user
// Tags are used in RLS expressions to provide flexible, role-independent access control
type RLSUserTags struct {
	UserName string
	Tags     map[string]string
}

// NewRLSUserTags creates a new RLS user tags instance
func NewRLSUserTags(userName string, tags map[string]string) *RLSUserTags {
	if tags == nil {
		tags = make(map[string]string)
	}
	return &RLSUserTags{
		UserName: userName,
		Tags:     tags,
	}
}

// SetTag sets or updates a tag value
func (ut *RLSUserTags) SetTag(key, value string) error {
	if key == "" {
		return fmt.Errorf("tag key cannot be empty")
	}
	ut.Tags[key] = value
	return nil
}

// GetTag retrieves a tag value, returning empty string if not found
func (ut *RLSUserTags) GetTag(key string) string {
	if value, ok := ut.Tags[key]; ok {
		return value
	}
	return ""
}

// DeleteTag removes a tag
func (ut *RLSUserTags) DeleteTag(key string) {
	delete(ut.Tags, key)
}

// HasTag checks if a tag exists
func (ut *RLSUserTags) HasTag(key string) bool {
	_, ok := ut.Tags[key]
	return ok
}

// Clear removes all tags
func (ut *RLSUserTags) Clear() {
	ut.Tags = make(map[string]string)
}

// Merge merges new tags into existing tags (new tags override existing)
func (ut *RLSUserTags) Merge(newTags map[string]string) {
	for key, value := range newTags {
		ut.Tags[key] = value
	}
}

// Copy creates a deep copy of user tags
func (ut *RLSUserTags) Copy() *RLSUserTags {
	tagsCopy := make(map[string]string)
	for k, v := range ut.Tags {
		tagsCopy[k] = v
	}
	return &RLSUserTags{
		UserName: ut.UserName,
		Tags:     tagsCopy,
	}
}
