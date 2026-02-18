package model

import (
	"fmt"
	"time"
)

// RLSPolicyType represents the type of RLS policy (PERMISSIVE or RESTRICTIVE)
type RLSPolicyType int32

const (
	RLSPolicyTypePermissive  RLSPolicyType = 0
	RLSPolicyTypeRestrictive RLSPolicyType = 1
)

// RLSAction represents actions that can be controlled by RLS policies
type RLSAction string

const (
	RLSActionQuery  RLSAction = "query"
	RLSActionSearch RLSAction = "search"
	RLSActionInsert RLSAction = "insert"
	RLSActionDelete RLSAction = "delete"
	RLSActionUpsert RLSAction = "upsert"
)

// RLSPolicy defines a row-level security policy for a collection
type RLSPolicy struct {
	// Immutable metadata
	PolicyName   string
	CollectionID int64
	DBID         int64

	// Policy configuration
	PolicyType  RLSPolicyType
	Actions     []string // query, search, insert, delete, upsert
	Roles       []string // role names or "PUBLIC"
	UsingExpr   string   // Expression for SELECT/DELETE filtering
	CheckExpr   string   // Expression for INSERT/UPDATE validation
	Description string

	// Metadata
	CreatedAt uint64 // Unix timestamp in milliseconds
}

// Validate checks if the policy is valid
func (p *RLSPolicy) Validate() error {
	if p.PolicyName == "" {
		return fmt.Errorf("policy_name cannot be empty")
	}
	if p.CollectionID <= 0 {
		return fmt.Errorf("collection_id must be positive")
	}
	if p.PolicyType != RLSPolicyTypePermissive && p.PolicyType != RLSPolicyTypeRestrictive {
		return fmt.Errorf("invalid policy_type: %v", p.PolicyType)
	}
	if len(p.Actions) == 0 {
		return fmt.Errorf("actions cannot be empty")
	}
	if len(p.Roles) == 0 {
		return fmt.Errorf("roles cannot be empty")
	}
	if p.UsingExpr == "" && p.CheckExpr == "" {
		return fmt.Errorf("at least one of using_expr or check_expr must be specified")
	}
	return nil
}

// IsSupportedAction checks if the given action is supported
func IsSupportedAction(action string) bool {
	switch action {
	case "query", "search", "insert", "delete", "upsert":
		return true
	default:
		return false
	}
}

// IsPublicRole checks if the policy applies to all roles
func (p *RLSPolicy) IsPublicRole() bool {
	for _, role := range p.Roles {
		if role == "PUBLIC" {
			return true
		}
	}
	return false
}

// AppliesTo checks if this policy applies to the given action
func (p *RLSPolicy) AppliesTo(action string) bool {
	for _, a := range p.Actions {
		if a == action {
			return true
		}
	}
	return false
}

// NewRLSPolicy creates a new RLS policy with current timestamp
func NewRLSPolicy(policyName string, collectionID int64, dbID int64,
	policyType RLSPolicyType, actions, roles []string,
	usingExpr, checkExpr, description string) *RLSPolicy {
	return &RLSPolicy{
		PolicyName:   policyName,
		CollectionID: collectionID,
		DBID:         dbID,
		PolicyType:   policyType,
		Actions:      actions,
		Roles:        roles,
		UsingExpr:    usingExpr,
		CheckExpr:    checkExpr,
		Description:  description,
		CreatedAt:    uint64(time.Now().UnixMilli()),
	}
}
