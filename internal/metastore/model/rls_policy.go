package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
	if strings.Contains(p.PolicyName, "/") {
		return fmt.Errorf("policy_name cannot contain '/'")
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

	needsUsing := false
	needsCheck := false
	for i, action := range p.Actions {
		normalized := strings.ToLower(strings.TrimSpace(action))
		if !IsSupportedAction(normalized) {
			return fmt.Errorf("unsupported action: %s", action)
		}
		p.Actions[i] = normalized

		switch normalized {
		case string(RLSActionQuery), string(RLSActionSearch), string(RLSActionDelete):
			needsUsing = true
		case string(RLSActionInsert), string(RLSActionUpsert):
			needsCheck = true
		}
	}

	for i, role := range p.Roles {
		normalized := strings.TrimSpace(role)
		if normalized == "" {
			return fmt.Errorf("roles cannot contain empty role name")
		}
		if strings.EqualFold(normalized, "PUBLIC") {
			normalized = "PUBLIC"
		}
		p.Roles[i] = normalized
	}

	p.UsingExpr = strings.TrimSpace(p.UsingExpr)
	p.CheckExpr = strings.TrimSpace(p.CheckExpr)
	if p.UsingExpr == "" && p.CheckExpr == "" {
		return fmt.Errorf("at least one of using_expr or check_expr must be specified")
	}
	if needsUsing && p.UsingExpr == "" {
		return fmt.Errorf("using_expr is required for query/search/delete actions")
	}
	if needsCheck && p.CheckExpr == "" {
		return fmt.Errorf("check_expr is required for insert/upsert actions")
	}

	// Validate expression lengths
	if err := validateExprLength(p.UsingExpr, "using_expr"); err != nil {
		return err
	}
	if err := validateExprLength(p.CheckExpr, "check_expr"); err != nil {
		return err
	}

	// Basic syntax validation (balanced parentheses)
	if p.UsingExpr != "" && !hasBalancedParens(p.UsingExpr) {
		return fmt.Errorf("using_expr has unbalanced parentheses")
	}
	if p.CheckExpr != "" && !hasBalancedParens(p.CheckExpr) {
		return fmt.Errorf("check_expr has unbalanced parentheses")
	}

	return nil
}

// MaxExpressionLength is the default maximum expression length for RLS policies.
const MaxExpressionLength = 4096

func validateExprLength(expr, name string) error {
	maxLen := paramtable.Get().ProxyCfg.RLSMaxExpressionLength.GetAsInt()
	if maxLen <= 0 {
		maxLen = MaxExpressionLength
	}
	if len(expr) > maxLen {
		return fmt.Errorf("%s too long: %d characters (max %d)", name, len(expr), maxLen)
	}
	return nil
}

func hasBalancedParens(expr string) bool {
	count := 0
	for _, ch := range expr {
		if ch == '(' {
			count++
		} else if ch == ')' {
			count--
		}
		if count < 0 {
			return false
		}
	}
	return count == 0
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
