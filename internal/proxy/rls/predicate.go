// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rls

import (
	"context"
	"sort"

	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type exprKind int

const (
	usingExprKind exprKind = iota
	checkExprKind
)

type compiledKey struct {
	action rlsutil.PolicyAction
	kind   exprKind
}

type compiledCacheEntry struct {
	schemaVersion int32
	timezone      string
	expression    *compiledExpression
}

func (entry *compiledCacheEntry) matchesSchema(schemaVersion int32, timezone string) bool {
	return entry != nil && entry.schemaVersion == schemaVersion && entry.timezone == timezone
}

// ResolveUsingPredicate resolves the row filter for an RLS-protected operation.
func ResolveUsingPredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, schema *typeutil.SchemaHelper) (*planpb.Expr, error) {
	return defaultManager.resolveUsingPredicate(ctx, collectionID, principalName, action, schema)
}

func (m *manager) resolveUsingPredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, schema *typeutil.SchemaHelper) (*planpb.Expr, error) {
	return m.resolvePredicate(ctx, collectionID, principalName, action, usingExprKind, schema)
}

func (m *manager) resolveCheckPredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, schema *typeutil.SchemaHelper) (*planpb.Expr, error) {
	return m.resolvePredicate(ctx, collectionID, principalName, action, checkExprKind, schema)
}

func (m *manager) resolvePredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, kind exprKind, schema *typeutil.SchemaHelper) (*planpb.Expr, error) {
	if m == nil || collectionID == 0 {
		return nil, denyNoApplicableRLSPolicy(action, kind)
	}
	if err := m.ensurePoliciesFresh(ctx, collectionID); err != nil {
		return nil, merr.Wrapf(err, "failed to validate RLS metadata for collection %d", collectionID)
	}
	state := m.getCollectionState(collectionID)
	if state == nil {
		return nil, denyNoApplicableRLSPolicy(action, kind)
	}
	compiled, err := state.getCompiledExpression(action, kind, schema)
	if err != nil {
		return nil, err
	}
	if compiled == nil {
		return nil, denyNoApplicableRLSPolicy(action, kind)
	}

	var tags map[string]rlsutil.TagValue
	if compiled.needsTags() {
		tags, err = m.ensurePrincipalTags(ctx, collectionID, principalName)
		if err != nil {
			return nil, err
		}
	}
	expr, err := compiled.Instantiate(principalName, tags)
	if err != nil {
		return nil, err
	}
	if expr == nil {
		return nil, denyNoApplicableRLSPolicy(action, kind)
	}
	if rewriter.IsAlwaysTrueExpr(expr) {
		return nil, nil
	}
	return expr, nil
}

func (state *collectionState) getCompiledExpression(action rlsutil.PolicyAction, kind exprKind, schema *typeutil.SchemaHelper) (*compiledExpression, error) {
	key := compiledKey{action: action, kind: kind}
	var schemaVersion int32
	var timezone string
	if schema != nil {
		schemaVersion = schema.GetVersion()
		timezone = schema.GetTimezone()
	}

	state.mu.RLock()
	if len(state.policies) == 0 {
		state.mu.RUnlock()
		return nil, nil
	}
	if entry := state.compiled[key]; entry.matchesSchema(schemaVersion, timezone) {
		state.mu.RUnlock()
		return entry.expression, nil
	}
	state.mu.RUnlock()

	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.policies) == 0 {
		return nil, nil
	}
	if entry := state.compiled[key]; entry.matchesSchema(schemaVersion, timezone) {
		return entry.expression, nil
	}

	policies := orderedPolicies(state.policies)
	templates, combinedExpr := preparePolicyExprTemplates(policies, action, kind)
	maxLength := paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.GetAsInt()
	if len(combinedExpr) > maxLength {
		return nil, merr.WrapErrServiceQuotaExceededMsg("RLS combined expression exceeds max length %d", maxLength)
	}
	compiled, err := compileExprTemplates(schema, templates, timezone)
	if err != nil {
		return nil, err
	}
	if state.compiled == nil {
		state.compiled = make(map[compiledKey]*compiledCacheEntry)
	}
	state.compiled[key] = &compiledCacheEntry{
		schemaVersion: schemaVersion,
		timezone:      timezone,
		expression:    compiled,
	}
	return compiled, nil
}

func orderedPolicies(policiesByName map[string]*rlsutil.RowPolicy) []*rlsutil.RowPolicy {
	names := make([]string, 0, len(policiesByName))
	for name := range policiesByName {
		names = append(names, name)
	}
	sort.Strings(names)

	policies := make([]*rlsutil.RowPolicy, 0, len(names))
	for _, name := range names {
		policies = append(policies, policiesByName[name])
	}
	return policies
}

func denyNoApplicableRLSPolicy(action rlsutil.PolicyAction, kind exprKind) error {
	return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS: no applicable %s policies", rlsActionOperation(action), kind.policyLabel())
}

func (kind exprKind) policyLabel() string {
	if kind == checkExprKind {
		return "check"
	}
	return "using"
}

func (kind exprKind) expression(policy *rlsutil.RowPolicy) string {
	if kind == checkExprKind {
		return policy.GetCheckExpr()
	}
	return policy.GetUsingExpr()
}

func rlsActionOperation(action rlsutil.PolicyAction) string {
	switch action {
	case rlsutil.PolicyActionQuery:
		return "query"
	case rlsutil.PolicyActionQueryIterator:
		return "query iterator"
	case rlsutil.PolicyActionSearch:
		return "search"
	case rlsutil.PolicyActionSearchIterator:
		return "search iterator"
	case rlsutil.PolicyActionHybridSearch:
		return "hybrid search"
	case rlsutil.PolicyActionDelete:
		return "delete"
	case rlsutil.PolicyActionInsert:
		return "insert"
	case rlsutil.PolicyActionUpsert:
		return "upsert"
	default:
		return "unknown"
	}
}
