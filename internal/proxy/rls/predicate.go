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

	"github.com/cockroachdb/errors"

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
	maxLength     int
	expression    *compiledExpression
	err           error
}

func (entry *compiledCacheEntry) matches(schemaVersion int32, timezone string, maxLength int) bool {
	return entry != nil && entry.schemaVersion == schemaVersion && entry.timezone == timezone && entry.maxLength == maxLength
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

// ResolveUpsertPredicates resolves USING and CHECK from one policy generation
// and one principal-tag snapshot.
func ResolveUpsertPredicates(ctx context.Context, collectionID UniqueID, principalName string, schema *typeutil.SchemaHelper) (*planpb.Expr, *planpb.Expr, error) {
	return defaultManager.resolveUpsertPredicates(ctx, collectionID, principalName, schema)
}

func (m *manager) resolveUpsertPredicates(ctx context.Context, collectionID UniqueID, principalName string, schema *typeutil.SchemaHelper) (*planpb.Expr, *planpb.Expr, error) {
	if m == nil {
		return nil, nil, merr.WrapErrServiceInternalMsg("failed to resolve RLS predicates without metadata manager")
	}
	if collectionID == 0 {
		return nil, nil, merr.WrapErrServiceInternalMsg("failed to resolve RLS predicates with empty collection id")
	}
	if _, _, err := ResolveRuntimePrincipal(true, principalName, "upsert"); err != nil {
		return nil, nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if err := m.ensurePoliciesFresh(ctx, collectionID); err != nil {
			return nil, nil, merr.Wrapf(err, "failed to validate RLS metadata for collection %d", collectionID)
		}
		state := m.getCollectionState(collectionID)
		if state == nil {
			return nil, nil, merr.WrapErrServiceUnavailableMsg("RLS metadata is unavailable for collection %d", collectionID)
		}
		state.mu.RLock()
		generation := state.policyGeneration
		state.mu.RUnlock()

		usingCompiled, err := state.getCompiledExpression(rlsutil.PolicyActionUpsert, usingExprKind, schema)
		if err != nil {
			if !m.policyRefreshCurrent(collectionID, state, generation) {
				continue
			}
			return nil, nil, err
		}
		checkCompiled, err := state.getCompiledExpression(rlsutil.PolicyActionUpsert, checkExprKind, schema)
		if !m.policyRefreshCurrent(collectionID, state, generation) {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if checkCompiled == nil {
			return nil, nil, denyNoApplicableRLSPolicy(rlsutil.PolicyActionUpsert, checkExprKind)
		}

		var tags map[string]rlsutil.TagValue
		if (usingCompiled != nil && usingCompiled.needsTags) || (checkCompiled != nil && checkCompiled.needsTags) {
			tags, err = m.ensurePrincipalTags(ctx, collectionID, principalName)
			if !m.policyRefreshCurrent(collectionID, state, generation) {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
		}

		// A missing USING policy denies existing rows. New-only upserts never
		// evaluate USING and remain governed by CHECK.
		using := alwaysFalsePredicate()
		if usingCompiled != nil {
			using, err = usingCompiled.Instantiate(principalName, tags)
			if !m.policyRefreshCurrent(collectionID, state, generation) {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			if using == nil {
				using = alwaysFalsePredicate()
			}
		}
		check, err := checkCompiled.Instantiate(principalName, tags)
		if !m.policyRefreshCurrent(collectionID, state, generation) {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if check == nil {
			return nil, nil, denyNoApplicableRLSPolicy(rlsutil.PolicyActionUpsert, checkExprKind)
		}
		if rewriter.IsAlwaysTrueExpr(using) {
			using = nil
		}
		if rewriter.IsAlwaysTrueExpr(check) {
			check = nil
		}
		return using, check, nil
	}
}

func (m *manager) resolvePredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, kind exprKind, schema *typeutil.SchemaHelper) (*planpb.Expr, error) {
	if m == nil {
		return nil, merr.WrapErrServiceInternalMsg("failed to resolve RLS predicate without metadata manager")
	}
	if collectionID == 0 {
		return nil, merr.WrapErrServiceInternalMsg("failed to resolve RLS predicate with empty collection id")
	}
	if _, _, err := ResolveRuntimePrincipal(true, principalName, rlsActionOperation(action)); err != nil {
		return nil, err
	}
	if err := m.ensurePoliciesFresh(ctx, collectionID); err != nil {
		return nil, merr.Wrapf(err, "failed to validate RLS metadata for collection %d", collectionID)
	}
	state := m.getCollectionState(collectionID)
	if state == nil {
		return nil, merr.WrapErrServiceUnavailableMsg("RLS metadata is unavailable for collection %d", collectionID)
	}
	compiled, err := state.getCompiledExpression(action, kind, schema)
	if err != nil {
		return nil, err
	}
	if compiled == nil {
		return nil, denyNoApplicableRLSPolicy(action, kind)
	}

	var tags map[string]rlsutil.TagValue
	if compiled.needsTags {
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

	maxLength := paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.GetAsInt()
	state.mu.RLock()
	if len(state.policies) == 0 {
		state.mu.RUnlock()
		return nil, nil
	}
	if entry := state.compiled[key]; entry.matches(schemaVersion, timezone, maxLength) {
		state.mu.RUnlock()
		return entry.expression, entry.err
	}
	state.mu.RUnlock()

	state.policyCompileMu.Lock()
	defer state.policyCompileMu.Unlock()
	for {
		maxLength = paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.GetAsInt()
		state.mu.RLock()
		if len(state.policies) == 0 {
			state.mu.RUnlock()
			return nil, nil
		}
		generation := state.policyGeneration
		if entry := state.compiled[key]; entry.matches(schemaVersion, timezone, maxLength) {
			state.mu.RUnlock()
			return entry.expression, entry.err
		}
		policies := make([]*rlsutil.RowPolicy, 0, len(state.policies))
		for _, policy := range state.policies {
			policies = append(policies, policy)
		}
		state.mu.RUnlock()

		sort.Slice(policies, func(i, j int) bool {
			return policies[i].GetPolicyName() < policies[j].GetPolicyName()
		})
		templates, combinedLength := preparePolicyExprTemplates(policies, action, kind)
		var compiled *compiledExpression
		var compileErr error
		if combinedLength > maxLength {
			compileErr = merr.WrapErrServiceQuotaExceededMsg("RLS combined expression exceeds max length %d", maxLength)
		} else {
			compiled, compileErr = compileExprTemplates(schema, templates, timezone, kind)
		}

		state.mu.Lock()
		if state.policyGeneration != generation {
			state.mu.Unlock()
			continue
		}
		if entry := state.compiled[key]; entry.matches(schemaVersion, timezone, maxLength) {
			state.mu.Unlock()
			return entry.expression, entry.err
		}
		if compileErr != nil && !cacheableCompileError(compileErr) {
			state.mu.Unlock()
			return nil, compileErr
		}
		if state.compiled == nil {
			state.compiled = make(map[compiledKey]*compiledCacheEntry)
		}
		state.compiled[key] = &compiledCacheEntry{
			schemaVersion: schemaVersion,
			timezone:      timezone,
			maxLength:     maxLength,
			expression:    compiled,
			err:           compileErr,
		}
		state.mu.Unlock()
		return compiled, compileErr
	}
}

func cacheableCompileError(err error) bool {
	return errors.Is(err, merr.ErrServiceQuotaExceeded) || errors.Is(err, merr.ErrDataIntegrity)
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
