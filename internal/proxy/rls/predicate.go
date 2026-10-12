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
	"time"

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
	expression    *rlsutil.CompiledExpression
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
	snapshot, err := m.resolvePredicateSnapshot(ctx, collectionID, principalName, rlsutil.PolicyActionUpsert, schema, usingExprKind, checkExprKind)
	if err != nil {
		return nil, nil, err
	}
	checkCompiled := snapshot.expressions[checkExprKind]
	if checkCompiled == nil {
		return nil, nil, denyNoApplicableRLSPolicy(rlsutil.PolicyActionUpsert, checkExprKind)
	}

	// Missing USING denies existing rows; new-only upserts still use CHECK.
	using := alwaysFalsePredicate()
	if compiled := snapshot.expressions[usingExprKind]; compiled != nil {
		using, err = compiled.Instantiate(principalName, snapshot.tags)
		if err != nil {
			return nil, nil, err
		}
		if using == nil {
			using = alwaysFalsePredicate()
		}
	}
	check, err := checkCompiled.Instantiate(principalName, snapshot.tags)
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

func (m *manager) resolvePredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, kind exprKind, schema *typeutil.SchemaHelper) (*planpb.Expr, error) {
	snapshot, err := m.resolvePredicateSnapshot(ctx, collectionID, principalName, action, schema, kind)
	if err != nil {
		return nil, err
	}
	compiled := snapshot.expressions[kind]
	if compiled == nil {
		return nil, denyNoApplicableRLSPolicy(action, kind)
	}
	expr, err := compiled.Instantiate(principalName, snapshot.tags)
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

// predicateSnapshot owns immutable references valid for one request. Later
// invalidations affect new snapshots, not authorization already captured here.
type predicateSnapshot struct {
	expressions [2]*rlsutil.CompiledExpression
	tags        map[string]rlsutil.TagValue
}

func (m *manager) resolvePredicateSnapshot(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, schema *typeutil.SchemaHelper, kinds ...exprKind) (predicateSnapshot, error) {
	if m == nil || collectionID == 0 {
		return predicateSnapshot{}, merr.WrapErrServiceInternalMsg("failed to resolve RLS predicate with invalid manager or collection id")
	}
	if _, _, err := rlsutil.ResolveRuntimePrincipal(true, principalName, rlsutil.PolicyActionOperation(action)); err != nil {
		return predicateSnapshot{}, err
	}
	for {
		if err := ctx.Err(); err != nil {
			return predicateSnapshot{}, err
		}
		if err := m.ensurePoliciesFresh(ctx, collectionID); err != nil {
			return predicateSnapshot{}, merr.Wrapf(err, "failed to validate RLS metadata for collection %d", collectionID)
		}
		state := m.getCollectionState(collectionID)
		if state == nil {
			return predicateSnapshot{}, merr.WrapErrServiceUnavailableMsg("RLS metadata is unavailable for collection %d", collectionID)
		}
		state.mu.RLock()
		generation := state.policyGeneration
		state.mu.RUnlock()

		var snapshot predicateSnapshot
		var compileErr error
		loaded := true
		needsTags := false
		for _, kind := range kinds {
			snapshot.expressions[kind], loaded, compileErr = state.getCompiledExpression(action, kind, schema)
			if !loaded || compileErr != nil {
				break
			}
			needsTags = needsTags || snapshot.expressions[kind].NeedsTags()
		}

		// Compile without the metadata lock, then capture policies and cached
		// tags together. Never return a no-policy result from an invalidation gap.
		m.mu.RLock()
		state.mu.RLock()
		current := m.collections[collectionID] == state
		policiesCurrent := loaded && state.policies != nil && state.policyGeneration == generation
		if !current || !policiesCurrent || compileErr != nil {
			state.mu.RUnlock()
			m.mu.RUnlock()
			if !current {
				return predicateSnapshot{}, merr.WrapErrServiceUnavailableMsg("RLS collection %d was removed while resolving predicate", collectionID)
			}
			if !policiesCurrent {
				continue
			}
			return predicateSnapshot{}, compileErr
		}
		if !needsTags {
			state.mu.RUnlock()
			m.mu.RUnlock()
			return snapshot, nil
		}

		entry := state.principalTags[principalName]
		ttl := paramtable.Get().ProxyCfg.RLSMetaRefreshInterval.GetAsDuration(time.Second)
		tagsReady := principalTagsEntryFresh(entry, ttl, time.Now())
		if tagsReady {
			snapshot.tags = entry.tags
		}
		state.mu.RUnlock()
		m.mu.RUnlock()
		if tagsReady {
			return snapshot, nil
		}

		// A miss may return tags too large to cache. If the policy generation
		// stayed fixed throughout this read, both snapshots coexisted when the
		// tags were captured; do not require a cache insertion or retry forever.
		tags, err := m.ensurePrincipalTags(ctx, collectionID, principalName)
		if !m.policyRefreshCurrent(collectionID, state, generation) {
			continue
		}
		if err != nil {
			return predicateSnapshot{}, err
		}
		snapshot.tags = tags
		return snapshot, nil
	}
}

func (state *collectionState) getCompiledExpression(action rlsutil.PolicyAction, kind exprKind, schema *typeutil.SchemaHelper) (*rlsutil.CompiledExpression, bool, error) {
	key := compiledKey{action: action, kind: kind}
	var schemaVersion int32
	var timezone string
	if schema != nil {
		schemaVersion = schema.GetVersion()
		timezone = schema.GetTimezone()
	}

	maxLength := paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.GetAsInt()
	state.mu.RLock()
	if state.policies == nil {
		state.mu.RUnlock()
		return nil, false, nil
	}
	if len(state.policies) == 0 {
		state.mu.RUnlock()
		return nil, true, nil
	}
	if entry := state.compiled[key]; entry.matches(schemaVersion, timezone, maxLength) {
		state.mu.RUnlock()
		return entry.expression, true, entry.err
	}
	state.mu.RUnlock()

	state.policyCompileMu.Lock()
	defer state.policyCompileMu.Unlock()
	for {
		maxLength = paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.GetAsInt()
		state.mu.RLock()
		if state.policies == nil {
			state.mu.RUnlock()
			return nil, false, nil
		}
		if len(state.policies) == 0 {
			state.mu.RUnlock()
			return nil, true, nil
		}
		generation := state.policyGeneration
		if entry := state.compiled[key]; entry.matches(schemaVersion, timezone, maxLength) {
			state.mu.RUnlock()
			return entry.expression, true, entry.err
		}
		policies := make([]*rlsutil.RowPolicy, 0, len(state.policies))
		for _, policy := range state.policies {
			policies = append(policies, policy)
		}
		state.mu.RUnlock()

		var compiled *rlsutil.CompiledExpression
		var compileErr error
		if kind == checkExprKind {
			compiled, compileErr = rlsutil.CompileCheckExpression(policies, action, schema, maxLength)
		} else {
			compiled, compileErr = rlsutil.CompileUsingExpression(policies, action, schema, maxLength)
		}

		state.mu.Lock()
		if state.policyGeneration != generation {
			state.mu.Unlock()
			continue
		}
		if entry := state.compiled[key]; entry.matches(schemaVersion, timezone, maxLength) {
			state.mu.Unlock()
			return entry.expression, true, entry.err
		}
		if compileErr != nil && !cacheableCompileError(compileErr) {
			state.mu.Unlock()
			return nil, true, compileErr
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
		return compiled, true, compileErr
	}
}

func cacheableCompileError(err error) bool {
	return errors.Is(err, merr.ErrServiceQuotaExceeded) || errors.Is(err, merr.ErrDataIntegrity)
}

func denyNoApplicableRLSPolicy(action rlsutil.PolicyAction, kind exprKind) error {
	return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS: no applicable %s policies", rlsutil.PolicyActionOperation(action), kind.policyLabel())
}

func (kind exprKind) policyLabel() string {
	if kind == checkExprKind {
		return "check"
	}
	return "using"
}
