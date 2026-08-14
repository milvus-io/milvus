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
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/util/rlsutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type policySnapshot struct {
	Version        int64
	RefreshedAt    time.Time
	DBName         string
	CollectionName string
	Policies       []*rlsutil.RowPolicy
}

type SnapshotVersionAllocator func(ctx context.Context) (uint64, error)

type Manager interface {
	Init(ctx context.Context, coord CoordClient, allocVersion SnapshotVersionAllocator) error
	RefreshPolicySnapshot(ctx context.Context, coord CoordClient, dbName string, collectionName string, collectionID UniqueID, version uint64) error

	GetRLSUsingPredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, enforceRLS bool, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs) (*planpb.Expr, error)
	ApplyRLSUsingPredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, enforceRLS bool, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs, plan *planpb.PlanNode) error
	GetRLSCheckPredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, enforceRLS bool, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs) (*planpb.Expr, error)
}

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

func (entry *compiledCacheEntry) matchesSchemaContext(schemaVersion int32, timezone string) bool {
	return entry != nil && entry.schemaVersion == schemaVersion && entry.timezone == timezone
}

type collectionState struct {
	mu                          sync.RWMutex
	dbName                      string
	collectionName              string
	policyVersion               int64
	policyLastSuccessfulRefresh time.Time
	policies                    map[string]*rlsutil.RowPolicy
	compiled                    map[compiledKey]*compiledCacheEntry
	principalTags               map[string]*principalTagsEntry
}

type collectionKey struct {
	collectionID UniqueID
}

type principalKey struct {
	collectionID  UniqueID
	principalName string
}

type principalTagsEntry struct {
	refreshedAt time.Time
	tags        map[string]rlsutil.TagValue
}

type manager struct {
	mu                    sync.RWMutex
	collections           map[collectionKey]*collectionState
	dependencyMu          sync.RWMutex
	coord                 CoordClient
	allocVersion          SnapshotVersionAllocator
	validateFreshness     bool
	metadataRefreshes     conc.Singleflight[struct{}]
	principalRefreshes    conc.Singleflight[map[string]rlsutil.TagValue]
	refreshLocks          *lock.KeyLock[UniqueID]
	principalRefreshLocks *lock.KeyLock[principalKey]
	ttlCancel             context.CancelFunc
}

const principalCacheScanInterval = 10 * time.Minute

var defaultManager = newManager()

func DefaultManager() Manager {
	return defaultManager
}

func RemoveCollection(ctx context.Context, collectionID UniqueID) {
	defaultManager.removeCollection(ctx, collectionID)
}

func RemovePolicyCollection(ctx context.Context, collectionID UniqueID) {
	defaultManager.removePolicyCollection(ctx, collectionID)
}

func RemovePrincipal(ctx context.Context, collectionID UniqueID, principalName string) {
	defaultManager.removePrincipal(ctx, collectionID, principalName)
}

func RemoveDatabase(ctx context.Context, dbName string) {
	defaultManager.removeDatabase(ctx, dbName)
}

func newManager() *manager {
	return &manager{
		collections:           map[collectionKey]*collectionState{},
		refreshLocks:          lock.NewKeyLock[UniqueID](),
		principalRefreshLocks: lock.NewKeyLock[principalKey](),
	}
}

func (m *manager) configure(ctx context.Context, coord CoordClient, allocVersion SnapshotVersionAllocator) {
	m.dependencyMu.Lock()
	if m.ttlCancel != nil {
		m.ttlCancel()
	}
	scanCtx, cancel := context.WithCancel(ctx)
	m.coord = coord
	m.allocVersion = allocVersion
	m.validateFreshness = true
	m.ttlCancel = cancel
	m.dependencyMu.Unlock()
	if ctx.Done() != nil {
		go m.runPrincipalCacheScanner(scanCtx, principalCacheScanInterval)
	}
}

func (m *manager) refreshDependencies() (CoordClient, SnapshotVersionAllocator, bool) {
	m.dependencyMu.RLock()
	defer m.dependencyMu.RUnlock()
	return m.coord, m.allocVersion, m.validateFreshness
}

func (m *manager) ensureFreshMetadata(ctx context.Context, collectionID UniqueID) error {
	if m == nil || collectionID == 0 {
		return merr.WrapErrServiceInternalMsg("failed to validate RLS metadata freshness with invalid manager or collection id")
	}
	coord, allocVersion, validateFreshness := m.refreshDependencies()
	if !validateFreshness {
		return nil
	}
	if coord == nil || allocVersion == nil {
		return merr.WrapErrServiceInternalMsg("failed to refresh RLS metadata without required dependencies")
	}
	refreshTTL := paramtable.Get().ProxyCfg.RLSMetaRefreshInterval.GetAsDuration(time.Second)
	if refreshTTL <= 0 {
		return merr.WrapErrServiceInternalMsg("failed to validate RLS metadata freshness with invalid TTL %s", refreshTTL)
	}
	m.refreshLocks.RLock(collectionID)
	defer m.refreshLocks.RUnlock(collectionID)
	if !m.snapshotRefreshDue(collectionID, refreshTTL, time.Now()) {
		return nil
	}

	_, err, _ := m.metadataRefreshes.Do(strconv.FormatInt(collectionID, 10), func() (struct{}, error) {
		if !m.snapshotRefreshDue(collectionID, refreshTTL, time.Now()) {
			return struct{}{}, nil
		}
		coord, allocVersion, validateFreshness := m.refreshDependencies()
		if !validateFreshness || coord == nil || allocVersion == nil {
			return struct{}{}, merr.WrapErrServiceInternalMsg("failed to refresh RLS metadata without required dependencies")
		}
		version, err := allocVersion(ctx)
		if err != nil {
			return struct{}{}, merr.Wrap(err, "failed to allocate RLS metadata refresh version")
		}
		if err := m.refreshPolicySnapshotUnlocked(ctx, coord, "", "", collectionID, version); err != nil {
			return struct{}{}, merr.Wrap(err, "failed to refresh expired RLS metadata")
		}
		return struct{}{}, nil
	})
	return err
}

func (m *manager) snapshotRefreshDue(collectionID UniqueID, refreshTTL time.Duration, now time.Time) bool {
	state := m.getCollectionState(newCollectionKey(collectionID))
	if state == nil {
		return true
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.policyLastSuccessfulRefresh.IsZero() || !state.policyLastSuccessfulRefresh.Add(refreshTTL).After(now)
}

func (m *manager) setRLSPolicySnapshot(_ string, collectionID UniqueID, snapshot policySnapshot) bool {
	if m == nil || collectionID == 0 {
		return false
	}
	return m.getOrCreateCollectionState(newCollectionKey(collectionID)).setRLSPolicySnapshot(snapshot)
}

func (state *collectionState) setRLSPolicySnapshot(snapshot policySnapshot) bool {
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if isStaleSnapshotVersion(snapshot.Version, state.policyVersion) {
		return false
	}
	if snapshot.RefreshedAt.IsZero() {
		snapshot.RefreshedAt = time.Now()
	}
	state.policyVersion = snapshot.Version
	state.policyLastSuccessfulRefresh = snapshot.RefreshedAt
	if snapshot.DBName != "" {
		state.dbName = snapshot.DBName
	}
	if snapshot.CollectionName != "" {
		state.collectionName = snapshot.CollectionName
	}
	state.policies = map[string]*rlsutil.RowPolicy{}
	state.compiled = map[compiledKey]*compiledCacheEntry{}
	for _, policy := range snapshot.Policies {
		if policy == nil || policy.GetPolicyName() == "" {
			continue
		}
		state.policies[policy.GetPolicyName()] = cloneRowPolicy(policy)
	}
	return true
}

func (m *manager) setPrincipalTags(key principalKey, entry *principalTagsEntry) bool {
	state := m.getCollectionState(newCollectionKey(key.collectionID))
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.principalTags == nil {
		state.principalTags = make(map[string]*principalTagsEntry)
	}
	state.principalTags[key.principalName] = entry
	return true
}

func (m *manager) getPrincipalTagsEntry(key principalKey) *principalTagsEntry {
	state := m.getCollectionState(newCollectionKey(key.collectionID))
	if state == nil {
		return nil
	}
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.principalTags[key.principalName]
}

func (m *manager) removePolicyCollection(_ context.Context, collectionID UniqueID) {
	if m == nil || collectionID == 0 {
		return
	}
	m.refreshLocks.Lock(collectionID)
	defer m.refreshLocks.Unlock(collectionID)
	state := m.getCollectionState(newCollectionKey(collectionID))
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.policyVersion = 0
	state.policyLastSuccessfulRefresh = time.Time{}
	state.policies = map[string]*rlsutil.RowPolicy{}
	state.compiled = map[compiledKey]*compiledCacheEntry{}
}

func (m *manager) removePrincipal(_ context.Context, collectionID UniqueID, principalName string) {
	if m == nil || collectionID == 0 || principalName == "" {
		return
	}
	key := principalKey{collectionID: collectionID, principalName: principalName}
	m.principalRefreshLocks.Lock(key)
	defer m.principalRefreshLocks.Unlock(key)
	state := m.getCollectionState(newCollectionKey(collectionID))
	if state == nil {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.principalTags, principalName)
}

func (m *manager) removeCollection(ctx context.Context, collectionID UniqueID) {
	if m == nil || collectionID == 0 {
		return
	}
	m.refreshLocks.Lock(collectionID)
	defer m.refreshLocks.Unlock(collectionID)
	m.mu.Lock()
	delete(m.collections, newCollectionKey(collectionID))
	m.mu.Unlock()
}

func (m *manager) removeDatabase(ctx context.Context, dbName string) {
	if m == nil || dbName == "" {
		return
	}
	m.mu.RLock()
	keys := make([]collectionKey, 0)
	for key, state := range m.collections {
		state.mu.RLock()
		matches := state.dbName == dbName
		state.mu.RUnlock()
		if matches {
			keys = append(keys, key)
		}
	}
	m.mu.RUnlock()
	for _, key := range keys {
		m.removeCollection(ctx, key.collectionID)
	}
}

func (m *manager) runPrincipalCacheScanner(ctx context.Context, interval time.Duration) {
	for {
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case now := <-timer.C:
			m.expirePrincipalTags(now)
		}
	}
}

func (m *manager) expirePrincipalTags(now time.Time) {
	_, _, validateFreshness := m.refreshDependencies()
	if !validateFreshness {
		return
	}
	refreshTTL := paramtable.Get().ProxyCfg.RLSMetaRefreshInterval.GetAsDuration(time.Second)
	if refreshTTL <= 0 {
		return
	}
	m.mu.RLock()
	states := make(map[collectionKey]*collectionState, len(m.collections))
	for key, state := range m.collections {
		states[key] = state
	}
	m.mu.RUnlock()
	for key, state := range states {
		state.mu.RLock()
		principals := make([]string, 0, len(state.principalTags))
		for principalName, entry := range state.principalTags {
			if entry == nil || !entry.refreshedAt.Add(refreshTTL).After(now) {
				principals = append(principals, principalName)
			}
		}
		state.mu.RUnlock()
		for _, principalName := range principals {
			principalKey := principalKey{collectionID: key.collectionID, principalName: principalName}
			m.principalRefreshLocks.Lock(principalKey)
			state.mu.Lock()
			entry := state.principalTags[principalName]
			if entry == nil || !entry.refreshedAt.Add(refreshTTL).After(now) {
				delete(state.principalTags, principalName)
			}
			state.mu.Unlock()
			m.principalRefreshLocks.Unlock(principalKey)
		}
	}
}

func (m *manager) GetRLSUsingPredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, enforceRLS bool, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs) (*planpb.Expr, error) {
	return m.getRLSPredicate(ctx, collectionID, principalName, action, enforceRLS, usingExprKind, schemaHelper, visitorArgs, func(policy *rlsutil.RowPolicy) string {
		return policy.GetUsingExpr()
	})
}

func (m *manager) ApplyRLSUsingPredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, enforceRLS bool, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs, plan *planpb.PlanNode) error {
	predicate, err := m.GetRLSUsingPredicate(ctx, collectionID, principalName, action, enforceRLS, schemaHelper, visitorArgs)
	if err != nil {
		return err
	}
	return MergePredicateToPlan(plan, predicate)
}

func (m *manager) GetRLSCheckPredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, enforceRLS bool, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs) (*planpb.Expr, error) {
	return m.getRLSPredicate(ctx, collectionID, principalName, action, enforceRLS, checkExprKind, schemaHelper, visitorArgs, func(policy *rlsutil.RowPolicy) string {
		return policy.GetCheckExpr()
	})
}

func (m *manager) getRLSPredicate(ctx context.Context, collectionID UniqueID, principalName string, action rlsutil.PolicyAction, enforceRLS bool, kind exprKind, schemaHelper *typeutil.SchemaHelper, visitorArgs *planparserv2.ParserVisitorArgs, exprSelector func(*rlsutil.RowPolicy) string) (*planpb.Expr, error) {
	if !enforceRLS {
		return nil, nil
	}
	if m == nil || collectionID == 0 {
		return nil, denyNoApplicableRLSPolicy(action, kind)
	}
	if err := m.ensureFreshMetadata(ctx, collectionID); err != nil {
		return nil, merr.Wrapf(err, "failed to validate RLS metadata for collection %d", collectionID)
	}
	tags, err := m.ensurePrincipalTags(ctx, collectionID, principalName)
	if err != nil {
		return nil, err
	}

	state := m.getCollectionState(newCollectionKey(collectionID))
	if state == nil {
		return nil, denyNoApplicableRLSPolicy(action, kind)
	}
	compiledExpr, err := state.getCompiledExpr(action, kind, schemaHelper, visitorArgs, exprSelector)
	if err != nil || compiledExpr == nil {
		if err != nil {
			return nil, err
		}
		return nil, denyNoApplicableRLSPolicy(action, kind)
	}

	expr, err := compiledExpr.Instantiate(principalName, tags)
	if err != nil {
		return nil, err
	}
	if expr == nil {
		return nil, denyNoApplicableRLSPolicy(action, kind)
	}
	if isAlwaysTrueExpr(expr) {
		return nil, nil
	}
	return expr, nil
}

func denyNoApplicableRLSPolicy(action rlsutil.PolicyAction, kind exprKind) error {
	return merr.WrapErrPrivilegeNotPermitted("%s operation denied by RLS: no applicable %s policies", rlsActionOperation(action), kind.policyLabel())
}

func (kind exprKind) policyLabel() string {
	switch kind {
	case checkExprKind:
		return "check"
	default:
		return "using"
	}
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

func (state *collectionState) getCompiledExpr(action rlsutil.PolicyAction, kind exprKind, schemaHelper *typeutil.SchemaHelper, _ *planparserv2.ParserVisitorArgs, exprSelector func(*rlsutil.RowPolicy) string) (*compiledExpression, error) {
	key := compiledKey{
		action: action,
		kind:   kind,
	}
	var schemaVersion int32
	var timezone string
	if schemaHelper != nil {
		schemaVersion = schemaHelper.GetVersion()
		timezone = schemaHelper.GetTimezone()
	}

	state.mu.RLock()
	if len(state.policies) == 0 {
		state.mu.RUnlock()
		return nil, nil
	}
	if entry, ok := state.compiled[key]; ok && entry.matchesSchemaContext(schemaVersion, timezone) {
		state.mu.RUnlock()
		return entry.expression, nil
	}
	state.mu.RUnlock()

	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.policies) == 0 {
		return nil, nil
	}

	if state.compiled == nil {
		state.compiled = map[compiledKey]*compiledCacheEntry{}
	}
	if entry, ok := state.compiled[key]; ok && entry.matchesSchemaContext(schemaVersion, timezone) {
		return entry.expression, nil
	}

	policies := orderedPolicies(state.policies)
	templates, combinedExpr := preparePolicyExprTemplates(policies, action, exprSelector)
	if maxExpressionLength := paramtable.Get().ProxyCfg.RLSMaxCombinedExpressionLength.GetAsInt(); len(combinedExpr) > maxExpressionLength {
		return nil, merr.WrapErrServiceQuotaExceededMsg("RLS combined expression exceeds max length %d", maxExpressionLength)
	}
	var policyVisitorArgs *planparserv2.ParserVisitorArgs
	if schemaHelper != nil {
		policyVisitorArgs = &planparserv2.ParserVisitorArgs{Timezone: timezone}
	}
	compiledExpr, err := compileExprTemplates(schemaHelper, templates, policyVisitorArgs)
	if err != nil {
		return nil, err
	}
	state.compiled[key] = &compiledCacheEntry{
		schemaVersion: schemaVersion,
		timezone:      timezone,
		expression:    compiledExpr,
	}
	return compiledExpr, nil
}

func newCollectionState() *collectionState {
	return &collectionState{
		policies:      map[string]*rlsutil.RowPolicy{},
		compiled:      map[compiledKey]*compiledCacheEntry{},
		principalTags: map[string]*principalTagsEntry{},
	}
}

func cloneRowPolicy(policy *rlsutil.RowPolicy) *rlsutil.RowPolicy {
	if policy == nil {
		return nil
	}
	return &rlsutil.RowPolicy{
		PolicyName:  policy.PolicyName,
		PolicyType:  policy.PolicyType,
		Actions:     slices.Clone(policy.Actions),
		UsingExpr:   policy.UsingExpr,
		CheckExpr:   policy.CheckExpr,
		Description: policy.Description,
		PolicyId:    policy.PolicyId,
	}
}

func newCollectionKey(collectionID UniqueID) collectionKey {
	return collectionKey{
		collectionID: collectionID,
	}
}

func (m *manager) getCollectionState(key collectionKey) *collectionState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.collections[key]
}

func (m *manager) getOrCreateCollectionState(key collectionKey) *collectionState {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.collections[key]
	if state == nil {
		state = newCollectionState()
		m.collections[key] = state
	}
	return state
}

func clonePrincipalTags(tags map[string]rlsutil.TagValue) map[string]rlsutil.TagValue {
	return rlsutil.CloneTags(tags)
}

func isStaleSnapshotVersion(incomingVersion int64, currentVersion int64) bool {
	if currentVersion == 0 {
		return false
	}
	// Version 0 is used by startup bootstrap and must not overwrite a snapshot
	// delivered by a timestamped invalidation.
	if incomingVersion == 0 {
		return true
	}
	return incomingVersion <= currentVersion
}
