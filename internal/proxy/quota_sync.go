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

package proxy

import (
	"math"
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/quotasync"
	rlinternal "github.com/milvus-io/milvus/internal/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type quotaDefaults map[int32]*proxypb.Limiter

type quotaReceiver struct {
	controlled map[quotasync.Key]struct{}
	enabled    bool
	session    string
	version    quotasync.Version // setRatesMu protects version
	defaults   atomic.Pointer[quotaDefaults]
	ready      atomic.Bool
}

func newQuotaReceiver() quotaReceiver {
	if !Params.QuotaConfig.VersionedRatesEnabled.GetAsBool() {
		return quotaReceiver{}
	}
	return quotaReceiver{enabled: true, session: uuid.NewString()}
}

// QuotaSyncStatus is independent of throughput metric collection failures.
func (m *SimpleLimiter) QuotaSyncStatus() *commonpb.Status {
	if m == nil || !m.quotaSync.enabled {
		return merr.Success()
	}
	m.setRatesMu.Lock()
	defer m.setRatesMu.Unlock()
	return quotasync.Status(quotasync.AlreadyApplied, m.quotaSync.session, m.quotaSync.version)
}

func (m *SimpleLimiter) quotaFactory(scope internalpb.RateScope) func() *rlinternal.RateLimiterNode {
	return func() *rlinternal.RateLimiterNode {
		node := rlinternal.NewRateLimiterNode(scope)
		if defaults := m.quotaSync.defaults.Load(); defaults != nil {
			for _, rate := range (*defaults)[int32(scope)].GetRates() {
				node.GetLimiters().Insert(rate.Rt, ratelimitutil.NewLimiter(ratelimitutil.Limit(rate.R), rate.R))
			}
		} else {
			initLimiter(node, getDefaultLimiterConfig(scope), nil)
		}
		return node
	}
}

// validateQuotaPolicy checks the entire scope before any live bucket changes.
// A malformed coordinator message is a system protocol error, not user input.
func validateQuotaPolicy(scope internalpb.RateScope, allowed map[internalpb.RateType]*paramtable.ParamItem, p *proxypb.Limiter, defaults bool) error {
	seen := make(map[internalpb.RateType]bool)
	for _, r := range p.GetRates() {
		if r == nil || allowed[r.GetRt()] == nil || seen[r.GetRt()] || math.IsNaN(r.GetR()) || math.IsInf(r.GetR(), 0) || r.GetR() < 0 {
			return merr.WrapErrServiceInternalMsg("invalid quota rate at scope %s", scope.String())
		}
		seen[r.GetRt()] = true
	}
	if defaults && len(seen) != len(allowed) {
		return merr.WrapErrServiceInternalMsg("incomplete quota scope defaults")
	}
	if len(p.GetStates()) != len(p.GetCodes()) || len(p.GetReasons()) > len(p.GetStates()) {
		return merr.WrapErrServiceInternalMsg("quota state/code/reason lengths do not match")
	}
	states := make(map[milvuspb.QuotaState]bool)
	for _, state := range p.GetStates() {
		if states[state] || (state != milvuspb.QuotaState_DenyToWrite && state != milvuspb.QuotaState_DenyToRead && state != milvuspb.QuotaState_DenyToDDL) {
			return merr.WrapErrServiceInternalMsg("invalid quota state")
		}
		states[state] = true
	}
	if defaults && len(states) != 0 {
		return merr.WrapErrServiceInternalMsg("quota defaults cannot carry deny states")
	}
	return nil
}

func (m *SimpleLimiter) ApplyQuotaRequest(req *proxypb.SetRatesRequest) *commonpb.Status {
	m.setRatesMu.Lock()
	defer m.setRatesMu.Unlock()
	state := &m.quotaSync
	reply := func(result string) *commonpb.Status { return quotasync.Status(result, state.session, state.version) }
	if req.GetQuotaProtocol() == 0 {
		if state.version.Valid() {
			return reply(quotasync.Stale)
		}
		if err := m.setLegacyRates(req.GetRootLimiter()); err != nil {
			return merr.Status(err)
		}
		state.ready.Store(true)
		return merr.Success()
	}
	if !state.enabled || req.GetQuotaProtocol() != quotasync.Protocol {
		return merr.Status(merr.Wrap(merr.ErrServiceUnimplemented, "quota protocol unsupported"))
	}
	if req.GetQuotaMode() == proxypb.QuotaSyncMode_QUOTA_PROBE {
		return reply(quotasync.AlreadyApplied)
	}
	if req.GetQuotaSession() != state.session {
		return reply(quotasync.NeedFull)
	}
	target := quotasync.FromVersion(req.GetTargetVersion())
	if !target.Valid() {
		return merr.Status(merr.WrapErrServiceInternalMsg("quota target version is empty"))
	}
	if state.version.Valid() {
		switch target.Compare(state.version) {
		case -1:
			return reply(quotasync.Stale)
		case 0:
			return reply(quotasync.AlreadyApplied)
		}
	}
	full := req.GetQuotaMode() == proxypb.QuotaSyncMode_QUOTA_FULL
	if !full && req.GetQuotaMode() != proxypb.QuotaSyncMode_QUOTA_DELTA {
		return merr.Status(merr.WrapErrServiceInternalMsg("invalid quota mode"))
	}
	if !full {
		base := quotasync.FromVersion(req.GetBaseVersion())
		if !base.Valid() || !state.version.Valid() || base != state.version || base.Epoch != target.Epoch || target.Seq != base.Seq+1 {
			return reply(quotasync.NeedFull)
		}
		if req.RootLimiter != nil || len(req.ScopeDefaults) != 0 {
			return merr.Status(merr.WrapErrServiceInternalMsg("quota delta must not contain a full snapshot or defaults"))
		}
	}
	defaults := quotaDefaults(nil)
	allowed := make(map[internalpb.RateScope]map[internalpb.RateType]*paramtable.ParamItem, 4)
	for _, scope := range []internalpb.RateScope{internalpb.RateScope_Cluster, internalpb.RateScope_Database, internalpb.RateScope_Collection, internalpb.RateScope_Partition} {
		allowed[scope] = getDefaultLimiterConfig(scope)
	}
	if full {
		if len(req.Upserts) != 0 || len(req.Deletes) != 0 || len(req.ScopeDefaults) != 4 {
			return merr.Status(merr.WrapErrServiceInternalMsg("malformed quota FULL"))
		}
		defaults = make(quotaDefaults, 4)
		for _, scope := range []internalpb.RateScope{internalpb.RateScope_Cluster, internalpb.RateScope_Database, internalpb.RateScope_Collection, internalpb.RateScope_Partition} {
			p := req.ScopeDefaults[int32(scope)]
			if err := validateQuotaPolicy(scope, allowed[scope], p, true); err != nil {
				return merr.Status(err)
			}
			defaults[int32(scope)] = quotasync.Canonical(p)
		}
	} else {
		defaults = *state.defaults.Load()
	}
	var policies map[quotasync.Key]*proxypb.Limiter
	if full {
		var err error
		policies, err = quotasync.Flatten(req.RootLimiter)
		if err != nil {
			return merr.Status(err)
		}
	} else {
		policies = make(map[quotasync.Key]*proxypb.Limiter, len(req.Upserts))
		for _, upsert := range req.Upserts {
			if upsert == nil || upsert.Key == nil {
				return merr.Status(merr.WrapErrServiceInternalMsg("nil quota upsert"))
			}
			k := quotasync.FromKey(upsert.Key)
			if _, ok := policies[k]; ok {
				return merr.Status(merr.WrapErrServiceInternalMsg("duplicate quota upsert"))
			}
			policies[k] = upsert.Limiter
		}
	}
	deleted := make(map[quotasync.Key]bool, len(req.Deletes))
	for _, key := range req.Deletes {
		k := quotasync.FromKey(key)
		if key == nil || !k.Valid() || k.Scope == internalpb.RateScope_Cluster || deleted[k] {
			return merr.Status(merr.WrapErrServiceInternalMsg("invalid quota deletion"))
		}
		deleted[k] = true
	}
	for k, p := range policies {
		if !k.Valid() {
			return merr.Status(merr.WrapErrServiceInternalMsg("invalid quota scope path"))
		}
		if err := validateQuotaPolicy(k.Scope, allowed[k.Scope], p, false); err != nil {
			return merr.Status(err)
		}
		for ancestor := k; ; ancestor = ancestor.Parent() {
			if deleted[ancestor] {
				return merr.Status(merr.WrapErrServiceInternalMsg("quota upsert is inside a deleted scope"))
			}
			if ancestor.Scope == internalpb.RateScope_Cluster {
				break
			}
			parent := ancestor.Parent()
			if _, ok := policies[parent]; !ok && !hasQuotaScope(state.controlled, parent) {
				return merr.Status(merr.WrapErrServiceInternalMsg("quota scope parent is missing"))
			}
		}
	}
	// Policies are read only during this call and copied as scalar values into
	// live buckets. No need to clone/sort every incoming protobuf scope; only
	// the four retained defaults above require an owned copy.
	// All recoverable validation is complete. Reuse live buckets and install
	// effective values directly, rather than reset-to-default then overwrite.
	if full {
		state.defaults.Store(&defaults)
		state.controlled = make(map[quotasync.Key]struct{}, len(policies))
		var replace func(quotasync.Key, *rlinternal.RateLimiterNode)
		replace = func(k quotasync.Key, node *rlinternal.RateLimiterNode) {
			m.installQuotaPolicy(k, node, policies[k], defaults)
			if _, controlled := policies[k]; controlled {
				state.controlled[k] = struct{}{}
				delete(policies, k)
			}
			node.GetChildren().Range(func(id int64, child *rlinternal.RateLimiterNode) bool {
				next := k
				next.Scope = child.Level()
				switch next.Scope {
				case internalpb.RateScope_Database:
					next.DB = id
				case internalpb.RateScope_Collection:
					next.Collection = id
				case internalpb.RateScope_Partition:
					next.Partition = id
				}
				replace(next, child)
				return true
			})
		}
		replace(quotasync.Key{Scope: internalpb.RateScope_Cluster}, m.rateLimiter.GetRootLimiters())
	}
	for k, p := range policies {
		m.installQuotaPolicy(k, m.quotaNode(k, true), p, defaults)
		state.controlled[k] = struct{}{}
	}
	for k := range deleted {
		parent := m.quotaNode(k.Parent(), false)
		if parent != nil {
			id := k.DB
			if k.Scope == internalpb.RateScope_Collection {
				id = k.Collection
			}
			if k.Scope == internalpb.RateScope_Partition {
				id = k.Partition
			}
			if node := parent.GetChild(id); node != nil {
				forgetQuotaSubtree(state.controlled, k, node)
			}
			parent.GetChildren().Remove(id)
		}
	}
	if full {
		m.rateLimiter.ClearInvalidLimiterNode(req.RootLimiter)
	}
	state.version = target
	state.ready.Store(true)
	return reply(quotasync.Applied)
}

func (m *SimpleLimiter) quotaNode(k quotasync.Key, create bool) *rlinternal.RateLimiterNode {
	db := m.quotaFactory(internalpb.RateScope_Database)
	coll := m.quotaFactory(internalpb.RateScope_Collection)
	part := m.quotaFactory(internalpb.RateScope_Partition)
	switch k.Scope {
	case internalpb.RateScope_Cluster:
		return m.rateLimiter.GetRootLimiters()
	case internalpb.RateScope_Database:
		if create {
			return m.rateLimiter.GetOrCreateDatabaseLimiters(k.DB, db)
		}
		return m.rateLimiter.GetDatabaseLimiters(k.DB)
	case internalpb.RateScope_Collection:
		if create {
			return m.rateLimiter.GetOrCreateCollectionLimiters(k.DB, k.Collection, db, coll)
		}
		return m.rateLimiter.GetCollectionLimiters(k.DB, k.Collection)
	case internalpb.RateScope_Partition:
		if create {
			return m.rateLimiter.GetOrCreatePartitionLimiters(k.DB, k.Collection, k.Partition, db, coll, part)
		}
		return m.rateLimiter.GetPartitionLimiters(k.DB, k.Collection, k.Partition)
	}
	return nil
}

func (m *SimpleLimiter) installQuotaPolicy(k quotasync.Key, node *rlinternal.RateLimiterNode, p *proxypb.Limiter, defaults quotaDefaults) {
	source := "cluster"
	switch k.Scope {
	case internalpb.RateScope_Database:
		source = "db." + strconv.FormatInt(k.DB, 10)
	case internalpb.RateScope_Collection:
		source = "collection." + strconv.FormatInt(k.Collection, 10)
	case internalpb.RateScope_Partition:
		source = "partition." + strconv.FormatInt(k.Partition, 10)
	}
	for _, base := range defaults[int32(k.Scope)].GetRates() {
		value := base.R
		for _, override := range p.GetRates() {
			if override.Rt == base.Rt {
				value = override.R
				break
			}
		}
		limiter, ok := node.GetLimiters().Get(base.Rt)
		if !ok {
			node.GetLimiters().Insert(base.Rt, ratelimitutil.NewLimiter(ratelimitutil.Limit(value), value))
		} else if limiter.Limit() != ratelimitutil.Limit(value) {
			limiter.SetLimit(ratelimitutil.Limit(value))
		}
		if ok && limiter.Limit() == ratelimitutil.Inf {
			continue
		}
		setRateGaugeByRateType(base.Rt, paramtable.GetNodeID(), source, value)
	}
	if len(p.GetStates()) == 0 && node.GetQuotaStates().Len() == 0 {
		return
	}
	states := typeutil.NewConcurrentMap[milvuspb.QuotaState, *rlinternal.QuotaStateInfo]()
	for i, state := range p.GetStates() {
		reason := ""
		if i < len(p.Reasons) {
			reason = p.Reasons[i]
		}
		states.Insert(state, &rlinternal.QuotaStateInfo{ErrorCode: p.Codes[i], Reason: reason})
	}
	node.SetQuotaStates(states)
}

func hasQuotaScope(scopes map[quotasync.Key]struct{}, key quotasync.Key) bool {
	_, ok := scopes[key]
	return ok
}

func forgetQuotaSubtree(scopes map[quotasync.Key]struct{}, key quotasync.Key, node *rlinternal.RateLimiterNode) {
	delete(scopes, key)
	node.GetChildren().Range(func(id int64, child *rlinternal.RateLimiterNode) bool {
		next := key
		next.Scope = child.Level()
		switch next.Scope {
		case internalpb.RateScope_Database:
			next.DB = id
		case internalpb.RateScope_Collection:
			next.Collection = id
		case internalpb.RateScope_Partition:
			next.Partition = id
		}
		forgetQuotaSubtree(scopes, next, child)
		return true
	})
}
