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

package quotasync

import (
	"maps"
	"sort"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/quota"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const Protocol = 1
const (
	Applied        = "applied"
	AlreadyApplied = "already_applied"
	NeedFull       = "need_full"
	Stale          = "stale"
)

var emptyPolicy = &proxypb.Limiter{}

type Version struct{ Epoch, Seq uint64 }

func FromVersion(v *proxypb.QuotaRuleVersion) Version { return Version{v.GetEpoch(), v.GetSeq()} }
func (v Version) Proto() *proxypb.QuotaRuleVersion {
	return &proxypb.QuotaRuleVersion{Epoch: v.Epoch, Seq: v.Seq}
}

func (v Version) Compare(other Version) int {
	if v == other {
		return 0
	}
	if v.Epoch < other.Epoch || (v.Epoch == other.Epoch && v.Seq < other.Seq) {
		return -1
	}
	return 1
}
func (v Version) Valid() bool { return v.Epoch != 0 && v.Seq != 0 }

type Key struct {
	Scope                     internalpb.RateScope
	DB, Collection, Partition int64
}

func FromKey(k *proxypb.QuotaScopeKey) Key {
	return Key{k.GetScope(), k.GetDatabaseId(), k.GetCollectionId(), k.GetPartitionId()}
}

func (k Key) Proto() *proxypb.QuotaScopeKey {
	return &proxypb.QuotaScopeKey{Scope: k.Scope, DatabaseId: k.DB, CollectionId: k.Collection, PartitionId: k.Partition}
}

func (k Key) Valid() bool {
	switch k.Scope {
	case internalpb.RateScope_Cluster:
		return k.DB == 0 && k.Collection == 0 && k.Partition == 0
	case internalpb.RateScope_Database:
		return k.DB > 0 && k.Collection == 0 && k.Partition == 0
	case internalpb.RateScope_Collection:
		return k.DB > 0 && k.Collection > 0 && k.Partition == 0
	case internalpb.RateScope_Partition:
		return k.DB > 0 && k.Collection > 0 && k.Partition > 0
	}
	return false
}

func (k Key) Parent() Key {
	switch k.Scope {
	case internalpb.RateScope_Partition:
		return Key{Scope: internalpb.RateScope_Collection, DB: k.DB, Collection: k.Collection}
	case internalpb.RateScope_Collection:
		return Key{Scope: internalpb.RateScope_Database, DB: k.DB}
	default:
		return Key{Scope: internalpb.RateScope_Cluster}
	}
}

func (k Key) Contains(child Key) bool {
	if k.Scope == internalpb.RateScope_Cluster {
		return true
	}
	if k.DB != child.DB {
		return false
	}
	if k.Scope == internalpb.RateScope_Database {
		return true
	}
	if k.Collection != child.Collection {
		return false
	}
	return k.Scope == internalpb.RateScope_Collection || (k.Scope == child.Scope && k.Partition == child.Partition)
}

// ScopeDefaults returns the configured default policy of every scope: the
// values a Proxy applies to scopes without an explicit policy.
func ScopeDefaults() map[int32]*proxypb.Limiter {
	defaults := make(map[int32]*proxypb.Limiter, 4)
	for _, scope := range []internalpb.RateScope{internalpb.RateScope_Cluster, internalpb.RateScope_Database, internalpb.RateScope_Collection, internalpb.RateScope_Partition} {
		p := &proxypb.Limiter{}
		for rt, param := range quota.GetQuotaConfigMap(scope) {
			p.Rates = append(p.Rates, &internalpb.Rate{Rt: rt, R: param.GetAsFloat()})
		}
		defaults[int32(scope)] = Canonical(p)
	}
	return defaults
}

// Canonical copies values and makes semantic equality independent of map/rate
// iteration order. Callers validate array lengths before calling this helper.
func Canonical(p *proxypb.Limiter) *proxypb.Limiter {
	if p == nil || (len(p.Rates) == 0 && len(p.States) == 0 && len(p.Codes) == 0 && len(p.Reasons) == 0) {
		return emptyPolicy
	}
	n := proto.Clone(p).(*proxypb.Limiter)
	sort.Slice(n.Rates, func(i, j int) bool { return n.Rates[i].Rt < n.Rates[j].Rt })
	order := make([]int, len(n.States))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return n.States[order[i]] < n.States[order[j]] })
	n.States = nil
	n.Codes = nil
	n.Reasons = nil
	for _, i := range order {
		n.States = append(n.States, p.States[i])
		n.Codes = append(n.Codes, p.Codes[i])
		reason := ""
		if i < len(p.Reasons) {
			reason = p.Reasons[i]
		}
		n.Reasons = append(n.Reasons, reason)
	}
	return n
}

// Flatten validates structural hierarchy, including partition leaf nodes.
func Flatten(root *proxypb.LimiterNode) (map[Key]*proxypb.Limiter, error) {
	if root == nil {
		return nil, merr.WrapErrServiceInternalMsg("quota FULL has no root")
	}
	result := make(map[Key]*proxypb.Limiter)
	var visit func(Key, *proxypb.LimiterNode) error
	visit = func(k Key, n *proxypb.LimiterNode) error {
		if n == nil || !k.Valid() {
			return merr.WrapErrServiceInternalMsg("invalid quota scope")
		}
		result[k] = n.GetLimiter()
		for id, child := range n.GetChildren() {
			next := k
			switch k.Scope {
			case internalpb.RateScope_Cluster:
				next.Scope = internalpb.RateScope_Database
				next.DB = id
			case internalpb.RateScope_Database:
				next.Scope = internalpb.RateScope_Collection
				next.Collection = id
			case internalpb.RateScope_Collection:
				next.Scope = internalpb.RateScope_Partition
				next.Partition = id
			default:
				return merr.WrapErrServiceInternalMsg("quota partition cannot have children")
			}
			if err := visit(next, child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(Key{Scope: internalpb.RateScope_Cluster}, root); err != nil {
		return nil, err
	}
	return result, nil
}

// Publisher retains exactly one logical snapshot and one latest delta. It is
// owned by the serial quota loop; RPCs finish before the next publication.
// Full protobuf trees are built only when a receiver needs them.
type Publisher struct {
	Version  Version
	Policies map[Key]*proxypb.Limiter
	Defaults map[int32]*proxypb.Limiter
	Delta    *proxypb.SetRatesRequest
}

func (s *Publisher) Publish(epoch uint64, policies map[Key]*proxypb.Limiter, defaults map[int32]*proxypb.Limiter) bool {
	upserts := make([]*proxypb.QuotaScopePolicy, 0)
	deletes := make([]*proxypb.QuotaScopeKey, 0)
	for k, p := range policies {
		if old, ok := s.Policies[k]; !ok || !proto.Equal(old, p) {
			upserts = append(upserts, &proxypb.QuotaScopePolicy{Key: k.Proto(), Limiter: p})
		}
	}
	for k := range s.Policies {
		if _, ok := policies[k]; !ok {
			deletes = append(deletes, k.Proto())
		}
	}
	changedDefaults := !maps.EqualFunc(s.Defaults, defaults, func(a, b *proxypb.Limiter) bool { return proto.Equal(a, b) })
	if len(upserts) == 0 && len(deletes) == 0 && !changedDefaults && s.Version.Valid() && s.Version.Epoch == epoch {
		return false
	}
	base := s.Version
	next := Version{Epoch: epoch, Seq: base.Seq + 1}
	if base.Epoch != epoch {
		next.Seq = 1
	}
	s.Policies = policies
	s.Defaults = defaults
	s.Version = next
	s.Delta = &proxypb.SetRatesRequest{QuotaProtocol: Protocol, QuotaMode: proxypb.QuotaSyncMode_QUOTA_DELTA, BaseVersion: base.Proto(), TargetVersion: next.Proto(), Upserts: upserts, Deletes: deletes}
	// Defaults affect lazy buckets as well as explicit policies. Keep this case
	// simple: a defaults change establishes a new full baseline for every Proxy.
	if changedDefaults || base.Epoch != epoch {
		s.Delta = nil
	}
	return true
}

func (s *Publisher) Full() *proxypb.SetRatesRequest {
	root := &proxypb.LimiterNode{}
	for k, p := range s.Policies {
		n := root
		ids := []int64{}
		switch k.Scope {
		case internalpb.RateScope_Database:
			ids = []int64{k.DB}
		case internalpb.RateScope_Collection:
			ids = []int64{k.DB, k.Collection}
		case internalpb.RateScope_Partition:
			ids = []int64{k.DB, k.Collection, k.Partition}
		}
		for _, id := range ids {
			if n.Children == nil {
				n.Children = make(map[int64]*proxypb.LimiterNode)
			}
			if n.Children[id] == nil {
				n.Children[id] = &proxypb.LimiterNode{}
			}
			n = n.Children[id]
		}
		n.Limiter = p
	}
	return &proxypb.SetRatesRequest{QuotaProtocol: Protocol, QuotaMode: proxypb.QuotaSyncMode_QUOTA_FULL, TargetVersion: s.Version.Proto(), RootLimiter: root, ScopeDefaults: s.Defaults}
}

// Status uses existing ExtraInfo without changing merr's wire classification.
func Status(result, session string, v Version) *commonpb.Status {
	s := merr.Success()
	s.ExtraInfo = map[string]string{"quota_protocol": "1", "quota_sync_result": result, "quota_session": session, "quota_epoch": strconv.FormatUint(v.Epoch, 10), "quota_seq": strconv.FormatUint(v.Seq, 10)}
	return s
}

func ParseStatus(s *commonpb.Status) (string, string, Version, bool) {
	if s == nil || !merr.Ok(s) || s.ExtraInfo["quota_protocol"] != "1" {
		return "", "", Version{}, false
	}
	epoch, e1 := strconv.ParseUint(s.ExtraInfo["quota_epoch"], 10, 64)
	seq, e2 := strconv.ParseUint(s.ExtraInfo["quota_seq"], 10, 64)
	session := s.ExtraInfo["quota_session"]
	return s.ExtraInfo["quota_sync_result"], session, Version{epoch, seq}, e1 == nil && e2 == nil && session != ""
}

// PublishPatch is the local calculation path. Only changed/removed scopes are
// examined; the current full state and the latest delta commit together.
// The caller enumerates deleted descendants before detaching a calculation
// subtree, keeping deletion work proportional to that subtree's size.
func (s *Publisher) PublishPatch(epoch uint64, policies map[Key]*proxypb.Limiter, deletes map[Key]bool, defaults map[int32]*proxypb.Limiter) bool {
	upserts := make([]*proxypb.QuotaScopePolicy, 0, len(policies))
	removed := make([]*proxypb.QuotaScopeKey, 0, len(deletes))
	for key, p := range policies {
		if old, ok := s.Policies[key]; !ok || !proto.Equal(old, p) {
			upserts = append(upserts, &proxypb.QuotaScopePolicy{Key: key.Proto(), Limiter: p})
		}
	}
	for key := range deletes {
		if _, replaced := policies[key]; replaced {
			continue
		}
		if _, exists := s.Policies[key]; exists {
			removed = append(removed, key.Proto())
		}
	}
	defaultsChanged := !maps.EqualFunc(s.Defaults, defaults, func(a, b *proxypb.Limiter) bool { return proto.Equal(a, b) })
	if len(upserts) == 0 && len(removed) == 0 && !defaultsChanged && s.Version.Valid() && s.Version.Epoch == epoch {
		return false
	}
	if s.Policies == nil {
		s.Policies = make(map[Key]*proxypb.Limiter)
	}
	base := s.Version
	target := Version{Epoch: epoch, Seq: base.Seq + 1}
	if base.Epoch != epoch {
		target.Seq = 1
	}
	for _, p := range upserts {
		s.Policies[FromKey(p.Key)] = p.Limiter
	}
	for _, key := range removed {
		delete(s.Policies, FromKey(key))
	}
	s.Version = target
	s.Defaults = defaults
	s.Delta = &proxypb.SetRatesRequest{QuotaProtocol: Protocol, QuotaMode: proxypb.QuotaSyncMode_QUOTA_DELTA, TargetVersion: target.Proto(), BaseVersion: base.Proto(), Upserts: upserts, Deletes: removed}
	if defaultsChanged || base.Epoch != epoch {
		s.Delta = nil
	}
	return true
}
