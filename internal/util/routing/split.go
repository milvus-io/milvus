// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package routing

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SplitTargetCount is the number of shards one split creates.
const SplitTargetCount = 2

// Split is one shard split: the shard that is split, the shards that replace
// it with the residues each of them owns, and the routing modulus afterwards.
type Split struct {
	Source string
	// Targets are the new shards. Their residues are taken against Modulus.
	Targets []Shard
	// Modulus is the routing modulus after the split. It is the modulus before
	// the split, or a multiple of it.
	Modulus uint64
	// ShardBy is the routing key expression after the split. Empty keeps the
	// one the collection has.
	ShardBy string
}

// PlanSplit plans the split of source into two target vchannels.
//
// If source owns two residues or more, the modulus is kept and the residues
// are dealt to the targets alternately, in increasing order. If source owns
// one residue r at modulus M, the modulus doubles: the first target owns r
// and the second owns r+M.
func PlanSplit(current Routing, source string, targets [SplitTargetCount]string) (Split, error) {
	explicit, err := current.Explicit()
	if err != nil {
		return Split{}, err
	}
	idx := slices.IndexFunc(explicit.Shards, func(s Shard) bool { return s.VChannel == source })
	if idx < 0 {
		return Split{}, merr.WrapErrServiceInternalMsg("split source %s is not a shard of the routing", source)
	}
	buckets := slices.Clone(explicit.Shards[idx].Buckets)
	modulus := explicit.Modulus
	if len(buckets) == 1 {
		buckets = expandBuckets(buckets, modulus, modulus*2)
		modulus *= 2
	}
	slices.Sort(buckets)
	split := Split{
		Source:  source,
		Modulus: modulus,
		ShardBy: current.ShardBy,
		Targets: []Shard{{VChannel: targets[0]}, {VChannel: targets[1]}},
	}
	for i, bucket := range buckets {
		target := &split.Targets[i%SplitTargetCount]
		target.Buckets = append(target.Buckets, bucket)
	}
	// Validate the plan through the one implementation that applies it.
	if _, err := current.ApplySplit(split); err != nil {
		return Split{}, err
	}
	return split, nil
}

// ApplySplit returns the routing after a split, and refuses a split that would
// move a key of any shard other than the source.
//
// The targets take the position of the source in the shard order. Every
// refusal is a System error: a split is planned by a coordinator, never typed
// by a user.
func (r Routing) ApplySplit(split Split) (Routing, error) {
	current, err := r.Explicit()
	if err != nil {
		return Routing{}, err
	}
	if split.Modulus == 0 || split.Modulus > MaxModulus || split.Modulus%current.Modulus != 0 {
		return Routing{}, merr.WrapErrServiceInternalMsg(
			"split of %s sets routing modulus %d, which must be a multiple of the current modulus %d and at most %d",
			split.Source, split.Modulus, current.Modulus, MaxModulus)
	}
	if split.ShardBy != "" && current.ShardBy != "" && split.ShardBy != current.ShardBy {
		return Routing{}, merr.WrapErrServiceInternalMsg(
			"split of %s changes the routing key from %s to %s", split.Source, current.ShardBy, split.ShardBy)
	}
	sourceIdx := slices.IndexFunc(current.Shards, func(s Shard) bool { return s.VChannel == split.Source })
	if sourceIdx < 0 {
		return Routing{}, merr.WrapErrServiceInternalMsg("split source %s is not a shard of the routing", split.Source)
	}
	if len(split.Targets) != SplitTargetCount {
		return Routing{}, merr.WrapErrServiceInternalMsg("split of %s names %d targets, a split creates exactly %d",
			split.Source, len(split.Targets), SplitTargetCount)
	}

	// The targets together own exactly the keys of the source.
	owed := make(map[uint64]struct{})
	for _, bucket := range expandBuckets(current.Shards[sourceIdx].Buckets, current.Modulus, split.Modulus) {
		owed[bucket] = struct{}{}
	}
	names := make(map[string]struct{}, len(split.Targets))
	for _, target := range split.Targets {
		if target.VChannel == "" {
			return Routing{}, merr.WrapErrServiceInternalMsg("split of %s names a target without a vchannel", split.Source)
		}
		if _, dup := names[target.VChannel]; dup {
			return Routing{}, merr.WrapErrServiceInternalMsg("split of %s names target %s twice", split.Source, target.VChannel)
		}
		names[target.VChannel] = struct{}{}
		if slices.ContainsFunc(current.Shards, func(s Shard) bool { return s.VChannel == target.VChannel }) {
			return Routing{}, merr.WrapErrServiceInternalMsg("split target %s is already a shard of the routing", target.VChannel)
		}
		if len(target.Buckets) == 0 {
			return Routing{}, merr.WrapErrServiceInternalMsg("split target %s owns no residue", target.VChannel)
		}
		for _, bucket := range target.Buckets {
			if _, ok := owed[bucket]; !ok {
				return Routing{}, merr.WrapErrServiceInternalMsg(
					"split target %s claims residue %d (mod %d), which the source %s does not own or another target already took",
					target.VChannel, bucket, split.Modulus, split.Source)
			}
			delete(owed, bucket)
		}
	}
	if len(owed) > 0 {
		return Routing{}, merr.WrapErrServiceInternalMsg("split of %s leaves %d of its residues (mod %d) without an owner",
			split.Source, len(owed), split.Modulus)
	}

	next := Routing{Modulus: split.Modulus, ShardBy: current.ShardBy, Shards: make([]Shard, 0, len(current.Shards)+1)}
	if next.ShardBy == "" {
		next.ShardBy = split.ShardBy
	}
	for i, shard := range current.Shards {
		if i == sourceIdx {
			for _, target := range split.Targets {
				buckets := slices.Clone(target.Buckets)
				slices.Sort(buckets)
				next.Shards = append(next.Shards, Shard{VChannel: target.VChannel, Buckets: buckets})
			}
			continue
		}
		next.Shards = append(next.Shards, Shard{
			VChannel: shard.VChannel,
			Buckets:  expandBuckets(shard.Buckets, current.Modulus, split.Modulus),
		})
	}
	if _, err := next.Table(); err != nil {
		return Routing{}, err
	}
	return next, nil
}

// CheckSplitPostImage verifies that next is the routing current becomes when
// source is split into targets, and nothing else: no key of another shard
// moves. The issuer runs it on the routing it puts into the SplitShard
// message, and the consumer runs it again before it applies that routing.
func CheckSplitPostImage(current Routing, next Routing, source string, targets []string) error {
	split := Split{Source: source, Modulus: next.Modulus, ShardBy: next.ShardBy}
	for _, target := range targets {
		idx := slices.IndexFunc(next.Shards, func(s Shard) bool { return s.VChannel == target })
		if idx < 0 {
			return merr.WrapErrServiceInternalMsg("split target %s is not a shard of the routing after the split", target)
		}
		split.Targets = append(split.Targets, next.Shards[idx])
	}
	expected, err := current.ApplySplit(split)
	if err != nil {
		return err
	}
	if !expected.Equal(next) {
		return merr.WrapErrServiceInternalMsg(
			"the routing after the split of %s differs from the current routing by more than the split", source)
	}
	return nil
}

// Equal reports whether two routings place every key on the same shard and
// list the shards in the same order. The order of the residues of a shard
// does not matter.
func (r Routing) Equal(other Routing) bool {
	if r.Modulus != other.Modulus || r.ShardBy != other.ShardBy || len(r.Shards) != len(other.Shards) {
		return false
	}
	for i := range r.Shards {
		if r.Shards[i].VChannel != other.Shards[i].VChannel {
			return false
		}
		a, b := slices.Clone(r.Shards[i].Buckets), slices.Clone(other.Shards[i].Buckets)
		slices.Sort(a)
		slices.Sort(b)
		if !slices.Equal(a, b) {
			return false
		}
	}
	return true
}

// expandBuckets expresses residues taken against modulus from at modulus to,
// which is a multiple of from. Residue r becomes r, r+from, r+2*from, ...
// The result is sorted.
func expandBuckets(buckets []uint64, from uint64, to uint64) []uint64 {
	expanded := make([]uint64, 0, len(buckets)*int(to/from))
	for _, bucket := range buckets {
		for residue := bucket; residue < to; residue += from {
			expanded = append(expanded, residue)
		}
	}
	slices.Sort(expanded)
	return expanded
}

// CheckSplitChannels verifies the vchannels a split names against the
// collection.
//
//   - shards are the vchannels of the collection that accept DML.
//   - splitSources are the vchannels of the collection that were split before.
//     Such a vchannel may still exist on its pchannel: whether a cluster has
//     retired it is not known to the collection metadata.
//
// A target must be a new name, and it must be placed on a pchannel that holds
// no vchannel of the collection. A StreamingNode keeps one registration per
// collection on one pchannel, so a second vchannel of the collection on the
// same pchannel would collide with the first.
func CheckSplitChannels(shards []string, splitSources []string, source string, targets []string) error {
	if funcutil.IsControlChannel(source) || !slices.Contains(shards, source) {
		return merr.WrapErrServiceInternalMsg("split source %s is not a shard of the collection that accepts DML", source)
	}
	if len(targets) != SplitTargetCount {
		return merr.WrapErrServiceInternalMsg("split of %s names %d targets, a split creates exactly %d",
			source, len(targets), SplitTargetCount)
	}
	occupied := make(map[string]string, len(shards)+len(splitSources))
	for _, vchannel := range shards {
		occupied[funcutil.ToPhysicalChannel(vchannel)] = vchannel
	}
	for _, vchannel := range splitSources {
		occupied[funcutil.ToPhysicalChannel(vchannel)] = vchannel
	}
	for _, target := range targets {
		if target == "" || funcutil.IsControlChannel(target) {
			return merr.WrapErrServiceInternalMsg("split of %s names an invalid target vchannel %q", source, target)
		}
		if slices.Contains(shards, target) || slices.Contains(splitSources, target) {
			return merr.WrapErrServiceInternalMsg("split target %s is already a vchannel of the collection", target)
		}
		pchannel := funcutil.ToPhysicalChannel(target)
		if holder, ok := occupied[pchannel]; ok {
			return merr.WrapErrServiceInternalMsg(
				"split target %s is placed on pchannel %s, which already holds vchannel %s of the collection",
				target, pchannel, holder)
		}
		occupied[pchannel] = target
	}
	return nil
}

// CheckSplitAdmission refuses to split a collection whose rows are not placed
// by the hash of the primary key.
//
// With namespace sharding enabled a row is placed by its namespace. Splitting
// such a collection is a separate design. A malformed property is refused as
// well, as a System error: the property is collection metadata here, not a
// user request.
func CheckSplitAdmission(properties []*commonpb.KeyValuePair) error {
	enabled, err := common.IsNamespaceShardingEnabled(properties...)
	if err != nil {
		// A deliberate change of class. The parser serves user requests and
		// returns an Input error; here the value was read from metadata.
		return merr.WrapErrServiceInternalErr(err, "cannot decide how the collection places rows")
	}
	if enabled {
		return merr.WrapErrServiceInternalMsg(
			"the collection places rows by namespace (%s=true), a shard split supports only placement by primary key",
			common.NamespaceShardingEnabledKey)
	}
	return nil
}
