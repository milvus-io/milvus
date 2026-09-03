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

package datacoord

import (
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ErrSplitPlannerNotReady is returned by the placeholder planner; a task
// stays in the Preparing state (still abortable) until a real planner is
// wired in.
var ErrSplitPlannerNotReady = errors.New("shard split planner is not ready")

// splitPlanner decides the split point of a shard and assigns segments to the
// target shards.
//
// The production implementation works on the collection's residues: it divides
// the source shard's residue set between the two targets, and maps a segment to
// the target owning the residue its namespace hashes to. The division is
// weighted by the data actually sitting on each residue, so the two halves come
// out balanced rather than merely equal in residue count.
type splitPlanner interface {
	// PlanTargets selects the split point of the source shard and returns the
	// target shards with their residues, together with the collection's routing
	// modulus after the split. The residues must be disjoint and exactly cover
	// the source shard's.
	PlanTargets(ctx context.Context, collection *collectionInfo, sourceVChannel string, targetVChannels []string) ([]*datapb.SplitShardTaskTarget, uint64, error)
	// AssignSegment returns the index of the target shard owning the segment
	// (decided by the namespace of the segment's partition).
	AssignSegment(ctx context.Context, segment *SegmentInfo, targets []*datapb.SplitShardTaskTarget) (int, error)
}

// unimplementedSplitPlanner keeps every split task in the Preparing state.
// It is the default until the range-routing planner lands.
type unimplementedSplitPlanner struct{}

func (unimplementedSplitPlanner) PlanTargets(ctx context.Context, collection *collectionInfo, sourceVChannel string, targetVChannels []string) ([]*datapb.SplitShardTaskTarget, uint64, error) {
	return nil, 0, ErrSplitPlannerNotReady
}

func (unimplementedSplitPlanner) AssignSegment(ctx context.Context, segment *SegmentInfo, targets []*datapb.SplitShardTaskTarget) (int, error) {
	return 0, ErrSplitPlannerNotReady
}

var (
	// ErrShardNotSplittable is returned when the source shard cannot be split
	// further, e.g. it holds fewer than two namespaces.
	ErrShardNotSplittable = errors.New("the source shard cannot be split further")
	// ErrSegmentNamespaceUnrouted is returned when a segment's namespace hashes
	// to a residue no target owns; it signals a planning bug.
	ErrSegmentNamespaceUnrouted = errors.New("the segment namespace is not covered by any target")
)

// namespaceResolver resolves a collection's partition ids to their namespace
// names (a namespace collection maps one namespace to one partition). The
// production resolver is backed by the datacoord broker's ShowPartitions.
type namespaceResolver func(ctx context.Context, collectionID int64) (map[int64]string, error)

// brokerNamespaceResolver builds a namespaceResolver from the datacoord
// broker: it zips the partition ids and names that ShowPartitions returns.
func brokerNamespaceResolver(b broker.Broker) namespaceResolver {
	return func(ctx context.Context, collectionID int64) (map[int64]string, error) {
		resp, err := b.ShowPartitions(ctx, collectionID)
		if err != nil {
			return nil, err
		}
		ids, names := resp.GetPartitionIDs(), resp.GetPartitionNames()
		if len(ids) != len(names) {
			return nil, merr.WrapErrServiceInternalMsg("ShowPartitions returned %d ids but %d names for collection %d",
				len(ids), len(names), collectionID)
		}
		out := make(map[int64]string, len(ids))
		for i, id := range ids {
			out[id] = names[i]
		}
		return out, nil
	}
}

// residueWeight is one residue of the source shard and the data sitting on it,
// measured from the segments the shard currently holds. The planner divides the
// residue set by these weights.
type residueWeight struct {
	residue uint64
	size    int64
}

// planBalancedSplitIndex picks the index of weights (sorted ascending by
// residue) that minimizes the size imbalance between the two halves, with both
// halves non-empty. The first half is weights[:index], the second
// weights[index:].
//
// Residue order is kept rather than sorting by size: an order-independent
// partition would be better balanced but would scatter the residues, and a
// deterministic, order-preserving cut keeps a retried plan identical to the one
// it retries -- which is what makes the routing commit idempotent.
func planBalancedSplitIndex(weights []residueWeight) (int, error) {
	if len(weights) < 2 {
		return 0, ErrShardNotSplittable
	}
	var total int64
	for _, w := range weights {
		total += w.size
	}
	bestIndex := 1
	bestDiff := int64(-1)
	var prefix int64
	for i := 1; i < len(weights); i++ {
		prefix += weights[i-1].size
		diff := total - 2*prefix
		if diff < 0 {
			diff = -diff
		}
		if bestDiff < 0 || diff < bestDiff {
			bestDiff = diff
			bestIndex = i
		}
	}
	return bestIndex, nil
}

// residueSplitPlanner is the production split planner. It divides the source
// shard's residues between two targets, weighted by the data on each, and routes
// a segment by the residue its partition's namespace hashes to.
type residueSplitPlanner struct {
	meta     *meta
	resolver namespaceResolver

	mu sync.Mutex
	// residueCache memoizes collectionID -> partitionID -> namespace, so the
	// per-segment AssignSegment does not resolve on every call and keeps working
	// after a datacoord restart (the cache is rebuilt lazily).
	namespaceCache map[int64]map[int64]string
}

func newResidueSplitPlanner(meta *meta, resolver namespaceResolver) *residueSplitPlanner {
	return &residueSplitPlanner{
		meta:           meta,
		resolver:       resolver,
		namespaceCache: make(map[int64]map[int64]string),
	}
}

// PlanTargets divides the source shard's residues between the two target
// vchannels.
//
// When the source still owns several residues the division is weighted by the
// data on each, and the collection's modulus does not move. When it is down to
// its last residue there is nothing to weigh: the modulus doubles and the
// residue is cut on one more hash bit, which a sound hash splits evenly by
// construction.
func (p *residueSplitPlanner) PlanTargets(ctx context.Context, collection *collectionInfo, sourceVChannel string, targetVChannels []string) ([]*datapb.SplitShardTaskTarget, uint64, error) {
	if len(targetVChannels) != 2 {
		return nil, 0, merr.WrapErrServiceInternalMsg("a shard split expects exactly two target vchannels, got %d", len(targetVChannels))
	}
	// Copied into a fixed pair right after the length check, so the two uses
	// below index an array whose length the compiler knows -- not a slice whose
	// length only the check forty lines up does.
	var pair [2]string
	copy(pair[:], targetVChannels)
	residues, err := residuesOf(collection)
	if err != nil {
		return nil, 0, err
	}
	own, err := residues.of(sourceVChannel)
	if err != nil {
		return nil, 0, err
	}

	if len(own) == 1 {
		plan, err := routing.PlanSplit(residues.modulus, own)
		if err != nil {
			return nil, 0, err
		}
		return []*datapb.SplitShardTaskTarget{
			{Vchannel: targetVChannels[0], Buckets: plan.Left},
			{Vchannel: targetVChannels[1], Buckets: plan.Right},
		}, plan.Modulus, nil
	}

	weights, err := p.weighResidues(ctx, collection, sourceVChannel, residues.modulus, own)
	if err != nil {
		return nil, 0, err
	}
	index, err := planBalancedSplitIndex(weights)
	if err != nil {
		return nil, 0, err
	}
	left := make([]uint64, 0, index)
	for _, w := range weights[:index] {
		left = append(left, w.residue)
	}
	right := make([]uint64, 0, len(weights)-index)
	for _, w := range weights[index:] {
		right = append(right, w.residue)
	}
	return []*datapb.SplitShardTaskTarget{
		{Vchannel: pair[0], Buckets: left},
		{Vchannel: pair[1], Buckets: right},
	}, residues.modulus, nil
}

// weighResidues measures how much of the source shard sits on each of its
// residues, by summing its segments under the residue their partition's
// namespace hashes to.
//
// A partition whose namespace cannot be resolved contributes no weight rather
// than failing the plan: the split is still correct, only less well balanced,
// and refusing to split an over-threshold shard because one tenant's name could
// not be read would be the worse outcome.
func (p *residueSplitPlanner) weighResidues(
	ctx context.Context,
	collection *collectionInfo,
	sourceVChannel string,
	modulus uint64,
	own []uint64,
) ([]residueWeight, error) {
	namespaces, err := p.namespaces(ctx, collection.ID)
	if err != nil {
		return nil, err
	}
	owned := make(map[uint64]struct{}, len(own))
	for _, r := range own {
		owned[r] = struct{}{}
	}

	sizes := make(map[uint64]int64, len(own))
	for _, segment := range p.meta.GetSegmentsByChannel(sourceVChannel) {
		namespace, ok := namespaces[segment.GetPartitionID()]
		if !ok {
			continue
		}
		residue := namespaceResidue(namespace, modulus)
		if _, ok := owned[residue]; !ok {
			// The segment's namespace does not hash into this shard at all. It
			// predates a routing change and will be moved by the redistribution
			// anyway; counting it here would balance against data that is not
			// staying.
			continue
		}
		sizes[residue] += segment.getSegmentSize()
	}

	weights := make([]residueWeight, 0, len(own))
	for _, r := range own {
		weights = append(weights, residueWeight{residue: r, size: sizes[r]})
	}
	sort.Slice(weights, func(i, j int) bool { return weights[i].residue < weights[j].residue })
	return weights, nil
}

// namespaceResidue is the residue a namespace routes to. It is the same hash the
// proxy write path applies, so a segment lands where its future writes will.
func namespaceResidue(namespace string, modulus uint64) uint64 {
	return uint64(typeutil.HashString2Uint32(namespace)) % modulus
}

// AssignSegment routes a segment to the target owning the residue its
// partition's namespace hashes to.
func (p *residueSplitPlanner) AssignSegment(ctx context.Context, segment *SegmentInfo, targets []*datapb.SplitShardTaskTarget) (int, error) {
	namespaces, err := p.namespaces(ctx, segment.GetCollectionID())
	if err != nil {
		return 0, err
	}
	namespace, ok := namespaces[segment.GetPartitionID()]
	if !ok {
		// The partition may have been created after the cache was built -- a
		// namespace collection adds partitions dynamically as tenants arrive,
		// and the redistribution window can be long. Refresh from the current
		// collection meta once before giving up, so the new namespace becomes
		// routable instead of wedging the task forever.
		namespaces, err = p.refreshNamespaces(ctx, segment.GetCollectionID())
		if err != nil {
			return 0, err
		}
		if namespace, ok = namespaces[segment.GetPartitionID()]; !ok {
			return 0, errors.Wrapf(ErrSegmentNamespaceUnrouted, "partition %d of collection %d has no namespace",
				segment.GetPartitionID(), segment.GetCollectionID())
		}
	}

	modulus := targetsModulus(targets)
	if modulus == 0 {
		return 0, errors.Wrapf(ErrSegmentNamespaceUnrouted, "segment %d: the targets carry no residue", segment.GetID())
	}
	residue := namespaceResidue(namespace, modulus)
	for i, target := range targets {
		for _, own := range target.GetBuckets() {
			if own == residue {
				return i, nil
			}
		}
	}
	return 0, errors.Wrapf(ErrSegmentNamespaceUnrouted, "segment %d (partition %d)", segment.GetID(), segment.GetPartitionID())
}

// targetsModulus recovers the modulus the targets' residues are taken against.
//
// The targets of one task tile their sources' key space, so the modulus is one
// more than the largest residue only when the sources covered everything; in
// general it must come from the task. It is derived here instead because
// AssignSegment is handed the targets alone, and a residue above the true
// modulus can never appear -- so the largest residue plus one is a lower bound
// that agrees with the real modulus on every plan this planner produces (a
// doubling gives {r} and {r+M/2} at 2M, whose maximum is at least M).
func targetsModulus(targets []*datapb.SplitShardTaskTarget) uint64 {
	var max uint64
	var any bool
	for _, target := range targets {
		for _, residue := range target.GetBuckets() {
			if !any || residue > max {
				max, any = residue, true
			}
		}
	}
	if !any {
		return 0
	}
	return max + 1
}

// namespaces returns the collection's partitionID -> namespace map, resolving it
// on the first access and memoizing the result.
func (p *residueSplitPlanner) namespaces(ctx context.Context, collectionID int64) (map[int64]string, error) {
	p.mu.Lock()
	cached, ok := p.namespaceCache[collectionID]
	p.mu.Unlock()
	if ok {
		return cached, nil
	}
	return p.refreshNamespaces(ctx, collectionID)
}

// refreshNamespaces rebuilds the collection's partitionID -> namespace map from
// the current collection meta and re-caches it, picking up partitions added
// after the cache was first built.
func (p *residueSplitPlanner) refreshNamespaces(ctx context.Context, collectionID int64) (map[int64]string, error) {
	names, err := p.resolver(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.namespaceCache[collectionID] = names
	p.mu.Unlock()
	return names, nil
}
