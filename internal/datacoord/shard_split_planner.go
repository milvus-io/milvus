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
	"bytes"
	"context"
	"sort"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// ErrSplitPlannerNotReady is returned by the placeholder planner; a task
// stays in the Preparing state (still abortable) until a real planner is
// wired in.
var ErrSplitPlannerNotReady = errors.New("shard split planner is not ready")

// splitPlanner decides the split point of a shard and assigns segments to
// the target shards. The production implementation works on the range
// routing key space (big_endian(hash(namespace)) || namespace): it picks a
// namespace boundary balancing the two halves as the split point, and maps
// a segment to the target whose key range contains its namespace.
type splitPlanner interface {
	// PlanTargets selects the split point of the source shard and returns
	// the target shards with their routing key ranges. The ranges must be
	// disjoint and exactly cover the source shard's range.
	PlanTargets(ctx context.Context, collection *collectionInfo, sourceVChannel string, targetVChannels []string) ([]*datapb.SplitShardTaskTarget, error)
	// AssignSegment returns the index of the target shard owning the
	// segment (decided by the namespace of the segment's partition).
	AssignSegment(ctx context.Context, segment *SegmentInfo, targets []*datapb.SplitShardTaskTarget) (int, error)
}

// unimplementedSplitPlanner keeps every split task in the Preparing state.
// It is the default until the range-routing planner lands.
type unimplementedSplitPlanner struct{}

func (unimplementedSplitPlanner) PlanTargets(ctx context.Context, collection *collectionInfo, sourceVChannel string, targetVChannels []string) ([]*datapb.SplitShardTaskTarget, error) {
	return nil, ErrSplitPlannerNotReady
}

func (unimplementedSplitPlanner) AssignSegment(ctx context.Context, segment *SegmentInfo, targets []*datapb.SplitShardTaskTarget) (int, error) {
	return 0, ErrSplitPlannerNotReady
}

var (
	// ErrShardNotSplittable is returned when the source shard cannot be split
	// further, e.g. it holds fewer than two namespaces.
	ErrShardNotSplittable = errors.New("the source shard cannot be split further")
	// ErrSegmentNamespaceUnrouted is returned when a segment's namespace key
	// falls outside every target range; it signals a planning/encoding bug.
	ErrSegmentNamespaceUnrouted = errors.New("the segment namespace key is not covered by any target range")
)

// routingKeyEncoder encodes a namespace into its byte-comparable routing key
// (the design's big_endian(hash(namespace)) || namespace). It is the single
// seam the split planner depends on: the concrete range-routing encoder lives
// in the shared routing package so the proxy, the streamingnode and this
// planner all agree on the encoding. The planner is wired with that encoder
// once the routing package lands; until then the planner is exercised in unit
// tests with an order-preserving stand-in.
type routingKeyEncoder interface {
	EncodeNamespace(namespace string) []byte
}

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
			return nil, errors.Errorf("ShowPartitions returned %d ids but %d names for collection %d",
				len(ids), len(names), collectionID)
		}
		out := make(map[int64]string, len(ids))
		for i, id := range ids {
			out[id] = names[i]
		}
		return out, nil
	}
}

// routingKeyRange is a half-open byte range [lower, upper). A nil bound is
// unbounded, so the full key space is routingKeyRange{nil, nil}.
type routingKeyRange struct {
	lower []byte
	upper []byte
}

// contains reports whether key falls in [lower, upper).
func (r routingKeyRange) contains(key []byte) bool {
	if r.lower != nil && bytes.Compare(key, r.lower) < 0 {
		return false
	}
	if r.upper != nil && bytes.Compare(key, r.upper) >= 0 {
		return false
	}
	return true
}

// namespaceWeight is one namespace's routing key and its size on the source
// shard; the planner balances the two halves by these sizes.
type namespaceWeight struct {
	partitionID int64
	key         []byte
	size        int64
}

// planBalancedSplitIndex picks the split index of weights (sorted ascending by
// key) that minimizes the size imbalance between the two halves, with both
// halves non-empty. weights[index].key becomes the split key: the first half
// is [.., splitKey), the second half is [splitKey, ..). It needs at least two
// namespaces.
func planBalancedSplitIndex(weights []namespaceWeight) (int, error) {
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

// rangeSplitPlanner is the production split planner over the range routing key
// space. It balances the namespaces of the source shard into two target
// shards by size and routes a segment by its partition's namespace key.
type rangeSplitPlanner struct {
	meta     *meta
	encoder  routingKeyEncoder
	resolver namespaceResolver

	mu sync.Mutex
	// keyCache memoizes collectionID -> partitionID -> routing key, so the
	// per-segment AssignSegment does not resolve and encode on every call and
	// keeps working after a datacoord restart (the cache is rebuilt lazily).
	keyCache map[int64]map[int64][]byte
}

func newRangeSplitPlanner(meta *meta, encoder routingKeyEncoder, resolver namespaceResolver) *rangeSplitPlanner {
	return &rangeSplitPlanner{
		meta:     meta,
		encoder:  encoder,
		resolver: resolver,
		keyCache: make(map[int64]map[int64][]byte),
	}
}

// PlanTargets balances the namespaces of the source shard into the two target
// vchannels and returns their disjoint, gap-free key ranges.
func (p *rangeSplitPlanner) PlanTargets(ctx context.Context, collection *collectionInfo, sourceVChannel string, targetVChannels []string) ([]*datapb.SplitShardTaskTarget, error) {
	if len(targetVChannels) != 2 {
		return nil, errors.Errorf("range split expects exactly two target vchannels, got %d", len(targetVChannels))
	}
	keys, err := p.partitionKeys(ctx, collection.ID)
	if err != nil {
		return nil, err
	}

	sizes := make(map[int64]int64)
	for _, segment := range p.meta.GetSegmentsByChannel(sourceVChannel) {
		sizes[segment.GetPartitionID()] += segment.getSegmentSize()
	}

	weights := make([]namespaceWeight, 0, len(keys))
	for partitionID, key := range keys {
		weights = append(weights, namespaceWeight{
			partitionID: partitionID,
			key:         key,
			size:        sizes[partitionID],
		})
	}
	sort.Slice(weights, func(i, j int) bool {
		return bytes.Compare(weights[i].key, weights[j].key) < 0
	})

	index, err := planBalancedSplitIndex(weights)
	if err != nil {
		return nil, err
	}
	splitKey := weights[index].key

	// The source shard's current key range comes from its authoritative shard
	// meta (read from DescribeCollection). The two targets split that range at
	// splitKey, so they exactly cover what the source owned — correct whether
	// the source is the whole space (a first split) or a sub-range (a later
	// split of a multi-shard collection).
	source := routingKeyRange{lower: nil, upper: nil}
	if info, ok := collection.ShardInfos[sourceVChannel]; ok {
		// the source being split is a single contiguous range (a fresh collection's
		// whole space, or a sub-range from an earlier split).
		if ranges := info.GetRangeRouting().GetRanges(); len(ranges) > 0 {
			source.lower = ranges[0].GetLower()
			source.upper = ranges[0].GetUpper()
		}
	}
	return []*datapb.SplitShardTaskTarget{
		{Vchannel: targetVChannels[0], RoutingKeyLower: source.lower, RoutingKeyUpper: splitKey},
		{Vchannel: targetVChannels[1], RoutingKeyLower: splitKey, RoutingKeyUpper: source.upper},
	}, nil
}

// AssignSegment routes a segment to the target whose key range contains the
// segment's partition namespace key.
func (p *rangeSplitPlanner) AssignSegment(ctx context.Context, segment *SegmentInfo, targets []*datapb.SplitShardTaskTarget) (int, error) {
	keys, err := p.partitionKeys(ctx, segment.GetCollectionID())
	if err != nil {
		return 0, err
	}
	key, ok := keys[segment.GetPartitionID()]
	if !ok {
		return 0, errors.Wrapf(ErrSegmentNamespaceUnrouted, "partition %d of collection %d has no namespace key",
			segment.GetPartitionID(), segment.GetCollectionID())
	}
	for i, target := range targets {
		r := routingKeyRange{lower: target.GetRoutingKeyLower(), upper: target.GetRoutingKeyUpper()}
		if r.contains(key) {
			return i, nil
		}
	}
	return 0, errors.Wrapf(ErrSegmentNamespaceUnrouted, "segment %d (partition %d)", segment.GetID(), segment.GetPartitionID())
}

// partitionKeys returns the collection's partitionID -> routing key map,
// resolving and encoding it on the first access and memoizing the result.
func (p *rangeSplitPlanner) partitionKeys(ctx context.Context, collectionID int64) (map[int64][]byte, error) {
	p.mu.Lock()
	if cached, ok := p.keyCache[collectionID]; ok {
		p.mu.Unlock()
		return cached, nil
	}
	p.mu.Unlock()

	names, err := p.resolver(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	keys := make(map[int64][]byte, len(names))
	for partitionID, name := range names {
		keys[partitionID] = p.encoder.EncodeNamespace(name)
	}

	p.mu.Lock()
	p.keyCache[collectionID] = keys
	p.mu.Unlock()
	return keys, nil
}
