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

package dataview

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// DeclareShardsDataViewEvent adds empty shards to the DataView of a collection
// that already has one. A shard split declares its two target shards with it.
type DeclareShardsDataViewEvent struct {
	CollectionID int64
	VChannels    []string
	// TransformStarts is the Transform frontier each new shard starts with: W
	// and A of the shard are both set to it, as OnCreateCollection does with
	// the creation time tick. Every declared vchannel needs a non-zero value.
	TransformStarts map[string]uint64
}

// DeclareShards adds the shards of the event that the latest DataView of the
// collection does not have, in one new DataVersion. The shards the DataView
// already has, and every segment, are left as they are.
//
// It is idempotent: a shard that exists is not changed, whatever frontier the
// event carries for it, and an event that adds nothing writes nothing. A
// collection whose DataViews were dropped is a no-op, like every late event. A
// collection without any DataView is refused: only OnCreateCollection and the
// recovery bootstrap install the first shard skeleton.
func (m *dataViewManager) DeclareShards(ctx context.Context, event DeclareShardsDataViewEvent) (*viewpb.DataVersion, error) {
	for _, vchannel := range event.VChannels {
		if vchannel == "" || event.TransformStarts[vchannel] == 0 {
			return nil, merr.WrapErrServiceInternalMsg(
				"shard %q of collection %d is declared without a Transform frontier", vchannel, event.CollectionID)
		}
	}
	state, unlock := m.lockStateForMutation(event.CollectionID)
	if state == nil {
		return nil, nil
	}
	defer unlock()

	base := latestView(state)
	if base == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d DataView is not initialized", event.CollectionID)
	}
	next := canonicalDataViewClone(base)
	known := make(map[string]struct{}, len(next.GetShards()))
	for _, shard := range next.GetShards() {
		known[shard.GetVchannel()] = struct{}{}
	}
	added := false
	for _, vchannel := range event.VChannels {
		if _, ok := known[vchannel]; ok {
			continue
		}
		known[vchannel] = struct{}{}
		start := event.TransformStarts[vchannel]
		next.Shards = append(next.Shards, &viewpb.DataViewOfShard{
			Vchannel:                             vchannel,
			GrowingTransformStartAfterTimetick:   start,
			CompactedTransformStartAfterTimetick: start,
		})
		added = true
	}
	if !added {
		return dataVersionFromView(base), nil
	}
	canonicalizeDataView(next)
	next.DataVersion = nextDataVersion(base, dataViewAdvanceCompact)

	// The segments are those of the base version, so are their statistics.
	// Cost normalization mutates its input: never share the map of an entry.
	stats := make(map[int64]SegmentStats, len(state.latest.stats))
	for id, value := range state.latest.stats {
		stats[id] = value
	}
	if err := m.persistLockedWithStats(ctx, state, next, stats); err != nil {
		return nil, err
	}
	state.changed.NotifyAll()
	return dataVersionFromView(next), nil
}
