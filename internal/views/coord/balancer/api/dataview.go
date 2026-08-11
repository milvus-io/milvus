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

package api

import (
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// DataViewSnapshot is the immutable DataView Manager output consumed by
// SnapshotBuilder and BalancePolicy.
type DataViewSnapshot struct {
	version           uint64
	collectionByID    map[int64]*viewpb.DataViewOfCollection
	shardsByCollVCh   map[int64]map[string]*viewpb.DataViewOfShard
	dataVersionByColl map[int64]qviews.DataVersion
	segments          SegmentSnapshot
}

func NewDataViewSnapshot(
	version uint64,
	collections []*viewpb.DataViewOfCollection,
	segments SegmentSnapshot,
) *DataViewSnapshot {
	if segments == nil {
		segments = emptySegmentSnapshot{}
	}
	snapshot := &DataViewSnapshot{
		version:           version,
		collectionByID:    make(map[int64]*viewpb.DataViewOfCollection, len(collections)),
		shardsByCollVCh:   make(map[int64]map[string]*viewpb.DataViewOfShard, len(collections)),
		dataVersionByColl: make(map[int64]qviews.DataVersion, len(collections)),
		segments:          segments,
	}
	for _, coll := range collections {
		if coll == nil {
			continue
		}
		collectionID := coll.GetCollectionId()
		snapshot.collectionByID[collectionID] = coll
		snapshot.dataVersionByColl[collectionID] = qviews.FromProtoDataVersion(coll.GetDataVersion())
		shards := make(map[string]*viewpb.DataViewOfShard, len(coll.GetShards()))
		for _, shard := range coll.GetShards() {
			if shard != nil {
				shards[shard.GetVchannel()] = shard
			}
		}
		snapshot.shardsByCollVCh[collectionID] = shards
	}
	return snapshot
}

func (s *DataViewSnapshot) Version() uint64 {
	if s == nil {
		return 0
	}
	return s.version
}

func (s *DataViewSnapshot) DataVersion(collectionID int64) (qviews.DataVersion, bool) {
	if s == nil {
		return qviews.DataVersion{}, false
	}
	version, ok := s.dataVersionByColl[collectionID]
	return version, ok
}

func (s *DataViewSnapshot) ShardView(collectionID int64, vchannel string) (*viewpb.DataViewOfShard, bool) {
	if s == nil {
		return nil, false
	}
	shards := s.shardsByCollVCh[collectionID]
	if shards == nil {
		return nil, false
	}
	shard, ok := shards[vchannel]
	return shard, ok
}

func (s *DataViewSnapshot) RangeShards(collectionID int64, fn func(*viewpb.DataViewOfShard) bool) {
	if s == nil {
		return
	}
	coll := s.collectionByID[collectionID]
	if coll == nil {
		return
	}
	for _, shard := range coll.GetShards() {
		if !fn(shard) {
			return
		}
	}
}

func (s *DataViewSnapshot) SegmentInfo(segmentID int64) (*SegmentInfo, bool) {
	if s == nil || s.segments == nil {
		return nil, false
	}
	return s.segments.Get(segmentID)
}

// SegmentSnapshot is an immutable segment metadata lookup owned by the
// DataViewProvider.
type SegmentSnapshot interface {
	Get(segmentID int64) (*SegmentInfo, bool)
}

type emptySegmentSnapshot struct{}

func (emptySegmentSnapshot) Get(int64) (*SegmentInfo, bool) {
	return nil, false
}

// SegmentInfo carries the minimum per-segment metadata the Balancer needs.
type SegmentInfo struct {
	SegmentID   int64
	PartitionID int64
	// MemSize is retained in the snapshot for compatibility and diagnostics.
	// The row-count balance policy does not consume it.
	// MemSize is the estimated in-memory footprint in bytes once this segment
	// is loaded onto a QueryNode.
	MemSize int64
	// RowNum is the segment row count and the sole balance load metric.
	RowNum int64
}
