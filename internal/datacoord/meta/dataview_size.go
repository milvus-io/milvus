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

package meta

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// DataViewSizeBytes measures sealed data from the candidate's exact membership
// across all shards. SegmentMeta also holds L0 and unflushed data outside that
// membership. The caller holds the DataView collection lock, before segment
// writer locks, so finished field indexes can be read safely.
func (m *Meta) DataViewSizeBytes(ctx context.Context, view *viewpb.DataViewOfCollection, omitData []int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	collectionID := view.GetCollectionId()
	collection, err := m.GetCollectionOrNotFound(collectionID)
	if err != nil {
		return 0, err
	}
	members := typeutil.NewUniqueSet()
	for _, shard := range view.GetShards() {
		for _, partition := range shard.GetPartitions() {
			members.Insert(partition.GetSegmentIds()...)
		}
	}

	partitions := typeutil.NewSet(collection.Partitions...)
	omit := typeutil.NewSet(omitData...)
	var size int64
	indexed := make([]int64, 0, members.Len())
	// One collection snapshot supplies both the sealed members and the auxiliary logs.
	for id, info := range m.MetaStore.GetSegments(collectionID) {
		seg := SegmentInfo{SegmentInfo: info}
		if members.Contain(id) {
			// A selected parent can already be Dropped and a selected sort
			// input can be invisible. Membership decides sealed visibility.
			members.Remove(id)
		} else if omit.Contain(id) || !seg.isDataViewAuxiliary(partitions) {
			continue
		}
		indexed = append(indexed, id)
		if !omit.Contain(id) {
			size += seg.dataViewDataBytes()
		}
	}
	if members.Len() > 0 {
		return 0, merr.WrapErrServiceNotReadyMsg("collection %d DataView members missing from SegmentMeta: %v", collectionID, members.Collect())
	}
	if len(indexed) == 0 {
		return size, nil
	}
	if m.IndexCatalog == nil {
		return 0, merr.WrapErrServiceNotReadyMsg("field index metadata unavailable for collection %d", collectionID)
	}
	return size + m.IndexCatalog.FinishedIndexBytes(collectionID, indexed), nil
}

func (s *SegmentInfo) isDataViewAuxiliary(partitions typeutil.UniqueSet) bool {
	if !IsSegmentHealthy(s) || s.GetIsImporting() || s.GetIsInvisible() || s.GetIsFake() ||
		(s.GetPartitionID() != common.AllPartitionsID && !partitions.Contain(s.GetPartitionID())) {
		return false
	}
	if s.GetLevel() == datapb.SegmentLevel_L0 {
		return true
	}
	switch s.GetState() {
	case commonpb.SegmentState_Growing, commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing:
		return len(s.GetCompactionFrom()) == 0
	default:
		return false
	}
}

func (s *SegmentInfo) dataViewDataBytes() int64 {
	size := s.GetSegmentSize()
	for _, text := range s.GetTextStatsLogs() {
		size += text.GetLogSize()
	}
	for _, jsonKeys := range s.GetJsonKeyStats() {
		size += jsonKeys.GetLogSize()
	}
	return size
}
