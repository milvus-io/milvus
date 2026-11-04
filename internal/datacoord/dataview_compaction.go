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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// loadableProjection resolves the recovery frontier before publishing it. The
// previous immutable snapshot is borrowed under the DataView collection lock;
// it establishes admission, so later index DDL cannot roll a published segment
// back to its ancestors. Neither this function nor its predicates reenter the
// DataView manager. SegmentMeta is snapshotted under segMu before index reads.
func (m *meta) loadableProjection(ctx context.Context, collectionID int64, previous *viewpb.DataViewOfCollection) ([]dataview.LoadableSegment, error) {
	admitted := typeutil.NewUniqueSet()
	for _, shard := range previous.GetShards() {
		for _, partition := range shard.GetPartitions() {
			admitted.Insert(partition.GetSegmentIds()...)
		}
	}
	m.segMu.RLock()
	segments := m.segments.GetSegmentsBySelector(WithCollection(collectionID))
	valid := make(map[int64]*SegmentInfo, len(segments))
	flushed, dropped := typeutil.NewUniqueSet(), typeutil.NewUniqueSet()
	for _, segment := range segments {
		if _, staged := m.stagedSegmentToGroup[segment.GetID()]; staged {
			continue
		}
		if segment.GetLevel() == datapb.SegmentLevel_L0 || segment.GetIsImporting() || segment.GetIsFake() ||
			(len(segment.GetBinlogs()) == 0 && segment.GetManifestPath() == "") {
			continue
		}
		if !isFlushState(segment.GetState()) && segment.GetState() != commonpb.SegmentState_Dropped {
			continue
		}
		// A flush may publish an awaiting-sort input with IsInvisible set. It is
		// a valid fallback until the sort output is ready; an unpublished import
		// or clustering intermediate is not.
		if segment.GetIsInvisible() && (admitted.Contain(segment.GetID()) || segment.GetSealedAtDataVersion() != nil) {
			segment = segment.Clone()
			segment.IsInvisible = false
		}
		valid[segment.GetID()] = segment
		if segment.GetIsInvisible() {
			continue
		}
		if segment.GetState() == commonpb.SegmentState_Dropped {
			dropped.Insert(segment.GetID())
		} else {
			flushed.Insert(segment.GetID())
		}
	}
	m.segMu.RUnlock()

	var indexes []*model.Index
	if m.indexMeta != nil {
		indexes = m.indexMeta.GetIndexesForCollection(collectionID, "")
	}
	collection := m.GetCollection(collectionID)
	requireSorted := enableSortCompaction() && (collection == nil || !collection.IsExternal())
	ready := make(map[int64]bool)
	isReady := func(id int64) bool {
		if complete, ok := ready[id]; ok {
			return complete
		}
		segment := valid[id]
		if segment == nil {
			return false
		}
		if admitted.Contain(id) || len(segment.GetCompactionFrom()) == 0 {
			ready[id] = true // Ordinary Flush keeps its existing publication contract.
			return true
		}
		if requireSorted && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() {
			ready[id] = false
			return false
		}
		for _, index := range indexes {
			// Check IDs, not just fields: scalar and multiple JSON-path indexes all
			// participate. Small/no-training builds are Finished by the scheduler.
			if m.indexMeta.GetSegmentIndexState(collectionID, id, index.IndexID).GetState() != commonpb.IndexState_Finished {
				ready[id] = false
				return false
			}
		}
		ready[id] = true
		return true
	}

	// A chain may contain unindexed, unsorted intermediates. They may be
	// traversed, but never selected: every branch must reach a complete ready
	// ancestor frontier. Memoization bounds ancestry traversal to the DAG size.
	fallback := make(map[int64]bool, len(valid))
	visiting := typeutil.NewUniqueSet()
	var canFallback func(int64) bool
	canFallback = func(id int64) bool {
		if result, ok := fallback[id]; ok {
			return result
		}
		segment := valid[id]
		if segment == nil || segment.GetIsInvisible() || visiting.Contain(id) ||
			(segment.GetState() == commonpb.SegmentState_Dropped && !segment.GetCompacted()) {
			return false
		}
		if isReady(id) {
			fallback[id] = true
			return true
		}
		visiting.Insert(id)
		complete := len(segment.GetCompactionFrom()) > 0
		for _, parent := range segment.GetCompactionFrom() {
			if !canFallback(parent) {
				complete = false
				break
			}
		}
		visiting.Remove(id)
		fallback[id] = complete
		return complete
	}
	selected, _ := retrieveSegment(valid, flushed, dropped, isReady, canFallback)
	result := make([]dataview.LoadableSegment, 0, len(selected))
	for id := range selected {
		if !isReady(id) {
			// Never publish a partial result or expose an unready leaf when ancestry
			// is unavailable. The manager retains its last complete snapshot.
			return nil, merr.WrapErrServiceNotReadyMsg("compaction segment %d is not ready and has no complete ready ancestor frontier", id)
		}
		segment, err := loadableSegmentOf(valid[id])
		if err != nil {
			return nil, err
		}
		result = append(result, segment)
	}
	return result, nil
}
