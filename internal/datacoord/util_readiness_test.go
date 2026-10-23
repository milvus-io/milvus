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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Two readiness questions are asked of a segment: may a compaction output
// replace its parents in the query view (handoff), and may a segment be fed
// into another compaction. Handoff waits for every index defined on the
// collection; compaction input waits for the vector indexes only. With nothing
// to wait for, both answer yes.
func TestReadinessFilters(t *testing.T) {
	const (
		vectorField = int64(10)
		scalarField = int64(20)
		vectorIndex = int64(100)
		scalarIndex = int64(200)
	)
	newServer := func(fields []*schemapb.FieldSchema) *Server {
		s, _ := newIndexReadinessTestServer([]*datapb.SegmentInfo{
			newIndexLineageSegment(1, 100, commonpb.SegmentState_Flushed),
			newIndexLineageSegment(2, 100, commonpb.SegmentState_Flushed),
			newIndexLineageSegment(3, 100, commonpb.SegmentState_Growing),
		}, nil)
		s.meta.GetCollection(1).Schema.Fields = fields
		s.meta.indexMeta.indexes[1] = map[UniqueID]*model.Index{}
		return s
	}
	setIndex := func(s *Server, fieldID, indexID int64, states map[int64]commonpb.IndexState) {
		s.meta.indexMeta.indexes[1][indexID] = &model.Index{CollectionID: 1, FieldID: fieldID, IndexID: indexID, IndexName: "idx"}
		for segmentID, state := range states {
			indexes, ok := s.meta.indexMeta.segmentIndexes.Get(segmentID)
			if !ok {
				indexes = typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
				s.meta.indexMeta.segmentIndexes.Insert(segmentID, indexes)
			}
			indexes.Insert(indexID, &model.SegmentIndex{SegmentID: segmentID, CollectionID: 1, IndexID: indexID, IndexState: state})
		}
	}
	ids := func(segments []*SegmentInfo) []int64 {
		ret := make([]int64, 0, len(segments))
		for _, segment := range segments {
			ret = append(ret, segment.GetID())
		}
		return ret
	}
	allSegments := func(s *Server) []*SegmentInfo {
		return s.meta.GetRealSegmentsForChannel("lineage_channel")
	}
	ctx := context.Background()
	scalarOnly := []*schemapb.FieldSchema{{FieldID: scalarField, Name: "scalar", DataType: schemapb.DataType_Int64}}
	withVector := []*schemapb.FieldSchema{
		{FieldID: vectorField, Name: "vector", DataType: schemapb.DataType_FloatVector},
		{FieldID: scalarField, Name: "scalar", DataType: schemapb.DataType_Int64},
	}

	t.Run("no vector field and no index: nothing to wait for", func(t *testing.T) {
		s := newServer(scalarOnly)
		require.ElementsMatch(t, []int64{1, 2}, ids(FilterInHandoffReadySegments(ctx, s.handler, s.meta, allSegments(s)...)))
		require.ElementsMatch(t, []int64{1, 2}, ids(FilterInCompactionInputReadySegments(ctx, s.handler, s.meta, false, allSegments(s)...)))
	})

	t.Run("no vector field with a scalar index: handoff waits, compaction input does not", func(t *testing.T) {
		s := newServer(scalarOnly)
		setIndex(s, scalarField, scalarIndex, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_InProgress})
		require.ElementsMatch(t, []int64{1}, ids(FilterInHandoffReadySegments(ctx, s.handler, s.meta, allSegments(s)...)))
		require.ElementsMatch(t, []int64{1, 2}, ids(FilterInCompactionInputReadySegments(ctx, s.handler, s.meta, false, allSegments(s)...)))
	})

	t.Run("vector field without any index: compaction input waits for the index to be defined", func(t *testing.T) {
		s := newServer(withVector)
		require.ElementsMatch(t, []int64{1, 2}, ids(FilterInHandoffReadySegments(ctx, s.handler, s.meta, allSegments(s)...)))
		require.Empty(t, FilterInCompactionInputReadySegments(ctx, s.handler, s.meta, false, allSegments(s)...))
		require.ElementsMatch(t, []int64{1, 2}, ids(FilterInCompactionInputReadySegments(ctx, s.handler, s.meta, true, allSegments(s)...)))
	})

	t.Run("vector index built, scalar index in progress: compaction input ready, handoff not", func(t *testing.T) {
		s := newServer(withVector)
		setIndex(s, vectorField, vectorIndex, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_Finished})
		setIndex(s, scalarField, scalarIndex, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_InProgress})
		require.ElementsMatch(t, []int64{1}, ids(FilterInHandoffReadySegments(ctx, s.handler, s.meta, allSegments(s)...)))
		require.ElementsMatch(t, []int64{1, 2}, ids(FilterInCompactionInputReadySegments(ctx, s.handler, s.meta, false, allSegments(s)...)))
	})

	t.Run("vector index in progress blocks both", func(t *testing.T) {
		s := newServer(withVector)
		setIndex(s, vectorField, vectorIndex, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_InProgress})
		require.ElementsMatch(t, []int64{1}, ids(FilterInHandoffReadySegments(ctx, s.handler, s.meta, allSegments(s)...)))
		require.ElementsMatch(t, []int64{1}, ids(FilterInCompactionInputReadySegments(ctx, s.handler, s.meta, false, allSegments(s)...)))
	})
}
