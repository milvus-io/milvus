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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestIndexInspectorGetUnIndexTaskSegmentsSkipsSegmentsCannotBuildIndexNow(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key, "true")
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableCompaction.Key, "true")
	defer func() {
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableSortCompaction.Key)
		paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableCompaction.Key)
	}()

	const (
		collID  = UniqueID(1)
		partID  = UniqueID(2)
		fieldID = UniqueID(10)
		indexID = UniqueID(100)

		l0SegmentID       = UniqueID(2000)
		unsortedSegmentID = UniqueID(2001)
		sortedSegmentID   = UniqueID(2002)
	)
	m := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		indexMeta: &indexMeta{
			segmentBuildInfo: newSegmentIndexBuildInfo(),
			segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				collID: {
					indexID: {
						CollectionID: collID,
						FieldID:      fieldID,
						IndexID:      indexID,
						IndexName:    "default_idx",
					},
				},
			},
		},
	}
	m.collections.Insert(collID, &collectionInfo{
		ID: collID,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{Name: "pk", FieldID: fieldID, DataType: schemapb.DataType_Int64},
			},
		},
	})
	m.segments.SetSegment(l0SegmentID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           l0SegmentID,
		CollectionID: collID,
		PartitionID:  partID,
		NumOfRows:    1025,
		State:        commonpb.SegmentState_Flushed,
		Level:        datapb.SegmentLevel_L0,
	}})
	m.segments.SetSegment(unsortedSegmentID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           unsortedSegmentID,
		CollectionID: collID,
		PartitionID:  partID,
		NumOfRows:    1025,
		State:        commonpb.SegmentState_Flushed,
	}})
	m.segments.SetSegment(sortedSegmentID, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:           sortedSegmentID,
		CollectionID: collID,
		PartitionID:  partID,
		NumOfRows:    1025,
		State:        commonpb.SegmentState_Flushed,
		IsSorted:     true,
	}})

	segments := (&indexInspector{meta: m}).getUnIndexTaskSegments(context.TODO())

	segmentIDs := make([]UniqueID, 0, len(segments))
	for _, segment := range segments {
		segmentIDs = append(segmentIDs, segment.GetID())
	}
	assert.ElementsMatch(t, []UniqueID{sortedSegmentID}, segmentIDs)
}
