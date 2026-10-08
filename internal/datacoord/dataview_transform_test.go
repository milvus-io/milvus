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
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestTransformBoundsCoverUnpublishedPartitions(t *testing.T) {
	ctx := context.Background()
	metadata := &meta{ctx: ctx, collections: typeutil.NewConcurrentMap[int64, *collectionInfo](), segments: NewSegmentsInfo(), channelCPs: newChannelCps()}
	metadata.collections.Insert(100, &collectionInfo{ID: 100, VChannelNames: []string{"v1", "v2"}})
	metadata.channelCPs.checkpoints["v1"] = &msgpb.MsgPosition{Timestamp: 200}
	metadata.channelCPs.checkpoints["v2"] = &msgpb.MsgPosition{Timestamp: 20}
	for _, segment := range []*datapb.SegmentInfo{
		{ID: 1, CollectionID: 100, PartitionID: 1, InsertChannel: "v1", State: commonpb.SegmentState_Flushed, TransformStartAfterTimetick: 150},
		{ID: 2, CollectionID: 100, PartitionID: 2, InsertChannel: "v1", State: commonpb.SegmentState_Sealed, TransformStartAfterTimetick: 100},
		{ID: 3, CollectionID: 100, PartitionID: 3, InsertChannel: "v1", State: commonpb.SegmentState_Growing, TransformStartAfterTimetick: 60},
		{ID: 4, CollectionID: 100, PartitionID: 4, InsertChannel: "v1", State: commonpb.SegmentState_Dropped, TransformStartAfterTimetick: 10},
	} {
		metadata.segments.SetSegment(segment.ID, NewSegmentInfo(segment))
	}
	bounds, err := transformFrontierBounds(ctx, metadata, nil, 100)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{"v1": 60, "v2": 20}, bounds)
	// Durable but unpublished output still constrains the next DataView.
	metadata.segments.SetSegment(3, NewSegmentInfo(&datapb.SegmentInfo{ID: 3, CollectionID: 100, PartitionID: 3, InsertChannel: "v1", State: commonpb.SegmentState_Flushed, TransformStartAfterTimetick: 60}))
	bounds, err = transformFrontierBounds(ctx, metadata, nil, 100)
	require.NoError(t, err)
	require.Equal(t, uint64(60), bounds["v1"])
	delete(metadata.channelCPs.checkpoints, "v2")
	_, err = transformFrontierBounds(ctx, metadata, nil, 100)
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
}

func TestSegmentTransformCoverageDoesNotUseCompactedRowMinimum(t *testing.T) {
	ordinary := NewSegmentInfo(&datapb.SegmentInfo{StartPosition: &msgpb.MsgPosition{Timestamp: 10}})
	imported := NewSegmentInfo(&datapb.SegmentInfo{StartPosition: &msgpb.MsgPosition{Timestamp: 1}, CommitTimestamp: 30})
	compacted := NewSegmentInfo(&datapb.SegmentInfo{StartPosition: &msgpb.MsgPosition{Timestamp: 25}, CreatedByCompaction: true})
	require.Equal(t, uint64(10), segmentTransformStart(ordinary))
	require.Equal(t, uint64(30), segmentTransformStart(imported))
	require.Zero(t, segmentTransformStart(compacted), "surviving rows do not prove parent coverage")
	require.Equal(t, uint64(10), minSegmentTransformStart([]*SegmentInfo{ordinary, imported}))
	require.Zero(t, minSegmentTransformStart([]*SegmentInfo{ordinary, compacted}), "unknown parent must not be skipped")
	compacted.TransformStartAfterTimetick = 10
	require.Equal(t, uint64(10), segmentTransformStart(compacted))
}

func TestCheckpointRecomputeKeepsPublishedSortInputs(t *testing.T) {
	metadata := &meta{segments: NewSegmentsInfo()}
	for _, id := range []int64{1, 2} {
		info := &datapb.SegmentInfo{
			ID: id, CollectionID: 100, PartitionID: 1, InsertChannel: "v1",
			State: commonpb.SegmentState_Flushed, IsInvisible: true, TransformStartAfterTimetick: 50,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100}},
		}
		if id == 1 {
			info.SealedAtDataVersion = &viewpb.DataVersion{StreamingVersion: 2}
		}
		metadata.segments.SetSegment(id, NewSegmentInfo(info))
	}
	projection, err := metadata.loadableProjection(context.Background(), 100)
	require.NoError(t, err)
	require.Len(t, projection, 1)
	require.Equal(t, int64(1), projection[0].SegmentID, "published Flush input stays queryable until its replacement retires it")
}
