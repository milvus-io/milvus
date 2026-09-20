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
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestDroppedImportSegmentGCJobProtection(t *testing.T) {
	for _, state := range []internalpb.ImportJobState{
		internalpb.ImportJobState_Pending, internalpb.ImportJobState_PreImporting,
		internalpb.ImportJobState_Importing, internalpb.ImportJobState_Sorting,
		internalpb.ImportJobState_IndexBuilding, internalpb.ImportJobState_Uncommitted,
		internalpb.ImportJobState_Committing, internalpb.ImportJobState_Completed,
		internalpb.ImportJobState_Failed,
	} {
		t.Run(state.String(), func(t *testing.T) {
			ctx := context.Background()
			catalog := catalogmocks.NewDataCoordCatalog(t)
			catalog.EXPECT().ChannelExists(mock.Anything, "ch").Return(false)
			mt := &meta{catalog: catalog, segments: NewSegmentsInfo(), channelCPs: newChannelCps()}
			for _, id := range []int64{1, 2} {
				mt.segments.SetSegment(id, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
					ID: id, CollectionID: 100, InsertChannel: "ch",
					State: commonpb.SegmentState_Dropped, IsImporting: true,
					DroppedAt: uint64(time.Now().Add(-time.Minute).UnixNano()),
				}})
			}
			handler := NewNMockHandler(t)
			handler.EXPECT().ListLoadedSegments(mock.Anything).Return(nil, nil)
			im := NewMockImportMeta(t)
			job := &importJob{ImportJob: &datapb.ImportJob{JobID: 10, CollectionID: 100, State: state}}
			// One job lookup per scan, regardless of segment count. Even a
			// terminal job protects the markers until job cleanup removes it.
			im.EXPECT().GetJobBy(mock.Anything).Return([]ImportJob{job}).Once()
			im.EXPECT().GetJobBy(mock.Anything).Return([]ImportJob{
				&importJob{ImportJob: &datapb.ImportJob{JobID: 11, CollectionID: 200, State: state}},
			}).Once()
			gc := newGarbageCollector(mt, handler, GcOption{importMeta: im, dropTolerance: 0})
			var recycled []int64
			patch := mockey.Mock((*garbageCollector).recycleDroppedSegment).
				To(func(_ *garbageCollector, _ context.Context, id int64, _ *SegmentInfo) {
					recycled = append(recycled, id)
				}).Build()
			defer patch.UnPatch()

			gc.recycleDroppedSegments(ctx, nil)
			require.Empty(t, recycled)
			gc.recycleDroppedSegments(ctx, nil)
			require.ElementsMatch(t, []int64{1, 2}, recycled,
				"removing the collection's last job releases legacy markers, without a 24-hour delay")
		})
	}
}

func TestDroppedImportSegmentGCStillChecksRetentionAndCheckpoint(t *testing.T) {
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ChannelExists(mock.Anything, "ch").Return(true)
	gc := newGarbageCollector(&meta{catalog: catalog}, nil, GcOption{dropTolerance: time.Hour})
	segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 1, CollectionID: 100, InsertChannel: "ch", IsImporting: true,
		State: commonpb.SegmentState_Dropped, DroppedAt: uint64(time.Now().UnixNano()),
		DmlPosition: &msgpb.MsgPosition{Timestamp: 100}, CommitTimestamp: 200,
	}}
	noJobs := typeutil.NewUniqueSet()
	require.False(t, gc.checkDroppedSegmentGC(segment, nil, nil, 200, noJobs))
	segment.DroppedAt = uint64(time.Now().Add(-2 * time.Hour).UnixNano())
	require.False(t, gc.checkDroppedSegmentGC(segment, nil, nil, 100, noJobs))
	require.True(t, gc.checkDroppedSegmentGC(segment, nil, nil, 200, noJobs))
	require.False(t, gc.checkDroppedSegmentGC(segment, nil, nil, 200, nil),
		"unavailable import metadata must retain protection")
	segment.IsImporting = false
	require.True(t, gc.checkDroppedSegmentGC(segment, nil, nil, 200, typeutil.NewUniqueSet(100)),
		"import jobs must not block ordinary dropped segments")
}

func TestDroppedImportSegmentGCConcurrentNewJob(t *testing.T) {
	ctx := context.Background()
	catalog := catalogmocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ChannelExists(mock.Anything, "ch").Return(false)
	mt := &meta{catalog: catalog, segments: NewSegmentsInfo(), channelCPs: newChannelCps()}
	addMarker := func(id int64) {
		mt.segments.SetSegment(id, &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: id, CollectionID: 100, InsertChannel: "ch",
			State: commonpb.SegmentState_Dropped, IsImporting: true,
			DroppedAt: uint64(time.Now().Add(-time.Minute).UnixNano()),
		}})
	}
	addMarker(1)
	handler := NewNMockHandler(t)
	handler.EXPECT().ListLoadedSegments(mock.Anything).Return(nil, nil)
	im := NewMockImportMeta(t)
	// Simulate a job and its empty segment being published just after the
	// job snapshot. Segment 2 must not belong to this round's earlier snapshot.
	im.EXPECT().GetJobBy(mock.Anything).Run(func(context.Context, ...ImportJobFilter) {
		addMarker(2)
	}).Return([]ImportJob{}).Once()
	im.EXPECT().GetJobBy(mock.Anything).Return([]ImportJob{
		&importJob{ImportJob: &datapb.ImportJob{
			JobID: 10, CollectionID: 100, State: internalpb.ImportJobState_Importing,
		}},
	}).Once()
	gc := newGarbageCollector(mt, handler, GcOption{importMeta: im})
	var recycled []int64
	patch := mockey.Mock((*garbageCollector).recycleDroppedSegment).
		To(func(_ *garbageCollector, _ context.Context, id int64, _ *SegmentInfo) {
			recycled = append(recycled, id)
			mt.segments.DropSegment(id)
		}).Build()
	defer patch.UnPatch()

	gc.recycleDroppedSegments(ctx, nil)
	require.Equal(t, []int64{1}, recycled)
	require.NotNil(t, mt.GetSegment(ctx, 2))
	gc.recycleDroppedSegments(ctx, nil)
	require.Equal(t, []int64{1}, recycled)
	require.NotNil(t, mt.GetSegment(ctx, 2))
}
