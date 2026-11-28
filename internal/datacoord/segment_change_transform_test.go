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
	"github.com/milvus-io/milvus/internal/datacoord/bulkimport"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSegmentChangeImportSiblingTransformFences(t *testing.T) {
	for _, source := range []model.SegmentChangeSource{model.SegmentChangeSourceImportJob, model.SegmentChangeSourceCDCReplicated} {
		t.Run(source.String(), func(t *testing.T) {
			setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
			setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeGroupMaxMembers, "1")
			m, err := meta.NewMemoryMeta(t)
			require.NoError(t, err)
			m.AddCollection(&meta.CollectionInfo{ID: 10, Schema: readinessVectorSchema(), VChannelNames: []string{"ch-1"}})
			imports := &bulkimport.Meta{Jobs: map[int64]bulkimport.ImportJob{}}
			manager := dataview.NewManager(m.Catalog.(dataview.Catalog), nil,
				dataview.WithPendingTransformProjector(func(ctx context.Context, view *viewpb.DataViewOfCollection) (map[string]uint64, error) {
					return pendingTransformBounds(ctx, m, imports, view)
				}))
			m.DataViewManager = manager
			_, err = manager.OnCreateCollection(t.Context(), dataview.CreateCollectionDataViewEvent{
				CollectionID: 10, VChannels: []string{"ch-1"}, TransformStarts: map[string]uint64{"ch-1": 100},
			})
			require.NoError(t, err)
			for _, id := range []int64{1001, 1002} {
				require.NoError(t, m.AddSegment(t.Context(), meta.NewSegmentInfo(&datapb.SegmentInfo{
					ID: id, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed,
					IsImporting: true, IsSorted: true, NumOfRows: 10, Binlogs: []*datapb.FieldBinlog{{FieldID: 2}},
				})))
			}
			job := &bulkimport.Job{ImportJob: &datapb.ImportJob{JobID: 8000, CollectionID: 10, Vchannels: []string{"ch-1"}, State: internalpb.ImportJobState_Committing}}
			imports.Jobs[8000] = job
			s := &Server{meta: m, allocator: meta.NewMockAllocator(t)}
			groups, handled, err := s.stageImportGroups(t.Context(), job, "ch-1", []int64{1001, 1002}, 0, source)
			require.NoError(t, err)
			require.True(t, handled)
			require.Len(t, groups, 2)
			_, err = manager.RecomputeNow(t.Context(), 10, m.LoadableProjection)
			require.NoError(t, err, "pre-ACK private members must freeze W rather than block unrelated publication")
			inspector := newSegmentChangeInspector(t.Context(), m, s.allocator)
			defer inspector.Stop()
			inspector.checkAllGroups(t.Context())
			job.TransformCommitTimeticks = map[string]uint64{"ch-1": 200}
			handled, err = s.stageImportChangeFromSource(t.Context(), job, "ch-1", nil, 200, source)
			require.True(t, handled)
			require.NoError(t, err, "both subgroups must publish through the real pending-transform projector")
			for _, group := range groups {
				require.Equal(t, model.SegmentChangeStateCommitted, m.GetSegmentChangeGroup(t.Context(), 10, group.GroupID).State)
			}
			for _, id := range []int64{1001, 1002} {
				require.EqualValues(t, 200, m.GetSegment(t.Context(), id).GetTransformStartAfterTimetick())
			}
			require.ElementsMatch(t, []int64{1001, 1002}, segmentChangeViewIDs(t, m))
		})
	}
}

func TestPendingImportTransformExcludesFailedPrivateCleanup(t *testing.T) {
	setSegmentChangeTestParam(t, &Params.DataCoordCfg.SegmentChangeEnabled, "true")
	m, _, _ := seedMixProducerWithTransformStarts(t, map[string]uint64{"ch-1": 100})
	require.NoError(t, m.AddSegment(t.Context(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1001, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed,
		IsImporting: true, IsSorted: true, NumOfRows: 10, Binlogs: []*datapb.FieldBinlog{{FieldID: 2}},
	})))
	s := &Server{meta: m}
	job := &bulkimport.Job{ImportJob: &datapb.ImportJob{JobID: 8000, CollectionID: 10, Vchannels: []string{"ch-1"}}}
	groups, _, err := s.stageImportGroups(t.Context(), job, "ch-1", []int64{1001}, 0, model.SegmentChangeSourceImportJob)
	require.NoError(t, err)
	group := groups[0].Clone()
	group.State, group.FailReason = model.SegmentChangeStateFailed, "pre-commit preparation failed"
	require.NoError(t, m.UpdateSegmentChangeGroup(t.Context(), group))
	view := &viewpb.DataViewOfCollection{CollectionId: 10, Shards: []*viewpb.DataViewOfShard{{
		Vchannel:                           "ch-1",
		GrowingTransformStartAfterTimetick: 100, Partitions: []*viewpb.DataViewOfPartition{{SegmentIds: []int64{2001, 2002}}},
	}}}
	_, err = pendingTransformBounds(t.Context(), m, nil, view)
	require.NoError(t, err, "failed private members cannot become visible and must not block unrelated publication")
	require.NoError(t, m.AddSegment(t.Context(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1002, CollectionID: 10, InsertChannel: "ch-1", State: commonpb.SegmentState_Flushed, CreatedByCompaction: true,
	})))
	_, err = pendingTransformBounds(t.Context(), m, nil, view)
	require.ErrorIs(t, err, merr.ErrServiceNotReady, "unowned missing Transform coverage must still fail closed")
}
