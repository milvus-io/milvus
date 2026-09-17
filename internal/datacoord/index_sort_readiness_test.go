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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type indexSortReadinessCatalog struct {
	metastore.DataCoordCatalog
	segments []*datapb.SegmentInfo
}

func (c *indexSortReadinessCatalog) AlterSegments(_ context.Context, segments []*datapb.SegmentInfo, _ ...metastore.BinlogsIncrement) error {
	for _, segment := range segments {
		c.segments = append(c.segments, proto.Clone(segment).(*datapb.SegmentInfo))
	}
	return nil
}

func TestIndexServingReadinessSortTransition(t *testing.T) {
	for _, input := range []struct {
		name  string
		state commonpb.IndexState
		task  bool
	}{
		{name: "no input task"},
		{name: "input building", state: commonpb.IndexState_InProgress, task: true},
		{name: "input finished", state: commonpb.IndexState_Finished, task: true},
	} {
		for _, output := range []struct {
			name  string
			state commonpb.IndexState
			task  bool
		}{
			{name: "no output task"},
			{name: "output unissued", state: commonpb.IndexState_Unissued, task: true},
			{name: "output building", state: commonpb.IndexState_InProgress, task: true},
			{name: "output finished", state: commonpb.IndexState_Finished, task: true},
		} {
			t.Run(fmt.Sprintf("%s/%s", input.name, output.name), func(t *testing.T) {
				original := newIndexLineageSegment(1, 100, commonpb.SegmentState_Flushed)
				original.IsInvisible = true
				states := map[int64]commonpb.IndexState{}
				if input.task {
					states[1] = input.state
				}
				s, index := newIndexReadinessTestServer([]*datapb.SegmentInfo{original}, states)
				index.IndexName = "sort_index"
				beforeState := commonpb.IndexState_InProgress
				if input.state == commonpb.IndexState_Finished {
					beforeState = commonpb.IndexState_Finished
				}
				assertIndexServingState(t, s, index, beforeState)
				channel := &channelMeta{Name: "lineage_channel", CollectionID: 1, StartPosition: &msgpb.MsgPosition{Timestamp: 1}}
				before := newServerHandler(s).GetQueryVChanPositions(channel)
				require.Empty(t, before.GetFlushedSegmentIds())
				require.ElementsMatch(t, []int64{1}, before.GetUnflushedSegmentIds(), "ordinary invisible sort input is recovered as growing")

				// Exercise the real metadata producer after a successful worker
				// sort result. Native sorting/file generation is outside this test.
				catalog := &indexSortReadinessCatalog{}
				s.meta.catalog = catalog
				s.meta.ctx = context.Background()
				collection := s.meta.GetCollection(1)
				segments, err := s.meta.completeSortCompactionMutation(&datapb.CompactionTask{
					PlanID: 1000, CollectionID: 1, InputSegments: []int64{1}, Schema: collection.Schema,
				}, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{
					SegmentID: 2, NumOfRows: 90, IsSorted: true,
					InsertLogs: []*datapb.FieldBinlog{{FieldID: 10, Binlogs: []*datapb.Binlog{{LogID: 2, LogPath: "insert_log/1/1/2/10/2", EntriesNum: 90, TimestampFrom: 50, TimestampTo: 60}}}},
					Stats:      &datapb.Statistics{TimestampFrom: 50, TimestampTo: 60},
				}}})
				require.NoError(t, err)
				require.Len(t, segments, 1)
				require.Len(t, catalog.segments, 2, "producer persists the dropped input and sorted output")
				parent := s.meta.segments.GetSegment(1)
				require.Equal(t, commonpb.SegmentState_Dropped, parent.GetState())
				require.True(t, parent.GetCompacted())
				require.True(t, parent.GetIsInvisible())
				child := s.meta.segments.GetSegment(2)
				require.Equal(t, commonpb.SegmentState_Flushed, child.GetState())
				require.True(t, child.GetIsSorted())
				require.False(t, child.GetIsInvisible())
				require.False(t, child.GetCreatedByCompaction(), "sort output inherits the ordinary input's lineage flag")
				require.Equal(t, []int64{1}, child.GetCompactionFrom())
				if output.task {
					addIndexServingTask(s, 2, 10, output.state)
				}
				after := newServerHandler(s).GetQueryVChanPositions(channel)
				require.ElementsMatch(t, []int64{2}, after.GetFlushedSegmentIds())
				require.Empty(t, after.GetUnflushedSegmentIds())
				require.ElementsMatch(t, []int64{1}, after.GetDroppedSegmentIds())
				wantState := commonpb.IndexState_InProgress
				if output.state == commonpb.IndexState_Finished {
					wantState = commonpb.IndexState_Finished
				}
				assertIndexServingState(t, s, index, wantState)
				progress, err := s.GetIndexBuildProgress(context.Background(), &indexpb.GetIndexBuildProgressRequest{CollectionID: 1, IndexName: index.IndexName})
				require.NoError(t, err)
				require.EqualValues(t, 90, progress.GetTotalRows())
				if output.state == commonpb.IndexState_Finished {
					require.EqualValues(t, 90, progress.GetIndexedRows())
					require.Zero(t, progress.GetPendingIndexRows())
				} else {
					require.EqualValues(t, 90, progress.GetPendingIndexRows())
					if input.state == commonpb.IndexState_Finished {
						require.EqualValues(t, 100, progress.GetIndexedRows(), "historical row accounting does not certify the visible output's readiness")
					} else {
						require.Zero(t, progress.GetIndexedRows())
					}
				}
			})
		}
	}
}
