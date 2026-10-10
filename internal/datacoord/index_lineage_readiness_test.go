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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newIndexLineageSegment(id, rows int64, state commonpb.SegmentState, parents ...int64) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID: id, CollectionID: 1, NumOfRows: rows, State: state,
		Level: datapb.SegmentLevel_L1, CompactionFrom: parents,
		CreatedByCompaction: len(parents) > 0, LastExpireTime: 200,
		Stats: &datapb.Statistics{TimestampFrom: 50, TimestampTo: 60},
	}
}

type indexLineageReadinessCase struct {
	name             string
	segments         []*datapb.SegmentInfo
	states           map[int64]commonpb.IndexState
	otherIndexStates map[int64]commonpb.IndexState
	wantState        commonpb.IndexState
	boundCompletion  bool
}

func newIndexLineageCaseServer(test indexLineageReadinessCase) (*Server, *model.Index) {
	s, index := newIndexReadinessTestServer(test.segments, test.states)
	index.IndexName = "lineage_index"
	if len(test.otherIndexStates) > 0 {
		s.meta.indexMeta.indexes[1][20] = &model.Index{CollectionID: 1, FieldID: 20, IndexID: 20, IndexName: "other_index"}
	}
	for segmentID, state := range test.otherIndexStates {
		indexes, ok := s.meta.indexMeta.segmentIndexes.Get(segmentID)
		if !ok {
			indexes = typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
			s.meta.indexMeta.segmentIndexes.Insert(segmentID, indexes)
		}
		indexes.Insert(20, &model.SegmentIndex{
			SegmentID: segmentID, CollectionID: 1, IndexID: 20,
			IndexState: state, CurrentIndexVersion: 7,
		})
	}
	return s, index
}

func assertIndexLineageReadiness(t *testing.T, test indexLineageReadinessCase, realTime bool) *indexpb.IndexInfo {
	t.Helper()
	s, index := newIndexLineageCaseServer(test)
	stats := s.selectSegmentIndexesStats(context.Background(), WithCollection(1))
	for segmentID, state := range test.otherIndexStates {
		require.Equal(t, state, stats[segmentID].indexStates[20].GetState(), "another live index survives the collector boundary")
	}
	info := &indexpb.IndexInfo{IndexID: index.IndexID}
	if test.boundCompletion {
		done := make(chan struct{})
		go func() {
			s.completeIndexInfo(info, index, stats, realTime, 100)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("cyclic compaction metadata must not hang readiness or row counting")
		}
	} else {
		s.completeIndexInfo(info, index, stats, realTime, 100)
	}
	require.Equal(t, test.wantState, info.GetState())
	if test.wantState == commonpb.IndexState_Failed {
		require.Contains(t, info.GetIndexStateFailReason(), "build failed")
	} else {
		require.Empty(t, info.GetIndexStateFailReason())
	}
	return info
}

// These tests use the real metadata collector and query frontier. An ancestor
// contributes only when QueryCoord recovery can select it for the same IndexID.
func TestIndexLineageReadinessCompleteAncestors(t *testing.T) {
	for _, realTime := range []bool{false, true} {
		for _, root := range []struct {
			name  string
			state commonpb.IndexState
			task  bool
		}{
			{name: "no task"},
			{name: "stored unissued", state: commonpb.IndexState_Unissued, task: true},
			{name: "stored none", state: commonpb.IndexState_IndexStateNone, task: true},
			{name: "in progress", state: commonpb.IndexState_InProgress, task: true},
			{name: "failed", state: commonpb.IndexState_Failed, task: true},
		} {
			t.Run(fmt.Sprintf("realTime=%t/%s", realTime, root.name), func(t *testing.T) {
				states := map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_Finished}
				if root.task {
					states[3] = root.state
				}
				info := assertIndexLineageReadiness(t, indexLineageReadinessCase{
					segments: []*datapb.SegmentInfo{
						newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
						newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
						newIndexLineageSegment(3, 150, commonpb.SegmentState_Flushed, 1, 2),
					},
					states: states, wantState: commonpb.IndexState_Finished,
				}, realTime)
				require.EqualValues(t, 150, info.GetTotalRows())
				require.EqualValues(t, 150, info.GetPendingIndexRows(), "replacement work remains pending despite complete ancestor coverage")
				if realTime {
					require.Zero(t, info.GetIndexedRows(), "real-time rows count finished current indexes")
				} else {
					require.EqualValues(t, 200, info.GetIndexedRows(), "retain completed ancestor rows, including rows removed by compaction")
				}
			})
		}
	}
}

func TestIndexLineageReadinessIncompleteAncestors(t *testing.T) {
	for _, realTime := range []bool{false, true} {
		for _, parent := range []struct {
			name       string
			indexState commonpb.IndexState
			task       bool
			missing    bool
			invisible  bool
			importing  bool
			segState   commonpb.SegmentState
			otherIndex bool
		}{
			{name: "no parent index task", segState: commonpb.SegmentState_Dropped},
			{name: "parent index unissued", indexState: commonpb.IndexState_Unissued, task: true, segState: commonpb.SegmentState_Dropped},
			{name: "parent index none", indexState: commonpb.IndexState_IndexStateNone, task: true, segState: commonpb.SegmentState_Dropped},
			{name: "parent index in progress", indexState: commonpb.IndexState_InProgress, task: true, segState: commonpb.SegmentState_Dropped},
			{name: "parent index failed", indexState: commonpb.IndexState_Failed, task: true, segState: commonpb.SegmentState_Dropped},
			{name: "missing parent metadata", indexState: commonpb.IndexState_Finished, task: true, missing: true},
			{name: "invisible finished parent", indexState: commonpb.IndexState_Finished, task: true, invisible: true, segState: commonpb.SegmentState_Dropped},
			{name: "importing finished parent", indexState: commonpb.IndexState_Finished, task: true, importing: true, segState: commonpb.SegmentState_Dropped},
			{name: "growing finished parent", indexState: commonpb.IndexState_Finished, task: true, segState: commonpb.SegmentState_Growing},
			{name: "sealed finished parent", indexState: commonpb.IndexState_Finished, task: true, segState: commonpb.SegmentState_Sealed},
			{name: "importing state finished parent", indexState: commonpb.IndexState_Finished, task: true, segState: commonpb.SegmentState_Importing},
			{name: "finished parent for another index", indexState: commonpb.IndexState_Finished, task: true, otherIndex: true, segState: commonpb.SegmentState_Dropped},
		} {
			t.Run(fmt.Sprintf("realTime=%t/%s", realTime, parent.name), func(t *testing.T) {
				segments := []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(3, 150, commonpb.SegmentState_Flushed, 1, 2),
				}
				if !parent.missing {
					segment := newIndexLineageSegment(2, 100, parent.segState)
					segment.IsInvisible = parent.invisible
					segment.IsImporting = parent.importing
					segments = append(segments, segment)
				}
				test := indexLineageReadinessCase{
					segments: segments, wantState: commonpb.IndexState_InProgress,
					states: map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 3: commonpb.IndexState_InProgress},
				}
				if parent.task {
					if parent.otherIndex {
						test.otherIndexStates = map[int64]commonpb.IndexState{2: parent.indexState}
					} else {
						test.states[2] = parent.indexState
					}
				}
				assertIndexLineageReadiness(t, test, realTime)
			})
		}
	}
}

func TestIndexLineageReadinessGraphs(t *testing.T) {
	for _, realTime := range []bool{false, true} {
		for _, test := range []indexLineageReadinessCase{
			{
				name: "finished grandparents behind unready intermediate",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(3, 180, commonpb.SegmentState_Dropped, 1, 2),
					newIndexLineageSegment(4, 150, commonpb.SegmentState_Flushed, 3),
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_Finished, 3: commonpb.IndexState_Failed},
				wantState: commonpb.IndexState_Finished,
			},
			{
				name: "multiple levels partially covered",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(3, 180, commonpb.SegmentState_Dropped, 1, 2),
					newIndexLineageSegment(4, 150, commonpb.SegmentState_Flushed, 3),
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_InProgress},
				wantState: commonpb.IndexState_InProgress,
			},
			{
				name: "deep missing branch",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(3, 180, commonpb.SegmentState_Dropped, 1, 2),
					newIndexLineageSegment(4, 150, commonpb.SegmentState_Flushed, 3),
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished},
				wantState: commonpb.IndexState_InProgress,
			},
			{
				name: "finished shared ancestor behind unready intermediates",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(2, 40, commonpb.SegmentState_Dropped, 1),
					newIndexLineageSegment(3, 50, commonpb.SegmentState_Dropped, 1),
					newIndexLineageSegment(4, 80, commonpb.SegmentState_Flushed, 2, 3),
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished},
				wantState: commonpb.IndexState_Finished,
			},
			{
				name: "shared ancestor DAG has an uncovered branch",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(3, 80, commonpb.SegmentState_Dropped, 1),
					newIndexLineageSegment(4, 100, commonpb.SegmentState_Dropped, 1, 2),
					newIndexLineageSegment(5, 150, commonpb.SegmentState_Flushed, 3, 4),
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished},
				wantState: commonpb.IndexState_InProgress,
			},
			{
				name: "M:N outputs completely covered",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(3, 80, commonpb.SegmentState_Flushed, 1, 2),
					newIndexLineageSegment(4, 90, commonpb.SegmentState_Flushed, 1, 2),
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_Finished, 3: commonpb.IndexState_Failed, 4: commonpb.IndexState_InProgress},
				wantState: commonpb.IndexState_Finished,
			},
			{
				name: "M:N outputs only partially covered",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(3, 80, commonpb.SegmentState_Flushed, 1, 2),
					newIndexLineageSegment(4, 90, commonpb.SegmentState_Flushed, 1, 2),
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_InProgress},
				wantState: commonpb.IndexState_InProgress,
			},
			{
				name: "one finished M:N output does not cover another output",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(3, 80, commonpb.SegmentState_Flushed, 1, 2),
					newIndexLineageSegment(4, 90, commonpb.SegmentState_Flushed, 1, 2),
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 3: commonpb.IndexState_Finished},
				wantState: commonpb.IndexState_InProgress,
			},
			{
				name: "finished intermediate covers a missing deeper ancestor",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped, 1),
					newIndexLineageSegment(3, 90, commonpb.SegmentState_Flushed, 2),
				},
				states:    map[int64]commonpb.IndexState{2: commonpb.IndexState_Finished},
				wantState: commonpb.IndexState_Finished,
			},
			{
				name:      "root finished does not require ancestor metadata",
				segments:  []*datapb.SegmentInfo{newIndexLineageSegment(2, 90, commonpb.SegmentState_Flushed, 1)},
				states:    map[int64]commonpb.IndexState{2: commonpb.IndexState_Finished},
				wantState: commonpb.IndexState_Finished,
			},
			{
				name: "cycle without finished coverage",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Flushed, 2),
					newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped, 1),
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_InProgress},
				wantState: commonpb.IndexState_InProgress, boundCompletion: true,
			},
			{
				name:      "self cycle without finished coverage",
				segments:  []*datapb.SegmentInfo{newIndexLineageSegment(1, 100, commonpb.SegmentState_Flushed, 1)},
				wantState: commonpb.IndexState_InProgress, boundCompletion: true,
			},
			{
				name: "finished index in cycle has no selected frontier",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Flushed, 2),
					newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped, 1),
				},
				states:    map[int64]commonpb.IndexState{2: commonpb.IndexState_Finished},
				wantState: commonpb.IndexState_InProgress, boundCompletion: true,
			},
			{
				name: "flushing ancestor with finished index",
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Flushing),
					newIndexLineageSegment(2, 90, commonpb.SegmentState_Flushed, 1),
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished},
				wantState: commonpb.IndexState_Finished,
			},
			{
				name: "schema version does not gate coverage for the same index",
				segments: []*datapb.SegmentInfo{
					{ID: 1, CollectionID: 1, NumOfRows: 100, State: commonpb.SegmentState_Dropped, SchemaVersion: 1, Stats: &datapb.Statistics{TimestampFrom: 50, TimestampTo: 60}},
					{ID: 2, CollectionID: 1, NumOfRows: 90, State: commonpb.SegmentState_Flushed, SchemaVersion: 2, CompactionFrom: []int64{1}, Stats: &datapb.Statistics{TimestampFrom: 50, TimestampTo: 60}},
				},
				states:    map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_InProgress},
				wantState: commonpb.IndexState_Finished,
			},
		} {
			t.Run(fmt.Sprintf("realTime=%t/%s", realTime, test.name), func(t *testing.T) {
				assertIndexLineageReadiness(t, test, realTime)
			})
		}
	}
}

func TestIndexLineageReadinessWithoutAncestors(t *testing.T) {
	for _, realTime := range []bool{false, true} {
		for _, test := range []struct {
			name      string
			state     commonpb.IndexState
			task      bool
			wantState commonpb.IndexState
		}{
			{name: "no task", wantState: commonpb.IndexState_InProgress},
			{name: "none", state: commonpb.IndexState_IndexStateNone, task: true, wantState: commonpb.IndexState_IndexStateNone},
			{name: "unissued", state: commonpb.IndexState_Unissued, task: true, wantState: commonpb.IndexState_InProgress},
			{name: "in progress", state: commonpb.IndexState_InProgress, task: true, wantState: commonpb.IndexState_InProgress},
			{name: "failed", state: commonpb.IndexState_Failed, task: true, wantState: commonpb.IndexState_Failed},
			{name: "finished", state: commonpb.IndexState_Finished, task: true, wantState: commonpb.IndexState_Finished},
		} {
			t.Run(fmt.Sprintf("realTime=%t/%s", realTime, test.name), func(t *testing.T) {
				states := map[int64]commonpb.IndexState{}
				if test.task {
					states[1] = test.state
				}
				assertIndexLineageReadiness(t, indexLineageReadinessCase{
					segments: []*datapb.SegmentInfo{newIndexLineageSegment(1, 100, commonpb.SegmentState_Flushed)},
					states:   states, wantState: test.wantState,
				}, realTime)
			})
		}
	}
}

func TestIndexLineageReadinessStatusAPIs(t *testing.T) {
	for _, test := range []struct {
		name        string
		parentState commonpb.IndexState
		otherIndex  bool
		wantState   commonpb.IndexState
		wantIndexed int64
	}{
		{name: "complete coverage", parentState: commonpb.IndexState_Finished, wantState: commonpb.IndexState_Finished, wantIndexed: 200},
		{name: "partial coverage", parentState: commonpb.IndexState_InProgress, wantState: commonpb.IndexState_InProgress, wantIndexed: 100},
		{name: "another index is finished", parentState: commonpb.IndexState_Finished, otherIndex: true, wantState: commonpb.IndexState_InProgress, wantIndexed: 100},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := indexLineageReadinessCase{
				segments: []*datapb.SegmentInfo{
					newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
					newIndexLineageSegment(3, 150, commonpb.SegmentState_Flushed, 1, 2),
				},
				states: map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 3: commonpb.IndexState_InProgress},
			}
			if test.otherIndex {
				fixture.otherIndexStates = map[int64]commonpb.IndexState{2: test.parentState}
			} else {
				fixture.states[2] = test.parentState
			}
			s, _ := newIndexLineageCaseServer(fixture)
			ctx := context.Background()
			t.Run("DescribeIndex", func(t *testing.T) {
				response, err := s.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{CollectionID: 1, IndexName: "lineage_index", Timestamp: 100})
				require.NoError(t, err)
				require.Equal(t, commonpb.ErrorCode_Success, response.GetStatus().GetErrorCode())
				require.Len(t, response.GetIndexInfos(), 1)
				info := response.GetIndexInfos()[0]
				require.Equal(t, test.wantState, info.GetState())
				require.Empty(t, info.GetIndexStateFailReason())
				require.Equal(t, test.wantIndexed, info.GetIndexedRows())
				require.EqualValues(t, 150, info.GetTotalRows())
				require.EqualValues(t, 150, info.GetPendingIndexRows())
			})
			t.Run("GetIndexState", func(t *testing.T) {
				response, err := s.GetIndexState(ctx, &indexpb.GetIndexStateRequest{CollectionID: 1, IndexName: "lineage_index"})
				require.NoError(t, err)
				require.Equal(t, commonpb.ErrorCode_Success, response.GetStatus().GetErrorCode())
				require.Equal(t, test.wantState, response.GetState())
				require.Empty(t, response.GetFailReason())
			})
			t.Run("GetIndexBuildProgress", func(t *testing.T) {
				response, err := s.GetIndexBuildProgress(ctx, &indexpb.GetIndexBuildProgressRequest{CollectionID: 1, IndexName: "lineage_index"})
				require.NoError(t, err)
				require.Equal(t, commonpb.ErrorCode_Success, response.GetStatus().GetErrorCode())
				require.Equal(t, test.wantIndexed, response.GetIndexedRows())
				require.EqualValues(t, 150, response.GetTotalRows())
				require.EqualValues(t, 150, response.GetPendingIndexRows())
			})
			t.Run("GetIndexStatistics", func(t *testing.T) {
				response, err := s.GetIndexStatistics(ctx, &indexpb.GetIndexStatisticsRequest{CollectionID: 1, IndexName: "lineage_index"})
				require.NoError(t, err)
				require.Equal(t, commonpb.ErrorCode_Success, response.GetStatus().GetErrorCode())
				require.Len(t, response.GetIndexInfos(), 1)
				info := response.GetIndexInfos()[0]
				require.Equal(t, test.wantState, info.GetState())
				require.Empty(t, info.GetIndexStateFailReason())
				require.Zero(t, info.GetIndexedRows(), "real-time rows do not relabel unfinished output indexes as built")
				require.EqualValues(t, 150, info.GetTotalRows())
				require.EqualValues(t, 150, info.GetPendingIndexRows())
			})
		})
	}
}
