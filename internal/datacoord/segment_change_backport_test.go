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

	"github.com/milvus-io/milvus/internal/datacoord/meta"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSegmentChangeLegacyViewsAndSeek(t *testing.T) {
	for _, state := range []model.SegmentChangeState{model.SegmentChangeStateStaged, model.SegmentChangeStateReady} {
		t.Run(state.String(), func(t *testing.T) {
			ctx := context.Background()
			m, err := meta.NewMemoryMeta(t)
			require.NoError(t, err)
			m.AddCollection(&meta.CollectionInfo{ID: 10, Schema: readinessVectorSchema()})
			for _, fixture := range []struct {
				id        int64
				timestamp uint64
				invisible bool
			}{
				{1001, 10, true}, // Unpublished member, not created by compaction.
				{2001, 30, false},
				{3001, 20, true}, // Group-less flush awaiting sort must retain its DML position.
			} {
				require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
					ID: fixture.id, CollectionID: 10, PartitionID: 100, InsertChannel: "ch-1",
					State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1,
					IsInvisible: fixture.invisible, IsSorted: true, NumOfRows: 1,
					DmlPosition: &msgpb.MsgPosition{ChannelName: "ch-1", MsgID: []byte{1}, Timestamp: fixture.timestamp},
					Binlogs:     []*datapb.FieldBinlog{{FieldID: 2, Binlogs: []*datapb.Binlog{{LogID: fixture.id}}}},
				})))
			}
			group := meta.NewTestGroup()
			require.NoError(t, m.AddSegmentChangeGroup(ctx, group))
			if state == model.SegmentChangeStateReady {
				group.State = state
				require.NoError(t, m.UpdateSegmentChangeGroup(ctx, group))
			}
			h := &ServerHandler{s: &Server{ctx: ctx, meta: m}}
			channel := &meta.ChannelMeta{Name: "ch-1", CollectionID: 10}
			query := h.GetQueryVChanPositions(channel)
			require.NotContains(t, query.GetFlushedSegmentIds(), int64(1001))
			require.NotContains(t, query.GetUnflushedSegmentIds(), int64(1001))
			require.NotContains(t, query.GetDroppedSegmentIds(), int64(1001))
			require.NotContains(t, query.GetLevelZeroSegmentIds(), int64(1001))
			require.Contains(t, query.GetUnflushedSegmentIds(), int64(3001))
			view := h.GetCurrentSegmentsView(ctx, channel)
			require.NotContains(t, view.FlushedSegmentIDs, int64(1001))
			require.NotContains(t, view.GrowingSegmentIDs, int64(1001))
			seek := h.getEarliestSegmentDMLPos("ch-1")
			require.NotNil(t, seek)
			require.Equal(t, uint64(20), seek.GetTimestamp())
		})
	}
}

type segmentChangeBackportCatalog struct {
	metastore.DataCoordCatalog
	update func(context.Context, ...metastore.UpdateAction) error
}

func (c *segmentChangeBackportCatalog) Update(ctx context.Context, actions ...metastore.UpdateAction) error {
	return c.update(ctx, actions...)
}

func TestSegmentChangePublicationRetry(t *testing.T) {
	for _, failure := range []struct {
		name   string
		err    error
		cancel bool
	}{
		{"transient", merr.WrapErrServiceUnavailableMsg("catalog unavailable"), false},
		{"input_typed", merr.WrapErrParameterInvalidMsg("catalog failure"), false},
		{"canceled", merr.WrapErrServiceUnavailableMsg("catalog unavailable"), true},
	} {
		t.Run(failure.name, func(t *testing.T) {
			m, err := meta.NewMemoryMeta(t)
			require.NoError(t, err)
			ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			catalog := m.Catalog
			calls := 0
			var firstActions []metastore.UpdateAction
			m.Catalog = &segmentChangeBackportCatalog{
				DataCoordCatalog: catalog,
				update: func(ctx context.Context, actions ...metastore.UpdateAction) error {
					calls++
					if calls == 1 {
						firstActions = append([]metastore.UpdateAction(nil), actions...)
						if failure.cancel {
							cancel()
						}
						return failure.err
					}
					require.Equal(t, firstActions, actions)
					return catalog.Update(ctx, actions...)
				},
			}
			err = m.PublishGroup(ctx, ready, meta.NewMockAllocator(t))
			if failure.cancel {
				require.ErrorIs(t, err, failure.err)
				require.Equal(t, 1, calls)
				require.True(t, m.GetSegment(context.Background(), 1001).GetIsInvisible())
				require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(context.Background(), 2001).GetState())
				require.Equal(t, model.SegmentChangeStateReady, m.GetSegmentChangeGroup(context.Background(), 10, 1).State)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 2, calls)
			require.False(t, m.GetSegment(ctx, 1001).GetIsInvisible())
			require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 2001).GetState())
			groups, staged, superseded, err := m.LoadSegmentChangeGroups(ctx)
			require.NoError(t, err)
			require.Equal(t, model.SegmentChangeStateCommitted, groups[1].State)
			require.Empty(t, staged)
			require.Empty(t, superseded)
		})
	}
}
