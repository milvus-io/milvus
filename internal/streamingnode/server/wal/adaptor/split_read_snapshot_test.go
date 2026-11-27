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

package adaptor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSplitFixedSnapshotBelowBaselineFailsWithoutPromotion(t *testing.T) {
	for _, meta := range []*viewpb.QueryViewMeta{
		{TransformStartAfterTimetick: 100, Lineage: &viewpb.QueryViewLineage{SwitchTimetick: 200}},
		{TransformStartAfterTimetick: 200},                           // adopted target g2 has its own baseline
		{TransformStartAfterTimetick: 40, SnapshotHistoryFloor: 200}, // another segment still replays from 40
	} {
		mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 150, TransformingTimetick: 150}
		req := &viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: mvcc}}
		err := requireSplitReadSnapshot(meta, mvcc, req)
		require.Error(t, err)
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		require.EqualValues(t, 150, mvcc.GetGrowingTimetick())
		require.EqualValues(t, 150, mvcc.GetTransformingTimetick())
		require.NoError(t, requireSplitReadSnapshot(meta, &viewpb.QueryPlanMVCC{GrowingTimetick: 200, TransformingTimetick: 200}, req))
	}
}

func TestSplitFixedSnapshotGuardRunsBeforeQueryRuntime(t *testing.T) {
	mockey.PatchConvey("a rewritten g1 baseline cannot serve B before its switch", t, func() {
		released := false
		meta := &viewpb.QueryViewMeta{CollectionId: 100, Vchannel: "p_100v0", ReplicaId: 1, Lineage: &viewpb.QueryViewLineage{SwitchTimetick: 200}}
		lease := &snview.QueryViewLease{Meta: meta, Release: func() { released = true }}
		mockey.Mock((*snview.SNQueryViewHandler).AcquireUpViewAtGeneration).Return(lease, nil).Build()
		mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 150, TransformingTimetick: 150}
		mockey.Mock((*walAdaptorImpl).resolveQueryPlanMVCC).Return(mvcc, nil).Build()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{availableCtx: context.Background(), lifetime: typeutil.NewLifetime()}, queryViewHandler: &snview.SNQueryViewHandler{}}
		req := &viewpb.GetQueryPlanRequest{CollectionId: 100, ShardId: qviews.NewShardIDFromQVMeta(meta).IntoProto(), Mvcc: &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: 150}, Request: &viewpb.GetQueryPlanRequest_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{}}}
		response, err := w.GetQueryPlan(t.Context(), req)
		require.Nil(t, response)
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		require.True(t, released)
		require.EqualValues(t, 150, req.GetBarrierMvcc())
		require.EqualValues(t, 150, mvcc.GetGrowingTimetick())
	})
}

func TestPhysicalHistoryFloorUsesRequestedBarrierAndSourceReadPoint(t *testing.T) {
	meta := &viewpb.QueryViewMeta{TransformStartAfterTimetick: 40, SnapshotHistoryFloor: 200}
	mvcc := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 100}
	for _, b := range []uint64{199, 200, 250} {
		barrier := &viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_BarrierMvcc{BarrierMvcc: b}}
		source := &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 100, ReadTimetick: b}
		executor := &viewpb.GetQueryPlanRequest{Mvcc: &viewpb.GetQueryPlanRequest_QueryPlanMvcc{QueryPlanMvcc: source}}
		for _, err := range []error{requireSplitReadSnapshot(meta, mvcc, barrier), requireSplitReadSnapshot(meta, source, executor)} {
			if b < 200 {
				require.Error(t, err)
				require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
			} else {
				require.NoError(t, err)
			}
		}
	}
}
