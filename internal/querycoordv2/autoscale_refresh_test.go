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

package querycoordv2

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestRefreshMissingSegmentsPerReplica(t *testing.T) {
	version := qviews.DataVersion{StreamingVersion: 3}
	view := &viewpb.DataViewOfCollection{Shards: []*viewpb.DataViewOfShard{{
		Vchannel: "v0",
		Partitions: []*viewpb.DataViewOfPartition{
			{PartitionId: 10, SegmentIds: []int64{100, 101, 102}},
			{PartitionId: 20, SegmentIds: []int64{200}},
		},
	}}}
	config := &loadmgr.LoadConfig{
		Version: 1, PartitionIDs: []int64{10},
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 11, ResourceGroup: "a"},
			{ReplicaID: 12, ResourceGroup: "a"},
			{ReplicaID: 13, ResourceGroup: "b"},
			{ReplicaID: 14, ResourceGroup: "c"},
		},
	}
	up := &qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 2}}
	stats := map[qviews.ShardID]*coordview.ShardStats{
		{ReplicaID: 11, VChannel: "v0"}: {
			UpVersion: up, UpLoadInfoVersion: 1,
			Segments: map[int64]*coordview.SegmentStats{
				100: {Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStateUp, 2: coordview.SegmentStateReady}},
				101: {Nodes: map[int64]coordview.SegmentState{1: coordview.SegmentStatePreparing, 2: coordview.SegmentStatePreparing}},
			},
		},
		{ReplicaID: 12, VChannel: "v0"}: {UpVersion: up, UpLoadInfoVersion: 1},
		{ReplicaID: 13, VChannel: "v0"}: {UpVersion: up, UpLoadInfoVersion: 1},
		{ReplicaID: 14, VChannel: "v0"}: {UpVersion: &qviews.QueryViewVersion{DataVersion: version}, UpLoadInfoVersion: 1},
	}
	previous := typeutil.NewSet(refreshSegmentReplica{segmentID: 101, replicaID: 11}, refreshSegmentReplica{segmentID: 99, replicaID: 11})

	missing, reported, ready := refreshMissingSegments(view, version, config, stats, previous)
	require.True(t, ready)
	require.Equal(t, map[int64][]*loadmgr.ReplicaAssignment{
		100: {config.Replicas[1], config.Replicas[2]},
		101: {config.Replicas[1], config.Replicas[2]},
		102: {config.Replicas[0], config.Replicas[1], config.Replicas[2]},
	}, missing)
	require.Equal(t, typeutil.NewSet(refreshSegmentReplica{segmentID: 101, replicaID: 11}), reported)

	delete(stats, qviews.ShardID{ReplicaID: 14, VChannel: "v0"})
	_, _, ready = refreshMissingSegments(view, version, config, stats, previous)
	require.False(t, ready)
}

type refreshTestDataView struct {
	qviews.DataViewRef
	view *viewpb.DataViewOfCollection
}

func (r refreshTestDataView) DataView() *viewpb.DataViewOfCollection { return r.view }
func (r refreshTestDataView) Deref()                                 {}

type refreshTestDataViewProvider struct {
	balancer.DataViewProvider
	ref refreshTestDataView
}

func (p refreshTestDataViewProvider) Get(context.Context, int64, *viewpb.DataVersion) (qviews.DataViewRef, error) {
	return p.ref, nil
}

func TestRefreshLoadDemandBeforeHandoff(t *testing.T) {
	enableAutoscalePrecheckForTest(t)
	s, up := newHandoffServer(t)
	const collectionID, segmentID = int64(1), int64(100)
	store := s.qviewsRuntime.loadConfigStore
	stats := map[qviews.ShardID]*coordview.ShardStats{
		{ReplicaID: 11, VChannel: "v0"}: {
			UpVersion: &qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 1}}, UpLoadInfoVersion: store.GetConfigVersion(collectionID),
		},
	}
	s.qviewsRuntime.resources.Reconciler = &datacoord.Server{}
	s.qviewsRuntime.resources.DataViews = refreshTestDataViewProvider{ref: refreshTestDataView{view: &viewpb.DataViewOfCollection{
		Shards: []*viewpb.DataViewOfShard{{Vchannel: "v0", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 10, SegmentIds: []int64{segmentID}}}}},
	}}}
	mockey.PatchConvey("report estimates before a new view serves", t, func() {
		mockey.Mock((*datacoord.Server).RefreshDataView).Return(&viewpb.DataVersion{StreamingVersion: 2}, nil).Build()
		mockey.Mock((*coordview.ShardViewRegistry).SnapshotForCollection).Return(coordview.NewShardViewSnapshot(1, stats)).Build()

		broker := meta.NewMockBroker(t)
		s.broker = broker
		broker.EXPECT().DescribeCollection(mock.Anything, collectionID).Return(&milvuspb.DescribeCollectionResponse{
			VirtualChannelNames: []string{"v0", "v1"},
			Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
			}},
		}, nil)
		broker.EXPECT().GetSegmentInfo(mock.Anything, segmentID).Return(nil, context.DeadlineExceeded).Once()
		broker.EXPECT().GetSegmentInfo(mock.Anything, segmentID).Return([]*datapb.SegmentInfo{{
			ID: segmentID, PartitionID: 10, NumOfRows: 1,
			Binlogs: []*datapb.FieldBinlog{{FieldID: common.RowIDField, Binlogs: []*datapb.Binlog{{MemorySize: 128}}}},
		}}, nil).Once()
		broker.EXPECT().GetIndexInfo(mock.Anything, collectionID, segmentID).Return(nil, nil).Once()

		rg := store.GetConfig(collectionID).Replicas[0].ResourceGroup
		counter := metrics.QueryCoordLoadDemandMemoryBytes.WithLabelValues(rg)
		before := testutil.ToFloat64(counter)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		failedEstimateCtx, cancelFailedEstimate := context.WithCancel(ctx)
		defer cancelFailedEstimate()
		first := make(chan error, 1)
		go func() { first <- s.refreshCollection(failedEstimateCtx, collectionID) }()
		require.Eventually(t, func() bool {
			_, waiting := s.qviewsRuntime.refreshVersions.Load(collectionID)
			return waiting
		}, time.Second, time.Millisecond)
		require.Equal(t, before, testutil.ToFloat64(counter))
		cancelFailedEstimate()
		require.ErrorIs(t, <-first, context.Canceled)

		collectionDone, partitionsDone := make(chan error, 1), make(chan error, 1)
		go func() {
			status, err := s.LoadCollection(ctx, &querypb.LoadCollectionRequest{CollectionID: collectionID, Refresh: true})
			collectionDone <- merr.CheckRPCCall(status, err)
		}()
		go func() {
			status, err := s.LoadPartitions(ctx, &querypb.LoadPartitionsRequest{CollectionID: collectionID, PartitionIDs: []int64{10}, Refresh: true})
			partitionsDone <- merr.CheckRPCCall(status, err)
		}()
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(counter) > before
		}, time.Second, time.Millisecond)
		assertReadinessBlocked(t, collectionDone)
		assertReadinessBlocked(t, partitionsDone)
		up("v0", 2)
		up("v1", 2)
		require.NoError(t, <-collectionDone)
		require.NoError(t, <-partitionsDone)
		require.Equal(t, before+128, testutil.ToFloat64(counter))
	})
}
