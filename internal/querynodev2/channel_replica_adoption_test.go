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

package querynodev2

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/cluster"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// QueryCoord can adopt an existing subscription when its node moves between
// replicas. Exercise the QueryNode side with the subsequent routing/target syncs;
// only the worker RPC boundary is mocked, not the delegator or sync handlers.
func (suite *ServiceSuite) TestWatchDmChannelsReplicaAdoptionConvergesWorkerRoute() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const replicaA, replicaB = int64(10), int64(20)
	const workerA, workerB = int64(101), int64(202)

	workers := cluster.NewMockManager(suite.T())
	a := cluster.NewMockWorker(suite.T())
	b := cluster.NewMockWorker(suite.T())
	workers.EXPECT().GetWorker(mock.Anything, workerA).Return(a, nil)
	workers.EXPECT().GetWorker(mock.Anything, workerB).Return(b, nil)
	a.EXPECT().LoadSegments(mock.Anything, mock.MatchedBy(func(req *querypb.LoadSegmentsRequest) bool {
		return req.GetReplicaID() == replicaA && req.GetDstNodeID() == workerA
	})).Return(nil).Once()
	b.EXPECT().LoadSegments(mock.Anything, mock.MatchedBy(func(req *querypb.LoadSegmentsRequest) bool {
		return req.GetReplicaID() == replicaB && req.GetDstNodeID() == workerB
	})).Return(nil).Once()
	// Real bloom-filter and delete replay remains on the delegator; the remote
	// workers acknowledge any matching deletes forwarded during the load.
	a.EXPECT().Delete(mock.Anything, mock.Anything).Return(nil).Maybe()
	b.EXPECT().Delete(mock.Anything, mock.Anything).Return(nil).Maybe()
	suite.node.clusterManager = workers

	watch := suite.newInt64WatchDmChannelsRequest()
	watch.ReplicaID, watch.Version = replicaA, 100
	status, err := suite.node.WatchDmChannels(ctx, watch)
	suite.Require().NoError(merr.CheckRPCCall(status, err))
	existing, ok := suite.node.delegators.Get(suite.vchannel)
	suite.Require().True(ok)
	pipeline := suite.node.pipelineManager.Get(suite.vchannel)
	suite.Require().NotNil(pipeline)

	growing, err := segments.NewSegment(ctx, suite.node.manager.Collection.Get(suite.collectionID),
		suite.node.manager.Segment, segments.SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{
			SegmentID:     suite.validSegmentIDs[1],
			CollectionID:  suite.collectionID,
			PartitionID:   suite.partitionIDs[0],
			InsertChannel: suite.vchannel,
			Level:         datapb.SegmentLevel_Legacy,
		})
	suite.Require().NoError(err)
	suite.node.manager.Segment.Put(ctx, segments.SegmentTypeGrowing, growing)
	info := suite.genSegmentLoadInfos(watch.GetSchema(), watch.GetIndexInfoList())[0]

	sync := func(replicaID int64, partitions []int64, action *querypb.SyncAction) {
		loadMeta := typeutil.Clone(watch.GetLoadMeta())
		loadMeta.PartitionIDs = partitions
		status, err := suite.node.SyncDistribution(ctx, &querypb.SyncDistributionRequest{
			Base:          &commonpb.MsgBase{TargetID: suite.node.session.ServerID},
			CollectionID:  suite.collectionID,
			ReplicaID:     replicaID,
			Channel:       suite.vchannel,
			Schema:        watch.GetSchema(),
			LoadMeta:      loadMeta,
			IndexInfoList: watch.GetIndexInfoList(),
			Actions:       []*querypb.SyncAction{action},
		})
		suite.Require().NoError(merr.CheckRPCCall(status, err))
	}
	setRoute := func(replicaID, workerID, version int64) {
		sync(replicaID, suite.partitionIDs, &querypb.SyncAction{
			Type: querypb.SyncType_Set, SegmentID: info.GetSegmentID(),
			NodeID: workerID, Version: version, Info: info,
		})
		suite.Require().Eventually(func() bool {
			sealed, _ := existing.GetSegmentInfo(false)
			for _, snapshot := range sealed {
				for _, entry := range snapshot.Segments {
					if entry.SegmentID == info.GetSegmentID() {
						return snapshot.NodeID == workerID && entry.NodeID == workerID
					}
				}
			}
			return false
		}, time.Second, time.Millisecond)
	}
	setTarget := func(replicaID, version int64, partitions []int64) {
		sync(replicaID, partitions, &querypb.SyncAction{
			Type: querypb.SyncType_UpdateVersion, TargetVersion: version,
			SealedInTarget: []int64{info.GetSegmentID()},
			SealedSegmentRowCount: map[int64]int64{
				info.GetSegmentID(): info.GetNumOfRows(),
			},
		})
		suite.Equal(version, existing.GetChannelQueryView().GetVersion())
		suite.Require().True(existing.GetChannelQueryView().Serviceable())
	}
	setRoute(replicaA, workerA, 10)
	setTarget(replicaA, 10, suite.partitionIDs)
	viewA := existing.GetChannelQueryView()

	// A newer Watch for B retains the current serving state. The routing table
	// changes through SyncDistribution, not through the replica ID in Watch.
	adopt := typeutil.Clone(watch)
	adopt.ReplicaID, adopt.Version = replicaB, 200
	status, err = suite.node.WatchDmChannels(ctx, adopt)
	suite.Require().NoError(merr.CheckRPCCall(status, err))
	current, ok := suite.node.delegators.Get(suite.vchannel)
	suite.Require().True(ok)
	suite.Same(existing, current)
	suite.Equal(int64(200), current.Version())
	suite.Same(viewA, current.GetChannelQueryView())
	suite.Same(pipeline, suite.node.pipelineManager.Get(suite.vchannel))
	suite.Same(growing, suite.node.manager.Segment.GetGrowing(growing.ID()))

	setRoute(replicaB, workerB, 20)
	partitionsB := append(append([]int64(nil), suite.partitionIDs...), suite.partitionIDs[0]+1)
	setTarget(replicaB, 20, partitionsB)
	existing.UpdateTSafe(1000)
	b.EXPECT().QuerySegments(mock.Anything, mock.MatchedBy(func(req *querypb.QueryRequest) bool {
		return req.GetReq().GetBase().GetTargetID() == workerB &&
			len(req.GetSegmentIDs()) == 1 && req.GetSegmentIDs()[0] == info.GetSegmentID()
	})).Return(&internalpb.RetrieveResults{Status: merr.Success()}, nil).Once()
	query := func(partitionID int64) ([]*internalpb.RetrieveResults, error) {
		return existing.Query(ctx, &querypb.QueryRequest{
			DmlChannels: []string{suite.vchannel},
			Req: &internalpb.RetrieveRequest{
				Base: &commonpb.MsgBase{}, CollectionID: suite.collectionID,
				PartitionIDs: []int64{partitionID}, IgnoreGrowing: true,
				ConsistencyLevel: commonpb.ConsistencyLevel_Eventually, GuaranteeTimestamp: 1,
			},
		})
	}
	results, err := query(suite.partitionIDs[0])
	suite.Require().NoError(err)
	suite.Len(results, 1)
	// The new partition is accepted by the synchronized query view, even though
	// this fixture has no segments in it. No old worker should receive a query.
	results, err = query(partitionsB[1])
	suite.Require().NoError(err)
	suite.Empty(results)
	a.AssertNotCalled(suite.T(), "QuerySegments", mock.Anything, mock.Anything)
}
