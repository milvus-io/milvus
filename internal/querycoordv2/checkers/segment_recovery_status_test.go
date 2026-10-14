// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package checkers

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func segmentRecoveryReplica(id int64, rg string, nodes ...int64) *meta.Replica {
	return meta.NewReplica(&querypb.Replica{ID: id, CollectionID: 1, ResourceGroup: rg, Nodes: nodes})
}

func segmentRecoveryTarget(id int64, shard string) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{ID: id, CollectionID: 1, PartitionID: 1, InsertChannel: shard, NumOfRows: 10}
}

func (suite *SegmentCheckerTestSuite) prepareSegmentRecoveryStatus(replicas []*meta.Replica, segments ...*datapb.SegmentInfo) {
	ctx := context.Background()
	suite.Require().NoError(suite.meta.PutCollection(ctx, utils.CreateTestCollection(1, int32(len(replicas)))))
	suite.Require().NoError(suite.meta.PutPartition(ctx, utils.CreateTestPartition(1, 1)))
	suite.Require().NoError(suite.meta.Put(ctx, replicas...))
	for _, replica := range replicas {
		for _, node := range replica.GetNodes() {
			suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: node}))
		}
	}
	suite.updateSegmentRecoveryTarget(segments...)
}

func (suite *SegmentCheckerTestSuite) updateSegmentRecoveryTarget(segments ...*datapb.SegmentInfo) {
	var channels []*datapb.VchannelInfo
	seen := make(map[string]struct{})
	for _, segment := range segments {
		if _, exists := seen[segment.GetInsertChannel()]; !exists {
			channels = append(channels, &datapb.VchannelInfo{CollectionID: 1, ChannelName: segment.GetInsertChannel()})
			seen[segment.GetInsertChannel()] = struct{}{}
		}
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, int64(1)).Return(channels, segments, nil).Once()
	suite.Require().NoError(suite.checker.targetMgr.UpdateCollectionNextTarget(context.Background(), 1))
}

func (suite *SegmentCheckerTestSuite) setSegmentRecoveryLeader(node int64, shards ...string) {
	channels := make([]*meta.DmChannel, 0, len(shards))
	for _, shard := range shards {
		channels = append(channels, &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: shard},
			View: &meta.LeaderView{ID: node, CollectionID: 1, Channel: shard,
				Status: &querypb.LeaderViewStatus{Serviceable: true}},
		})
	}
	suite.checker.dist.ChannelDistManager.Update(node, channels...)
}

func (suite *SegmentCheckerTestSuite) TestSegmentRecoveryStatusCurrentTargetFallbackAndVersion() {
	ctx := context.Background()
	first := segmentRecoveryTarget(1, "a")
	suite.prepareSegmentRecoveryStatus([]*meta.Replica{segmentRecoveryReplica(1, "rg", 1)}, first)
	suite.True(suite.checker.targetMgr.UpdateCollectionCurrentTarget(ctx, 1))
	suite.False(suite.checker.targetMgr.IsNextTargetExist(ctx, 1))
	suite.setSegmentRecoveryLeader(1, "a")
	var accepted []task.Task
	suite.scheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(work task.Task) error {
		accepted = append(accepted, work)
		suite.T().Cleanup(func() { work.Cancel(nil) })
		return nil
	})
	_, recovery := suite.checker.CheckWithRecovery(ctx)
	suite.Equal(map[string]struct{}{"rg": {}}, recovery)
	suite.Require().Len(accepted, 1, "Current-only gaps must produce real recovery plans")
	suite.EqualValues(1, accepted[0].(*task.SegmentTask).SegmentID())

	suite.checker.dist.SegmentDistManager.Update(1, &meta.Segment{SegmentInfo: first})
	_, recovery = suite.checker.CheckWithRecovery(ctx)
	suite.Empty(recovery)
	suite.Contains(suite.checker.versionCache, int64(1))
	previous := suite.checker.versionCache[1]
	_, recovery = suite.checker.CheckWithRecovery(ctx)
	suite.Empty(recovery)
	suite.Same(previous, suite.checker.versionCache[1], "converged idle checks retain their fast path")

	// Both before and after promotion, NextTarget has version zero. Current
	// changed without a distribution update and must invalidate convergence.
	suite.updateSegmentRecoveryTarget(first, segmentRecoveryTarget(2, "a"))
	suite.True(suite.checker.targetMgr.UpdateCollectionCurrentTarget(ctx, 1))
	_, recovery = suite.checker.CheckWithRecovery(ctx)
	suite.Equal(map[string]struct{}{"rg": {}}, recovery)
	suite.Require().Len(accepted, 2)
	suite.EqualValues(2, accepted[1].(*task.SegmentTask).SegmentID())
}

func (suite *SegmentCheckerTestSuite) TestSegmentRecoveryStatusDoesNotRestoreRemovedCurrentSegment() {
	ctx := context.Background()
	suite.prepareSegmentRecoveryStatus([]*meta.Replica{segmentRecoveryReplica(1, "rg", 1)}, segmentRecoveryTarget(1, "a"))
	suite.True(suite.checker.targetMgr.UpdateCollectionCurrentTarget(ctx, 1))
	next := segmentRecoveryTarget(2, "a")
	suite.updateSegmentRecoveryTarget(next)
	suite.checker.dist.SegmentDistManager.Update(1, &meta.Segment{SegmentInfo: next})
	_, recovery := suite.checker.CheckWithRecovery(ctx)
	suite.Empty(recovery, "an object present only in old Current must not be restored against a newer Next")
	suite.scheduler.AssertNotCalled(suite.T(), "Add", mock.Anything)
}

func (suite *SegmentCheckerTestSuite) TestSegmentRecoveryStatusSeesNewReplicaWithoutDistChange() {
	ctx := context.Background()
	segment := segmentRecoveryTarget(1, "a")
	suite.prepareSegmentRecoveryStatus([]*meta.Replica{segmentRecoveryReplica(1, "first", 1)}, segment)
	suite.checker.dist.SegmentDistManager.Update(1, &meta.Segment{SegmentInfo: segment})
	_, recovery := suite.checker.CheckWithRecovery(ctx)
	suite.Empty(recovery)
	suite.Contains(suite.checker.versionCache, int64(1))

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))
	suite.Require().NoError(suite.meta.Put(ctx, segmentRecoveryReplica(2, "second", 2)))
	for tick := 0; tick < 2; tick++ {
		_, recovery = suite.checker.CheckWithRecovery(ctx)
		suite.Equal(map[string]struct{}{"second": {}}, recovery, "no shard leader or accepted task must not hide a new replica's gap")
	}
	suite.scheduler.AssertNotCalled(suite.T(), "Add", mock.Anything)
}

func (suite *SegmentCheckerTestSuite) TestSegmentRecoveryStatusSeesReplicaNodeAndRGChange() {
	ctx := context.Background()
	segment := segmentRecoveryTarget(1, "a")
	suite.prepareSegmentRecoveryStatus([]*meta.Replica{segmentRecoveryReplica(1, "old", 1)}, segment)
	suite.checker.dist.SegmentDistManager.Update(1, &meta.Segment{SegmentInfo: segment})
	_, recovery := suite.checker.CheckWithRecovery(ctx)
	suite.Empty(recovery)
	suite.Contains(suite.checker.versionCache, int64(1))

	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: 2}))
	suite.Require().NoError(suite.meta.Put(ctx, segmentRecoveryReplica(1, "new", 2)))
	cleanup, recovery := suite.checker.CheckWithRecovery(ctx)
	for _, work := range cleanup {
		defer work.Cancel(nil)
	}
	suite.Equal(map[string]struct{}{"new": {}}, recovery, "a changed replica snapshot invalidates a cached empty recovery status")
	suite.scheduler.AssertNotCalled(suite.T(), "Add", mock.Anything)
}

func (suite *SegmentCheckerTestSuite) TestSegmentRecoveryStatusMissingLeaderDoesNotBlockOtherShard() {
	ctx := context.Background()
	suite.prepareSegmentRecoveryStatus([]*meta.Replica{segmentRecoveryReplica(1, "rg", 1)},
		segmentRecoveryTarget(1, "ready"), segmentRecoveryTarget(2, "waiting"))
	suite.setSegmentRecoveryLeader(1, "ready")
	var accepted []task.Task
	suite.scheduler.EXPECT().Add(mock.Anything).RunAndReturn(func(work task.Task) error {
		accepted = append(accepted, work)
		suite.T().Cleanup(func() { work.Cancel(nil) })
		return nil
	}).Once()
	_, recovery := suite.checker.CheckWithRecovery(ctx)
	suite.Equal(map[string]struct{}{"rg": {}}, recovery)
	suite.Require().Len(accepted, 1)
	suite.EqualValues(1, accepted[0].(*task.SegmentTask).SegmentID())

	// The first load is now in flight, but neither segment is distributed yet.
	// A repeated stage must retain the deficit even when it emits no tasks.
	suite.nodeMgr.Get(1).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{
		LoadingSegments: []*querypb.LoadingSegmentInfo{{CollectionId: 1, SegmentId: 1, NumRows: 10}},
	}, true, true, time.Now())
	_, recovery = suite.checker.CheckWithRecovery(ctx)
	suite.Equal(map[string]struct{}{"rg": {}}, recovery)
	suite.Len(accepted, 1)
}

func (suite *SegmentCheckerTestSuite) TestSegmentRecoveryStatusSurvivesFullAdmissionCap() {
	params := paramtable.Get()
	key := params.QueryCoordCfg.BalanceMaxPendingLoads.Key
	previous := params.QueryCoordCfg.BalanceMaxPendingLoads.GetValue()
	suite.Require().NoError(params.Save(key, "1"))
	defer params.Save(key, previous)
	suite.prepareSegmentRecoveryStatus([]*meta.Replica{segmentRecoveryReplica(1, "rg", 1)}, segmentRecoveryTarget(1, "a"))
	suite.setSegmentRecoveryLeader(1, "a")
	suite.nodeMgr.Get(1).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{
		LoadingSegments: []*querypb.LoadingSegmentInfo{{CollectionId: 99, SegmentId: 99, NumRows: 10}},
	}, true, true, time.Now())
	for tick := 0; tick < 2; tick++ {
		_, recovery := suite.checker.CheckWithRecovery(context.Background())
		suite.Equal(map[string]struct{}{"rg": {}}, recovery)
		suite.NotContains(suite.checker.versionCache, int64(1))
	}
	suite.scheduler.AssertNotCalled(suite.T(), "Add", mock.Anything)
}

func (suite *SegmentCheckerTestSuite) TestSegmentRecoveryStatusSurvivesRejectedAdmission() {
	suite.prepareSegmentRecoveryStatus([]*meta.Replica{segmentRecoveryReplica(1, "rg", 1)}, segmentRecoveryTarget(1, "a"))
	suite.setSegmentRecoveryLeader(1, "a")
	suite.scheduler.EXPECT().Add(mock.Anything).Return(context.Canceled).Once()
	_, recovery := suite.checker.CheckWithRecovery(context.Background())
	suite.Equal(map[string]struct{}{"rg": {}}, recovery)
	suite.NotContains(suite.checker.versionCache, int64(1))
}

func (suite *SegmentCheckerTestSuite) TestSegmentRecoveryStatusDoesNotMistakeMoveForRecovery() {
	ctx := context.Background()
	segment := segmentRecoveryTarget(1, "a")
	suite.prepareSegmentRecoveryStatus([]*meta.Replica{segmentRecoveryReplica(1, "rg", 1, 2)}, segment)
	suite.checker.dist.SegmentDistManager.Update(1, &meta.Segment{SegmentInfo: segment})
	_, recovery := suite.checker.CheckWithRecovery(ctx)
	suite.Empty(recovery)
	// A balance Grow at the destination does not create a deficit while its
	// source still supplies the target segment in the same replica.
	suite.nodeMgr.Get(2).UpdateLoadReport(&querypb.QueryNodeLoadResourceReport{
		LoadingSegments: []*querypb.LoadingSegmentInfo{{CollectionId: 1, SegmentId: 1, NumRows: 10}},
	}, true, true, time.Now())
	_, recovery = suite.checker.CheckWithRecovery(ctx)
	suite.Empty(recovery)
	suite.scheduler.AssertNotCalled(suite.T(), "Add", mock.Anything)
}
