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

package meta

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type TargetManagerSuite struct {
	suite.Suite

	// Data
	collections    []int64
	partitions     map[int64][]int64
	channels       map[int64][]string
	segments       map[int64]map[int64][]int64 // CollectionID, PartitionID -> Segments
	level0Segments []int64
	// Derived data
	allChannels []string
	allSegments []int64

	kv      kv.MetaKv
	catalog metastore.QueryCoordCatalog
	meta    *Meta
	broker  *MockBroker
	// Test object
	mgr *TargetManager
}

func (suite *TargetManagerSuite) SetupSuite() {
	paramtable.Init()
	suite.collections = []int64{1000, 1001}
	suite.partitions = map[int64][]int64{
		1000: {100, 101},
		1001: {102, 103},
	}
	suite.channels = map[int64][]string{
		1000: {"1000-dmc0", "1000-dmc1"},
		1001: {"1001-dmc0", "1001-dmc1"},
	}
	suite.segments = map[int64]map[int64][]int64{
		1000: {
			100: {1, 2},
			101: {3, 4},
		},
		1001: {
			102: {5, 6},
			103: {7, 8},
		},
	}
	suite.level0Segments = []int64{10000, 10001}

	suite.allChannels = make([]string, 0)
	suite.allSegments = make([]int64, 0)
	for _, channels := range suite.channels {
		suite.allChannels = append(suite.allChannels, channels...)
	}
	for _, partitions := range suite.segments {
		for _, segments := range partitions {
			suite.allSegments = append(suite.allSegments, segments...)
		}
	}
}

func (suite *TargetManagerSuite) SetupTest() {
	var err error
	config := GenerateEtcdConfig()
	cli, err := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	suite.Require().NoError(err)
	suite.kv = etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())

	// meta
	suite.catalog = querycoord.NewCatalog(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.meta = NewMeta(idAllocator, suite.catalog, session.NewNodeManager())
	suite.broker = NewMockBroker(suite.T())
	suite.mgr = NewTargetManager(suite.broker, suite.meta)

	for _, collection := range suite.collections {
		dmChannels := make([]*datapb.VchannelInfo, 0)
		for _, channel := range suite.channels[collection] {
			dmChannels = append(dmChannels, &datapb.VchannelInfo{
				CollectionID:        collection,
				ChannelName:         channel,
				LevelZeroSegmentIds: suite.level0Segments,
			})
		}

		allSegments := make([]*datapb.SegmentInfo, 0)
		for partitionID, segments := range suite.segments[collection] {
			for _, segment := range segments {
				allSegments = append(allSegments, &datapb.SegmentInfo{
					ID:            segment,
					InsertChannel: suite.channels[collection][0],
					CollectionID:  collection,
					PartitionID:   partitionID,
				})
			}
		}
		suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collection).Return(dmChannels, allSegments, nil)

		suite.meta.PutCollection(&Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  collection,
				ReplicaNumber: 1,
			},
		})
		for _, partition := range suite.partitions[collection] {
			suite.meta.PutPartition(&Partition{
				PartitionLoadInfo: &querypb.PartitionLoadInfo{
					CollectionID: collection,
					PartitionID:  partition,
				},
			})
		}

		suite.mgr.UpdateCollectionNextTarget(collection)
	}
}

func (suite *TargetManagerSuite) TearDownSuite() {
	suite.kv.Close()
}

func (suite *TargetManagerSuite) TestUpdateCurrentTarget() {
	collectionID := int64(1000)
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]),
		suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))

	suite.mgr.UpdateCollectionCurrentTarget(collectionID)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]),
		suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))
}

func (suite *TargetManagerSuite) TestUpdateNextTarget() {
	collectionID := int64(1003)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))

	suite.meta.PutCollection(&Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})
	suite.meta.PutPartition(&Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: collectionID,
			PartitionID:  1,
		},
	})

	nextTargetChannels := []*datapb.VchannelInfo{
		{
			CollectionID: collectionID,
			ChannelName:  "channel-1",
		},
		{
			CollectionID: collectionID,
			ChannelName:  "channel-2",
		},
	}

	nextTargetSegments := []*datapb.SegmentInfo{
		{
			ID:            11,
			PartitionID:   1,
			InsertChannel: "channel-1",
		},
		{
			ID:            12,
			PartitionID:   1,
			InsertChannel: "channel-2",
		},
	}

	nextTargetBinlogs := []*datapb.SegmentBinlogs{
		{
			SegmentID:     11,
			InsertChannel: "channel-1",
		},
		{
			SegmentID:     12,
			InsertChannel: "channel-2",
		},
	}

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nextTargetChannels, nextTargetSegments, nil)
	suite.mgr.UpdateCollectionNextTarget(collectionID)
	suite.assertSegments([]int64{11, 12}, suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels([]string{"channel-1", "channel-2"}, suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))

	suite.broker.ExpectedCalls = nil
	// test getRecoveryInfoV2 failed , then back to getRecoveryInfo succeed
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(
		nil, nil, merr.WrapErrServiceUnimplemented(status.Errorf(codes.Unimplemented, "fake not found")))
	suite.broker.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return([]int64{1}, nil)
	suite.broker.EXPECT().GetRecoveryInfo(mock.Anything, collectionID, int64(1)).Return(nextTargetChannels, nextTargetBinlogs, nil)
	err := suite.mgr.UpdateCollectionNextTarget(collectionID)
	suite.NoError(err)

	suite.broker.ExpectedCalls = nil
	// test getRecoveryInfoV2 failed , then retry getRecoveryInfoV2 succeed
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nil, nil, errors.New("fake error")).Times(1)
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nextTargetChannels, nextTargetSegments, nil)
	err = suite.mgr.UpdateCollectionNextTarget(collectionID)
	suite.NoError(err)

	err = suite.mgr.UpdateCollectionNextTarget(collectionID)
	suite.NoError(err)
}

func (suite *TargetManagerSuite) TestRemovePartition() {
	collectionID := int64(1000)
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))

	suite.mgr.RemovePartition(collectionID, 100)
	suite.assertSegments(append([]int64{3, 4}, suite.level0Segments...), suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))
}

func (suite *TargetManagerSuite) TestRemoveCollection() {
	collectionID := int64(1000)
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))

	suite.mgr.RemoveCollection(collectionID)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))

	collectionID = int64(1001)
	suite.mgr.UpdateCollectionCurrentTarget(collectionID)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))

	suite.mgr.RemoveCollection(collectionID)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))
}

func (suite *TargetManagerSuite) getAllSegment(collectionID int64, partitionIDs []int64) []int64 {
	allSegments := make([]int64, 0)
	for collection, partitions := range suite.segments {
		if collectionID == collection {
			for partition, segments := range partitions {
				if lo.Contains(partitionIDs, partition) {
					allSegments = append(allSegments, segments...)
				}
			}
		}
	}

	return append(allSegments, suite.level0Segments...)
}

func (suite *TargetManagerSuite) assertChannels(expected []string, actual map[string]*DmChannel) bool {
	if !suite.Equal(len(expected), len(actual)) {
		return false
	}

	set := typeutil.NewSet(expected...)
	for _, channel := range actual {
		set.Remove(channel.ChannelName)
	}

	return suite.Len(set, 0)
}

func (suite *TargetManagerSuite) assertSegments(expected []int64, actual map[int64]*datapb.SegmentInfo) bool {
	if !suite.Equal(len(expected), len(actual)) {
		return false
	}

	set := typeutil.NewUniqueSet(expected...)
	for _, segment := range actual {
		set.Remove(segment.ID)
	}

	return suite.Len(set, 0)
}

func (suite *TargetManagerSuite) TestGetCollectionTargetVersion() {
	t1 := time.Now().UnixNano()
	target := NewCollectionTarget(nil, nil, nil)
	t2 := time.Now().UnixNano()

	version := target.GetTargetVersion()
	suite.True(t1 <= version)
	suite.True(t2 >= version)

	collectionID := suite.collections[0]
	t3 := time.Now().UnixNano()
	suite.mgr.UpdateCollectionNextTarget(collectionID)
	t4 := time.Now().UnixNano()

	collectionVersion := suite.mgr.GetCollectionTargetVersion(collectionID, NextTarget)
	suite.True(t3 <= collectionVersion)
	suite.True(t4 >= collectionVersion)
}

func (suite *TargetManagerSuite) TestGetSegmentByChannel() {
	collectionID := int64(1003)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))

	suite.meta.PutCollection(&Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})
	suite.meta.PutPartition(&Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: collectionID,
			PartitionID:  1,
		},
	})

	nextTargetChannels := []*datapb.VchannelInfo{
		{
			CollectionID:        collectionID,
			ChannelName:         "channel-1",
			UnflushedSegmentIds: []int64{1, 2, 3, 4},
			DroppedSegmentIds:   []int64{11, 22, 33},
		},
		{
			CollectionID:        collectionID,
			ChannelName:         "channel-2",
			UnflushedSegmentIds: []int64{5},
		},
	}

	nextTargetSegments := []*datapb.SegmentInfo{
		{
			ID:            11,
			PartitionID:   1,
			InsertChannel: "channel-1",
		},
		{
			ID:            12,
			PartitionID:   1,
			InsertChannel: "channel-2",
		},
	}

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nextTargetChannels, nextTargetSegments, nil)
	suite.mgr.UpdateCollectionNextTarget(collectionID)
	suite.Len(suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget), 2)
	suite.Len(suite.mgr.GetSealedSegmentsByChannel(collectionID, "channel-1", NextTarget), 1)
	suite.Len(suite.mgr.GetSealedSegmentsByChannel(collectionID, "channel-2", NextTarget), 1)
	suite.Len(suite.mgr.GetGrowingSegmentsByChannel(collectionID, "channel-1", NextTarget), 4)
	suite.Len(suite.mgr.GetGrowingSegmentsByChannel(collectionID, "channel-2", NextTarget), 1)
	suite.Len(suite.mgr.GetDroppedSegmentsByChannel(collectionID, "channel-1", NextTarget), 3)
	suite.Len(suite.mgr.GetGrowingSegmentsByCollection(collectionID, NextTarget), 5)
	suite.Len(suite.mgr.GetSealedSegmentsByPartition(collectionID, 1, NextTarget), 2)
	suite.NotNil(suite.mgr.GetSealedSegment(collectionID, 11, NextTarget))
	suite.NotNil(suite.mgr.GetDmChannel(collectionID, "channel-1", NextTarget))
}

func (suite *TargetManagerSuite) TestGetTarget() {
	type testCase struct {
		tag          string
		mgr          *TargetManager
		scope        TargetScope
		expectTarget int
	}

	current := &CollectionTarget{}
	next := &CollectionTarget{}

	bothMgr := &TargetManager{
		current: &target{
			collectionTargetMap: map[int64]*CollectionTarget{
				1000: current,
			},
		},
		next: &target{
			collectionTargetMap: map[int64]*CollectionTarget{
				1000: next,
			},
		},
	}
	currentMgr := &TargetManager{
		current: &target{
			collectionTargetMap: map[int64]*CollectionTarget{
				1000: current,
			},
		},
		next: &target{},
	}
	nextMgr := &TargetManager{
		next: &target{
			collectionTargetMap: map[int64]*CollectionTarget{
				1000: current,
			},
		},
		current: &target{},
	}

	cases := []testCase{
		{
			tag:   "both_scope_unknown",
			mgr:   bothMgr,
			scope: -1,

			expectTarget: 0,
		},
		{
			tag:          "both_scope_current",
			mgr:          bothMgr,
			scope:        CurrentTarget,
			expectTarget: 1,
		},
		{
			tag:          "both_scope_next",
			mgr:          bothMgr,
			scope:        NextTarget,
			expectTarget: 1,
		},
		{
			tag:          "both_scope_current_first",
			mgr:          bothMgr,
			scope:        CurrentTargetFirst,
			expectTarget: 2,
		},
		{
			tag:          "both_scope_next_first",
			mgr:          bothMgr,
			scope:        NextTargetFirst,
			expectTarget: 2,
		},
		{
			tag:          "next_scope_current",
			mgr:          nextMgr,
			scope:        CurrentTarget,
			expectTarget: 0,
		},
		{
			tag:          "next_scope_next",
			mgr:          nextMgr,
			scope:        NextTarget,
			expectTarget: 1,
		},
		{
			tag:          "next_scope_current_first",
			mgr:          nextMgr,
			scope:        CurrentTargetFirst,
			expectTarget: 1,
		},
		{
			tag:          "next_scope_next_first",
			mgr:          nextMgr,
			scope:        NextTargetFirst,
			expectTarget: 1,
		},
		{
			tag:          "current_scope_current",
			mgr:          currentMgr,
			scope:        CurrentTarget,
			expectTarget: 1,
		},
		{
			tag:          "current_scope_next",
			mgr:          currentMgr,
			scope:        NextTarget,
			expectTarget: 0,
		},
		{
			tag:          "current_scope_current_first",
			mgr:          currentMgr,
			scope:        CurrentTargetFirst,
			expectTarget: 1,
		},
		{
			tag:          "current_scope_next_first",
			mgr:          currentMgr,
			scope:        NextTargetFirst,
			expectTarget: 1,
		},
	}

	for _, tc := range cases {
		suite.Run(tc.tag, func() {
			targets := tc.mgr.getCollectionTarget(tc.scope, 1000)
			suite.Equal(tc.expectTarget, len(targets))
		})
	}
}

func (suite *TargetManagerSuite) TestRecover() {
	collectionID := int64(1003)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(collectionID, CurrentTarget))

	suite.meta.PutCollection(&Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})
	suite.meta.PutPartition(&Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: collectionID,
			PartitionID:  1,
		},
	})

	nextTargetChannels := []*datapb.VchannelInfo{
		{
			CollectionID:        collectionID,
			ChannelName:         "channel-1",
			UnflushedSegmentIds: []int64{1, 2, 3, 4},
			DroppedSegmentIds:   []int64{11, 22, 33},
		},
		{
			CollectionID:        collectionID,
			ChannelName:         "channel-2",
			UnflushedSegmentIds: []int64{5},
		},
	}

	nextTargetSegments := []*datapb.SegmentInfo{
		{
			ID:            11,
			PartitionID:   1,
			InsertChannel: "channel-1",
		},
		{
			ID:            12,
			PartitionID:   1,
			InsertChannel: "channel-2",
		},
	}

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nextTargetChannels, nextTargetSegments, nil)
	suite.mgr.UpdateCollectionNextTarget(collectionID)
	suite.mgr.UpdateCollectionCurrentTarget(collectionID)

	suite.mgr.SaveCurrentTarget(suite.catalog)

	// clear target in memory
	version := suite.mgr.current.getCollectionTarget(collectionID).GetTargetVersion()
	suite.mgr.current.removeCollectionTarget(collectionID)
	// try to recover
	suite.mgr.Recover(suite.catalog)

	target := suite.mgr.current.getCollectionTarget(collectionID)
	suite.NotNil(target)
	suite.Len(target.GetAllDmChannelNames(), 2)
	suite.Len(target.GetAllSegmentIDs(), 2)
	suite.Equal(target.GetTargetVersion(), version)

	// after recover, target info should be cleaned up
	targets, err := suite.catalog.GetCollectionTargets()
	suite.NoError(err)
	suite.Len(targets, 0)
}

func TestTargetManager(t *testing.T) {
	suite.Run(t, new(TargetManagerSuite))
}
