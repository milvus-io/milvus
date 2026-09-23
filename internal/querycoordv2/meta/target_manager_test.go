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
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

	ctx context.Context
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
	suite.ctx = context.Background()

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

		suite.meta.PutCollection(suite.ctx, &Collection{
			CollectionLoadInfo: &querypb.CollectionLoadInfo{
				CollectionID:  collection,
				ReplicaNumber: 1,
			},
		})
		for _, partition := range suite.partitions[collection] {
			suite.meta.PutPartition(suite.ctx, &Partition{
				PartitionLoadInfo: &querypb.PartitionLoadInfo{
					CollectionID: collection,
					PartitionID:  partition,
				},
			})
		}

		suite.mgr.UpdateCollectionNextTarget(suite.ctx, collection)
	}
}

func (suite *TargetManagerSuite) TearDownSuite() {
	suite.kv.Close()
}

func (suite *TargetManagerSuite) TestUpdateCurrentTarget() {
	ctx := suite.ctx
	collectionID := int64(1000)
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]),
		suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))

	suite.mgr.UpdateCollectionCurrentTarget(ctx, collectionID)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]),
		suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))
}

func (suite *TargetManagerSuite) TestUpdateNextTarget() {
	ctx := suite.ctx
	collectionID := int64(1003)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))

	suite.meta.PutCollection(ctx, &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})
	suite.meta.PutPartition(ctx, &Partition{
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

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nextTargetChannels, nextTargetSegments, nil)
	suite.mgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.assertSegments([]int64{11, 12}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels([]string{"channel-1", "channel-2"}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))

	suite.broker.ExpectedCalls = nil
	// test getRecoveryInfoV2 failed , then retry getRecoveryInfoV2 succeed
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nil, nil, errors.New("fake error")).Times(1)
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nextTargetChannels, nextTargetSegments, nil)
	err := suite.mgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.NoError(err)
}

func (suite *TargetManagerSuite) TestRemovePartition() {
	ctx := suite.ctx
	collectionID := int64(1000)
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))

	suite.mgr.RemovePartition(ctx, collectionID, 100)
	suite.assertSegments([]int64{3, 4}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))
}

func (suite *TargetManagerSuite) TestRemovePartitionFromNextTarget() {
	ctx := suite.ctx
	collectionID := int64(1000)
	ret := suite.mgr.UpdateCollectionCurrentTarget(ctx, collectionID)
	suite.True(ret)

	err := suite.mgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.NoError(err)

	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))

	suite.mgr.RemovePartitionFromNextTarget(ctx, collectionID, 100)
	suite.assertSegments([]int64{3, 4}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))
}

func (suite *TargetManagerSuite) TestRemoveCollection() {
	ctx := suite.ctx
	collectionID := int64(1000)
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))

	suite.mgr.RemoveCollection(ctx, collectionID)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))

	collectionID = int64(1001)
	suite.mgr.UpdateCollectionCurrentTarget(ctx, collectionID)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments(suite.getAllSegment(collectionID, suite.partitions[collectionID]), suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels(suite.channels[collectionID], suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))

	suite.mgr.RemoveCollection(ctx, collectionID)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))
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

	return allSegments
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
	ctx := suite.ctx
	t1 := time.Now().UnixNano()
	target := NewCollectionTarget(nil, nil, nil)
	t2 := time.Now().UnixNano()

	version := target.GetTargetVersion()
	suite.True(t1 <= version)
	suite.True(t2 >= version)

	collectionID := suite.collections[0]
	t3 := time.Now().UnixNano()
	suite.mgr.UpdateCollectionNextTarget(ctx, collectionID)
	t4 := time.Now().UnixNano()

	collectionVersion := suite.mgr.GetCollectionTargetVersion(ctx, collectionID, NextTarget)
	suite.True(t3 <= collectionVersion)
	suite.True(t4 >= collectionVersion)
}

func (suite *TargetManagerSuite) TestGetSegmentByChannel() {
	ctx := suite.ctx
	collectionID := int64(1003)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))

	suite.meta.PutCollection(ctx, &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})
	suite.meta.PutPartition(ctx, &Partition{
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
	suite.mgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.Len(suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget), 2)
	suite.Len(suite.mgr.GetSealedSegmentsByChannel(ctx, collectionID, "channel-1", NextTarget), 1)
	suite.Len(suite.mgr.GetSealedSegmentsByChannel(ctx, collectionID, "channel-2", NextTarget), 1)
	suite.Len(suite.mgr.GetGrowingSegmentsByChannel(ctx, collectionID, "channel-1", NextTarget), 4)
	suite.Len(suite.mgr.GetGrowingSegmentsByChannel(ctx, collectionID, "channel-2", NextTarget), 1)
	suite.Len(suite.mgr.GetDroppedSegmentsByChannel(ctx, collectionID, "channel-1", NextTarget), 3)
	suite.Len(suite.mgr.GetGrowingSegmentsByCollection(ctx, collectionID, NextTarget), 5)
	suite.Len(suite.mgr.GetSealedSegmentsByPartition(ctx, collectionID, 1, NextTarget), 2)
	suite.NotNil(suite.mgr.GetSealedSegment(ctx, collectionID, 11, NextTarget))
	suite.NotNil(suite.mgr.GetDmChannel(ctx, collectionID, "channel-1", NextTarget))
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

	t1 := typeutil.NewConcurrentMap[int64, *CollectionTarget]()
	t2 := typeutil.NewConcurrentMap[int64, *CollectionTarget]()
	t3 := typeutil.NewConcurrentMap[int64, *CollectionTarget]()
	t4 := typeutil.NewConcurrentMap[int64, *CollectionTarget]()
	t1.Insert(1000, current)
	t2.Insert(1000, next)
	t3.Insert(1000, current)
	t4.Insert(1000, current)

	bothMgr := &TargetManager{
		current: &target{
			collectionTargetMap: t1,
		},
		next: &target{
			collectionTargetMap: t2,
		},
	}
	currentMgr := &TargetManager{
		current: &target{
			collectionTargetMap: t3,
		},
		next: &target{
			collectionTargetMap: typeutil.NewConcurrentMap[int64, *CollectionTarget](),
		},
	}
	nextMgr := &TargetManager{
		next: &target{
			collectionTargetMap: t4,
		},
		current: &target{
			collectionTargetMap: typeutil.NewConcurrentMap[int64, *CollectionTarget](),
		},
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
	ctx := suite.ctx
	collectionID := int64(1003)
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, NextTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
	suite.assertSegments([]int64{}, suite.mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget))
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget))

	suite.meta.PutCollection(ctx, &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})
	suite.meta.PutPartition(ctx, &Partition{
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
			NumOfRows:     100,
		},
		{
			ID:            12,
			PartitionID:   1,
			InsertChannel: "channel-2",
			NumOfRows:     100,
		},
	}

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(nextTargetChannels, nextTargetSegments, nil)
	suite.mgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.mgr.UpdateCollectionCurrentTarget(ctx, collectionID)

	suite.mgr.SaveCurrentTarget(ctx, suite.catalog)

	// clear target in memory
	version := suite.mgr.current.getCollectionTarget(collectionID).GetTargetVersion()
	suite.mgr.current.removeCollectionTarget(collectionID)
	// try to recover
	suite.mgr.Recover(ctx, suite.catalog)

	target := suite.mgr.current.getCollectionTarget(collectionID)
	suite.NotNil(target)
	suite.Len(target.GetAllDmChannelNames(), 2)
	suite.Len(target.GetAllSegmentIDs(), 2)
	suite.Equal(target.GetTargetVersion(), version)
	for _, segment := range target.GetAllSegments() {
		suite.Equal(int64(100), segment.GetNumOfRows())
	}
	suite.True(target.Ready())
	suite.Equal(int64(200), target.GetRowCount())

	// after recover, target info should be cleaned up
	targets, err := suite.catalog.GetCollectionTargets(ctx)
	suite.NoError(err)
	suite.Len(targets, 0)
}

func TestRecoverGetTargetsFail(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	broker := NewMockBroker(t)
	meta := NewMeta(RandomIncrementIDAllocator(), nil, session.NewNodeManager())
	mgr := NewTargetManager(broker, meta)

	mockCatalog := catalogmocks.NewQueryCoordCatalog(t)
	mockCatalog.EXPECT().GetCollectionTargets(mock.Anything).Return(nil, errors.New("mock error"))

	err := mgr.Recover(ctx, mockCatalog)
	assert.Error(t, err)
}

func TestRecoverRemoveTargetsFail(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	broker := NewMockBroker(t)
	meta := NewMeta(RandomIncrementIDAllocator(), nil, session.NewNodeManager())
	mgr := NewTargetManager(broker, meta)

	collectionID := int64(2001)
	mockCatalog := catalogmocks.NewQueryCoordCatalog(t)
	mockCatalog.EXPECT().GetCollectionTargets(mock.Anything).Return(map[int64]*querypb.CollectionTarget{
		collectionID: {
			CollectionID: collectionID,
			Version:      1,
			ChannelTargets: []*querypb.ChannelTarget{
				{
					ChannelName: "channel-1",
				},
			},
		},
	}, nil)
	mockCatalog.EXPECT().RemoveCollectionTargets(mock.Anything).Return(errors.New("mock error"))

	// Recover should succeed even if RemoveCollectionTargets fails (it only logs a warning)
	err := mgr.Recover(ctx, mockCatalog)
	assert.NoError(t, err)

	// Target should still be recovered in memory
	target := mgr.current.getCollectionTarget(collectionID)
	assert.NotNil(t, target)
	assert.Len(t, target.GetAllDmChannelNames(), 1)
}

func TestRecoverEmptyTargets(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	broker := NewMockBroker(t)
	meta := NewMeta(RandomIncrementIDAllocator(), nil, session.NewNodeManager())
	mgr := NewTargetManager(broker, meta)

	mockCatalog := catalogmocks.NewQueryCoordCatalog(t)
	mockCatalog.EXPECT().GetCollectionTargets(mock.Anything).Return(map[int64]*querypb.CollectionTarget{}, nil)
	// RemoveCollectionTargets should NOT be called when targets are empty

	err := mgr.Recover(ctx, mockCatalog)
	assert.NoError(t, err)
	mockCatalog.AssertNotCalled(t, "RemoveCollectionTargets", mock.Anything)
}

func (suite *TargetManagerSuite) TestGetTargetJSON() {
	ctx := suite.ctx
	collectionID := int64(1003)
	suite.meta.PutCollection(ctx, &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})
	suite.meta.PutPartition(ctx, &Partition{
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
	suite.NoError(suite.mgr.UpdateCollectionNextTarget(ctx, collectionID))
	suite.True(suite.mgr.UpdateCollectionCurrentTarget(ctx, collectionID))

	jsonStr := suite.mgr.GetTargetJSON(ctx, CurrentTarget, 0)
	assert.NotEmpty(suite.T(), jsonStr)

	var currentTarget []*metricsinfo.QueryCoordTarget
	err := json.Unmarshal([]byte(jsonStr), &currentTarget)
	suite.NoError(err)
	assert.Len(suite.T(), currentTarget, 1)
	assert.Equal(suite.T(), collectionID, currentTarget[0].CollectionID)
	assert.Len(suite.T(), currentTarget[0].DMChannels, 2)
	assert.Len(suite.T(), currentTarget[0].Segments, 2)

	jsonStr = suite.mgr.GetTargetJSON(ctx, CurrentTarget, 1)
	assert.NotEmpty(suite.T(), jsonStr)

	var currentTarget2 []*metricsinfo.QueryCoordTarget
	err = json.Unmarshal([]byte(jsonStr), &currentTarget)
	suite.NoError(err)
	assert.Len(suite.T(), currentTarget2, 0)
}

func (suite *TargetManagerSuite) TestUpdateNextTarget_DroppedSentinelRejected() {
	ctx := suite.ctx
	collectionID := int64(2001)

	suite.meta.PutCollection(ctx, &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})
	suite.meta.PutPartition(ctx, &Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: collectionID,
			PartitionID:  1,
		},
	})

	sentinelChannels := []*datapb.VchannelInfo{
		{
			CollectionID: collectionID,
			ChannelName:  "sentinel-channel",
			SeekPosition: &msgpb.MsgPosition{
				ChannelName: "sentinel-channel",
				Timestamp:   funcutil.DroppedChannelCheckpointTimestamp,
			},
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).
		Return(sentinelChannels, nil, nil).Once()

	err := suite.mgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.Require().Error(err)
	suite.True(errors.Is(err, merr.ErrChannelDroppedSentinel),
		"error should wrap ErrChannelDroppedSentinel, got: %v", err)
	suite.Contains(err.Error(), "sentinel-channel")

	// Next target must NOT be advanced.
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
}

func (suite *TargetManagerSuite) TestUpdateNextTarget_SentinelAmongMultiple() {
	ctx := suite.ctx
	collectionID := int64(2002)

	suite.meta.PutCollection(ctx, &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})
	suite.meta.PutPartition(ctx, &Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: collectionID,
			PartitionID:  1,
		},
	})

	mixedChannels := []*datapb.VchannelInfo{
		{
			CollectionID: collectionID,
			ChannelName:  "normal-channel",
			SeekPosition: &msgpb.MsgPosition{
				ChannelName: "normal-channel",
				Timestamp:   450000000000000000,
			},
		},
		{
			CollectionID: collectionID,
			ChannelName:  "poisoned-channel",
			SeekPosition: &msgpb.MsgPosition{
				ChannelName: "poisoned-channel",
				Timestamp:   funcutil.DroppedChannelCheckpointTimestamp,
			},
		},
	}
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).
		Return(mixedChannels, nil, nil).Once()

	err := suite.mgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.Require().Error(err)
	suite.True(errors.Is(err, merr.ErrChannelDroppedSentinel))
	suite.Contains(err.Error(), "poisoned-channel")

	// Next target must remain empty — no partial build.
	suite.assertChannels([]string{}, suite.mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget))
}

func (suite *TargetManagerSuite) TestUpdateNextTarget_SentinelDoesNotBurnRetries() {
	ctx := suite.ctx
	collectionID := int64(2003)

	suite.meta.PutCollection(ctx, &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  collectionID,
			ReplicaNumber: 1,
		},
	})
	suite.meta.PutPartition(ctx, &Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: collectionID,
			PartitionID:  1,
		},
	})

	sentinelChannels := []*datapb.VchannelInfo{
		{
			CollectionID: collectionID,
			ChannelName:  "sentinel-once",
			SeekPosition: &msgpb.MsgPosition{
				ChannelName: "sentinel-once",
				Timestamp:   funcutil.DroppedChannelCheckpointTimestamp,
			},
		},
	}

	// .Once() asserts that GetRecoveryInfoV2 is called exactly once: the
	// sentinel must not re-enter the retry.Handle loop.
	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).
		Return(sentinelChannels, nil, nil).Once()

	err := suite.mgr.UpdateCollectionNextTarget(ctx, collectionID)
	suite.Require().Error(err)
	suite.broker.AssertExpectations(suite.T())
}

func BenchmarkTargetManager(b *testing.B) {
	paramtable.Init()
	config := GenerateEtcdConfig()
	cli, _ := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())

	kv := etcdkv.NewEtcdKV(cli, config.MetaRootPath.GetValue())

	catalog := querycoord.NewCatalog(kv)
	idAllocator := RandomIncrementIDAllocator()
	meta := NewMeta(idAllocator, catalog, session.NewNodeManager())
	mgr := NewTargetManager(nil, meta)

	segmentNum := 1000
	segments := make(map[int64]*datapb.SegmentInfo)
	for i := 0; i < segmentNum; i++ {
		segments[int64(i)] = &datapb.SegmentInfo{
			ID:            int64(i),
			InsertChannel: "channel-1",
		}
	}

	channels := map[string]*DmChannel{
		"channel-1": {
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID: int64(1),
				ChannelName:  "channel-1",
			},
		},
	}

	collectionNum := 10000
	for i := 0; i < collectionNum; i++ {
		mgr.current.collectionTargetMap.Insert(int64(i), NewCollectionTarget(segments, channels, nil))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.SaveCurrentTarget(context.TODO(), catalog)
	}
}

func TestTargetManager(t *testing.T) {
	suite.Run(t, new(TargetManagerSuite))
}

// TestNewTargetManagerWithSplitState_RefusesNilCache pins that the split-aware
// constructor never builds a target manager silently missing the window marks.
func (suite *TargetManagerSuite) TestNewTargetManagerWithSplitState_RefusesNilCache() {
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		NewTargetManagerWithSplitState(NewMockBroker(suite.T()), suite.meta, nil)
	}()
	err, ok := recovered.(error)
	suite.True(ok, "expected a panic with an error, got %v", recovered)
	suite.ErrorIs(err, merr.ErrServiceInternal)
}

// splitShardStates builds a DescribeCollection response whose shard_infos are
// parallel to the given vchannels.
func splitShardStates(channels []string, states ...schemapb.ShardState) *milvuspb.DescribeCollectionResponse {
	infos := make([]*schemapb.CollectionShardInfo, 0, len(states))
	for _, state := range states {
		infos = append(infos, &schemapb.CollectionShardInfo{State: state})
	}
	return &milvuspb.DescribeCollectionResponse{VirtualChannelNames: channels, ShardInfos: infos}
}

// TestUpdateNextTarget_MarksSplitWindowTargets pins QC1: the shard states are
// read BEFORE the recovery info, and a pull marks each channel it lists that the
// read did not see Normal, Splitting or Dropped. That never misses a target a
// racing fence added after the read; an adoption racing the pull over-marks.
func (suite *TargetManagerSuite) TestUpdateNextTarget_MarksSplitWindowTargets() {
	ctx := suite.ctx
	collectionID := int64(1000)
	channelNames := []string{"src", "t1", "t2"}
	vchannels := lo.Map(channelNames, func(name string, _ int) *datapb.VchannelInfo {
		return &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name}
	})
	segments := []*datapb.SegmentInfo{
		{ID: 1, CollectionID: collectionID, PartitionID: 100, InsertChannel: "src"},
	}

	suite.Run("a pull taken while a target is Creating marks it", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, time.Minute))
		described := false
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).RunAndReturn(
			func(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) {
				described = true
				return splitShardStates(channelNames,
					schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardNormal), nil
			}).Once()
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).RunAndReturn(
			func(context.Context, int64, ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
				suite.True(described, "the shard states must be read before the recovery info is pulled")
				return vchannels, segments, nil
			}).Once()

		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		suite.Equal(typeutil.NewSet("t1"), mgr.next.getCollectionTarget(collectionID).windowTargets)
		suite.Equal(typeutil.NewSet("t1"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))

		// the mark belongs to the pulled snapshot, so trimming a partition off
		// that snapshot must not wash it out.
		mgr.RemovePartitionFromNextTarget(ctx, collectionID, 101)
		suite.Equal(typeutil.NewSet("t1"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))
		suite.Empty(mgr.GetSplitWindowTargets(ctx, collectionID+1, NextTarget))
	})

	suite.Run("a pull racing the fence marks the targets the state read did not list", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, time.Minute))
		// the state read commits before the fence: only the source, still Normal.
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
			splitShardStates(channelNames[:1], schemapb.ShardState_ShardNormal), nil).Once()
		// the pull reads after the fence: it lists the targets, data under the source.
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels, segments, nil).Once()

		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		suite.Equal(typeutil.NewSet("t1", "t2"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))
	})

	suite.Run("a pull racing the adoption over-marks the adopted targets", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, time.Minute))
		// the state read commits before the adoption: targets still Creating.
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
			splitShardStates(channelNames,
				schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating), nil).Once()
		// the pull reads after it: the source is delisted.
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels[1:], nil, nil).Once()

		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		// an over-mark: the window-end refresh lifts it.
		suite.Equal(typeutil.NewSet("t1", "t2"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))
	})

	suite.Run("a failed state read falls back to the cached read and still marks a racing fence", func() {
		broker := NewMockBroker(suite.T())
		cache := NewShardSplitStateCache(broker, time.Minute)
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, cache)
		// an earlier cached query saw the pre-fence meta.
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
			splitShardStates(channelNames[:1], schemapb.ShardState_ShardNormal), nil).Once()
		suite.Empty(cache.CreatingTargetChannels(ctx, collectionID))
		// the fresh read fails; the pull, taken after the fence, still happens.
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(nil, merr.WrapErrServiceNotReady("rootcoord", 1, "initializing")).Once()
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels, segments, nil).Once()

		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		suite.Equal(typeutil.NewSet("t1", "t2"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))
	})

	suite.Run("a failed state read on a warm collection that is not splitting marks nothing", func() {
		broker := NewMockBroker(suite.T())
		cache := NewShardSplitStateCache(broker, time.Minute)
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, cache)
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
			splitShardStates(channelNames[:1], schemapb.ShardState_ShardNormal), nil).Once()
		suite.Empty(cache.CreatingTargetChannels(ctx, collectionID))
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(nil, merr.WrapErrServiceNotReady("rootcoord", 1, "initializing")).Once()
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels[:1], segments, nil).Once()

		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		suite.NotNil(mgr.next.getCollectionTarget(collectionID))
		suite.Empty(mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))
	})

	suite.Run("a pull with no split leaves the mark empty", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, time.Minute))
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
			splitShardStates(channelNames[:1], schemapb.ShardState_ShardNormal), nil).Once()
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels[:1], segments, nil).Once()

		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		suite.NotNil(mgr.next.getCollectionTarget(collectionID))
		suite.Empty(mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))
	})

	suite.Run("a failed state read is retried together with the pull", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, time.Minute))
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(nil, merr.WrapErrServiceNotReady("rootcoord", 1, "initializing")).Once()
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
			splitShardStates(channelNames,
				schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating), nil).Once()
		// the pull happens once, and only after a successful state read.
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels, segments, nil).Once()

		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		suite.Equal(typeutil.NewSet("t1", "t2"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))
	})

	suite.Run("no next target is built when the states cannot be read", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, time.Minute))
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(nil, merr.WrapErrServiceNotReady("rootcoord", 1, "initializing")).Maybe()
		// GetRecoveryInfoV2 has no expectation: an unmarked pull must never happen.
		timeoutCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		suite.Error(mgr.UpdateCollectionNextTarget(timeoutCtx, collectionID))
		suite.Nil(mgr.next.getCollectionTarget(collectionID))
	})
}

// TestUpdateCurrentTarget_SplitWindowSnapshot pins Task 17's promotion rule: a
// next target pulled inside a shard split window becomes the current target
// WITHOUT its window targets, so the collection is serviceable from the source
// for the whole window -- but only while the collection's shard states still
// describe exactly that window.
func (suite *TargetManagerSuite) TestUpdateCurrentTarget_SplitWindowSnapshot() {
	ctx := suite.ctx
	collectionID := int64(1000)
	names := []string{"src", "t1", "t2"}
	vchannels := lo.Map(names, func(name string, _ int) *datapb.VchannelInfo {
		return &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name}
	})
	// datacoord attributes the targets' flushed data to the still-listed source.
	segments := []*datapb.SegmentInfo{
		{ID: 1, CollectionID: collectionID, PartitionID: 100, InsertChannel: "src"},
		{ID: 2, CollectionID: collectionID, PartitionID: 101, InsertChannel: "src"},
	}
	windowStates := func() *milvuspb.DescribeCollectionResponse {
		return splitShardStates(names,
			schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating)
	}

	suite.Run("the window snapshot is promoted without its window targets", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, 0))
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).RunAndReturn(
			func(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) { return windowStates(), nil })
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels, segments, nil).Once()
		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))

		exclusions, promotable := mgr.GetSplitWindowExclusions(ctx, collectionID, NextTarget)
		suite.True(promotable)
		suite.Equal(typeutil.NewSet("t1", "t2"), exclusions)

		suite.True(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
		suite.ElementsMatch([]string{"src"}, lo.Keys(mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget)))
		suite.ElementsMatch([]int64{1, 2}, lo.Keys(mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget)))
		// the promoted copy carries no mark of its own: nothing holds it back.
		suite.Empty(mgr.GetSplitWindowTargets(ctx, collectionID, CurrentTarget))
		// every partition of the collection is still answered for, so a load of
		// either of them can finish.
		suite.True(mgr.IsCurrentTargetExist(ctx, collectionID, 100))
		suite.True(mgr.IsCurrentTargetExist(ctx, collectionID, 101))

		// the next target is NOT consumed: the window targets must stay in a
		// target, or the channel checker would release them once they appear in
		// dist at adoption.
		suite.True(mgr.IsNextTargetExist(ctx, collectionID))
		suite.ElementsMatch(names, lo.Keys(mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget)))
		suite.Equal(typeutil.NewSet("t1", "t2"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))

		// re-promoting the same snapshot is a no-op that still reports ready.
		suite.True(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
		suite.True(mgr.IsNextTargetExist(ctx, collectionID))
		suite.ElementsMatch([]string{"src"}, lo.Keys(mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget)))
	})

	suite.Run("a marked channel that is no longer Creating refuses the promotion", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, 0))
		adopted := false
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).RunAndReturn(
			func(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) {
				if adopted {
					return splitShardStates(names,
						schemapb.ShardState_ShardDropped, schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal), nil
				}
				return windowStates(), nil
			})
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels, segments, nil).Once()
		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))

		adopted = true
		exclusions, promotable := mgr.GetSplitWindowExclusions(ctx, collectionID, NextTarget)
		suite.False(promotable)
		suite.Empty(exclusions)
		suite.False(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
		suite.False(mgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID))
		suite.True(mgr.IsNextTargetExist(ctx, collectionID))
	})

	suite.Run("a fenced source the pull no longer lists refuses the promotion", func() {
		// The state read is behind the pull: it still calls src a fenced source,
		// but src was delisted at adoption and the pull lists only the targets.
		// Excluding them would report a collection Loaded that serves nothing.
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, 0))
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).RunAndReturn(
			func(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) { return windowStates(), nil })
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels[1:], nil, nil).Once()
		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		suite.Equal(typeutil.NewSet("t1", "t2"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))

		_, promotable := mgr.GetSplitWindowExclusions(ctx, collectionID, NextTarget)
		suite.False(promotable)
		suite.False(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
		suite.False(mgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID))
	})

	suite.Run("a mark with no fenced source behind it refuses the promotion", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, 0))
		// the state read never listed t1/t2, so the pull marks them; by the time
		// the promotion asks, nothing is fenced and they read Creating.
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
			splitShardStates(names,
				schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating), nil)
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels, segments, nil).Once()
		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		suite.Equal(typeutil.NewSet("t1", "t2"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))

		_, promotable := mgr.GetSplitWindowExclusions(ctx, collectionID, NextTarget)
		suite.False(promotable)
		suite.False(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
	})

	suite.Run("a cascaded split keeps the untouched shard and the new source", func() {
		cascaded := []string{"t1", "t1a", "t1b", "t2"}
		cascadedChannels := lo.Map(cascaded, func(name string, _ int) *datapb.VchannelInfo {
			return &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name}
		})
		cascadedSegments := []*datapb.SegmentInfo{
			{ID: 3, CollectionID: collectionID, PartitionID: 100, InsertChannel: "t1"},
			{ID: 4, CollectionID: collectionID, PartitionID: 100, InsertChannel: "t2"},
		}
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, 0))
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
			splitShardStates(cascaded,
				schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating,
				schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardNormal), nil)
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(cascadedChannels, cascadedSegments, nil).Once()
		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		suite.Equal(typeutil.NewSet("t1a", "t1b"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))

		suite.True(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
		suite.ElementsMatch([]string{"t1", "t2"}, lo.Keys(mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget)))
		suite.ElementsMatch([]int64{3, 4}, lo.Keys(mgr.GetSealedSegmentsByCollection(ctx, collectionID, CurrentTarget)))
	})

	suite.Run("a target of a split that has already adopted refuses the promotion", func() {
		// Two splits run at once (dataCoord.shardSplit.maxConcurrentTasks > 1;
		// the concurrency gate is per vchannel): A -> A1,A2 and B -> B1,B2. B
		// adopts first, so B1/B2 are Normal and B is delisted, while A is still
		// fenced. A pull whose state read predates B's adoption marks all four
		// targets, and conditions 2 and 3 both pass -- A is a fenced source and
		// it is one of the channels that would stay. Only condition 1 refuses,
		// and it has to: B1/B2 are the ONLY servers of B's key range, so
		// excluding them would leave that range in no target while the
		// collection reports Loaded and GetShardLeaders lists A alone.
		all := []string{"A", "A1", "A2", "B1", "B2"}
		pulled := lo.Map(all, func(name string, _ int) *datapb.VchannelInfo {
			return &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name}
		})
		pulledSegments := []*datapb.SegmentInfo{
			{ID: 5, CollectionID: collectionID, PartitionID: 100, InsertChannel: "A"},
			{ID: 6, CollectionID: collectionID, PartitionID: 100, InsertChannel: "B1"},
			{ID: 7, CollectionID: collectionID, PartitionID: 100, InsertChannel: "B2"},
		}
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, 0))
		bAdopted := false
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).RunAndReturn(
			func(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error) {
				if bAdopted {
					return splitShardStates(all,
						schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating,
						schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardNormal,
						schemapb.ShardState_ShardNormal), nil
				}
				// before B's adoption: B is still fenced and its targets Creating.
				return splitShardStates([]string{"A", "A1", "A2", "B", "B1", "B2"},
					schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating,
					schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardSplitting,
					schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardCreating), nil
			})
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(pulled, pulledSegments, nil).Once()

		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
		suite.Equal(typeutil.NewSet("A1", "A2", "B1", "B2"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))

		bAdopted = true
		// condition 2 passes: A is Splitting and is one of the channels that stay.
		// condition 3 passes: there is a fenced source. Only condition 1 refuses.
		exclusions, promotable := mgr.GetSplitWindowExclusions(ctx, collectionID, NextTarget)
		suite.False(promotable, "B1/B2 have been adopted and are the only servers of B's range")
		suite.Empty(exclusions)
		suite.False(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
		suite.False(mgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID))
	})

	suite.Run("a pull with no window targets is promoted whole and consumes the next target", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, 0))
		broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
			splitShardStates(names,
				schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardNormal), nil)
		broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels, segments, nil).Once()
		suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))

		exclusions, promotable := mgr.GetSplitWindowExclusions(ctx, collectionID, NextTarget)
		suite.True(promotable)
		suite.Empty(exclusions)
		suite.True(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
		suite.ElementsMatch(names, lo.Keys(mgr.GetDmChannelsByCollection(ctx, collectionID, CurrentTarget)))
		suite.False(mgr.IsNextTargetExist(ctx, collectionID))
	})

	suite.Run("an unknown collection excludes nothing and stays promotable", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, 0))
		exclusions, promotable := mgr.GetSplitWindowExclusions(ctx, collectionID+999, NextTarget)
		suite.True(promotable)
		suite.Empty(exclusions)
	})

	suite.Run("a target manager without a split state cache refuses to exclude", func() {
		broker := NewMockBroker(suite.T())
		mgr := NewTargetManager(broker, suite.meta)
		marked := NewCollectionTarget(map[int64]*datapb.SegmentInfo{}, map[string]*DmChannel{
			"src": DmChannelFromVChannel(vchannels[0]),
			"t1":  DmChannelFromVChannel(vchannels[1]),
		}, []int64{100})
		marked.windowTargets = typeutil.NewSet("t1")
		mgr.next.updateCollectionTarget(collectionID, marked)

		_, promotable := mgr.GetSplitWindowExclusions(ctx, collectionID, NextTarget)
		suite.False(promotable)
		suite.False(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
	})
}

// TestUpdateCurrentTarget_RefusesAnEmptyNarrowedTarget pins the defense in depth
// inside the promotion: the exclusion rule always leaves a fenced source, so an
// exclusion covering every channel cannot happen -- but promoting the empty
// result would leave the collection with a current target that lists nothing.
func (suite *TargetManagerSuite) TestUpdateCurrentTarget_RefusesAnEmptyNarrowedTarget() {
	ctx := suite.ctx
	collectionID := int64(1000)
	names := []string{"src", "t1"}
	vchannels := lo.Map(names, func(name string, _ int) *datapb.VchannelInfo {
		return &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name}
	})
	broker := NewMockBroker(suite.T())
	mgr := NewTargetManagerWithSplitState(broker, suite.meta, NewShardSplitStateCache(broker, 0))
	broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
		splitShardStates(names, schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating), nil)
	broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels, nil, nil).Once()
	suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))

	everything := typeutil.NewSet(names...)
	patch := mockey.Mock((*TargetManager).splitWindowExclusions).Return(everything, true).Build()
	defer patch.UnPatch()

	suite.False(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
	suite.False(mgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID))
	suite.True(mgr.IsNextTargetExist(ctx, collectionID), "the next target is left alone")
}

// TestSplitWindowExclusionsWithoutReadableStates pins that an unreadable shard
// state refuses the exclusion rather than guessing at the window.
func (suite *TargetManagerSuite) TestSplitWindowExclusionsWithoutReadableStates() {
	ctx := suite.ctx
	collectionID := int64(1000)
	names := []string{"src", "t1"}
	vchannels := lo.Map(names, func(name string, _ int) *datapb.VchannelInfo {
		return &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: name}
	})
	broker := NewMockBroker(suite.T())
	cache := NewShardSplitStateCache(broker, 0)
	mgr := NewTargetManagerWithSplitState(broker, suite.meta, cache)
	broker.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
		splitShardStates(names, schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating), nil).Once()
	broker.EXPECT().GetRecoveryInfoV2(mock.Anything, collectionID).Return(vchannels, nil, nil).Once()
	suite.NoError(mgr.UpdateCollectionNextTarget(ctx, collectionID))
	suite.Equal(typeutil.NewSet("t1"), mgr.GetSplitWindowTargets(ctx, collectionID, NextTarget))

	// a cache that holds no read (a restarted querycoord) and a coordinator that
	// is unreachable: the states cannot be read at all. An invalidated entry
	// would not do, since it stays the fallback of a failed refresh.
	unreachable := NewMockBroker(suite.T())
	unreachable.EXPECT().DescribeCollectionInternal(mock.Anything, collectionID).Return(
		nil, merr.WrapErrServiceNotReady("rootcoord", 1, "initializing"))
	mgr.splitStates = NewShardSplitStateCache(unreachable, 0)

	_, promotable := mgr.GetSplitWindowExclusions(ctx, collectionID, NextTarget)
	suite.False(promotable)
	suite.False(mgr.UpdateCollectionCurrentTarget(ctx, collectionID))
}

// TestWithoutChannels pins that narrowing a target drops the excluded channels
// and the segments attributed to them, and keeps everything else.
func (suite *TargetManagerSuite) TestWithoutChannels() {
	collectionID := int64(1000)
	segments := map[int64]*datapb.SegmentInfo{
		1: {ID: 1, CollectionID: collectionID, PartitionID: 100, InsertChannel: "src", NumOfRows: 10},
		2: {ID: 2, CollectionID: collectionID, PartitionID: 101, InsertChannel: "t1", NumOfRows: 20},
	}
	channels := map[string]*DmChannel{
		"src": DmChannelFromVChannel(&datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "src"}),
		"t1":  DmChannelFromVChannel(&datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "t1"}),
	}
	full := NewCollectionTarget(segments, channels, []int64{100, 101})
	full.windowTargets = typeutil.NewSet("t1")

	suite.Same(full, full.withoutChannels(nil), "an empty exclusion returns the same target")

	narrowed := full.withoutChannels(typeutil.NewSet("t1"))
	suite.Equal(full.GetTargetVersion(), narrowed.GetTargetVersion())
	suite.ElementsMatch([]string{"src"}, narrowed.GetAllDmChannelNames())
	suite.ElementsMatch([]int64{1}, narrowed.GetAllSegmentIDs())
	suite.EqualValues(10, narrowed.GetRowCount())
	suite.Empty(narrowed.SplitWindowTargets())
	// the partition set is not narrowed: partition 101 is still a partition of
	// this collection even with no segment left on a served channel.
	suite.True(narrowed.partitions.Contain(101))
	// the original is untouched.
	suite.ElementsMatch([]string{"src", "t1"}, full.GetAllDmChannelNames())

	// a partial exclusion keeps what it did not cover marked.
	partial := full.withoutChannels(typeutil.NewSet("src"))
	suite.Equal(typeutil.NewSet("t1"), partial.SplitWindowTargets())
}
