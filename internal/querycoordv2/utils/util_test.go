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

package utils

import (
	"context"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type UtilTestSuite struct {
	suite.Suite
	nodeMgr *session.NodeManager
}

func (suite *UtilTestSuite) SetupTest() {
	suite.nodeMgr = session.NewNodeManager()
}

func (suite *UtilTestSuite) setNodeAvailable(nodes ...int64) {
	for _, node := range nodes {
		nodeInfo := session.NewNodeInfo(session.ImmutableNodeInfo{
			NodeID:   node,
			Address:  "",
			Hostname: "localhost",
		})
		nodeInfo.SetLastHeartbeat(time.Now())
		suite.nodeMgr.Add(nodeInfo)
	}
}

func (suite *UtilTestSuite) TestCheckLeaderAvaliable() {
	leadview := &meta.LeaderView{
		ID:            1,
		Channel:       "test",
		Segments:      map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
		TargetVersion: 1011,
	}

	mockTargetManager := meta.NewMockTargetManager(suite.T())
	mockTargetManager.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{
		2: {
			ID:            2,
			InsertChannel: "test",
		},
	}).Maybe()
	mockTargetManager.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()

	suite.setNodeAvailable(1, 2)
	err := CheckDelegatorDataReady(suite.nodeMgr, mockTargetManager, leadview, meta.CurrentTarget)
	suite.NoError(err)
}

func (suite *UtilTestSuite) TestCheckLeaderAvaliableFailed() {
	suite.Run("leader not available", func() {
		leadview := &meta.LeaderView{
			ID:            1,
			Channel:       "test",
			Segments:      map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
			TargetVersion: 1011,
		}
		mockTargetManager := meta.NewMockTargetManager(suite.T())
		mockTargetManager.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{
			2: {
				ID:            2,
				InsertChannel: "test",
			},
		}).Maybe()
		mockTargetManager.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()
		// leader nodeID=1 not available
		suite.setNodeAvailable(2)
		err := CheckDelegatorDataReady(suite.nodeMgr, mockTargetManager, leadview, meta.CurrentTarget)
		suite.Error(err)
	})

	suite.Run("shard worker not available", func() {
		leadview := &meta.LeaderView{
			ID:            11111,
			Channel:       "test",
			Segments:      map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
			TargetVersion: 1011,
		}

		mockTargetManager := meta.NewMockTargetManager(suite.T())
		mockTargetManager.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{
			2: {
				ID:            2,
				InsertChannel: "test",
			},
		}).Maybe()
		mockTargetManager.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()
		// leader nodeID=2 not available
		suite.setNodeAvailable(1)
		err := CheckDelegatorDataReady(suite.nodeMgr, mockTargetManager, leadview, meta.CurrentTarget)
		suite.Error(err)
	})

	suite.Run("segment lacks", func() {
		leadview := &meta.LeaderView{
			ID:            1,
			Channel:       "test",
			Segments:      map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
			TargetVersion: 1011,
		}
		mockTargetManager := meta.NewMockTargetManager(suite.T())
		mockTargetManager.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{
			// target segmentID=1 not in leadView
			1: {
				ID:            1,
				InsertChannel: "test",
			},
		}).Maybe()
		mockTargetManager.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()
		suite.setNodeAvailable(1, 2)
		err := CheckDelegatorDataReady(suite.nodeMgr, mockTargetManager, leadview, meta.CurrentTarget)
		suite.Error(err)
	})

	suite.Run("target version not synced", func() {
		leadview := &meta.LeaderView{
			ID:       1,
			Channel:  "test",
			Segments: map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
		}
		mockTargetManager := meta.NewMockTargetManager(suite.T())
		mockTargetManager.EXPECT().GetSealedSegmentsByChannel(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(map[int64]*datapb.SegmentInfo{
			// target segmentID=1 not in leadView
			1: {
				ID:            1,
				InsertChannel: "test",
			},
		}).Maybe()
		mockTargetManager.EXPECT().GetCollectionTargetVersion(mock.Anything, mock.Anything, mock.Anything).Return(1011).Maybe()
		suite.setNodeAvailable(1, 2)
		err := CheckDelegatorDataReady(suite.nodeMgr, mockTargetManager, leadview, meta.CurrentTarget)
		suite.Error(err)
	})

	suite.Run("catching up streaming data", func() {
		leadview := &meta.LeaderView{
			ID:            1,
			Channel:       "test",
			Segments:      map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
			TargetVersion: 1011,
			Status: &querypb.LeaderViewStatus{
				Serviceable:             true,
				CatchingUpStreamingData: true, // still catching up
			},
		}
		// When catching up, function returns early without calling targetMgr
		// so we can pass nil as targetMgr
		suite.setNodeAvailable(1, 2)
		err := CheckDelegatorDataReady(suite.nodeMgr, nil, leadview, meta.CurrentTarget)
		suite.Error(err)
		suite.Contains(err.Error(), "catching up streaming data")
	})

	suite.Run("caught up streaming data", func() {
		leadview := &meta.LeaderView{
			ID:            1,
			Channel:       "test",
			Segments:      map[int64]*querypb.SegmentDist{2: {NodeID: 2}},
			TargetVersion: 1011,
			Status: &querypb.LeaderViewStatus{
				Serviceable:             true,
				CatchingUpStreamingData: false, // already caught up
			},
		}
		// Use mockey to mock TargetManager.GetSealedSegmentsByChannel
		targetMgr := &meta.TargetManager{}
		mockGetSealedSegments := mockey.Mock(mockey.GetMethod(targetMgr, "GetSealedSegmentsByChannel")).
			Return(map[int64]*datapb.SegmentInfo{
				2: {
					ID:            2,
					InsertChannel: "test",
				},
			}).Build()
		defer mockGetSealedSegments.UnPatch()

		suite.setNodeAvailable(1, 2)
		err := CheckDelegatorDataReady(suite.nodeMgr, targetMgr, leadview, meta.CurrentTarget)
		suite.NoError(err)
	})
}

func (suite *UtilTestSuite) TestGetChannelRWAndRONodesFor260() {
	nodes := []int64{1, 2, 3, 4, 5}
	nodeManager := session.NewNodeManager()
	r := meta.NewReplica(&querypb.Replica{
		Nodes: nodes,
	})
	rwNodes, roNodes := GetChannelRWAndRONodesFor260(r, nodeManager)
	suite.ElementsMatch(rwNodes, []int64{})
	suite.ElementsMatch(roNodes, []int64{1, 2, 3, 4, 5})

	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "127.0.0.1:0",
		Hostname: "localhost",
		Version:  semver.MustParse("2.5.0"),
	}))
	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "127.0.0.1:0",
		Hostname: "localhost",
		Version:  semver.MustParse("2.6.0-dev"),
	}))
	rwNodes, roNodes = GetChannelRWAndRONodesFor260(r, nodeManager)
	suite.ElementsMatch(rwNodes, []int64{})
	suite.ElementsMatch(roNodes, []int64{1, 3, 4, 5})
}

func (suite *UtilTestSuite) TestFilterOutNodeLessThan260() {
	nodes := []int64{1, 2, 3, 4, 5}
	nodeManager := session.NewNodeManager()
	filteredNodes := filterNodeLessThan260(nodes, nodeManager)
	suite.ElementsMatch(filteredNodes, []int64{1, 2, 3, 4, 5})

	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "127.0.0.1:0",
		Hostname: "localhost",
		Version:  semver.MustParse("2.5.0"),
	}))
	filteredNodes = filterNodeLessThan260(nodes, nodeManager)
	suite.ElementsMatch(filteredNodes, []int64{1, 2, 3, 4, 5})

	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   2,
		Address:  "127.0.0.1:0",
		Hostname: "localhost",
		Version:  semver.MustParse("2.6.0-dev"),
	}))
	filteredNodes = filterNodeLessThan260(nodes, nodeManager)
	suite.ElementsMatch(filteredNodes, []int64{1, 3, 4, 5})

	nodeManager.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   3,
		Address:  "127.0.0.1:0",
		Hostname: "localhost",
		Version:  semver.MustParse("2.6.0"),
	}))
	filteredNodes = filterNodeLessThan260(nodes, nodeManager)
	suite.ElementsMatch(filteredNodes, []int64{1, 4, 5})
}

func (suite *UtilTestSuite) TestCheckSegmentDataReady_ManifestComparison() {
	basePath := "/data/insert_log/col/part/seg"
	collectionID := int64(100)
	segmentID := int64(200)
	nodeID := int64(1)

	newDistManager := func(manifestPath string) *meta.DistributionManager {
		dm := meta.NewDistributionManager(session.NewNodeManager())
		dm.SegmentDistManager.Update(nodeID, &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           segmentID,
				CollectionID: collectionID,
			},
			Node:         nodeID,
			ManifestPath: manifestPath,
		})
		return dm
	}

	newTargetMgr := func(manifestPath string) meta.TargetManagerInterface {
		m := meta.NewMockTargetManager(suite.T())
		m.EXPECT().GetSealedSegmentsByCollection(mock.Anything, collectionID, mock.Anything).
			Return(map[int64]*datapb.SegmentInfo{
				segmentID: {
					ID:           segmentID,
					CollectionID: collectionID,
					ManifestPath: manifestPath,
				},
			}).Maybe()
		return m
	}

	suite.Run("same manifest version - ready", func() {
		manifest := packed.MarshalManifestPath(basePath, 5)
		err := CheckSegmentDataReady(context.Background(), collectionID, newDistManager(manifest), newTargetMgr(manifest), meta.NextTarget)
		suite.NoError(err)
	})

	suite.Run("dist newer than target - ready", func() {
		distManifest := packed.MarshalManifestPath(basePath, 10)
		targetManifest := packed.MarshalManifestPath(basePath, 5)
		err := CheckSegmentDataReady(context.Background(), collectionID, newDistManager(distManifest), newTargetMgr(targetManifest), meta.NextTarget)
		suite.NoError(err)
	})

	suite.Run("dist older than target - not ready", func() {
		distManifest := packed.MarshalManifestPath(basePath, 1)
		targetManifest := packed.MarshalManifestPath(basePath, 5)
		err := CheckSegmentDataReady(context.Background(), collectionID, newDistManager(distManifest), newTargetMgr(targetManifest), meta.NextTarget)
		suite.Error(err)
	})

	suite.Run("both empty manifest - ready", func() {
		err := CheckSegmentDataReady(context.Background(), collectionID, newDistManager(""), newTargetMgr(""), meta.NextTarget)
		suite.NoError(err)
	})

	suite.Run("segment not in dist - not ready", func() {
		dm := meta.NewDistributionManager(session.NewNodeManager())
		targetManifest := packed.MarshalManifestPath(basePath, 5)
		err := CheckSegmentDataReady(context.Background(), collectionID, dm, newTargetMgr(targetManifest), meta.NextTarget)
		suite.Error(err)
	})
}

func (suite *UtilTestSuite) TestCheckSegmentDataReady_DataVersion() {
	basePath := "/data/insert_log/col/part/seg"
	collectionID := int64(100)
	segmentID := int64(200)
	nodeID := int64(1)
	// Use matching manifest in dist and target so DataVersion is the only gate.
	manifest := packed.MarshalManifestPath(basePath, 5)

	newDistManager := func(distDataVersion *int32) *meta.DistributionManager {
		dm := meta.NewDistributionManager(session.NewNodeManager())
		dm.SegmentDistManager.Update(nodeID, &meta.Segment{
			SegmentInfo: &datapb.SegmentInfo{
				ID:           segmentID,
				CollectionID: collectionID,
			},
			Node:         nodeID,
			ManifestPath: manifest,
			DataVersion:  distDataVersion,
		})
		return dm
	}

	newTargetMgr := func(targetDataVersion int32) meta.TargetManagerInterface {
		m := meta.NewMockTargetManager(suite.T())
		m.EXPECT().GetSealedSegmentsByCollection(mock.Anything, collectionID, mock.Anything).
			Return(map[int64]*datapb.SegmentInfo{
				segmentID: {
					ID:           segmentID,
					CollectionID: collectionID,
					ManifestPath: manifest,
					DataVersion:  targetDataVersion,
				},
			}).Maybe()
		return m
	}

	int32Ptr := func(v int32) *int32 { return &v }

	suite.Run("dist data version older than target - not ready", func() {
		err := CheckSegmentDataReady(context.Background(), collectionID,
			newDistManager(int32Ptr(1)), newTargetMgr(5), meta.NextTarget)
		suite.Error(err)
	})

	suite.Run("dist data version equals target - ready", func() {
		err := CheckSegmentDataReady(context.Background(), collectionID,
			newDistManager(int32Ptr(5)), newTargetMgr(5), meta.NextTarget)
		suite.NoError(err)
	})

	suite.Run("dist data version newer than target - ready", func() {
		err := CheckSegmentDataReady(context.Background(), collectionID,
			newDistManager(int32Ptr(10)), newTargetMgr(5), meta.NextTarget)
		suite.NoError(err)
	})

	// mixed-version rollout: an old QueryNode does not report DataVersion,
	// so the dist-side pointer is nil. Skip the check rather than loop Reopen forever.
	suite.Run("dist data version nil (old QueryNode) - ready", func() {
		err := CheckSegmentDataReady(context.Background(), collectionID,
			newDistManager(nil), newTargetMgr(5), meta.NextTarget)
		suite.NoError(err)
	})

	suite.Run("dist data version nil and target zero - ready", func() {
		err := CheckSegmentDataReady(context.Background(), collectionID,
			newDistManager(nil), newTargetMgr(0), meta.NextTarget)
		suite.NoError(err)
	})
}

func TestUtilSuite(t *testing.T) {
	suite.Run(t, new(UtilTestSuite))
}

func TestAHandedOverSplitSourceIsSkippedNotFatal(t *testing.T) {
	// A split source lingers in the current target until the target advances,
	// but querycoord may already have let it go. Failing the call there takes
	// down every read of the collection, not just that shard -- measured as a
	// 44s outage in an E2E run. It owns no key range by then, so it is skipped.
	ctx := context.Background()
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: 1, Address: "localhost", Hostname: "localhost",
	}))

	catalog := catalogmocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()
	m := meta.NewMeta(params.RandomIncrementIDAllocator(), catalog, nodeMgr)
	require.NoError(t, m.PutCollection(ctx, CreateTestCollection(1, 1)))
	require.NoError(t, m.Put(ctx, CreateTestReplica(1, 1, []int64{1})))

	dist := meta.NewDistributionManager(nodeMgr)
	// only the live shard has a leader; the retired source has none.
	dist.ChannelDistManager.Update(1, &meta.DmChannel{
		VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v1"},
		Node:         1,
		Version:      1,
		View: &meta.LeaderView{
			ID: 1, Channel: "v1", Version: 1,
			Status: &querypb.LeaderViewStatus{Serviceable: true},
		},
	})

	channels := map[string]*meta.DmChannel{
		"v0": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v0"}},
		"v1": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 1, ChannelName: "v1"}},
	}

	// Without the retired-source set, one leaderless channel fails everything.
	_, err := GetShardLeadersWithChannelsAndReplicaFilter(ctx, m, dist, nodeMgr, 1, channels, false, nil, nil)
	assert.Error(t, err, "an ordinary leaderless channel still fails the call")

	// Knowing v0 has handed over, the live shard is served instead.
	got, err := GetShardLeadersWithChannelsAndReplicaFilter(ctx, m, dist, nodeMgr, 1, channels, false, nil,
		typeutil.NewSet("v0"))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "v1", got[0].GetChannelName())
}

func TestAReplacedSplitSourceIsLeftOutEvenWhileItStillServes(t *testing.T) {
	// The duplicate half: the source is still loaded and answering, and so are
	// the targets that replaced it, so the same primary key comes back from two
	// shards and the proxy rejects the whole query. Once the caller says the
	// replacements are serving, the source has to leave the read set even though
	// it could still answer.
	ctx := context.Background()
	nodeMgr := session.NewNodeManager()
	nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID: 1, Address: "localhost", Hostname: "localhost",
	}))

	catalog := catalogmocks.NewQueryCoordCatalog(t)
	catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveReplica(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().SaveResourceGroup(mock.Anything, mock.Anything).Return(nil).Maybe()
	m := meta.NewMeta(params.RandomIncrementIDAllocator(), catalog, nodeMgr)
	require.NoError(t, m.PutCollection(ctx, CreateTestCollection(2, 1)))
	require.NoError(t, m.Put(ctx, CreateTestReplica(2, 2, []int64{1})))

	serving := func(name string) *meta.DmChannel {
		return &meta.DmChannel{
			VchannelInfo: &datapb.VchannelInfo{CollectionID: 2, ChannelName: name},
			Node:         1,
			Version:      1,
			View: &meta.LeaderView{
				ID: 1, Channel: name, Version: 1,
				Status: &querypb.LeaderViewStatus{Serviceable: true},
			},
		}
	}
	dist := meta.NewDistributionManager(nodeMgr)
	// the source is still perfectly serviceable -- that is the point
	dist.ChannelDistManager.Update(1, serving("v0"), serving("v1"))

	channels := map[string]*meta.DmChannel{
		"v0": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 2, ChannelName: "v0"}},
		"v1": {VchannelInfo: &datapb.VchannelInfo{CollectionID: 2, ChannelName: "v1"}},
	}

	// Nothing retired: both shards are read, which is where the duplicate came from.
	got, err := GetShardLeadersWithChannelsAndReplicaFilter(ctx, m, dist, nodeMgr, 2, channels, false, nil, nil)
	require.NoError(t, err)
	assert.Len(t, got, 2)

	got, err = GetShardLeadersWithChannelsAndReplicaFilter(ctx, m, dist, nodeMgr, 2, channels, false, nil,
		typeutil.NewSet("v0"))
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "v1", got[0].GetChannelName())
}
