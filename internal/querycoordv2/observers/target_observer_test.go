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

package observers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type TargetObserverSuite struct {
	suite.Suite

	kv kv.MetaKv
	//dependency
	meta      *meta.Meta
	targetMgr *meta.TargetManager
	distMgr   *meta.DistributionManager
	broker    *meta.MockBroker

	observer *TargetObserver

	collectionID       int64
	partitionID        int64
	nextTargetSegments []*datapb.SegmentInfo
	nextTargetChannels []*datapb.VchannelInfo
}

func (suite *TargetObserverSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(Params.QueryCoordCfg.UpdateNextTargetInterval.Key, "3")
}

func (suite *TargetObserverSuite) SetupTest() {
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
	store := meta.NewMetaStore(suite.kv)
	idAllocator := RandomIncrementIDAllocator()
	suite.meta = meta.NewMeta(idAllocator, store, session.NewNodeManager())

	suite.broker = meta.NewMockBroker(suite.T())
	suite.targetMgr = meta.NewTargetManager(suite.broker, suite.meta)
	suite.distMgr = meta.NewDistributionManager()
	suite.observer = NewTargetObserver(suite.meta, suite.targetMgr, suite.distMgr, suite.broker)
	suite.observer.Start(context.TODO())
	suite.collectionID = int64(1000)
	suite.partitionID = int64(100)

	err = suite.meta.CollectionManager.PutCollection(utils.CreateTestCollection(suite.collectionID, 1))
	suite.NoError(err)
	err = suite.meta.CollectionManager.PutPartition(utils.CreateTestPartition(suite.collectionID, suite.partitionID))
	suite.NoError(err)
	replicas, err := suite.meta.ReplicaManager.Spawn(suite.collectionID, 1, meta.DefaultResourceGroupName)
	suite.NoError(err)
	replicas[0].AddNode(2)
	err = suite.meta.ReplicaManager.Put(replicas...)
	suite.NoError(err)

	suite.nextTargetChannels = []*datapb.VchannelInfo{
		{
			CollectionID: suite.collectionID,
			ChannelName:  "channel-1",
		},
		{
			CollectionID: suite.collectionID,
			ChannelName:  "channel-2",
		},
	}

	suite.nextTargetSegments = []*datapb.SegmentInfo{
		{
			ID:            11,
			PartitionID:   suite.partitionID,
			InsertChannel: "channel-1",
		},
		{
			ID:            12,
			PartitionID:   suite.partitionID,
			InsertChannel: "channel-2",
		},
	}

	suite.broker.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).Return(suite.nextTargetChannels, suite.nextTargetSegments, nil)
}

func (suite *TargetObserverSuite) TestTriggerUpdateTarget() {
	suite.Eventually(func() bool {
		return len(suite.targetMgr.GetHistoricalSegmentsByCollection(suite.collectionID, meta.NextTarget)) == 2 &&
			len(suite.targetMgr.GetDmChannelsByCollection(suite.collectionID, meta.NextTarget)) == 2
	}, 5*time.Second, 1*time.Second)

	suite.distMgr.LeaderViewManager.Update(2,
		&meta.LeaderView{
			ID:           2,
			CollectionID: suite.collectionID,
			Channel:      "channel-1",
			Segments: map[int64]*querypb.SegmentDist{
				11: {NodeID: 2},
			},
		},
		&meta.LeaderView{
			ID:           2,
			CollectionID: suite.collectionID,
			Channel:      "channel-2",
			Segments: map[int64]*querypb.SegmentDist{
				12: {NodeID: 2},
			},
		},
	)

	// Never update current target if it's empty, even the next target is ready
	suite.Eventually(func() bool {
		return len(suite.targetMgr.GetDmChannelsByCollection(suite.collectionID, meta.CurrentTarget)) == 0
	}, 3*time.Second, 1*time.Second)

	suite.broker.AssertExpectations(suite.T())
	suite.broker.ExpectedCalls = suite.broker.ExpectedCalls[:0]
	suite.nextTargetSegments = append(suite.nextTargetSegments, &datapb.SegmentInfo{
		ID:            13,
		PartitionID:   suite.partitionID,
		InsertChannel: "channel-1",
	})
	suite.targetMgr.UpdateCollectionCurrentTarget(suite.collectionID)

	// Pull next again
	suite.broker.EXPECT().
		GetRecoveryInfoV2(mock.Anything, mock.Anything).
		Return(suite.nextTargetChannels, suite.nextTargetSegments, nil)
	suite.Eventually(func() bool {
		return len(suite.targetMgr.GetHistoricalSegmentsByCollection(suite.collectionID, meta.NextTarget)) == 3 &&
			len(suite.targetMgr.GetDmChannelsByCollection(suite.collectionID, meta.NextTarget)) == 2
	}, 7*time.Second, 1*time.Second)
	suite.broker.AssertExpectations(suite.T())

	// Manually update next target
	ready, err := suite.observer.UpdateNextTarget(suite.collectionID)
	suite.NoError(err)

	suite.distMgr.LeaderViewManager.Update(2,
		&meta.LeaderView{
			ID:           2,
			CollectionID: suite.collectionID,
			Channel:      "channel-1",
			Segments: map[int64]*querypb.SegmentDist{
				11: {NodeID: 2},
				13: {NodeID: 2},
			},
		},
		&meta.LeaderView{
			ID:           2,
			CollectionID: suite.collectionID,
			Channel:      "channel-2",
			Segments: map[int64]*querypb.SegmentDist{
				12: {NodeID: 2},
			},
		},
	)

	// Able to update current if it's not empty
	suite.Eventually(func() bool {
		isReady := false
		select {
		case <-ready:
			isReady = true
		default:
		}
		return isReady &&
			len(suite.targetMgr.GetHistoricalSegmentsByCollection(suite.collectionID, meta.CurrentTarget)) == 3 &&
			len(suite.targetMgr.GetDmChannelsByCollection(suite.collectionID, meta.CurrentTarget)) == 2
	}, 7*time.Second, 1*time.Second)
}

func (suite *TargetObserverSuite) TearDownSuite() {
	suite.kv.Close()
	suite.observer.Stop()
}

func TestTargetObserver(t *testing.T) {
	suite.Run(t, new(TargetObserverSuite))
}
