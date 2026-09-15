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

package querynodev2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newSchemaInstallTestNode(nodeID int64) *QueryNode {
	return &QueryNode{
		lifetime:              lifetime.NewLifetime(commonpb.StateCode_Healthy),
		manager:               segments.NewManager(),
		delegators:            typeutil.NewConcurrentMap[string, delegator.ShardDelegator](),
		topologyMutationLocks: lock.NewKeyLock[int64](),
		distDeltaTracker:      newDataDistributionDeltaTracker(),
		serverID:              nodeID,
	}
}

func TestCoordinatorSchemaInstallUpdatesAllCollectionDelegators(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)
	node := newSchemaInstallTestNode(1)
	collectionID := int64(1000)
	schema := &schemapb.CollectionSchema{Name: "target", Version: 2}
	first := delegator.NewMockShardDelegator(t)
	second := delegator.NewMockShardDelegator(t)
	other := delegator.NewMockShardDelegator(t)
	first.EXPECT().Collection().Return(collectionID).Once()
	second.EXPECT().Collection().Return(collectionID).Once()
	other.EXPECT().Collection().Return(collectionID + 1).Once()
	first.EXPECT().InstallSchema(mock.Anything, schema, uint64(200)).Return(nil).Once()
	second.EXPECT().InstallSchema(mock.Anything, schema, uint64(200)).Return(nil).Once()
	node.delegators.Insert("first", first)
	node.delegators.Insert("second", second)
	node.delegators.Insert("other", other)

	status, err := node.UpdateSchema(context.Background(), &querypb.UpdateSchemaRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_AlterCollectionSchema),
			commonpbutil.WithSourceID(99),
		),
		CollectionID:    collectionID,
		Schema:          schema,
		SchemaBarrierTs: 200,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
}

func TestCoordinatorSchemaInstallDelegatorFailureFailsRPC(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)
	node := newSchemaInstallTestNode(1)
	collectionID := int64(1000)
	schema := &schemapb.CollectionSchema{Name: "target", Version: 2}
	installErr := merr.WrapErrServiceUnavailableMsg("schema install worker unavailable")
	shardDelegator := delegator.NewMockShardDelegator(t)
	shardDelegator.EXPECT().Collection().Return(collectionID).Once()
	shardDelegator.EXPECT().InstallSchema(mock.Anything, schema, uint64(200)).Return(installErr).Once()
	node.delegators.Insert("failed", shardDelegator)

	status, err := node.UpdateSchema(context.Background(), &querypb.UpdateSchemaRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_AlterCollectionSchema),
			commonpbutil.WithSourceID(99),
		),
		CollectionID:    collectionID,
		Schema:          schema,
		SchemaBarrierTs: 200,
	})
	require.ErrorIs(t, merr.CheckRPCCall(status, err), installErr)
}

func TestCoordinatorSchemaInstallUpdatesWorkerOnlyNode(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)
	node := newSchemaInstallTestNode(1)
	collectionID := int64(1000)
	targetSchema := &schemapb.CollectionSchema{Name: "target", Version: 2}
	collectionManager := segments.NewMockCollectionManager(t)
	collectionManager.EXPECT().UpdateSchema(collectionID, targetSchema, uint64(200)).Return(nil).Once()
	node.manager.Collection = collectionManager

	status, err := node.UpdateSchema(context.Background(), &querypb.UpdateSchemaRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_AlterCollectionSchema),
			commonpbutil.WithSourceID(99),
		),
		CollectionID:    collectionID,
		Schema:          targetSchema,
		SchemaBarrierTs: 200,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
}

func TestLegacyDirectSchemaUpdateIsNotCoordinatorFanout(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)
	node := newSchemaInstallTestNode(1)
	collectionID := int64(1000)
	targetSchema := &schemapb.CollectionSchema{Name: "target", Version: 2}
	collectionManager := segments.NewMockCollectionManager(t)
	collectionManager.EXPECT().UpdateSchema(collectionID, targetSchema, uint64(200)).Return(nil).Once()
	node.manager.Collection = collectionManager
	shardDelegator := delegator.NewMockShardDelegator(t)
	node.delegators.Insert("existing", shardDelegator)

	status, err := node.UpdateSchema(context.Background(), &querypb.UpdateSchemaRequest{
		CollectionID:    collectionID,
		Schema:          targetSchema,
		SchemaBarrierTs: 200,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
}

func TestRejectedLeaderLoadDoesNotPublishDistributionDelta(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)
	node := newSchemaInstallTestNode(1)
	collectionID := int64(1000)
	channel := "by-dev-rootcoord-dml_0_1000v0"
	staleErr := merr.WrapErrCollectionSchemaVersionNotReady(collectionID, 1, 2)
	shardDelegator := delegator.NewMockShardDelegator(t)
	shardDelegator.EXPECT().LoadSegments(mock.Anything, mock.Anything).Return(staleErr).Once()
	node.delegators.Insert(channel, shardDelegator)

	status, err := node.LoadSegments(context.Background(), &querypb.LoadSegmentsRequest{
		NeedTransfer: true,
		DstNodeID:    2,
		CollectionID: collectionID,
		Infos: []*querypb.SegmentLoadInfo{{
			SegmentID:     10,
			CollectionID:  collectionID,
			InsertChannel: channel,
			Level:         datapb.SegmentLevel_L1,
		}},
		Schema:        &schemapb.CollectionSchema{Name: "target", Version: 2},
		IndexInfoList: []*indexpb.IndexInfo{{}},
		LoadMeta:      &querypb.LoadMetaInfo{CollectionID: collectionID, SchemaBarrierTs: 200},
	})
	require.ErrorIs(t, merr.CheckRPCCall(status, err), staleErr)
	require.Empty(t, node.distDeltaTracker.dirtyChannels)
}

func TestLocalWorkerLoadReusesOuterTopologyLock(t *testing.T) {
	paramtable.Init()
	paramtable.SetNodeID(1)
	node := newSchemaInstallTestNode(1)
	collectionID := int64(1000)
	worker := NewLocalWorker(node)
	node.topologyMutationLocks.Lock(collectionID)
	defer node.topologyMutationLocks.Unlock(collectionID)

	done := make(chan error, 1)
	go func() {
		done <- worker.LoadSegments(context.Background(), &querypb.LoadSegmentsRequest{
			CollectionID: collectionID,
		})
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("local worker recursively acquired the collection topology lock")
	}
}
