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

package rollingupgrade

import (
	"context"
	"math/rand"
	"path"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	streamingcatalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type ManualRollingUpgradeSuite struct {
	integration.MiniClusterSuite
}

func (s *ManualRollingUpgradeSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "100")
	s.WithMilvusConfig(paramtable.Get().StreamingCfg.WALBalancerPolicyMinRebalanceIntervalThreshold.Key, "1ms")
	// Config updates are observed by the periodic WAL balance loop (default 1m).
	s.WithMilvusConfig(paramtable.Get().StreamingCfg.WALBalancerTriggerInterval.Key, "1s")

	// Keep new SNs empty until the test explicitly migrates or enables WAL balancing.
	s.WithMilvusConfig(paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.Key, "false")
	s.MiniClusterSuite.SetupSuite()
}

func (s *ManualRollingUpgradeSuite) TestTransfer() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestTransfer"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000
	insertRound := 5

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
		// Keep compaction outputs from invalidating the empty-node assertions before manual transfer.
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionAutoCompactionKey, Value: "false"},
		},
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	if err != nil {
		mlog.Warn(context.TODO(), "createCollectionStatus fail reason", mlog.Err(err))
	}

	mlog.Info(context.TODO(), "CreateCollection result", mlog.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))
	mlog.Info(context.TODO(), "ShowCollections result", mlog.Any("showCollectionsResp", showCollectionsResp))

	// insert data, and flush generate segment
	vecFieldData := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	for i := range lo.Range(insertRound) {
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{vecFieldData},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.True(merr.Ok(insertResult.GetStatus()))
		mlog.Info(context.TODO(), "Insert succeed", mlog.Int("round", i+1))
		resp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
			DbName:          dbName,
			CollectionNames: []string{collectionName},
		})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
	}

	resp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))
	s.WaitForFlush(ctx, resp.GetFlushCollSegIDs()[collectionName].GetData(), resp.GetCollFlushTs()[collectionName], dbName, collectionName)

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		mlog.Warn(context.TODO(), "createIndexStatus fail reason", mlog.Err(err))
	}

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)
	mlog.Info(context.TODO(), "Create index done")

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, collectionName)
	mlog.Info(context.TODO(), "Load collection done")

	defer c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})

	// QN optimization and WAL balancing are independent. The suite disables
	// WAL optimization before startup; suspend QN optimization before adding QNs.
	status, err := c.MixCoordClient.SuspendBalance(ctx, &querypb.SuspendBalanceRequest{})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		status, err := c.MixCoordClient.ResumeBalance(cleanupCtx, &querypb.ResumeBalanceRequest{})
		s.NoError(merr.CheckRPCCall(status, err))
	}()
	qn1, sn1 := c.DefaultQueryNode(), c.DefaultStreamingNode()
	distribution, err := c.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{NodeID: qn1.GetNodeID()})
	s.Require().NoError(merr.CheckRPCCall(distribution, err))
	expectedSegments := distribution.GetSealedSegmentIDs()
	s.Require().NotEmpty(expectedSegments)
	s.Empty(distribution.GetChannelNames(), "QN no longer hosts channel delegators")

	// Read persisted ASSIGNED state, not the old QN channel distribution. A
	// successful TransferChannel must have completed this barrier on return.
	catalog := streamingcatalog.NewCataLog(etcdkv.NewEtcdKV(c.EtcdCli, path.Join(c.RootPath(), "meta")))
	before, err := catalog.ListPChannel(ctx)
	s.Require().NoError(err)
	s.Require().NotEmpty(before)
	terms := make(map[string]int64, len(before))
	for _, channel := range before {
		s.Require().Equal(streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, channel.GetState())
		s.Require().Equal(sn1.GetNodeID(), channel.GetNode().GetServerId())
		terms[channel.GetChannel().GetName()] = channel.GetChannel().GetTerm()
	}

	qn2, sn2 := c.AddQueryNode(), c.AddStreamingNode()
	nodes, err := c.MixCoordClient.ListQueryNode(ctx, &querypb.ListQueryNodeRequest{})
	s.Require().NoError(merr.CheckRPCCall(nodes, err))
	s.ElementsMatch([]int64{qn1.GetNodeID(), qn2.GetNodeID()}, lo.Map(nodes.GetNodeInfos(), func(node *querypb.NodeInfo, _ int) int64 {
		return node.GetID()
	}))
	distribution, err = c.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{NodeID: qn2.GetNodeID()})
	s.Require().NoError(merr.CheckRPCCall(distribution, err))
	s.Empty(distribution.GetSealedSegmentIDs())
	s.Empty(distribution.GetChannelNames())

	status, err = c.MixCoordClient.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: sn1.GetNodeID(), TargetNodeID: sn2.GetNodeID(), TransferAll: true,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	after, err := catalog.ListPChannel(ctx)
	s.Require().NoError(err)
	s.Require().Len(after, len(before))
	for _, channel := range after {
		previousTerm, exists := terms[channel.GetChannel().GetName()]
		s.Require().True(exists, "migration must preserve the PChannel set")
		s.Equal(streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED, channel.GetState())
		s.Equal(sn2.GetNodeID(), channel.GetNode().GetServerId())
		s.Greater(channel.GetChannel().GetTerm(), previousTerm)
	}

	status, err = c.MixCoordClient.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: qn1.GetNodeID(), TargetNodeID: qn2.GetNodeID(), TransferAll: true,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	distribution, err = c.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{NodeID: qn2.GetNodeID()})
	s.Require().NoError(merr.CheckRPCCall(distribution, err))
	s.ElementsMatch(expectedSegments, distribution.GetSealedSegmentIDs())
	assertRows := func() {
		result, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			CollectionName: collectionName, OutputFields: []string{"count(*)"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		})
		s.Require().NoError(merr.CheckRPCCall(result, err))
		s.Require().Len(result.GetFieldsData(), 1)
		s.Equal([]int64{int64(rowNum * insertRound)}, result.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	}
	assertRows()
	// Serving on the target precedes retirement of old leased views. Check
	// eventual source release separately from the synchronous handoff result.
	s.Require().Eventually(func() bool {
		distribution, err := c.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{NodeID: qn1.GetNodeID()})
		return merr.CheckRPCCall(distribution, err) == nil && len(distribution.GetSealedSegmentIDs()) == 0
	}, 30*time.Second, 100*time.Millisecond)

	// The default tolerance deliberately keeps small workloads concentrated.
	// Make splitting this fixture profitable, then test QN and WAL convergence
	// independently, without prescribing which individual segment/channel moves.
	restoreScoring := c.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryViewCfg.BalancerResourceGroupAbsoluteTolerance.Key: "1",
		paramtable.Get().QueryViewCfg.BalancerResourceGroupRelativeTolerance.Key: "0",
		paramtable.Get().QueryViewCfg.BalancerMoveCostPerUnit.Key:                "0",
		paramtable.Get().QueryViewCfg.BalancerLoadCostPerUnit.Key:                "0",
	})
	defer restoreScoring()
	status, err = c.MixCoordClient.ResumeBalance(ctx, &querypb.ResumeBalanceRequest{})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.Require().Eventually(func() bool {
		first, err := c.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{NodeID: qn1.GetNodeID()})
		if merr.CheckRPCCall(first, err) != nil || len(first.GetSealedSegmentIDs()) == 0 {
			return false
		}
		second, err := c.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{NodeID: qn2.GetNodeID()})
		if merr.CheckRPCCall(second, err) != nil || len(second.GetSealedSegmentIDs()) == 0 {
			return false
		}
		// Retained Down views may temporarily duplicate physical resources.
		// Wait for disjoint final ownership covering every original segment.
		all := append(append([]int64(nil), first.GetSealedSegmentIDs()...), second.GetSealedSegmentIDs()...)
		return len(all) == len(expectedSegments) && len(lo.Uniq(all)) == len(all) && len(lo.Without(expectedSegments, all...)) == 0
	}, 30*time.Second, 100*time.Millisecond)

	restoreWAL := c.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().StreamingCfg.WALBalancerPolicyAllowRebalance.Key: "true",
	})
	defer restoreWAL()
	s.Require().Eventually(func() bool {
		channels, err := catalog.ListPChannel(ctx)
		if err != nil || len(channels) != len(before) {
			return false
		}
		owners := make(map[int64]int)
		for _, channel := range channels {
			if _, ok := terms[channel.GetChannel().GetName()]; !ok || channel.GetState() != streamingpb.PChannelMetaState_PCHANNEL_META_STATE_ASSIGNED {
				return false
			}
			owners[channel.GetNode().GetServerId()]++
		}
		return len(owners) == 2 && owners[sn1.GetNodeID()] > 0 && owners[sn2.GetNodeID()] > 0
	}, 30*time.Second, 100*time.Millisecond)
	assertRows()

	mlog.Info(context.TODO(), "==================")
	mlog.Info(context.TODO(), "==================")
	mlog.Info(context.TODO(), "TestManualRollingUpgrade succeed")
	mlog.Info(context.TODO(), "==================")
	mlog.Info(context.TODO(), "==================")
}

func (s *ManualRollingUpgradeSuite) TestSuspendNode() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestSuspendNode"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 3000
	insertRound := 5

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	if err != nil {
		mlog.Warn(context.TODO(), "createCollectionStatus fail reason", mlog.Err(err))
	}

	mlog.Info(context.TODO(), "CreateCollection result", mlog.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))
	mlog.Info(context.TODO(), "ShowCollections result", mlog.Any("showCollectionsResp", showCollectionsResp))

	// insert data, and flush generate segment
	vecFieldData := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	for i := range lo.Range(insertRound) {
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{vecFieldData},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.True(merr.Ok(insertResult.GetStatus()))
		mlog.Info(context.TODO(), "Insert succeed", mlog.Int("round", i+1))
		resp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
			DbName:          dbName,
			CollectionNames: []string{collectionName},
		})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
	}

	resp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
	s.WaitForFlush(ctx, resp.GetFlushCollSegIDs()[collectionName].GetData(), resp.GetCollFlushTs()[collectionName], dbName, collectionName)

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		mlog.Warn(context.TODO(), "createIndexStatus fail reason", mlog.Err(err))
	}

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)
	mlog.Info(context.TODO(), "Create index done")

	// add new querynode
	qn2 := s.Cluster.AddQueryNode()
	time.Sleep(5 * time.Second)

	// expected 2 querynode found
	resp3, err := s.Cluster.MixCoordClient.ListQueryNode(ctx, &querypb.ListQueryNodeRequest{})
	s.NoError(err)
	s.Require().Len(resp3.GetNodeInfos(), 2)

	// suspend Node
	resp2, err := s.Cluster.MixCoordClient.SuspendNode(ctx, &querypb.SuspendNodeRequest{
		NodeID: qn2.GetNodeID(),
	})
	s.Require().NoError(merr.CheckRPCCall(resp2, err))
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		status, err := s.Cluster.MixCoordClient.ResumeNode(cleanupCtx, &querypb.ResumeNodeRequest{NodeID: qn2.GetNodeID()})
		s.NoError(merr.CheckRPCCall(status, err))
	}()

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	if err != nil {
		mlog.Warn(context.TODO(), "LoadCollection fail reason", mlog.Err(err))
	}
	s.WaitForLoad(ctx, collectionName)
	mlog.Info(context.TODO(), "Load collection done")

	defer c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})

	// due to node has been suspended, no segment/channel will be loaded to this qn
	resp4, err := s.Cluster.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
		NodeID: qn2.GetNodeID(),
	})
	s.NoError(err)
	s.Len(resp4.GetChannelNames(), 0)
	s.Len(resp4.GetSealedSegmentIDs(), 0)

	// Resume the node, then evacuate the original node. A small shard need
	// not spread across all eligible QNs; evacuation proves qn2 can load again.
	resp5, err := s.Cluster.MixCoordClient.ResumeNode(ctx, &querypb.ResumeNodeRequest{
		NodeID: qn2.GetNodeID(),
	})
	s.NoError(err)
	s.True(merr.Ok(resp5))

	for _, node := range resp3.GetNodeInfos() {
		if node.GetID() == qn2.GetNodeID() {
			continue
		}
		status, err := s.Cluster.MixCoordClient.SuspendNode(ctx, &querypb.SuspendNodeRequest{NodeID: node.GetID()})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		defer func(nodeID int64) {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			status, err := s.Cluster.MixCoordClient.ResumeNode(cleanupCtx, &querypb.ResumeNodeRequest{NodeID: nodeID})
			s.NoError(merr.CheckRPCCall(status, err))
		}(node.GetID())
	}

	s.Require().Eventually(func() bool {
		resp, err := s.Cluster.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
			NodeID: qn2.GetNodeID(),
		})
		if merr.CheckRPCCall(resp, err) != nil || len(resp.GetSealedSegmentIDs()) == 0 {
			return false
		}
		replicas, err := c.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{
			DbName: dbName, CollectionName: collectionName, WithShardNodes: true,
		})
		if merr.CheckRPCCall(replicas, err) != nil || len(replicas.GetReplicas()) != 1 {
			return false
		}
		shards := replicas.GetReplicas()[0].GetShardReplicas()
		if len(shards) != 2 {
			return false
		}
		for _, shard := range shards {
			// The public replica API includes the StreamingNode leader as well as QNs.
			queryNodes := lo.Filter(shard.GetNodeIds(), func(id int64, _ int) bool { return id != shard.GetLeaderID() })
			if len(queryNodes) != 1 || queryNodes[0] != qn2.GetNodeID() {
				return false
			}
		}
		return true
	}, 30*time.Second, 1*time.Second)

	mlog.Info(context.TODO(), "==================")
	mlog.Info(context.TODO(), "==================")
	mlog.Info(context.TODO(), "TestSuspendNode succeed")
	mlog.Info(context.TODO(), "==================")
	mlog.Info(context.TODO(), "==================")
}

func TestManualRollingUpgrade(t *testing.T) {
	suite.Run(t, new(ManualRollingUpgradeSuite))
}
