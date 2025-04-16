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
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type ManualRollingUpgradeSuite struct {
	integration.MiniClusterSuite
}

func (s *ManualRollingUpgradeSuite) SetupSuite() {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.BalanceCheckInterval.Key, "2000")

	rand.Seed(time.Now().UnixNano())
	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *ManualRollingUpgradeSuite) TearDownSuite() {
	params := paramtable.Get()
	params.Reset(params.QueryCoordCfg.BalanceCheckInterval.Key)

	s.TearDownEmbedEtcd()
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

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	if err != nil {
		log.Warn("createCollectionStatus fail reason", zap.Error(err))
	}

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	// insert data, and flush generate segment
	pkFieldData := integration.NewInt64FieldData(integration.Int64Field, rowNum)
	hashKeys := integration.GenerateHashKeys(rowNum)
	for i := range lo.Range(insertRound) {
		insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{pkFieldData, pkFieldData},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.False(merr.Ok(insertResult.GetStatus()))
		log.Info("Insert succeed", zap.Int("round", i+1))
		resp, err := s.Cluster.Proxy.Flush(ctx, &milvuspb.FlushRequest{
			DbName:          dbName,
			CollectionNames: []string{collectionName},
		})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
	}

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)
	log.Info("Create index done")

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	if err != nil {
		log.Warn("LoadCollection fail reason", zap.Error(err))
	}
	s.WaitForLoad(ctx, collectionName)
	log.Info("Load collection done")

	// suspend balance
	resp2, err := s.Cluster.MixCoord.SuspendBalance(ctx, &querypb.SuspendBalanceRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp2))

	// get origin qn
	qnServer1 := s.Cluster.QueryNode
	qn1 := qnServer1.GetQueryNode()

	// add new querynode
	qnSever2 := s.Cluster.AddQueryNode()
	time.Sleep(5 * time.Second)
	qn2 := qnSever2.GetQueryNode()

	// expected 2 querynode found
	resp3, err := s.Cluster.MixCoordClient.ListQueryNode(ctx, &querypb.ListQueryNodeRequest{})
	s.NoError(err)
	s.Len(resp3.GetNodeInfos(), 2)

	// due to balance has been suspended, qn2 won't have any segment/channel distribution
	resp4, err := s.Cluster.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
		NodeID: qn2.GetNodeID(),
	})
	s.NoError(err)
	s.Len(resp4.GetChannelNames(), 0)
	s.Len(resp4.GetSealedSegmentIDs(), 0)

	resp5, err := s.Cluster.MixCoordClient.TransferChannel(ctx, &querypb.TransferChannelRequest{
		SourceNodeID: qn1.GetNodeID(),
		TargetNodeID: qn2.GetNodeID(),
		TransferAll:  true,
	})
	s.NoError(err)
	s.True(merr.Ok(resp5))

	// wait for transfer channel done
	s.Eventually(func() bool {
		resp, err := s.Cluster.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
			NodeID: qn1.GetNodeID(),
		})
		s.NoError(err)
		return len(resp.GetChannelNames()) == 0
	}, 10*time.Second, 1*time.Second)

	// test transfer segment
	resp6, err := s.Cluster.MixCoordClient.TransferSegment(ctx, &querypb.TransferSegmentRequest{
		SourceNodeID: qn1.GetNodeID(),
		TargetNodeID: qn2.GetNodeID(),
		TransferAll:  true,
	})
	s.NoError(err)
	s.True(merr.Ok(resp6))

	// wait for transfer segment done
	s.Eventually(func() bool {
		resp, err := s.Cluster.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
			NodeID: qn1.GetNodeID(),
		})
		s.NoError(err)
		return len(resp.GetSealedSegmentIDs()) == 0
	}, 10*time.Second, 1*time.Second)

	// resume balance, segment/channel will be balance to qn1
	resp7, err := s.Cluster.MixCoord.ResumeBalance(ctx, &querypb.ResumeBalanceRequest{})
	s.NoError(err)
	s.True(merr.Ok(resp7))

	s.Eventually(func() bool {
		resp, err := s.Cluster.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
			NodeID: qn1.GetNodeID(),
		})
		s.NoError(err)
		return len(resp.GetSealedSegmentIDs()) > 0 || len(resp.GetChannelNames()) > 0
	}, 10*time.Second, 1*time.Second)

	log.Info("==================")
	log.Info("==================")
	log.Info("TestManualRollingUpgrade succeed")
	log.Info("==================")
	log.Info("==================")
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

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	if err != nil {
		log.Warn("createCollectionStatus fail reason", zap.Error(err))
	}

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	// insert data, and flush generate segment
	pkFieldData := integration.NewInt64FieldData(integration.Int64Field, rowNum)
	hashKeys := integration.GenerateHashKeys(rowNum)
	for i := range lo.Range(insertRound) {
		insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{pkFieldData, pkFieldData},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.False(merr.Ok(insertResult.GetStatus()))
		log.Info("Insert succeed", zap.Int("round", i+1))
		resp, err := s.Cluster.Proxy.Flush(ctx, &milvuspb.FlushRequest{
			DbName:          dbName,
			CollectionNames: []string{collectionName},
		})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
	}

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)
	log.Info("Create index done")

	// add new querynode
	qnSever2 := s.Cluster.AddQueryNode()
	time.Sleep(5 * time.Second)
	qn2 := qnSever2.GetQueryNode()

	// expected 2 querynode found
	resp3, err := s.Cluster.MixCoordClient.ListQueryNode(ctx, &querypb.ListQueryNodeRequest{})
	s.NoError(err)
	s.Len(resp3.GetNodeInfos(), 2)

	// suspend Node
	resp2, err := s.Cluster.MixCoord.SuspendNode(ctx, &querypb.SuspendNodeRequest{
		NodeID: qn2.GetNodeID(),
	})
	s.NoError(err)
	s.True(merr.Ok(resp2))

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	if err != nil {
		log.Warn("LoadCollection fail reason", zap.Error(err))
	}
	s.WaitForLoad(ctx, collectionName)
	log.Info("Load collection done")

	// due to node has been suspended, no segment/channel will be loaded to this qn
	resp4, err := s.Cluster.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
		NodeID: qn2.GetNodeID(),
	})
	s.NoError(err)
	s.Len(resp4.GetChannelNames(), 0)
	s.Len(resp4.GetSealedSegmentIDs(), 0)

	// resume node, segment/channel will be balance to qn2
	resp5, err := s.Cluster.MixCoord.ResumeNode(ctx, &querypb.ResumeNodeRequest{
		NodeID: qn2.GetNodeID(),
	})
	s.NoError(err)
	s.True(merr.Ok(resp5))

	s.Eventually(func() bool {
		resp, err := s.Cluster.MixCoordClient.GetQueryNodeDistribution(ctx, &querypb.GetQueryNodeDistributionRequest{
			NodeID: qn2.GetNodeID(),
		})
		s.NoError(err)
		return len(resp.GetSealedSegmentIDs()) > 0 || len(resp.GetChannelNames()) > 0
	}, 10*time.Second, 1*time.Second)

	log.Info("==================")
	log.Info("==================")
	log.Info("TestSuspendNode succeed")
	log.Info("==================")
	log.Info("==================")
}

func TestManualRollingUpgrade(t *testing.T) {
	suite.Run(t, new(ManualRollingUpgradeSuite))
}
