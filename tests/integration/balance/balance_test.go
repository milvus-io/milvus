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

package balance

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type BalanceTestSuit struct {
	integration.MiniClusterSuite
}

func (s *BalanceTestSuit) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, "4")
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "100")
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.AutoBalanceInterval.Key, "100")
	s.WithMilvusConfig(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")
	s.WithMilvusConfig(paramtable.Get().StreamingCfg.WALBalancerPolicyMinRebalanceIntervalThreshold.Key, "1ms")

	// disable compaction
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableCompaction.Key, "false")

	s.WithOptions(integration.WithDropAllCollectionsWhenTestTearDown())
	s.MiniClusterSuite.SetupSuite()
}

func (s *BalanceTestSuit) initCollection(collectionName string, replica int, channelNum int, segmentNum int, segmentRowNum int, segmentDeleteNum int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		dim    = 128
		dbName = ""
	)

	for i := 1; i < replica; i++ {
		s.Cluster.AddStreamingNode()
		s.Cluster.AddQueryNode()
	}

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      int32(channelNum),
	})
	s.NoError(err)
	s.True(merr.Ok(createCollectionStatus))

	mlog.Info(context.TODO(), "CreateCollection result", mlog.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := s.Cluster.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.Status))
	mlog.Info(context.TODO(), "ShowCollections result", mlog.Any("showCollectionsResp", showCollectionsResp))

	for i := 0; i < segmentNum; i++ {
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, segmentRowNum, dim)
		hashKeys := integration.GenerateHashKeys(segmentRowNum)
		insertResult, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(segmentRowNum),
		})
		s.NoError(err)
		s.True(merr.Ok(insertResult.Status))

		if segmentDeleteNum > 0 {
			if segmentDeleteNum > segmentRowNum {
				segmentDeleteNum = segmentRowNum
			}

			pks := insertResult.GetIDs().GetIntId().GetData()
			mlog.Info(context.TODO(), "========================delete expr==================",
				mlog.Int("length of pk", len(pks)),
			)

			expr := fmt.Sprintf("%s in [%s]", integration.Int64Field, strings.Join(lo.Map(pks, func(pk int64, _ int) string { return strconv.FormatInt(pk, 10) }), ","))

			deleteResp, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				Expr:           expr,
			})
			s.Require().NoError(err)
			s.Require().True(merr.Ok(deleteResp.GetStatus()))
			s.Require().EqualValues(len(pks), deleteResp.GetDeleteCnt())
		}

		// flush
		flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
			DbName:          dbName,
			CollectionNames: []string{collectionName},
		})
		s.NoError(err)
		segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
		ids := segmentIDs.GetData()
		s.Require().NotEmpty(segmentIDs)
		s.Require().True(has)
		flushTs, has := flushResp.GetCollFlushTs()[collectionName]
		s.True(has)
		s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)
	}

	// create index
	createIndexStatus, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	s.True(merr.Ok(createIndexStatus))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)
	mlog.Info(context.TODO(), "index create done")

	// load
	loadStatus, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  int32(replica),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.True(merr.Ok(loadStatus))
	s.WaitForLoad(ctx, collectionName)
	mlog.Info(context.TODO(), "initCollection Done")
}

func (s *BalanceTestSuit) TestBalanceOnSingleReplica() {
	testBalanceOnSingleReplica(s)
}

func testBalanceOnSingleReplica(s *BalanceTestSuit) {
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 2, 2000, 500)

	ctx := context.Background()
	// add a querynode, expected balance happens
	qn := s.Cluster.AddQueryNode().MustGetClient(ctx)
	sn := s.Cluster.AddStreamingNode().MustGetClient(ctx)

	// check segment number on new querynode
	s.Eventually(func() bool {
		resp, err := qn.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		resp2, err := sn.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		mlog.Info(context.TODO(), "balance on single replica", mlog.Int("channel", len(resp2.Channels)), mlog.Int("segments", len(resp.Segments)))
		return len(resp2.Channels) == 1 && len(resp.Segments) == 2
	}, 30*time.Second, 1*time.Second)

	// check total segment number and total channel number
	s.Eventually(func() bool {
		segNum, chNum := 0, 0
		for _, node := range s.Cluster.GetAllStreamingAndQueryNodesClient() {
			resp1, err := node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp1.GetStatus()))
			segNum += len(resp1.Segments)
			chNum += len(resp1.Channels)
		}
		return segNum == 4 && chNum == 2
	}, 30*time.Second, 1*time.Second)
}

func (s *BalanceTestSuit) TestBalanceOnMultiReplica() {
	ctx := context.Background()

	// init collection with 2 channel, each channel has 4 segment, each segment has 2000 row
	// and load it with 2 replicas on 2 nodes.
	// then we add 2 query node, after balance happens, expected each node have 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 2, 2, 2, 2000, 500)

	resp, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{CollectionName: name})
	s.NoError(err)
	s.Len(resp.Replicas, 2)

	// add a querynode, expected balance happens
	qn1 := s.Cluster.AddQueryNode().MustGetClient(ctx)
	sn1 := s.Cluster.AddStreamingNode().MustGetClient(ctx)
	qn2 := s.Cluster.AddQueryNode().MustGetClient(ctx)
	sn2 := s.Cluster.AddStreamingNode().MustGetClient(ctx)

	// check segment num on new query node
	s.Eventually(func() bool {
		resp, err := qn1.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		resp2, err := sn1.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		resp3, err := qn2.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		resp4, err := sn2.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		mlog.Info(context.TODO(), "balance on multi replica",
			mlog.Int("channel1", len(resp2.Channels)), mlog.Int("segments1", len(resp.Segments)),
			mlog.Int("channel2", len(resp4.Channels)), mlog.Int("segments2", len(resp3.Segments)))
		// TODO:https://github.com/milvus-io/milvus/issues/42966
		// return len(resp2.Channels) == 1 && len(resp.Segments) == 2 &&
		// len(resp4.Channels) == 1 && len(resp3.Segments) == 2
		return len(resp.Segments) == 2 && len(resp3.Segments) == 2
	}, 60*time.Second, 1*time.Second)

	// check total segment number and total channel number
	s.Eventually(func() bool {
		segNum, chNum := 0, 0
		for _, node := range s.Cluster.GetAllStreamingAndQueryNodesClient() {
			resp1, err := node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp1.GetStatus()))
			segNum += len(resp1.Segments)
			chNum += len(resp1.Channels)
			mlog.Info(context.TODO(), "balance on multi replica",
				mlog.Int("channel", len(resp1.Channels)), mlog.Int("segments", len(resp1.Segments)))
		}
		// TODO:https://github.com/milvus-io/milvus/issues/42966
		// return segNum == 8 && chNum == 4
		return segNum == 8 && chNum >= 4
	}, 30*time.Second, 1*time.Second)
}

func (s *BalanceTestSuit) TestNodeDown() {
	ctx := context.Background()

	// disable balance channel
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryCoordCfg.AutoBalanceChannel.Key:    "false",
		paramtable.Get().QueryCoordCfg.EnableStoppingBalance.Key: "false",
	})
	defer revertGuard()

	// init collection with 2 channel, each channel has 15 segment, each segment has 2000 row
	// and load it with 1 replicas on 2 nodes.
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 15, 2000, 500)

	// then we add 2 query node, after balance happens, expected each node have 10 segments
	qn1 := s.Cluster.AddQueryNode()
	qn2 := s.Cluster.AddQueryNode().MustGetClient(ctx)

	// check segment num on new query node
	s.Eventually(func() bool {
		resp, err := qn1.MustGetClient(ctx).GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		mlog.Info(context.TODO(), "balance when node down", mlog.Any("channel", resp.Channels), mlog.Any("segments", len(resp.Segments)))
		return len(resp.Channels) == 0 && len(resp.Segments) >= 10
	}, 30*time.Second, 1*time.Second)

	s.Eventually(func() bool {
		resp, err := qn2.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		mlog.Info(context.TODO(), "balance when node down", mlog.Any("channel", resp.Channels), mlog.Any("segments", resp.Segments))
		return len(resp.Channels) == 0 && len(resp.Segments) >= 10
	}, 30*time.Second, 1*time.Second)

	// then we force stop qn1 and resume balance channel, let balance channel and load segment happens concurrently on qn2
	revertGuard()
	time.Sleep(1 * time.Second)
	qn1.Stop()

	info, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base:           commonpbutil.NewMsgBase(),
		CollectionName: name,
	})
	s.NoError(err)
	s.True(merr.Ok(info.GetStatus()))
	collectionID := info.GetCollectionID()

	// expected channel and segment concurrent move to qn2
	s.Eventually(func() bool {
		resp, err := qn2.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		mlog.Info(context.TODO(), "balance when node down", mlog.Any("channel", resp.Channels), mlog.Any("segments", resp.Segments))
		return len(resp.Channels) == 0 && len(resp.Segments) == 15
	}, 30*time.Second, 1*time.Second)

	// expect all delegator will recover to healthy
	s.Eventually(func() bool {
		resp, err := s.Cluster.MixCoordClient.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
			Base:         commonpbutil.NewMsgBase(),
			CollectionID: collectionID,
		})
		s.NoError(err)
		return len(resp.Shards) == 2
	}, 30*time.Second, 1*time.Second)
}

func (s *BalanceTestSuit) TestConcurrentBalanceChannelAndSegment() {
	ctx := context.Background()

	// init collection with 10 channel, each channel has 10 segment, each segment has 2000 row
	// and load it with 1 replicas on 2 nodes.
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 4, 10, 2000, 500)

	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.AutoBalanceChannel.Key, "false")

	stopSearchCh := make(chan struct{})
	failCounter := atomic.NewInt64(0)

	// keep query during balance
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopSearchCh:
				mlog.Info(context.TODO(), "stop search")
				return
			default:
				queryResult, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
					DbName:         "",
					CollectionName: name,
					Expr:           "",
					OutputFields:   []string{"count(*)"},
				})

				if err := merr.CheckRPCCall(queryResult.GetStatus(), err); err != nil {
					mlog.Info(context.TODO(), "query failed", mlog.Err(err))
					failCounter.Inc()
				}
			}
		}
	}()

	for _, qn := range s.Cluster.GetAllQueryNodes() {
		resp, err := qn.MustGetClient(ctx).GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		mlog.Info(context.TODO(), "segments on query node before balance", mlog.FieldNodeID(qn.GetNodeID()), mlog.Int("channel", len(resp.Channels)), mlog.Int("segments", len(resp.Segments)))
	}
	for _, sn := range s.Cluster.GetAllStreamingNodes() {
		resp, err := sn.MustGetClient(ctx).GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		mlog.Info(context.TODO(), "channel on streaming node before balance", mlog.FieldNodeID(sn.GetNodeID()), mlog.Int("channel", len(resp.Channels)), mlog.Int("segments", len(resp.Segments)))
	}

	// then we add 1 query node, expected segment and channel will be move to new query node concurrently
	qn1 := s.Cluster.AddQueryNode()
	sn1 := s.Cluster.AddStreamingNode()

	// wait until balance channel finished
	s.Eventually(func() bool {
		for _, qn := range s.Cluster.GetAllQueryNodes() {
			resp, err := qn.MustGetClient(ctx).GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp.GetStatus()))
			mlog.Info(context.TODO(), "segments on query node", mlog.FieldNodeID(qn.GetNodeID()), mlog.Int("channel", len(resp.Channels)), mlog.Int("segments", len(resp.Segments)))
		}
		for _, sn := range s.Cluster.GetAllStreamingNodes() {
			resp, err := sn.MustGetClient(ctx).GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp.GetStatus()))
			mlog.Info(context.TODO(), "channel on streaming node", mlog.FieldNodeID(sn.GetNodeID()), mlog.Int("channel", len(resp.Channels)), mlog.Int("segments", len(resp.Segments)))
		}

		resp, err := qn1.MustGetClient(ctx).GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		resp2, err := sn1.MustGetClient(ctx).GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		mlog.Info(context.TODO(), "concurrent balance channel and segment", mlog.Int("channel1", len(resp2.Channels)), mlog.Int("segments1", len(resp.Segments)))
		return len(resp2.Channels) == 2 && len(resp.Segments) >= 20
	}, 30*time.Second, 1*time.Second)

	// expected concurrent balance will execute successfully, shard serviceable won't be broken
	close(stopSearchCh)
	wg.Wait()
	s.Equal(int64(0), failCounter.Load())
}

func (s *BalanceTestSuit) TestMultiTargetBalancePolicy() {
	// Set balance policy to MultipleTargetBalancer
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryCoordCfg.Balancer.Key: "MultipleTargetBalancer",
	})
	defer revertGuard()

	testBalanceOnSingleReplica(s)
}

func TestBalance(t *testing.T) {
	suite.Run(t, new(BalanceTestSuit))
}
