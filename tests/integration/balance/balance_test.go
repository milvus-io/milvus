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
	// These fixtures contain only a few thousand rows. Use a matching scoring
	// scale; the production tolerance intentionally ignores loads this small.
	cfg := &paramtable.Get().QueryViewCfg
	s.WithMilvusConfig(cfg.BalancerReconcileInterval.Key, "100ms")
	s.WithMilvusConfig(cfg.BalancerMinScoreGain.Key, "0.0001")
	s.WithMilvusConfig(cfg.BalancerTargetLogicalSizePerNode.Key, "1000")
	s.WithMilvusConfig(cfg.BalancerResourceGroupAbsoluteTolerance.Key, "1")
	s.WithMilvusConfig(cfg.BalancerResourceGroupRelativeTolerance.Key, "0")
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

			pks := insertResult.GetIDs().GetIntId().GetData()[:segmentDeleteNum]
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
		s.Require().NoError(merr.CheckRPCCall(flushResp, err))
		segmentIDs, has := flushResp.GetFlushCollSegIDs()[collectionName]
		ids := segmentIDs.GetData()
		s.Require().NotEmpty(ids)
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

// distribution reads serving QueryViews. Preparing resources and retained Down
// views must not inflate either the segment or shard counts.
func (s *BalanceTestSuit) distribution(name string) (map[int64]int, map[int64]int, bool) {
	ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 5*time.Second)
	defer cancel()
	segments, err := s.Cluster.MilvusClient.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{CollectionName: name})
	if merr.CheckRPCCall(segments, err) != nil {
		return nil, nil, false
	}
	replicas, err := s.Cluster.MilvusClient.GetReplicas(ctx, &milvuspb.GetReplicasRequest{CollectionName: name, WithShardNodes: true})
	if merr.CheckRPCCall(replicas, err) != nil {
		return nil, nil, false
	}
	byQN, bySN := make(map[int64]int), make(map[int64]int)
	for _, segment := range segments.GetInfos() {
		for _, node := range segment.GetNodeIds() {
			byQN[node]++
		}
	}
	for _, replica := range replicas.GetReplicas() {
		for _, shard := range replica.GetShardReplicas() {
			if shard.GetLeaderID() == 0 {
				return nil, nil, false // WAL assignment is still recovering.
			}
			bySN[shard.GetLeaderID()]++
		}
	}
	mlog.Info(ctx, "serving balance distribution", mlog.Any("segments", byQN), mlog.Any("shards", bySN))
	return byQN, bySN, true
}

func (s *BalanceTestSuit) assertTotals(name string, segmentCount, shardCount int) {
	s.Require().Eventually(func() bool {
		segments, shards, ok := s.distribution(name)
		return ok && lo.Sum(lo.Values(segments)) == segmentCount && lo.Sum(lo.Values(shards)) == shardCount
	}, 30*time.Second, 200*time.Millisecond)
}

func (s *BalanceTestSuit) TestBalanceOnSingleReplica() {
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 2, 2000, 500)
	qn := s.Cluster.AddQueryNode()
	sn := s.Cluster.AddStreamingNode()
	s.Require().Eventually(func() bool {
		segments, shards, ok := s.distribution(name)
		return ok && segments[qn.GetNodeID()] == 2 && shards[sn.GetNodeID()] == 1
	}, 60*time.Second, 200*time.Millisecond)
	s.assertTotals(name, 4, 2)
}

func (s *BalanceTestSuit) TestBalanceOnMultiReplica() {
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 2, 2, 2, 2000, 500)
	qn1 := s.Cluster.AddQueryNode()
	s.Cluster.AddStreamingNode()
	qn2 := s.Cluster.AddQueryNode()
	s.Cluster.AddStreamingNode()
	s.Require().Eventually(func() bool {
		segments, _, ok := s.distribution(name)
		return ok && segments[qn1.GetNodeID()] == 2 && segments[qn2.GetNodeID()] == 2
	}, 60*time.Second, 200*time.Millisecond)
	s.assertTotals(name, 8, 4)
}

func (s *BalanceTestSuit) TestNodeDown() {
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 15, 2000, 500)
	qn1 := s.Cluster.AddQueryNode()
	qn2 := s.Cluster.AddQueryNode()
	s.Require().Eventually(func() bool {
		segments, _, ok := s.distribution(name)
		return ok && segments[qn1.GetNodeID()] == 10 && segments[qn2.GetNodeID()] == 10
	}, 60*time.Second, 200*time.Millisecond)
	qn1.Stop()
	s.Require().Eventually(func() bool {
		segments, _, ok := s.distribution(name)
		return ok && segments[qn1.GetNodeID()] == 0 && segments[qn2.GetNodeID()] == 15
	}, 60*time.Second, 200*time.Millisecond)
	s.WaitForLoad(s.Cluster.GetContext(), name)
	s.assertTotals(name, 30, 2)
}

func (s *BalanceTestSuit) TestConcurrentBalanceChannelAndSegment() {
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 4, 10, 2000, 500)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	failures, successes := atomic.NewInt64(0), atomic.NewInt64(0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(s.Cluster.GetContext(), 10*time.Second)
			result, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
				CollectionName: name, OutputFields: []string{"count(*)"},
			})
			cancel()
			if merr.CheckRPCCall(result, err) != nil || len(result.GetFieldsData()) != 1 ||
				len(result.GetFieldsData()[0].GetScalars().GetLongData().GetData()) != 1 ||
				result.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0] != 15000 {
				if failures.Inc() == 1 {
					mlog.Warn(ctx, "query during balance failed", mlog.Err(err), mlog.Any("result", result))
				}
			} else {
				successes.Inc()
			}
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
		s.Zero(failures.Load(), "handoff must preserve successful queries and row counts")
		s.Positive(successes.Load())
	}()
	qn := s.Cluster.AddQueryNode()
	sn := s.Cluster.AddStreamingNode()
	s.Require().Eventually(func() bool {
		segments, shards, ok := s.distribution(name)
		return ok && segments[qn.GetNodeID()] == 20 && shards[sn.GetNodeID()] == 2
	}, 60*time.Second, 200*time.Millisecond)
	s.assertTotals(name, 40, 4)
}

func (s *BalanceTestSuit) TestOptimizationDisabledThenEnabled() {
	// The legacy MultipleTargetBalancer switch does not control QueryView.
	// Exercise its live optimization switch instead, keeping forced placement.
	restore := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().QueryViewCfg.BalancerOptimizationEnabled.Key: "false",
	})
	defer restore()
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 2, 2000, 500)
	qn := s.Cluster.AddQueryNode()
	s.Require().Never(func() bool {
		segments, _, ok := s.distribution(name)
		return ok && segments[qn.GetNodeID()] != 0
	}, 3*time.Second, 200*time.Millisecond)
	restore()
	s.Require().Eventually(func() bool {
		segments, _, ok := s.distribution(name)
		return ok && segments[qn.GetNodeID()] == 2
	}, 60*time.Second, 200*time.Millisecond)
	s.assertTotals(name, 4, 2)
}

func TestBalance(t *testing.T) {
	suite.Run(t, new(BalanceTestSuit))
}
