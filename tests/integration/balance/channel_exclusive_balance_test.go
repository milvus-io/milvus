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
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	grpcquerynode "github.com/milvus-io/milvus/internal/distributed/querynode"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type ChannelExclusiveBalanceSuit struct {
	integration.MiniClusterSuite
}

func (s *ChannelExclusiveBalanceSuit) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")

	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, meta.ChannelLevelScoreBalancerName)
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.Key, "2")

	// disable compaction
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableCompaction.Key, "false")

	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *ChannelExclusiveBalanceSuit) TearDownSuite() {
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.EnableCompaction.Key)
	s.MiniClusterSuite.TearDownSuite()
}

func (s *ChannelExclusiveBalanceSuit) initCollection(collectionName string, replica int, channelNum int, segmentNum int, segmentRowNum int, segmentDeleteNum int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		dim    = 128
		dbName = ""
	)

	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := s.Cluster.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      int32(channelNum),
	})
	s.NoError(err)
	s.True(merr.Ok(createCollectionStatus))

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := s.Cluster.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.Status))
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	for i := 0; i < segmentNum; i++ {
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, segmentRowNum, dim)
		hashKeys := integration.GenerateHashKeys(segmentRowNum)
		insertResult, err := s.Cluster.Proxy.Insert(ctx, &milvuspb.InsertRequest{
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
			log.Info("========================delete expr==================",
				zap.Int("length of pk", len(pks)),
			)

			expr := fmt.Sprintf("%s in [%s]", integration.Int64Field, strings.Join(lo.Map(pks, func(pk int64, _ int) string { return strconv.FormatInt(pk, 10) }), ","))

			deleteResp, err := s.Cluster.Proxy.Delete(ctx, &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				Expr:           expr,
			})
			s.Require().NoError(err)
			s.Require().True(merr.Ok(deleteResp.GetStatus()))
			s.Require().EqualValues(len(pks), deleteResp.GetDeleteCnt())
		}

		// flush
		flushResp, err := s.Cluster.Proxy.Flush(ctx, &milvuspb.FlushRequest{
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
	createIndexStatus, err := s.Cluster.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	s.True(merr.Ok(createIndexStatus))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	for i := 1; i < replica; i++ {
		s.Cluster.AddQueryNode()
	}

	// load
	loadStatus, err := s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		ReplicaNumber:  int32(replica),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.True(merr.Ok(loadStatus))
	s.WaitForLoad(ctx, collectionName)
	log.Info("initCollection Done")
}

func (s *ChannelExclusiveBalanceSuit) TestBalanceOnSingleReplica() {
	name := "test_balance_" + funcutil.GenRandomStr()
	channelCount := 5
	channelNodeCount := 3

	s.initCollection(name, 1, channelCount, 5, 2000, 0)

	ctx := context.Background()
	qnList := make([]*grpcquerynode.Server, 0)
	// add a querynode, expected balance happens
	for i := 1; i < channelCount*channelNodeCount; i++ {
		qn := s.Cluster.AddQueryNode()
		qnList = append(qnList, qn)
	}

	// expected each channel own 3 exclusive node
	s.Eventually(func() bool {
		channelNodeCounter := make(map[string]int)
		for _, node := range s.Cluster.GetAllQueryNodes() {
			resp1, err := node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			s.NoError(err)
			s.True(merr.Ok(resp1.GetStatus()))

			log.Info("resp", zap.Any("segments", resp1.Segments))
			if channel, ok := s.isSameChannel(resp1.GetSegments()); ok {
				channelNodeCounter[channel] += 1
			}
		}

		log.Info("dist", zap.Any("nodes", channelNodeCounter))
		nodeCountMatch := true
		for _, cnt := range channelNodeCounter {
			if cnt != channelNodeCount {
				nodeCountMatch = false
				break
			}
		}

		return nodeCountMatch
	}, 60*time.Second, 3*time.Second)

	// add two new query node and stop two old querynode
	s.Cluster.AddQueryNode()
	s.Cluster.AddQueryNode()
	qnList[0].Stop()
	qnList[1].Stop()

	// expected each channel own 3 exclusive node
	s.Eventually(func() bool {
		channelNodeCounter := make(map[string]int)
		for _, node := range s.Cluster.GetAllQueryNodes() {
			resp1, err := node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
			if err != nil && merr.Ok(resp1.GetStatus()) {
				log.Info("resp", zap.Any("segments", resp1.Segments))
				if channel, ok := s.isSameChannel(resp1.GetSegments()); ok {
					channelNodeCounter[channel] += 1
				}
			}
		}

		log.Info("dist", zap.Any("nodes", channelNodeCounter))
		nodeCountMatch := true
		for _, cnt := range channelNodeCounter {
			if cnt != channelNodeCount {
				nodeCountMatch = false
				break
			}
		}

		return nodeCountMatch
	}, 60*time.Second, 3*time.Second)
}

func (s *ChannelExclusiveBalanceSuit) isSameChannel(segments []*querypb.SegmentVersionInfo) (string, bool) {
	if len(segments) == 0 {
		return "", false
	}

	channelName := segments[0].Channel

	_, find := lo.Find(segments, func(segment *querypb.SegmentVersionInfo) bool {
		return segment.Channel != channelName
	})

	return channelName, !find
}

func TestChannelExclusiveBalance(t *testing.T) {
	suite.Run(t, new(ChannelExclusiveBalanceSuit))
}
