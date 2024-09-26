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
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dim    = 128
	dbName = ""
)

type ReplicaTestSuit struct {
	integration.MiniClusterSuite
}

func (s *ReplicaTestSuit) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")
	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *ReplicaTestSuit) initCollection(collectionName string, replica int, channelNum int, segmentNum int, segmentRowNum int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.CreateCollectionWithConfiguration(ctx, &integration.CreateCollectionConfig{
		DBName:           dbName,
		Dim:              dim,
		CollectionName:   collectionName,
		ChannelNum:       channelNum,
		SegmentNum:       segmentNum,
		RowNumPerSegment: segmentRowNum,
	})

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

func (s *ReplicaTestSuit) TestNodeDownOnSingleReplica() {
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 1, 2, 2, 2000)

	ctx := context.Background()

	qn := s.Cluster.AddQueryNode()
	// check segment number on new querynode
	s.Eventually(func() bool {
		resp, err := qn.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		return len(resp.Channels) == 1 && len(resp.Segments) == 2
	}, 30*time.Second, 1*time.Second)

	stopSearchCh := make(chan struct{})
	failCounter := atomic.NewInt64(0)
	go func() {
		for {
			select {
			case <-stopSearchCh:
				log.Info("stop search")
				return
			case <-time.After(time.Second):
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

				searchResult, err := s.Cluster.Proxy.Search(ctx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					failCounter.Inc()
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))

	// stop qn in single replica expected got search failures
	qn.Stop()
	s.Eventually(func() bool {
		return failCounter.Load() > 0
	}, 30*time.Second, 1*time.Second)

	close(stopSearchCh)
}

func (s *ReplicaTestSuit) TestNodeDownOnMultiReplica() {
	ctx := context.Background()

	// init collection with 2 channel, each channel has 2 segment, each segment has 2000 row
	// and load it with 2 replicas on 2 nodes.
	// then we add 2 query node, after balance happens, expected each node have 1 channel and 2 segments
	name := "test_balance_" + funcutil.GenRandomStr()
	s.initCollection(name, 2, 2, 2, 2000)

	resp, err := s.Cluster.Proxy.GetReplicas(ctx, &milvuspb.GetReplicasRequest{CollectionName: name})
	s.NoError(err)
	s.Len(resp.Replicas, 2)

	// add a querynode, expected balance happens
	qn1 := s.Cluster.AddQueryNode()
	qn2 := s.Cluster.AddQueryNode()

	// check segment num on new query node
	s.Eventually(func() bool {
		resp, err := qn1.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		return len(resp.Channels) == 1 && len(resp.Segments) == 2
	}, 30*time.Second, 1*time.Second)

	s.Eventually(func() bool {
		resp, err := qn2.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		s.NoError(err)
		return len(resp.Channels) == 1 && len(resp.Segments) == 2
	}, 30*time.Second, 1*time.Second)

	stopSearchCh := make(chan struct{})
	failCounter := atomic.NewInt64(0)
	go func() {
		for {
			select {
			case <-stopSearchCh:
				log.Info("stop search")
				return
			case <-time.After(time.Second):
				expr := fmt.Sprintf("%s > 0", integration.Int64Field)
				nq := 10
				topk := 10
				roundDecimal := -1

				params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
				searchReq := integration.ConstructSearchRequest("", name, expr,
					integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

				searchResult, err := s.Cluster.Proxy.Search(ctx, searchReq)

				err = merr.CheckRPCCall(searchResult, err)
				if err != nil {
					failCounter.Inc()
				}
			}
		}
	}()

	time.Sleep(10 * time.Second)
	s.Equal(failCounter.Load(), int64(0))

	// stop qn in multi replica replica expected no search failures
	qn1.Stop()
	time.Sleep(20 * time.Second)
	s.Equal(failCounter.Load(), int64(0))

	close(stopSearchCh)
}

func TestReplicas(t *testing.T) {
	suite.Run(t, new(ReplicaTestSuit))
}
