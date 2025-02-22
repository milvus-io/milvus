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

package target

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	dim    = 128
	dbName = ""
)

type TargetTestSuit struct {
	integration.MiniClusterSuite
}

func (s *TargetTestSuit) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.GracefulStopTimeout.Key, "1")

	s.Require().NoError(s.SetupEmbedEtcd())
}

func (s *TargetTestSuit) initCollection(collectionName string, replica int, channelNum int, segmentNum int, segmentRowNum int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
		s.insertToCollection(ctx, dbName, collectionName, segmentRowNum, dim)
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

func (s *TargetTestSuit) insertToCollection(ctx context.Context, dbName string, collectionName string, rowCount int, dim int) {
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowCount, dim)
	hashKeys := integration.GenerateHashKeys(rowCount)
	insertResult, err := s.Cluster.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowCount),
	})
	s.NoError(err)
	s.True(merr.Ok(insertResult.Status))

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

func (s *TargetTestSuit) TestQueryCoordRestart() {
	name := "test_balance_" + funcutil.GenRandomStr()

	// generate 20 small segments here, which will make segment list changes by time
	s.initCollection(name, 1, 2, 2, 2000)

	ctx := context.Background()
	info, err := s.Cluster.Proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		Base:           commonpbutil.NewMsgBase(),
		CollectionName: name,
	})
	s.NoError(err)
	s.True(merr.Ok(info.GetStatus()))
	collectionID := info.GetCollectionID()

	// wait until all shards are ready
	// cause showCollections won't just wait all collection becomes loaded, proxy will use retry to block until all shard are ready
	s.Eventually(func() bool {
		resp, err := s.Cluster.QueryCoord.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
			Base:         commonpbutil.NewMsgBase(),
			CollectionID: collectionID,
		})
		return err == nil && merr.Ok(resp.GetStatus()) && len(resp.Shards) == 2
	}, 60*time.Second, 1*time.Second)

	// trigger old coord stop
	s.Cluster.StopQueryCoord()

	// keep insert, make segment list change every 3 seconds
	closeInsertCh := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-closeInsertCh:
				log.Info("insert to collection finished")
				return
			case <-time.After(time.Second):
				s.insertToCollection(ctx, dbName, name, 2000, dim)
				log.Info("insert 2000 rows to collection finished")
			}
		}
	}()

	// sleep 30s, wait new flushed segment generated
	time.Sleep(30 * time.Second)

	port, err := s.Cluster.GetAvailablePort()
	s.NoError(err)
	paramtable.Get().Save(paramtable.Get().QueryCoordGrpcServerCfg.Port.Key, fmt.Sprint(port))

	// start a new QC
	s.Cluster.StartQueryCoord()

	// after new QC become Active, expected the new target is ready immediately, and get shard leader success
	s.Eventually(func() bool {
		resp, err := s.Cluster.QueryCoord.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
		s.NoError(err)
		if resp.IsHealthy {
			resp, err := s.Cluster.QueryCoord.GetShardLeaders(ctx, &querypb.GetShardLeadersRequest{
				Base:         commonpbutil.NewMsgBase(),
				CollectionID: collectionID,
			})
			log.Info("resp", zap.Any("status", resp.GetStatus()), zap.Any("shards", resp.Shards))
			s.NoError(err)
			s.True(merr.Ok(resp.GetStatus()))

			return len(resp.Shards) == 2
		}
		return false
	}, 60*time.Second, 1*time.Second)

	close(closeInsertCh)
	wg.Wait()
}

func TestTarget(t *testing.T) {
	suite.Run(t, new(TargetTestSuit))
}
