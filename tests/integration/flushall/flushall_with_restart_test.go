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

package flushall

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const dmlChannelNum = 2

type FlushAllWithRestartSuite struct {
	FlushAllSuite
}

func (s *FlushAllWithRestartSuite) SetupSuite() {
	s.WithMilvusConfig(paramtable.Get().RootCoordCfg.DmlChannelNum.Key, "2")
	s.WithMilvusConfig(paramtable.Get().StreamingCfg.WALBalancerPolicyMinRebalanceIntervalThreshold.Key, "1ms")
	s.MiniClusterSuite.SetupSuite()
}

func (s *FlushAllWithRestartSuite) TestFlushAllWithStreamingNodeRestart() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		rowNum = 1000
	)

	// Use the cluster pchannel count as shard number.
	shardsNum := dmlChannelNum
	collectionName := "TestFlushAllWithRestart_" + funcutil.GenRandomStr()

	// Create collection with shard count equal to pchannel count.
	schema := integration.ConstructSchema(collectionName, dim, false)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      int32(shardsNum),
	})
	s.NoError(merr.CheckRPCCall(createStatus, err))

	// Step 1: Insert 1000 rows and FlushAll.
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, 0)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkColumn, fVecColumn},
		NumRows:        uint32(rowNum),
	})
	s.NoError(merr.CheckRPCCall(insertResult, err))

	flushAllResp, err := c.MilvusClient.FlushAll(ctx, &milvuspb.FlushAllRequest{})
	s.NoError(merr.CheckRPCCall(flushAllResp, err))
	s.WaitForFlushAll(ctx, flushAllResp.GetFlushAllMsgs())
	log.Info("first FlushAll completed")

	// Step 2: Restart all streaming nodes, then insert another 1000 rows and FlushAll.
	for _, sn := range c.GetAllStreamingNodes() {
		sn.Stop()
	}
	c.AddStreamingNode()
	log.Info("streaming node restarted")

	// Wait for channel rebalance to complete after streaming node restart,
	// then insert data with retry.
	s.Eventually(func() bool {
		fVecColumn2 := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
		pkColumn2 := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, int64(rowNum))
		insertResult2, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{pkColumn2, fVecColumn2},
			NumRows:        uint32(rowNum),
		})
		if err != nil {
			return false
		}
		return merr.Ok(insertResult2.GetStatus())
	}, 2*time.Minute, 3*time.Second)

	flushAllResp2, err := c.MilvusClient.FlushAll(ctx, &milvuspb.FlushAllRequest{})
	s.NoError(merr.CheckRPCCall(flushAllResp2, err))
	s.WaitForFlushAll(ctx, flushAllResp2.GetFlushAllMsgs())
	log.Info("second FlushAll completed after streaming node restart")

	// Step 3: Create index, load collection, and query count(*) to verify total is 2000.
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(merr.CheckRPCCall(createIndexStatus, err))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, collectionName)

	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		OutputFields:     []string{"count(*)"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.NoError(merr.CheckRPCCall(queryResult, err))
	count := queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0]
	s.Equal(int64(2*rowNum), count)
	log.Info("query count(*) verified: 2000 rows")

	// Cleanup.
	dropStatus, err := c.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(dropStatus, err))
}

func TestFlushAllWithRestart(t *testing.T) {
	suite.Run(t, new(FlushAllWithRestartSuite))
}
