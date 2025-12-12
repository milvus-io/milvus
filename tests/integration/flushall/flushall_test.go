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
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

type FlushAllSuite struct {
	integration.MiniClusterSuite
}

func (s *FlushAllSuite) WaitForFlushAll(ctx context.Context, flushTss map[string]uint64) {
	flushed := func() bool {
		resp, err := s.Cluster.MilvusClient.GetFlushAllState(ctx, &milvuspb.GetFlushAllStateRequest{
			FlushAllTss: flushTss,
		})
		if err != nil {
			return false
		}
		return resp.GetFlushed()
	}
	for !flushed() {
		select {
		case <-ctx.Done():
			s.FailNow("failed to wait for flush until ctx done")
			return
		default:
			time.Sleep(500 * time.Millisecond)
		}
	}
}

// Test flush all database and dbs
func (s *FlushAllSuite) TestFlushAll() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	c := s.Cluster

	const (
		dim         = 8
		rowNum      = 100
		dbCnt       = 10
		colCntPerDB = 10
	)

	collectionNames := make(map[string]string) // collection name -> db name
	for i := 0; i < dbCnt; i++ {
		// create db
		dbName := fmt.Sprintf("TestFlushAll_db_%d_%s", i, funcutil.GenRandomStr())
		status, err := c.MilvusClient.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
			DbName: dbName,
		})
		s.NoError(merr.CheckRPCCall(status, err))

		for j := 0; j < colCntPerDB; j++ {
			collectionName := fmt.Sprintf("TestFlushAll_collection_%d_%d_%s", i, j, funcutil.GenRandomStr())
			collectionNames[collectionName] = dbName
		}
	}

	execFunc := func(collectionName string, dbName string) {
		// create collection
		schema := integration.ConstructSchemaOfVecDataType(collectionName, dim, true, schemapb.DataType_FloatVector)
		marshaledSchema, err := proto.Marshal(schema)
		s.NoError(err)

		createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      common.DefaultShardsNum,
		})
		s.NoError(merr.CheckRPCCall(createCollectionStatus, err))

		// insert data
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
		hashKeys := integration.GenerateHashKeys(rowNum)
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(merr.CheckRPCCall(insertResult, err))
	}

	wg := sync.WaitGroup{}
	for collectionName, dbName := range collectionNames {
		wg.Add(1)
		go func(collectionName string, dbName string) {
			defer wg.Done()
			execFunc(collectionName, dbName)
		}(collectionName, dbName)
	}
	wg.Wait()

	// flush all
	flushAllResp, err := c.MilvusClient.FlushAll(ctx, &milvuspb.FlushAllRequest{})
	s.NoError(merr.CheckRPCCall(flushAllResp, err))
	log.Info("FlushAll succeed", zap.Any("flushAllTss", flushAllResp.GetFlushAllTss()))
	s.WaitForFlushAll(ctx, flushAllResp.GetFlushAllTss())

	// show and validate segments
	for collectionName, dbName := range collectionNames {
		resp, err := c.MilvusClient.GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(merr.CheckRPCCall(resp, err))
		s.Len(resp.GetInfos(), 1)
		segment := resp.GetInfos()[0]
		s.Equal(segment.GetState(), commonpb.SegmentState_Flushed)
		s.Equal(segment.GetNumRows(), int64(rowNum))
	}

	// drop collections
	for collectionName, dbName := range collectionNames {
		status, err := c.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		s.NoError(merr.CheckRPCCall(status, err))
	}

	log.Info("TestFlushAll succeed")
}

func TestFlushAll(t *testing.T) {
	suite.Run(t, new(FlushAllSuite))
}
