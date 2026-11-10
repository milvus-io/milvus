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

package ratelimit

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *DBPropertiesSuite) TestFlush() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 3000
	)

	vecType := schemapb.DataType_FloatVector

	collectionName := "TestFlush_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchemaOfVecDataType(collectionName, dim, true, vecType)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	err = merr.CheckRPCCall(createCollectionStatus, err)
	s.NoError(err)

	mlog.Info(context.TODO(), "CreateCollection result", mlog.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.MilvusClient.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	err = merr.CheckRPCCall(showCollectionsResp.GetStatus(), err)
	s.NoError(err)
	mlog.Info(context.TODO(), "ShowCollections result", mlog.Any("showCollectionsResp", showCollectionsResp))

	var fVecColumn *schemapb.FieldData
	if vecType == schemapb.DataType_SparseFloatVector {
		fVecColumn = integration.NewSparseFloatVectorFieldData(integration.SparseFloatVecField, rowNum)
	} else {
		fVecColumn = integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	}
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	err = merr.CheckRPCCall(insertResult.GetStatus(), err)
	s.NoError(err)

	// Share one deadline so the first Flush cannot take long enough to refill
	// the explicit test quota before the second request checks throttling.
	flushCtx, cancelFlush := context.WithTimeout(ctx, 30*time.Second)
	defer cancelFlush()

	// flush 1
	flushResp, err := c.MilvusClient.Flush(flushCtx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.Require().NoError(merr.CheckRPCCall(flushResp, err))

	// flush 2
	flushResp, err = c.MilvusClient.Flush(flushCtx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.Require().NoError(err)
	s.Require().NotNil(flushResp)
	s.True(merr.ErrServiceRateLimit.Is(merr.Error(flushResp.GetStatus())), "unexpected Flush status: %v", flushResp.GetStatus())

	status, err := c.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	mlog.Info(context.TODO(), "TestFlush succeed")
}
