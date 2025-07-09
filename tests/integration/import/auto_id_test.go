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

package importv2

import (
	"context"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *BulkInsertSuite) runTestAutoID() {
	const (
		rowCount = 10
		fileNum  = 10
	)

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 240*time.Second)
	defer cancel()

	collectionName := "TestBulkInsert" + funcutil.GenRandomStr()

	var schema *schemapb.CollectionSchema
	fieldSchema1 := &schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: s.pkType, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}}, IsPrimaryKey: true, AutoID: true}
	fieldSchema2 := &schemapb.FieldSchema{FieldID: 101, Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}}
	fieldSchema3 := &schemapb.FieldSchema{FieldID: 102, Name: "embeddings", DataType: s.vecType, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}}
	schema = integration.ConstructSchema(collectionName, dim, true, fieldSchema1, fieldSchema2, fieldSchema3)

	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createCollectionStatus.GetErrorCode())

	wg := &sync.WaitGroup{}
	importReqs := make([]*internalpb.ImportRequest, fileNum)
	for i := 0; i < fileNum; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			rowBasedFile := GenerateJSONFile(s.T(), c, schema, rowCount)
			files := []*internalpb.ImportFile{
				{
					Paths: []string{
						rowBasedFile,
					},
				},
			}
			importReqs[i] = &internalpb.ImportRequest{
				CollectionName: collectionName,
				Files:          files,
				Options:        []*commonpb.KeyValuePair{},
			}
		}()
	}
	defer func() {
		for _, req := range importReqs {
			os.Remove(req.GetFiles()[0].GetPaths()[0])
		}
	}()
	wg.Wait()

	for i := 0; i < fileNum; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			importResp, err := c.ProxyClient.ImportV2(ctx, importReqs[i])
			s.NoError(err)
			s.Equal(int32(0), importResp.GetStatus().GetCode())
			log.Info("Import result", zap.Any("importResp", importResp))
			err = WaitForImportDone(ctx, c, importResp.GetJobID())
			s.NoError(err)
		}()
	}
	wg.Wait()

	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		s.True(len(segment.GetBinlogs()) > 0)
		s.NoError(CheckLogID(segment.GetBinlogs()))
		s.True(len(segment.GetDeltalogs()) == 0)
		s.True(len(segment.GetStatslogs()) > 0)
		s.NoError(CheckLogID(segment.GetStatslogs()))
	}

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, s.indexType, s.metricType),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
	s.WaitForIndexBuilt(ctx, collectionName, "embeddings")

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	// search
	expr := ""
	const (
		nq           = 2
		topk         = 2
		roundDecimal = -1
	)
	params := integration.GetSearchParams(s.indexType, s.metricType)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		"embeddings", s.vecType, nil, s.metricType, params, nq, dim, topk, roundDecimal)
	searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Eventually
	searchResult, err := c.MilvusClient.Search(ctx, searchReq)
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
	s.Equal(nq*topk, len(searchResult.GetResults().GetScores()))

	// verify no duplicate autoID
	expr = "id >= 0"
	if s.pkType == schemapb.DataType_VarChar {
		expr = `id >= "0"`
	}
	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             expr,
		OutputFields:     []string{"id"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Eventually,
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)
	count := len(queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData())
	if s.pkType == schemapb.DataType_VarChar {
		count = len(queryResult.GetFieldsData()[0].GetScalars().GetStringData().GetData())
	}
	s.Equal(rowCount*fileNum, count)
}

func (s *BulkInsertSuite) TestAutoID() {
	// make buffer size small to trigger multiple sync
	revertGuard := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().DataNodeCfg.ImportBaseBufferSize.Key: "0.000001",
	})
	defer revertGuard()

	s.pkType = schemapb.DataType_Int64
	s.runTestAutoID()

	s.pkType = schemapb.DataType_VarChar
	s.runTestAutoID()
}
