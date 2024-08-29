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

package hellomilvus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

type NullDataSuite struct {
	integration.MiniClusterSuite

	indexType  string
	metricType string
	vecType    schemapb.DataType
}

func getTargetFieldData(fieldName string, fieldDatas []*schemapb.FieldData) *schemapb.FieldData {
	var actual *schemapb.FieldData
	for _, result := range fieldDatas {
		if result.FieldName == fieldName {
			actual = result
			break
		}
	}
	return actual
}

func (s *NullDataSuite) checkNullableFieldData(fieldName string, fieldDatas []*schemapb.FieldData, start int64) {
	actual := getTargetFieldData(fieldName, fieldDatas)
	fieldData := actual.GetScalars().GetLongData().Data
	validData := actual.GetValidData()
	s.Equal(len(validData), len(fieldData))
	for i, ans := range actual.GetScalars().GetLongData().Data {
		if ans < start {
			s.False(validData[i])
		} else {
			s.True(validData[i])
		}
	}
}

func (s *NullDataSuite) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 100
		start  = 1000
	)

	collectionName := "TestNullData" + funcutil.GenRandomStr()

	schema := integration.ConstructSchemaOfVecDataType(collectionName, dim, false, s.vecType)
	nullableFid := &schemapb.FieldSchema{
		FieldID:     102,
		Name:        "nullableFid",
		Description: "",
		DataType:    schemapb.DataType_Int64,
		TypeParams:  nil,
		IndexParams: nil,
		AutoID:      false,
		Nullable:    true,
	}
	schema.Fields = append(schema.Fields, nullableFid)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	fieldsData := make([]*schemapb.FieldData, 0)
	fieldsData = append(fieldsData, integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, start))

	var fVecColumn *schemapb.FieldData
	if s.vecType == schemapb.DataType_SparseFloatVector {
		fVecColumn = integration.NewSparseFloatVectorFieldData(integration.SparseFloatVecField, rowNum)
	} else {
		fVecColumn = integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	}
	fieldsData = append(fieldsData, fVecColumn)
	nullableFidData := integration.NewInt64FieldDataNullableWithStart(nullableFid.GetName(), rowNum, start)
	fieldsData = append(fieldsData, nullableFidData)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     fieldsData,
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := c.Proxy.Flush(ctx, &milvuspb.FlushRequest{
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

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}
	s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      fVecColumn.FieldName,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, s.indexType, s.metricType),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, fVecColumn.FieldName)

	desCollResp, err := c.Proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(desCollResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	compactResp, err := c.Proxy.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{
		CollectionID: desCollResp.GetCollectionID(),
	})

	s.NoError(err)
	s.Equal(compactResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	compacted := func() bool {
		resp, err := c.Proxy.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{
			CompactionID: compactResp.GetCompactionID(),
		})
		if err != nil {
			return false
		}
		return resp.GetState() == commonpb.CompactionState_Completed
	}
	for !compacted() {
		time.Sleep(3 * time.Second)
	}

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	if loadStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("loadStatus fail reason", zap.String("reason", loadStatus.GetReason()))
	}
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	// search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(s.indexType, s.metricType)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		fVecColumn.FieldName, s.vecType, []string{"nullableFid"}, s.metricType, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.Proxy.Search(ctx, searchReq)
	err = merr.CheckRPCCall(searchResult, err)
	s.NoError(err)
	s.checkNullableFieldData(nullableFid.GetName(), searchResult.GetResults().GetFieldsData(), start)

	queryResult, err := c.Proxy.Query(ctx, &milvuspb.QueryRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{"nullableFid"},
	})
	if queryResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("searchResult fail reason", zap.String("reason", queryResult.GetStatus().GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, queryResult.GetStatus().GetErrorCode())
	s.checkNullableFieldData(nullableFid.GetName(), queryResult.GetFieldsData(), start)

	// // expr will not select null data
	// exprResult, err := c.Proxy.Query(ctx, &milvuspb.QueryRequest{
	// 	DbName:         dbName,
	// 	CollectionName: collectionName,
	// 	Expr:           "nullableFid in [0,1000]",
	// 	OutputFields:   []string{"nullableFid"},
	// })
	// if exprResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
	// 	log.Warn("searchResult fail reason", zap.String("reason", queryResult.GetStatus().GetReason()))
	// }
	// s.NoError(err)
	// s.Equal(commonpb.ErrorCode_Success, queryResult.GetStatus().GetErrorCode())
	// target := getTargetFieldData(nullableFid.Name, exprResult.GetFieldsData())
	// s.Equal(len(target.GetScalars().GetLongData().GetData()), 1)
	// s.Equal(len(target.GetValidData()), 1)

	deleteResult, err := c.Proxy.Delete(ctx, &milvuspb.DeleteRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Expr:           integration.Int64Field + " in [1, 2]",
	})
	if deleteResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("deleteResult fail reason", zap.String("reason", deleteResult.GetStatus().GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, deleteResult.GetStatus().GetErrorCode())

	status, err := c.Proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	status, err = c.Proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(status, err)
	s.NoError(err)

	log.Info("TestNullData succeed")
}

func (s *NullDataSuite) TestNullData_basic() {
	s.indexType = integration.IndexFaissIvfFlat
	s.metricType = metric.L2
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func TestNullData(t *testing.T) {
	suite.Run(t, new(NullDataSuite))
}
