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

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/distance"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestJson_EnableDynamicSchema(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	prefix := "TestHelloMilvus"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 100

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:               collectionName,
			Description:        "",
			AutoID:             false,
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	assert.NoError(t, err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	assert.Equal(t, createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	describeCollectionResp, err := c.proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collectionName})
	assert.NoError(t, err)
	assert.True(t, describeCollectionResp.Schema.EnableDynamicField)
	assert.Equal(t, 3, len(describeCollectionResp.GetSchema().GetFields()))

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	jsonData := newJSONData(common.MetaFieldName, rowNum)
	insertFlushIndexLoad(ctx, t, c, dbName, collectionName, rowNum, dim, []*schemapb.FieldData{fVecColumn, jsonData})

	checkSearch(t, c, collectionName, common.MetaFieldName, dim)
}

func checkSearch(t *testing.T, c *MiniCluster, collectionName, fieldName string, dim int) {
	expr := ""
	// search
	expr = `$meta["A"] > 90`
	checkFunc := func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 5, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{"A"}, expr, dim, t, checkFunc)
	log.Info("GT expression run successfully")

	expr = `$meta["A"] < 10`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 5, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{"B"}, expr, dim, t, checkFunc)
	log.Info("LT expression run successfully")

	expr = `$meta["A"] <= 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 3, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{"C"}, expr, dim, t, checkFunc)
	log.Info("LE expression run successfully")

	expr = `A >= 95`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 3, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("GE expression run successfully")

	expr = `$meta["A"] == 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 1, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("EQ expression run successfully")

	expr = `A != 95`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("NE expression run successfully")

	expr = `not (A != 95)`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 1, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("NOT NE expression run successfully")

	expr = `A > 90 && B < 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 2, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("NE expression run successfully")

	expr = `A > 95 || 5 > B`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 4, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("NE expression run successfully")

	expr = `not (A == 95)`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("NOT expression run successfully")

	expr = `A in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 3, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("IN expression run successfully")

	expr = `A not in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("NIN expression run successfully")

	expr = `C[0] in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 4, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("IN expression run successfully")

	expr = `C[0] not in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("NIN expression run successfully")

	expr = `0 <= A < 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 2, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("BinaryRange expression run successfully")

	expr = `100 > A >= 90`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 5, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("BinaryRange expression run successfully")

	expr = `1+5 <= A < 5+10`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 4, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("BinaryRange expression run successfully")

	expr = `A + 5 == 10`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 1, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("Arithmetic expression run successfully")

	expr = `exists A`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("EXISTS expression run successfully")

	expr = `exists AAA`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 0, len(result.Results.FieldsData))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("EXISTS expression run successfully")

	expr = `not exists A`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("NOT EXISTS expression run successfully")

	expr = `E["G"] > 100`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 9, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("nested path expression run successfully")

	expr = `D like "name-%"`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("like expression run successfully")

	expr = `D like "name-11"`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 1, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("like expression run successfully")

	expr = `A like "10"`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 0, len(result.Results.FieldsData))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("like expression run successfully")

	expr = `A in []`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 0, len(result.Results.FieldsData))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("term empty expression run successfully")

	expr = `A not in []`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, fieldName, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{fieldName}, expr, dim, t, checkFunc)
	log.Info("term empty expression run successfully")

	// invalid expr
	expr = `E[F] > 100`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)

	expr = `A >> 10`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)

	expr = `not A > 5`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)

	expr = `not A == 5`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)

	expr = `A > B`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)

	expr = `A > Int64Field`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)

	expr = `A like abc`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)

	expr = `D like "%name-%"`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)

	expr = `D like "na%me"`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)

	expr = `1+5 <= A+1 < 5+10`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)

	expr = `$meta == ""`
	doSearchWithInvalidExpr(c, collectionName, []string{fieldName}, expr, dim, t)
}

func insertFlushIndexLoad(ctx context.Context, t *testing.T, c *MiniCluster, dbName, collectionName string, rowNum int, dim int, fieldData []*schemapb.FieldData) {
	hashKeys := generateHashKeys(rowNum)
	insertResult, err := c.proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     fieldData,
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	assert.NoError(t, err)
	assert.Equal(t, insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// flush
	flushResp, err := c.proxy.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	assert.NoError(t, err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	ids := segmentIDs.GetData()
	assert.NotEmpty(t, segmentIDs)

	segments, err := c.metaWatcher.ShowSegments()
	assert.NoError(t, err)
	assert.NotEmpty(t, segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}

	if has && len(ids) > 0 {
		flushed := func() bool {
			resp, err := c.proxy.GetFlushState(ctx, &milvuspb.GetFlushStateRequest{
				SegmentIDs: ids,
			})
			if err != nil {
				//panic(errors.New("GetFlushState failed"))
				return false
			}
			return resp.GetFlushed()
		}
		for !flushed() {
			// respect context deadline/cancel
			select {
			case <-ctx.Done():
				panic(errors.New("deadline exceeded"))
			default:
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	// create index
	createIndexStatus, err := c.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      floatVecField,
		IndexName:      "_default",
		ExtraParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
			{
				Key:   common.MetricTypeKey,
				Value: distance.L2,
			},
			{
				Key:   common.IndexTypeKey,
				Value: "IVF_FLAT",
			},
			{
				Key:   "nlist",
				Value: strconv.Itoa(10),
			},
		},
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
	waitingForIndexBuilt(ctx, c, t, collectionName, floatVecField)

	// load
	loadStatus, err := c.proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	assert.NoError(t, err)
	if loadStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("loadStatus fail reason", zap.String("reason", loadStatus.GetReason()))
	}
	assert.Equal(t, commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	for {
		loadProgress, err := c.proxy.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{
			CollectionName: collectionName,
		})
		if err != nil {
			panic("GetLoadingProgress fail")
		}
		if loadProgress.GetProgress() == 100 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func doSearch(cluster *MiniCluster, collectionName string, outputField []string, expr string, dim int, t *testing.T, checkFunc func(results *milvuspb.SearchResults)) {
	nq := 1
	topk := 10
	roundDecimal := -1

	params := getSearchParams(IndexFaissIvfFlat, distance.L2)
	searchReq := constructSearchRequest("", collectionName, expr,
		floatVecField, schemapb.DataType_FloatVector, outputField, distance.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := cluster.proxy.Search(context.Background(), searchReq)

	if searchResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("searchResult fail reason", zap.String("reason", searchResult.GetStatus().GetReason()))
	}
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())

	log.Info("TestHelloMilvus succeed", zap.Any("result", searchResult.Results),
		zap.Any("result num", len(searchResult.Results.FieldsData)))
	for _, data := range searchResult.Results.FieldsData {
		log.Info("output field", zap.Any("outputfield", data.String()))
	}
	checkFunc(searchResult)
}

func newJSONData(fieldName string, rowNum int) *schemapb.FieldData {
	jsonData := make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		data := map[string]interface{}{
			"A": i,
			"B": rowNum - i,
			"C": []int{i, rowNum - i},
			"D": fmt.Sprintf("name-%d", i),
			"E": map[string]interface{}{
				"F": i,
				"G": i + 10,
			},
		}
		if i%2 == 0 {
			data = map[string]interface{}{
				"B": rowNum - i,
				"C": []int{i, rowNum - i},
				"D": fmt.Sprintf("name-%d", i),
				"E": map[string]interface{}{
					"F": i,
					"G": i + 10,
				},
			}
		}
		if i == 100 {
			data = nil
		}
		jsonBytes, err := json.MarshalIndent(data, "", "  ")
		if err != nil {
			return nil
		}
		jsonData = append(jsonData, jsonBytes)
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: jsonData,
					},
				},
			},
		},
	}
}

func doSearchWithInvalidExpr(cluster *MiniCluster, collectionName string, outputField []string, expr string, dim int, t *testing.T) {
	nq := 1
	topk := 10
	roundDecimal := -1

	params := getSearchParams(IndexFaissIvfFlat, distance.L2)
	searchReq := constructSearchRequest("", collectionName, expr,
		floatVecField, schemapb.DataType_FloatVector, outputField, distance.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := cluster.proxy.Search(context.Background(), searchReq)

	if searchResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("searchResult fail reason", zap.String("reason", searchResult.GetStatus().GetReason()))
	}
	assert.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
}

func TestJSON_InsertWithoutDynamicData(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	prefix := "TestHelloMilvus"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 100

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:               collectionName,
			Description:        "",
			AutoID:             false,
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	assert.NoError(t, err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	assert.Equal(t, createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	describeCollectionResp, err := c.proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collectionName})
	assert.NoError(t, err)
	assert.True(t, describeCollectionResp.Schema.EnableDynamicField)
	assert.Equal(t, 3, len(describeCollectionResp.GetSchema().GetFields()))

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	insertFlushIndexLoad(ctx, t, c, dbName, collectionName, rowNum, dim, []*schemapb.FieldData{fVecColumn})

	expr := ""
	// search
	expr = `$meta["A"] > 90`
	checkFunc := func(result *milvuspb.SearchResults) {
		assert.Equal(t, 0, len(result.Results.FieldsData))
	}
	doSearch(c, collectionName, []string{common.MetaFieldName}, expr, dim, t, checkFunc)
	log.Info("GT expression run successfully")
}

func TestJSON_DynamicSchemaWithJSON(t *testing.T) {
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	prefix := "TestHelloMilvus"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	dim := 128
	rowNum := 100

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.DimKey,
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		j := &schemapb.FieldSchema{
			Name:        jsonField,
			Description: "json field",
			DataType:    schemapb.DataType_JSON,
		}
		return &schemapb.CollectionSchema{
			Name:               collectionName,
			Description:        "",
			AutoID:             false,
			EnableDynamicField: true,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
				j,
			},
		}
	}
	schema := constructCollectionSchema()
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	assert.NoError(t, err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	assert.Equal(t, createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	describeCollectionResp, err := c.proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collectionName})
	assert.NoError(t, err)
	assert.True(t, describeCollectionResp.Schema.EnableDynamicField)
	assert.Equal(t, 4, len(describeCollectionResp.GetSchema().GetFields()))

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	jsonData := newJSONData(jsonField, rowNum)
	dynamicData := newJSONData(common.MetaFieldName, rowNum)
	insertFlushIndexLoad(ctx, t, c, dbName, collectionName, rowNum, dim, []*schemapb.FieldData{fVecColumn, jsonData, dynamicData})

	checkSearch(t, c, collectionName, common.MetaFieldName, dim)

	expr := ""
	// search
	expr = `jsonField["A"] < 10`
	checkFunc := func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, jsonField, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 5, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t, checkFunc)
	log.Info("LT expression run successfully")

	expr = `jsonField["A"] <= 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, jsonField, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 3, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t, checkFunc)
	log.Info("LE expression run successfully")

	expr = `jsonField["A"] == 5`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, jsonField, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 1, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t, checkFunc)
	log.Info("EQ expression run successfully")

	expr = `jsonField["C"][0] in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, jsonField, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 4, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t, checkFunc)
	log.Info("IN expression run successfully")

	expr = `jsonField["C"][0] not in [90, 91, 95, 97]`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, jsonField, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 10, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t, checkFunc)
	log.Info("NIN expression run successfully")

	expr = `jsonField["E"]["G"] > 100`
	checkFunc = func(result *milvuspb.SearchResults) {
		assert.Equal(t, 1, len(result.Results.FieldsData))
		assert.Equal(t, jsonField, result.Results.FieldsData[0].GetFieldName())
		assert.Equal(t, schemapb.DataType_JSON, result.Results.FieldsData[0].GetType())
		assert.Equal(t, 9, len(result.Results.FieldsData[0].GetScalars().GetJsonData().GetData()))
	}
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t, checkFunc)
	log.Info("nested path expression run successfully")

	expr = `jsonField == ""`
	doSearchWithInvalidExpr(c, collectionName, []string{jsonField}, expr, dim, t)
}
