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

func TestJsonExpr(t *testing.T) {
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
	jsonField := "json"
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
					Key:   "dim",
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		jsonF := &schemapb.FieldSchema{
			Name:        jsonField,
			Description: "this is a json field",
			DataType:    schemapb.DataType_JSON,
		}
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
				jsonF,
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

	fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
	jsonData := newJSONData(jsonField, rowNum)
	hashKeys := generateHashKeys(rowNum)
	insertResult, err := c.proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn, jsonData},
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
				Key:   "dim",
				Value: strconv.Itoa(dim),
			},
			{
				Key:   common.MetricTypeKey,
				Value: distance.L2,
			},
			{
				Key:   "index_type",
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

	expr := ""
	// search
	expr = `json["A"] > 90`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("GT expression run successfully")

	expr = `json["A"] < 10`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("LT expression run successfully")

	expr = `json["A"] <= 5`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("LE expression run successfully")

	expr = `json["A"] >= 95`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("GE expression run successfully")

	expr = `json["A"] == 5`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("EQ expression run successfully")

	expr = `json["A"] != 95`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("NE expression run successfully")

	expr = `(json["A"] > 90) && (json["B"] < 5)`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("NE expression run successfully")

	expr = `(json["A"] > 95) || (json["B"] < 5)`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("NE expression run successfully")

	expr = `not (json["A"] == 95)`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("NOT expression run successfully")

	expr = `json["A"] in [90, 91, 95, 97]`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("IN expression run successfully")

	expr = `json["A"] not in [90, 91, 95, 97]`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("NIN expression run successfully")

	expr = `json["C"]["0"] in [90, 91, 95, 97]`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("IN expression run successfully")

	expr = `json["C"]["0"] not in [90, 91, 95, 97]`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("NIN expression run successfully")

	expr = `0 <= json["A"] < 5`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("BinaryRange expression run successfully")

	expr = `json["A"] + 5 < 10`
	doSearch(c, collectionName, []string{jsonField}, expr, dim, t)
	log.Info("BinaryRange expression run successfully")
}

func doSearch(cluster *MiniCluster, collectionName string, outputField []string, expr string, dim int, t *testing.T) {
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
}

func newJSONData(fieldName string, rowNum int) *schemapb.FieldData {
	jsonData := make([][]byte, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		//json := fmt.Sprintf("{A: %d}", i)
		data := map[string]interface{}{
			"A":    i,
			"B":    rowNum - i,
			"C":    []int{i, rowNum - i},
			"name": "John",
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
