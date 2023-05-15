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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/distance"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

const (
	TempFilesPath = "/tmp/integration_test/import/"
	Dim           = 128
)

// test bulk insert E2E
// 1, create collection with a vector column and a varchar column
// 2, generate numpy files
// 3, import
// 4, create index
// 5, load
// 6, search
func TestBulkInsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*180)
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer func() {
		err = c.Stop()
		assert.NoError(t, err)
		cancel()
	}()

	prefix := "TestBulkInsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	//floatVecField := floatVecField
	dim := 128

	schema := constructSchema(collectionName, dim, true,
		&schemapb.FieldSchema{Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		&schemapb.FieldSchema{Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}}},
		&schemapb.FieldSchema{Name: "embeddings", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
	)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createCollectionStatus, err := c.proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	assert.NoError(t, err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
		t.FailNow()
	}
	assert.Equal(t, createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	assert.NoError(t, err)
	assert.Equal(t, showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	err = GenerateNumpyFile(c.chunkManager.RootPath()+"/"+"embeddings.npy", 100, schemapb.DataType_FloatVector, []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: strconv.Itoa(Dim),
		},
	})
	assert.NoError(t, err)
	err = GenerateNumpyFile(c.chunkManager.RootPath()+"/"+"image_path.npy", 100, schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
		{
			Key:   "max_length",
			Value: strconv.Itoa(65535),
		},
	})
	assert.NoError(t, err)

	bulkInsertFiles := []string{
		c.chunkManager.RootPath() + "/" + "embeddings.npy",
		c.chunkManager.RootPath() + "/" + "image_path.npy",
	}

	health1, err := c.dataCoord.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	assert.NoError(t, err)
	log.Info("dataCoord health", zap.Any("health1", health1))
	importResp, err := c.proxy.Import(ctx, &milvuspb.ImportRequest{
		CollectionName: collectionName,
		Files:          bulkInsertFiles,
	})
	assert.NoError(t, err)
	log.Info("Import result", zap.Any("importResp", importResp), zap.Int64s("tasks", importResp.GetTasks()))

	tasks := importResp.GetTasks()
	for _, task := range tasks {
	loop:
		for {
			importTaskState, err := c.proxy.GetImportState(ctx, &milvuspb.GetImportStateRequest{
				Task: task,
			})
			assert.NoError(t, err)
			switch importTaskState.GetState() {
			case commonpb.ImportState_ImportCompleted:
				break loop
			case commonpb.ImportState_ImportFailed:
				break loop
			case commonpb.ImportState_ImportFailedAndCleaned:
				break loop
			default:
				log.Info("import task state", zap.Int64("id", task), zap.String("state", importTaskState.GetState().String()))
				time.Sleep(time.Second * time.Duration(3))
				continue
			}
		}
	}

	health2, err := c.dataCoord.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	assert.NoError(t, err)
	log.Info("dataCoord health", zap.Any("health2", health2))

	segments, err := c.metaWatcher.ShowSegments()
	assert.NoError(t, err)
	assert.NotEmpty(t, segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}

	// create index
	createIndexStatus, err := c.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "_default",
		ExtraParams:    constructIndexParam(dim, IndexHNSW, distance.L2),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	waitingForIndexBuilt(ctx, c, t, collectionName, "embeddings")

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
	waitingForLoad(ctx, c, collectionName)

	// search
	expr := "" //fmt.Sprintf("%s > 0", int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := getSearchParams(IndexHNSW, distance.L2)
	searchReq := constructSearchRequest("", collectionName, expr,
		"embeddings", schemapb.DataType_FloatVector, nil, distance.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.proxy.Search(ctx, searchReq)

	if searchResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("searchResult fail reason", zap.String("reason", searchResult.GetStatus().GetReason()))
	}
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())

	log.Info("======================")
	log.Info("======================")
	log.Info("TestBulkInsert succeed")
	log.Info("======================")
	log.Info("======================")
}

func GenerateNumpyFile(filePath string, rowCount int, dType schemapb.DataType, typeParams []*commonpb.KeyValuePair) error {
	if dType == schemapb.DataType_VarChar {
		var data []string
		for i := 0; i < rowCount; i++ {
			data = append(data, "str")
		}
		err := importutil.CreateNumpyFile(filePath, data)
		if err != nil {
			log.Warn("failed to create numpy file", zap.Error(err))
			return err
		}
	}
	if dType == schemapb.DataType_FloatVector {
		dimStr, ok := funcutil.KeyValuePair2Map(typeParams)["dim"]
		if !ok {
			return errors.New("FloatVector field needs dim parameter")
		}
		dim, err := strconv.Atoi(dimStr)
		if err != nil {
			return err
		}
		//data := make([][]float32, rowCount)
		var data [][Dim]float32
		for i := 0; i < rowCount; i++ {
			vec := [Dim]float32{}
			for j := 0; j < dim; j++ {
				vec[j] = 1.1
			}
			//v := reflect.Indirect(reflect.ValueOf(vec))
			//log.Info("type", zap.Any("type", v.Kind()))
			data = append(data, vec)
			//v2 := reflect.Indirect(reflect.ValueOf(data))
			//log.Info("type", zap.Any("type", v2.Kind()))
		}
		err = importutil.CreateNumpyFile(filePath, data)
		if err != nil {
			log.Warn("failed to create numpy file", zap.Error(err))
			return err
		}
	}
	return nil
}

func TestGenerateNumpyFile(t *testing.T) {
	err := os.MkdirAll(TempFilesPath, os.ModePerm)
	require.NoError(t, err)
	err = GenerateNumpyFile(TempFilesPath+"embeddings.npy", 100, schemapb.DataType_FloatVector, []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: strconv.Itoa(Dim),
		},
	})
	assert.NoError(t, err)
}
