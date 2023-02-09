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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/distance"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/importutil"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
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
	ctx := context.Background()
	c, err := StartMiniCluster(ctx)
	assert.NoError(t, err)
	err = c.Start()
	assert.NoError(t, err)
	defer c.Stop()
	assert.NoError(t, err)

	prefix := "TestBulkInsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	int64Field := "int64"
	floatVecField := "embeddings"
	scalarField := "image_path"
	dim := 128

	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "pk",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
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
		scalar := &schemapb.FieldSchema{
			Name:         scalarField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "max_length",
					Value: "65535",
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
				scalar,
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
				Value: "HNSW",
			},
			{
				Key:   "M",
				Value: "64",
			},
			{
				Key:   "efConstruction",
				Value: "512",
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

	// search
	expr := fmt.Sprintf("%s > 0", "int64")
	nq := 10
	topk := 10
	roundDecimal := -1
	nprobe := 10

	searchReq := constructSearchRequest("", collectionName, expr,
		floatVecField, nq, dim, nprobe, topk, roundDecimal)

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
			return err
		}
	}
	return nil
}

func TestGenerateNumpyFile(t *testing.T) {
	err := GenerateNumpyFile(TempFilesPath+"embeddings.npy", 100, schemapb.DataType_FloatVector, []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: strconv.Itoa(Dim),
		},
	})
	assert.NoError(t, err)
	log.Error("err", zap.Error(err))
}
