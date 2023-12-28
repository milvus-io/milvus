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
	rand2 "crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/samber/lo"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	TempFilesPath = "/tmp/integration_test/import/"
	Dim           = 128
)

type BulkInsertSuite struct {
	fileType importutilv2.FileType
	integration.MiniClusterSuite
}

// test bulk insert E2E
// 1, create collection with a vector column and a varchar column
// 2, generate files
// 3, import
// 4, create index
// 5, load
// 6, search
func (s *BulkInsertSuite) run() {
	c := s.Cluster
	ctx, cancel := context.WithCancel(c.GetContext())
	defer cancel()

	prefix := "TestBulkInsert"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	// floatVecField := floatVecField
	dim := 128

	schema := integration.ConstructSchema(collectionName, dim, true,
		&schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		&schemapb.FieldSchema{FieldID: 101, Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}},
		&schemapb.FieldSchema{FieldID: 102, Name: "embeddings", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
	)
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
		s.FailNow("failed to create collection")
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))

	var files []*milvuspb.ImportFile
	if s.fileType == importutilv2.Numpy {
		err = GenerateNumpyFile(c.ChunkManager.RootPath()+"/"+"embeddings.npy", 100, schemapb.DataType_FloatVector, []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(Dim),
			},
		})
		s.NoError(err)
		err = GenerateNumpyFile(c.ChunkManager.RootPath()+"/"+"image_path.npy", 100, schemapb.DataType_VarChar, []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: strconv.Itoa(65535),
			},
		})
		s.NoError(err)

		files = []*milvuspb.ImportFile{
			{
				File: &milvuspb.ImportFile_ColumnBasedFile{
					ColumnBasedFile: &milvuspb.ColumnBasedFile{
						Files: []string{
							c.ChunkManager.RootPath() + "/" + "embeddings.npy",
							c.ChunkManager.RootPath() + "/" + "image_path.npy",
						},
					},
				},
			},
		}
	} else if s.fileType == importutilv2.JSON {
		rowBasedFile := c.ChunkManager.RootPath() + "/" + "test.json"
		GenerateJsonFile(s.T(), rowBasedFile, schema, 100)
		files = []*milvuspb.ImportFile{
			{
				File: &milvuspb.ImportFile_RowBasedFile{
					RowBasedFile: rowBasedFile,
				},
			},
		}
	}

	health1, err := c.DataCoord.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	s.NoError(err)
	log.Info("dataCoord health", zap.Any("health1", health1))
	importResp, err := c.Proxy.ImportV2(ctx, &milvuspb.ImportRequestV2{
		CollectionName: collectionName,
		Files:          files,
	})
	s.NoError(err)
	log.Info("Import result", zap.Any("importResp", importResp))

	request := importResp.GetRequestID()
loop:
	for {
		resp, err := c.Proxy.GetImportProgress(ctx, &milvuspb.GetImportProgressRequest{
			RequestID: request,
		})
		s.NoError(err)
		switch resp.GetState() {
		case milvuspb.ImportState_Completed:
			break loop
		case milvuspb.ImportState_Failed:
			s.True(false)
			s.T().Logf("import failed, err=%s", resp.GetReason())
			return
		default:
			log.Info("import progress", zap.String("request", request),
				zap.Int64("progress", resp.GetProgress()),
				zap.String("state", resp.GetState().String()))
			time.Sleep(time.Second * time.Duration(3))
			continue
		}
	}

	health2, err := c.DataCoord.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	s.NoError(err)
	log.Info("dataCoord health", zap.Any("health2", health2))

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.L2),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, "embeddings")

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
	expr := "" // fmt.Sprintf("%s > 0", int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexHNSW, metric.L2)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		"embeddings", schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.Proxy.Search(ctx, searchReq)

	if searchResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("searchResult fail reason", zap.String("reason", searchResult.GetStatus().GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())

	log.Info("======================")
	log.Info("======================")
	log.Info("TestBulkInsert succeed")
	log.Info("======================")
	log.Info("======================")
}

func (s *BulkInsertSuite) TestNumpy() {
	s.fileType = importutilv2.Numpy
	s.run()
}

func (s *BulkInsertSuite) TestJSON() {
	s.fileType = importutilv2.JSON
	s.run()
}

func TestBulkInsert(t *testing.T) {
	suite.Run(t, new(BulkInsertSuite))
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
		dimStr, ok := funcutil.KeyValuePair2Map(typeParams)[common.DimKey]
		if !ok {
			return errors.New("FloatVector field needs dim parameter")
		}
		dim, err := strconv.Atoi(dimStr)
		if err != nil {
			return err
		}
		// data := make([][]float32, rowCount)
		var data [][Dim]float32
		for i := 0; i < rowCount; i++ {
			vec := [Dim]float32{}
			for j := 0; j < dim; j++ {
				vec[j] = 1.1
			}
			// v := reflect.Indirect(reflect.ValueOf(vec))
			// log.Info("type", zap.Any("type", v.Kind()))
			data = append(data, vec)
			// v2 := reflect.Indirect(reflect.ValueOf(data))
			// log.Info("type", zap.Any("type", v2.Kind()))
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
			Key:   common.DimKey,
			Value: strconv.Itoa(Dim),
		},
	})
	assert.NoError(t, err)
}

func createInsertData(t *testing.T, schema *schemapb.CollectionSchema, rowCount int) *storage.InsertData {
	insertData, err := storage.NewInsertData(schema)
	assert.NoError(t, err)
	for _, field := range schema.GetFields() {
		if field.GetAutoID() {
			continue
		}
		switch field.GetDataType() {
		case schemapb.DataType_Bool:
			boolData := make([]bool, 0)
			for i := 0; i < rowCount; i++ {
				boolData = append(boolData, i%3 != 0)
			}
			insertData.Data[field.GetFieldID()] = &storage.BoolFieldData{Data: boolData}
		case schemapb.DataType_Float:
			floatData := make([]float32, 0)
			for i := 0; i < rowCount; i++ {
				floatData = append(floatData, float32(i/2))
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatFieldData{Data: floatData}
		case schemapb.DataType_Double:
			doubleData := make([]float64, 0)
			for i := 0; i < rowCount; i++ {
				doubleData = append(doubleData, float64(i/5))
			}
			insertData.Data[field.GetFieldID()] = &storage.DoubleFieldData{Data: doubleData}
		case schemapb.DataType_Int8:
			int8Data := make([]int8, 0)
			for i := 0; i < rowCount; i++ {
				int8Data = append(int8Data, int8(i%256))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int8FieldData{Data: int8Data}
		case schemapb.DataType_Int16:
			int16Data := make([]int16, 0)
			for i := 0; i < rowCount; i++ {
				int16Data = append(int16Data, int16(i%65536))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int16FieldData{Data: int16Data}
		case schemapb.DataType_Int32:
			int32Data := make([]int32, 0)
			for i := 0; i < rowCount; i++ {
				int32Data = append(int32Data, int32(i%1000))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int32FieldData{Data: int32Data}
		case schemapb.DataType_Int64:
			int64Data := make([]int64, 0)
			for i := 0; i < rowCount; i++ {
				int64Data = append(int64Data, int64(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.Int64FieldData{Data: int64Data}
		case schemapb.DataType_BinaryVector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			binVecData := make([]byte, 0)
			total := rowCount * int(dim) / 8
			for i := 0; i < total; i++ {
				binVecData = append(binVecData, byte(i%256))
			}
			insertData.Data[field.GetFieldID()] = &storage.BinaryVectorFieldData{Data: binVecData, Dim: int(dim)}
		case schemapb.DataType_FloatVector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			floatVecData := make([]float32, 0)
			total := rowCount * int(dim)
			for i := 0; i < total; i++ {
				floatVecData = append(floatVecData, rand.Float32())
			}
			insertData.Data[field.GetFieldID()] = &storage.FloatVectorFieldData{Data: floatVecData, Dim: int(dim)}
		case schemapb.DataType_Float16Vector:
			dim, err := typeutil.GetDim(field)
			assert.NoError(t, err)
			total := int64(rowCount) * dim * 2
			float16VecData := make([]byte, total)
			_, err = rand2.Read(float16VecData)
			assert.NoError(t, err)
			insertData.Data[field.GetFieldID()] = &storage.Float16VectorFieldData{Data: float16VecData, Dim: int(dim)}
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			varcharData := make([]string, 0)
			for i := 0; i < rowCount; i++ {
				varcharData = append(varcharData, strconv.Itoa(i))
			}
			insertData.Data[field.GetFieldID()] = &storage.StringFieldData{Data: varcharData}
		case schemapb.DataType_JSON:
			jsonData := make([][]byte, 0)
			for i := 0; i < rowCount; i++ {
				jsonData = append(jsonData, []byte(fmt.Sprintf("{\"y\": %d}", i)))
			}
			insertData.Data[field.GetFieldID()] = &storage.JSONFieldData{Data: jsonData}
		case schemapb.DataType_Array:
			arrayData := make([]*schemapb.ScalarField, 0)
			for i := 0; i < rowCount; i++ {
				arrayData = append(arrayData, &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{int32(i), int32(i + 1), int32(i + 2)},
						},
					},
				})
			}
			insertData.Data[field.GetFieldID()] = &storage.ArrayFieldData{Data: arrayData}
		default:
			panic(fmt.Sprintf("unexpected data type: %s", field.GetDataType().String()))
		}
	}
	return insertData
}

func GenerateJsonFile(t *testing.T, filePath string, schema *schemapb.CollectionSchema, count int) {
	insertData := createInsertData(t, schema, count)
	rows := make([]map[string]any, 0, count)
	fieldIDToField := lo.KeyBy(schema.GetFields(), func(field *schemapb.FieldSchema) int64 {
		return field.GetFieldID()
	})
	for i := 0; i < count; i++ {
		data := make(map[int64]interface{})
		for fieldID, v := range insertData.Data {
			dataType := fieldIDToField[fieldID].GetDataType()
			if fieldIDToField[fieldID].GetAutoID() {
				continue
			}
			if dataType == schemapb.DataType_Array {
				data[fieldID] = v.GetRow(i).(*schemapb.ScalarField).GetIntData().GetData()
			} else if dataType == schemapb.DataType_JSON {
				data[fieldID] = string(v.GetRow(i).([]byte))
			} else if dataType == schemapb.DataType_BinaryVector || dataType == schemapb.DataType_Float16Vector {
				bytes := v.GetRow(i).([]byte)
				ints := make([]int, 0, len(bytes))
				for _, b := range bytes {
					ints = append(ints, int(b))
				}
				data[fieldID] = ints
			} else {
				data[fieldID] = v.GetRow(i)
			}
		}
		row := lo.MapKeys(data, func(_ any, fieldID int64) string {
			return fieldIDToField[fieldID].GetName()
		})
		rows = append(rows, row)
	}

	jsonBytes, err := json.Marshal(rows)
	assert.NoError(t, err)

	err = os.WriteFile(filePath, jsonBytes, 0644)
	assert.NoError(t, err)

	//jsonFile, err := os.Open(filePath)
	//if err != nil {
	//	panic(err)
	//}
	//defer jsonFile.Close()
	//byteValue, _ := io.ReadAll(jsonFile)
	//sss := string(byteValue)
	//fmt.Println(sss)
}
