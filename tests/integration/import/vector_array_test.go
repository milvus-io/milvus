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
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

func TestGenerateJsonFileWithVectorArray(t *testing.T) {
	const (
		rowCount         = 100
		dim              = 32
		maxArrayCapacity = 10
	)

	collectionName := "TestBulkInsert_VectorArray_" + funcutil.GenRandomStr()

	// Create schema with StructArrayField containing vector array
	schema := integration.ConstructSchema(collectionName, 0, true, &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
		AutoID:       false,
	}, &schemapb.FieldSchema{
		FieldID:  101,
		Name:     integration.VarCharField,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "256",
			},
		},
	})

	// Add StructArrayField with vector array
	structField := &schemapb.StructArrayFieldSchema{
		FieldID: 102,
		Name:    "struct_with_vector_array",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      103,
				Name:         "vector_array_field",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_ArrayOfVector,
				ElementType:  schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
					{
						Key:   common.MaxCapacityKey,
						Value: strconv.Itoa(maxArrayCapacity),
					},
				},
			},
			{
				FieldID:      104,
				Name:         "scalar_array_field",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_Int32,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxCapacityKey,
						Value: strconv.Itoa(maxArrayCapacity),
					},
				},
			},
		},
	}
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{structField}
	schema.EnableDynamicField = false

	insertData, err := testutil.CreateInsertData(schema, rowCount)
	assert.NoError(t, err)

	rows, err := testutil.CreateInsertDataRowsForJSON(schema, insertData)
	assert.NoError(t, err)
	fmt.Println(rows)
}

func (s *BulkInsertSuite) runForStructArray() {
	const (
		rowCount         = 100
		dim              = 32
		maxArrayCapacity = 10
	)

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 600*time.Second)
	defer cancel()

	collectionName := "TestBulkInsert_VectorArray_" + funcutil.GenRandomStr()

	// Create schema with StructArrayField containing vector array
	schema := integration.ConstructSchema(collectionName, 0, true, &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
		AutoID:       false,
	}, &schemapb.FieldSchema{
		FieldID:  101,
		Name:     integration.VarCharField,
		DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "256",
			},
		},
	})

	// Add StructArrayField with vector array
	structField := &schemapb.StructArrayFieldSchema{
		FieldID: 102,
		Name:    "struct_with_vector_array",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      103,
				Name:         "vector_array_field",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_ArrayOfVector,
				ElementType:  schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: strconv.Itoa(dim),
					},
					{
						Key:   common.MaxCapacityKey,
						Value: strconv.Itoa(maxArrayCapacity),
					},
				},
			},
			{
				FieldID:      104,
				Name:         "scalar_array_field",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Array,
				ElementType:  schemapb.DataType_Int32,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxCapacityKey,
						Value: strconv.Itoa(maxArrayCapacity),
					},
				},
			},
		},
	}
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{structField}
	schema.EnableDynamicField = false

	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "",
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(int32(0), createCollectionStatus.GetCode())

	var files []*internalpb.ImportFile

	options := []*commonpb.KeyValuePair{}

	switch s.fileType {
	case importutilv2.JSON:
		rowBasedFile := GenerateJSONFile(s.T(), c, schema, rowCount)
		files = []*internalpb.ImportFile{
			{
				Paths: []string{
					rowBasedFile,
				},
			},
		}
	case importutilv2.Parquet:
		filePath, err := GenerateParquetFile(s.Cluster, schema, rowCount)
		s.NoError(err)
		files = []*internalpb.ImportFile{
			{
				Paths: []string{
					filePath,
				},
			},
		}
	case importutilv2.CSV:
		filePath, sep := GenerateCSVFile(s.T(), s.Cluster, schema, rowCount)
		options = []*commonpb.KeyValuePair{{Key: "sep", Value: string(sep)}}
		s.NoError(err)
		files = []*internalpb.ImportFile{
			{
				Paths: []string{
					filePath,
				},
			},
		}
	}

	// Import data
	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          files,
		Options:        options,
	})
	s.NoError(err)
	s.NotNil(importResp)
	s.Equal(int32(0), importResp.GetStatus().GetCode())

	log.Info("Import response", zap.Any("resp", importResp))
	jobID := importResp.GetJobID()

	// Wait for import to complete
	err = WaitForImportDone(ctx, s.Cluster, jobID)
	s.NoError(err)

	// Create index for vector array field
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "vector_array_field",
		IndexName:      "_default_idx",
		ExtraParams:    integration.ConstructIndexParam(dim, s.indexType, s.metricType),
	})
	if err == nil {
		s.Equal(int32(0), createIndexStatus.GetCode(), createIndexStatus.GetReason())
	}

	// Load collection
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(int32(0), loadStatus.GetCode(), loadStatus.GetReason())
	s.WaitForLoad(ctx, collectionName)

	// search
	nq := 10
	topk := 10

	outputFields := []string{"vector_array_field"}
	params := integration.GetSearchParams(s.indexType, s.metricType)
	searchReq := integration.ConstructEmbeddingListSearchRequest("", collectionName, "",
		"vector_array_field", s.vecType, outputFields, s.metricType, params, nq, dim, topk, -1)

	searchResp, err := s.Cluster.MilvusClient.Search(ctx, searchReq)
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, searchResp.GetStatus().GetErrorCode(), searchResp.GetStatus().GetReason())

	result := searchResp.GetResults()
	s.Require().Len(result.GetIds().GetIntId().GetData(), nq*topk)
	s.Require().Len(result.GetScores(), nq*topk)
	s.Require().GreaterOrEqual(len(result.GetFieldsData()), 1)
	s.Require().EqualValues(nq, result.GetNumQueries())
	s.Require().EqualValues(topk, result.GetTopK())
}

func (s *BulkInsertSuite) TestImportWithVectorArray() {
	fileTypeArr := []importutilv2.FileType{importutilv2.CSV, importutilv2.Parquet, importutilv2.JSON}
	for _, fileType := range fileTypeArr {
		s.fileType = fileType
		s.vecType = schemapb.DataType_FloatVector
		s.indexType = integration.IndexEmbListHNSW
		s.metricType = metric.MaxSim
		s.runForStructArray()
	}
}
