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
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *MultiFileTypeImportSuite) TestMultiFileTypes() {
	vectorTypes := []struct {
		vecType    schemapb.DataType
		indexType  indexparamcheck.IndexType
		metricType metric.MetricType
	}{
		{schemapb.DataType_BinaryVector, "BIN_IVF_FLAT", metric.HAMMING},
		{schemapb.DataType_FloatVector, "HNSW", metric.L2},
		{schemapb.DataType_Float16Vector, "HNSW", metric.L2},
		{schemapb.DataType_BFloat16Vector, "HNSW", metric.L2},
		{schemapb.DataType_Int8Vector, "HNSW", metric.L2},
		{schemapb.DataType_SparseFloatVector, "SPARSE_WAND", metric.IP},
	}
	for _, vectorType := range vectorTypes {
		s.Run(vectorType.vecType.String(), func() {
			s.runVectorFormats(vectorType.vecType, vectorType.indexType, vectorType.metricType)
		})
	}
}

func (s *MultiFileTypeImportSuite) runVectorFormats(vecType schemapb.DataType, indexType indexparamcheck.IndexType, metricType metric.MetricType) {
	const rowCount = 100
	fileTypes := []importutilv2.FileType{importutilv2.JSON, importutilv2.Numpy, importutilv2.Parquet, importutilv2.CSV}
	if vecType == schemapb.DataType_SparseFloatVector {
		// Numpy does not support sparse vectors.
		fileTypes = []importutilv2.FileType{importutilv2.JSON, importutilv2.Parquet, importutilv2.CSV}
	}

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 240*time.Second)
	defer cancel()
	collectionName := "TestMultiFileTypes_" + funcutil.RandomString(8)
	vectorField := &schemapb.FieldSchema{
		FieldID: 102, Name: "embeddings", DataType: vecType,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
	}
	if vecType == schemapb.DataType_SparseFloatVector {
		vectorField.FieldID = 103
		vectorField.TypeParams = nil
	}
	schema := integration.ConstructSchema(collectionName, dim, false,
		&schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: false},
		&schemapb.FieldSchema{FieldID: 101, Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}},
		vectorField,
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))

	// Share the collection and job, while keeping 100 rows in each format.
	files := make([]*internalpb.ImportFile, 0, len(fileTypes))
	var options []*commonpb.KeyValuePair
	for i, fileType := range fileTypes {
		insertData, err := testutil.CreateInsertData(schema, rowCount)
		s.Require().NoError(err)
		// AutoID stays disabled. Disjoint ranges prevent duplicate primary keys
		// and let each format verify its own imported rows and search results.
		pks := insertData.Data[100].(*storage.Int64FieldData).Data
		for row := range pks {
			pks[row] = int64(i*rowCount + row)
		}
		var file *internalpb.ImportFile
		switch fileType {
		case importutilv2.JSON:
			file = &internalpb.ImportFile{Paths: []string{writeJSONFile(s.T(), c, schema, insertData)}}
		case importutilv2.Numpy:
			file, err = writeNumpyFiles(c, schema, insertData)
			s.Require().NoError(err)
		case importutilv2.Parquet:
			filePath, err := writeParquetFile(c, schema, insertData)
			s.Require().NoError(err)
			file = &internalpb.ImportFile{Paths: []string{filePath}}
		case importutilv2.CSV:
			filePath, sep := writeCSVFile(s.T(), c, schema, insertData)
			options = []*commonpb.KeyValuePair{{Key: "sep", Value: string(sep)}}
			file = &internalpb.ImportFile{Paths: []string{filePath}}
		}
		files = append(files, file)
	}
	importResp, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          files,
		Options:        options,
	})
	s.Require().NoError(merr.CheckRPCCall(importResp, err))
	s.Require().NoError(WaitForImportDone(ctx, c, importResp.GetJobID()))

	segments, err := c.ShowSegments(collectionName)
	s.Require().NoError(err)
	s.Require().NotEmpty(segments)
	for _, segment := range segments {
		s.NotEmpty(segment.GetBinlogs())
		s.NoError(CheckLogID(segment.GetBinlogs()))
		s.Empty(segment.GetDeltalogs())
		s.NotEmpty(segment.GetStatslogs())
		s.NoError(CheckLogID(segment.GetStatslogs()))
	}

	status, err = c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, indexType, metricType),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuilt(ctx, collectionName, "embeddings")
	status, err = c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: collectionName})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, collectionName)

	for i, fileType := range fileTypes {
		s.Run(fileType.String(), func() {
			startPK, endPK := int64(i*rowCount), int64((i+1)*rowCount)
			expr := fmt.Sprintf("id >= %d && id < %d", startPK, endPK)
			queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
				CollectionName:   collectionName,
				Expr:             expr,
				OutputFields:     []string{"id"},
				ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			})
			s.Require().NoError(merr.CheckRPCCall(queryResult, err))
			s.Require().Len(queryResult.GetFieldsData(), 1)
			expectedPKs := make([]int64, rowCount)
			for row := range expectedPKs {
				expectedPKs[row] = startPK + int64(row)
			}
			s.ElementsMatch(expectedPKs, queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData())

			searchReq := integration.ConstructSearchRequest("", collectionName, expr,
				"embeddings", vecType, nil, metricType, integration.GetSearchParams(indexType, metricType), 10, dim, 10, -1)
			searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Eventually
			searchResult, err := c.MilvusClient.Search(ctx, searchReq)
			s.Require().NoError(merr.CheckRPCCall(searchResult, err))
			ids := searchResult.GetResults().GetIds().GetIntId().GetData()
			s.NotEmpty(ids)
			for _, id := range ids {
				s.GreaterOrEqual(id, startPK)
				s.Less(id, endPK)
			}
		})
	}
}
