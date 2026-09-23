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
	"strings"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *BulkInsertSuite) TestImportDynamicField() {
	const (
		rowCount      = 100
		formatFieldID = 102
		formatField   = "source_format"
	)
	fileTypes := []importutilv2.FileType{importutilv2.JSON, importutilv2.Numpy, importutilv2.Parquet, importutilv2.CSV}

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 240*time.Second)
	defer cancel()

	collectionName := "TestBulkInsert_B_" + funcutil.RandomString(8)

	schema := integration.ConstructSchema(collectionName, dim, true, &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
		AutoID:       true,
	}, &schemapb.FieldSchema{
		FieldID:  101,
		Name:     integration.FloatVecField,
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
	}, &schemapb.FieldSchema{
		FieldID:  formatFieldID,
		Name:     formatField,
		DataType: schemapb.DataType_Int64,
	})
	schema.EnableDynamicField = true
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

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	s.Equal(int32(0), createIndexStatus.GetCode())

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// Keep every format's 100 rows in a separate ImportFile, but share one job.
	// A scalar marker lets each format assert its own query and search results.
	var files []*internalpb.ImportFile
	options := []*commonpb.KeyValuePair{}
	for i, fileType := range fileTypes {
		insertData, err := testutil.CreateInsertData(schema, rowCount)
		s.Require().NoError(err)
		markers := insertData.Data[formatFieldID].(*storage.Int64FieldData).Data
		for j := range markers {
			markers[j] = int64(i)
		}
		var file *internalpb.ImportFile
		switch fileType {
		case importutilv2.Numpy:
			file, err = writeNumpyFiles(c, schema, insertData)
			s.Require().NoError(err)
			// Preserve import into a dynamic-enabled collection without $meta.
			file.Paths = lo.Filter(file.Paths, func(path string, _ int) bool {
				return !strings.Contains(path, "$meta")
			})
		case importutilv2.JSON:
			file = &internalpb.ImportFile{Paths: []string{writeJSONFile(s.T(), c, schema, insertData)}}
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
	s.NoError(err)
	s.Equal(int32(0), importResp.GetStatus().GetCode())
	log.Info("Import result", zap.Any("importResp", importResp))

	jobID := importResp.GetJobID()
	err = WaitForImportDone(ctx, c, jobID)
	s.NoError(err)

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	log.Info("Show segments", zap.Any("segments", segments))

	// load refresh
	loadStatus, err = c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
		Refresh:        true,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoadRefresh(ctx, "", collectionName)

	// Verify each format separately so another format cannot mask missing rows.
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	for i, fileType := range fileTypes {
		s.Run(fileType.String(), func() {
			expr := fmt.Sprintf("%s > 0 && %s == %d", integration.Int64Field, formatField, i)
			queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
				CollectionName:   collectionName,
				Expr:             expr,
				OutputFields:     []string{"count(*)"},
				ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			})
			s.Require().NoError(merr.CheckRPCCall(queryResult, err))
			s.Require().Len(queryResult.GetFieldsData(), 1)
			s.Equal([]int64{rowCount}, queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData())

			searchReq := integration.ConstructSearchRequest("", collectionName, expr,
				integration.FloatVecField, schemapb.DataType_FloatVector, []string{formatField}, metric.L2, params, nq, dim, topk, roundDecimal)
			searchReq.ConsistencyLevel = commonpb.ConsistencyLevel_Eventually
			searchResult, err := c.MilvusClient.Search(ctx, searchReq)
			s.Require().NoError(merr.CheckRPCCall(searchResult, err))
			s.Equal(nq*topk, len(searchResult.GetResults().GetScores()))
			s.Require().Len(searchResult.GetResults().GetFieldsData(), 1)
			formats := searchResult.GetResults().GetFieldsData()[0].GetScalars().GetLongData().GetData()
			s.Len(formats, nq*topk)
			for _, source := range formats {
				s.Equal(int64(i), source)
			}
		})
	}
}
