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
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type BulkInsertSuite struct {
	integration.MiniClusterSuite

	failed       bool
	failedReason string

	pkType   schemapb.DataType
	autoID   bool
	fileType importutilv2.FileType

	vecType    schemapb.DataType
	indexType  indexparamcheck.IndexType
	metricType metric.MetricType
}

func (s *BulkInsertSuite) SetupTest() {
	paramtable.Init()
	s.MiniClusterSuite.SetupTest()
	s.failed = false
	s.fileType = importutilv2.Parquet
	s.pkType = schemapb.DataType_Int64
	s.autoID = false

	s.vecType = schemapb.DataType_FloatVector
	s.indexType = "HNSW"
	s.metricType = metric.L2
}

func (s *BulkInsertSuite) run() {
	const (
		rowCount = 100
	)

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 120*time.Second)
	defer cancel()

	collectionName := "TestBulkInsert" + funcutil.GenRandomStr()

	var schema *schemapb.CollectionSchema
	fieldSchema1 := &schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: s.pkType, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}}, IsPrimaryKey: true, AutoID: s.autoID}
	fieldSchema2 := &schemapb.FieldSchema{FieldID: 101, Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}}
	fieldSchema3 := &schemapb.FieldSchema{FieldID: 102, Name: "embeddings", DataType: s.vecType, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}}
	fieldSchema4 := &schemapb.FieldSchema{FieldID: 103, Name: "embeddings", DataType: s.vecType, TypeParams: []*commonpb.KeyValuePair{}}
	if s.vecType != schemapb.DataType_SparseFloatVector {
		schema = integration.ConstructSchema(collectionName, dim, s.autoID, fieldSchema1, fieldSchema2, fieldSchema3)
	} else {
		schema = integration.ConstructSchema(collectionName, dim, s.autoID, fieldSchema1, fieldSchema2, fieldSchema4)
	}

	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createCollectionStatus.GetErrorCode())

	var files []*internalpb.ImportFile
	err = os.MkdirAll(c.ChunkManager.RootPath(), os.ModePerm)
	s.NoError(err)

	options := []*commonpb.KeyValuePair{}

	switch s.fileType {
	case importutilv2.Numpy:
		importFile, err := GenerateNumpyFiles(c.ChunkManager, schema, rowCount)
		s.NoError(err)
		files = []*internalpb.ImportFile{importFile}
	case importutilv2.JSON:
		rowBasedFile := c.ChunkManager.RootPath() + "/" + "test.json"
		GenerateJSONFile(s.T(), rowBasedFile, schema, rowCount)
		defer os.Remove(rowBasedFile)
		files = []*internalpb.ImportFile{
			{
				Paths: []string{
					rowBasedFile,
				},
			},
		}
	case importutilv2.Parquet:
		filePath := fmt.Sprintf("/tmp/test_%d.parquet", rand.Int())
		err = GenerateParquetFile(filePath, schema, rowCount)
		s.NoError(err)
		defer os.Remove(filePath)
		files = []*internalpb.ImportFile{
			{
				Paths: []string{
					filePath,
				},
			},
		}
	case importutilv2.CSV:
		filePath := fmt.Sprintf("/tmp/test_%d.csv", rand.Int())
		sep := GenerateCSVFile(s.T(), filePath, schema, rowCount)
		defer os.Remove(filePath)
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

	importResp, err := c.Proxy.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          files,
		Options:        options,
	})
	s.NoError(err)
	s.Equal(int32(0), importResp.GetStatus().GetCode())
	log.Info("Import result", zap.Any("importResp", importResp))

	jobID := importResp.GetJobID()
	err = WaitForImportDone(ctx, c, jobID)
	if s.failed {
		s.T().Logf("expect failed import, err=%s", err)
		s.Error(err)
		s.Contains(err.Error(), s.failedReason)
		return
	}
	s.NoError(err)

	segments, err := c.MetaWatcher.ShowSegments()
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
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      "embeddings",
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, s.indexType, s.metricType),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, "embeddings")

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	// search
	expr := ""
	nq := 10
	topk := 10
	roundDecimal := -1

	params := integration.GetSearchParams(s.indexType, s.metricType)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		"embeddings", s.vecType, nil, s.metricType, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.Proxy.Search(ctx, searchReq)
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
	// s.Equal(nq*topk, len(searchResult.GetResults().GetScores()))
}

func (s *BulkInsertSuite) TestMultiFileTypes() {
	fileTypeArr := []importutilv2.FileType{importutilv2.JSON, importutilv2.Numpy, importutilv2.Parquet, importutilv2.CSV}

	for _, fileType := range fileTypeArr {
		s.fileType = fileType

		s.vecType = schemapb.DataType_BinaryVector
		s.indexType = "BIN_IVF_FLAT"
		s.metricType = metric.HAMMING
		s.run()

		s.vecType = schemapb.DataType_FloatVector
		s.indexType = "HNSW"
		s.metricType = metric.L2
		s.run()

		s.vecType = schemapb.DataType_Float16Vector
		s.indexType = "HNSW"
		s.metricType = metric.L2
		s.run()

		s.vecType = schemapb.DataType_BFloat16Vector
		s.indexType = "HNSW"
		s.metricType = metric.L2
		s.run()

		// TODO: not support numpy for SparseFloatVector by now
		if fileType != importutilv2.Numpy {
			s.vecType = schemapb.DataType_SparseFloatVector
			s.indexType = "SPARSE_WAND"
			s.metricType = metric.IP
			s.run()
		}
	}
}

func (s *BulkInsertSuite) TestAutoID() {
	s.pkType = schemapb.DataType_Int64
	s.autoID = true
	s.run()

	s.pkType = schemapb.DataType_VarChar
	s.autoID = true
	s.run()
}

func (s *BulkInsertSuite) TestPK() {
	s.pkType = schemapb.DataType_Int64
	s.run()

	s.pkType = schemapb.DataType_VarChar
	s.run()
}

func (s *BulkInsertSuite) TestZeroRowCount() {
	const (
		rowCount = 0
	)

	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 60*time.Second)
	defer cancel()

	collectionName := "TestBulkInsert_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchema(collectionName, dim, true,
		&schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true, AutoID: true},
		&schemapb.FieldSchema{FieldID: 101, Name: "image_path", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}}},
		&schemapb.FieldSchema{FieldID: 102, Name: "embeddings", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}}},
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createCollectionStatus.GetErrorCode())

	var files []*internalpb.ImportFile
	filePath := fmt.Sprintf("/tmp/test_%d.parquet", rand.Int())
	err = GenerateParquetFile(filePath, schema, rowCount)
	s.NoError(err)
	defer os.Remove(filePath)
	files = []*internalpb.ImportFile{
		{
			Paths: []string{
				filePath,
			},
		},
	}

	importResp, err := c.Proxy.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: collectionName,
		Files:          files,
	})
	s.NoError(err)
	log.Info("Import result", zap.Any("importResp", importResp))

	jobID := importResp.GetJobID()
	err = WaitForImportDone(ctx, c, jobID)
	s.NoError(err)

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.Empty(segments)
}

func (s *BulkInsertSuite) TestDiskQuotaExceeded() {
	paramtable.Get().Save(paramtable.Get().QuotaConfig.DiskProtectionEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().QuotaConfig.DiskQuota.Key, "100")
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.DiskProtectionEnabled.Key)
	defer paramtable.Get().Reset(paramtable.Get().QuotaConfig.DiskQuota.Key)
	s.failed = false
	s.run()

	paramtable.Get().Save(paramtable.Get().QuotaConfig.DiskQuota.Key, "0.01")
	s.failed = true
	s.failedReason = "disk quota exceeded"
	s.run()
}

func TestBulkInsert(t *testing.T) {
	suite.Run(t, new(BulkInsertSuite))
}
