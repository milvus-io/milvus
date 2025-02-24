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

package sparse_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

type SparseTestSuite struct {
	integration.MiniClusterSuite
}

func (s *SparseTestSuite) createCollection(ctx context.Context, c *integration.MiniClusterV2, dbName string) string {
	collectionName := "TestSparse" + funcutil.GenRandomStr()

	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         integration.SparseFloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_SparseFloatVector,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	schema := &schemapb.CollectionSchema{
		Name:   collectionName,
		AutoID: true,
		Fields: []*schemapb.FieldSchema{pk, fVec},
	}

	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("CreateCollection result", zap.Any("createCollectionStatus", createCollectionStatus))
	showCollectionsResp, err := c.Proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.Equal(showCollectionsResp.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	log.Info("ShowCollections result", zap.Any("showCollectionsResp", showCollectionsResp))
	return collectionName
}

func (s *SparseTestSuite) TestSparse_should_not_speficy_dim() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dbName = ""
		rowNum = 3000
	)

	collectionName := "TestSparse" + funcutil.GenRandomStr()

	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         integration.SparseFloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_SparseFloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", 10),
			},
		},
		IndexParams: nil,
	}
	schema := &schemapb.CollectionSchema{
		Name:   collectionName,
		AutoID: true,
		Fields: []*schemapb.FieldSchema{pk, fVec},
	}

	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.NotEqual(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)
}

func (s *SparseTestSuite) TestSparse_invalid_insert() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dbName = ""
		rowNum = 3000
	)

	collectionName := s.createCollection(ctx, c, dbName)

	// valid insert
	fVecColumn := integration.NewSparseFloatVectorFieldData(integration.SparseFloatVecField, rowNum)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	sparseVecs := fVecColumn.Field.(*schemapb.FieldData_Vectors).Vectors.GetSparseFloatVector()

	// of each row, length of indices and data must equal
	sparseVecs.Contents[0] = append(sparseVecs.Contents[0], make([]byte, 4)...)
	insertResult, err = c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.NotEqual(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	sparseVecs.Contents[0] = sparseVecs.Contents[0][:len(sparseVecs.Contents[0])-4]

	// empty row is allowed
	sparseVecs.Contents[0] = []byte{}
	insertResult, err = c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)

	// unsorted column index is not allowed
	sparseVecs.Contents[0] = make([]byte, 16)
	typeutil.SparseFloatRowSetAt(sparseVecs.Contents[0], 0, 20, 0.1)
	typeutil.SparseFloatRowSetAt(sparseVecs.Contents[0], 1, 10, 0.2)
	insertResult, err = c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.NotEqual(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
}

func (s *SparseTestSuite) TestSparse_invalid_index_build() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dbName = ""
		rowNum = 3000
	)

	collectionName := s.createCollection(ctx, c, dbName)

	// valid insert
	fVecColumn := integration.NewSparseFloatVectorFieldData(integration.SparseFloatVecField, rowNum)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
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

	// unsupported index type
	indexParams := []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: integration.IndexFaissIvfPQ,
		},
		{
			Key:   common.MetricTypeKey,
			Value: metric.IP,
		},
	}

	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.SparseFloatVecField,
		IndexName:      "_default",
		ExtraParams:    indexParams,
	})
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	// nonexist index
	indexParams = []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: "INDEX_WHAT",
		},
		{
			Key:   common.MetricTypeKey,
			Value: metric.IP,
		},
	}

	createIndexStatus, err = c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.SparseFloatVecField,
		IndexName:      "_default",
		ExtraParams:    indexParams,
	})
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	// incorrect metric type
	indexParams = []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: integration.IndexSparseInvertedIndex,
		},
		{
			Key:   common.MetricTypeKey,
			Value: metric.L2,
		},
	}

	createIndexStatus, err = c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.SparseFloatVecField,
		IndexName:      "_default",
		ExtraParams:    indexParams,
	})
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	// incorrect drop ratio build
	indexParams = []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: integration.IndexSparseInvertedIndex,
		},
		{
			Key:   common.MetricTypeKey,
			Value: metric.L2,
		},
		{
			Key:   common.DropRatioBuildKey,
			Value: "-0.1",
		},
	}

	createIndexStatus, err = c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.SparseFloatVecField,
		IndexName:      "_default",
		ExtraParams:    indexParams,
	})
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	// incorrect drop ratio build
	indexParams = []*commonpb.KeyValuePair{
		{
			Key:   common.IndexTypeKey,
			Value: integration.IndexSparseInvertedIndex,
		},
		{
			Key:   common.MetricTypeKey,
			Value: metric.L2,
		},
		{
			Key:   common.DropRatioBuildKey,
			Value: "1.1",
		},
	}

	createIndexStatus, err = c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.SparseFloatVecField,
		IndexName:      "_default",
		ExtraParams:    indexParams,
	})
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
}

func (s *SparseTestSuite) TestSparse_invalid_search_request() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dbName = ""
		rowNum = 3000
	)

	collectionName := s.createCollection(ctx, c, dbName)

	// valid insert
	fVecColumn := integration.NewSparseFloatVectorFieldData(integration.SparseFloatVecField, rowNum)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
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

	indexType := integration.IndexSparseInvertedIndex
	metricType := metric.IP

	indexParams := []*commonpb.KeyValuePair{
		{
			Key:   common.MetricTypeKey,
			Value: metricType,
		},
		{
			Key:   common.IndexTypeKey,
			Value: indexType,
		},
		{
			Key:   common.DropRatioBuildKey,
			Value: "0.1",
		},
	}

	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.SparseFloatVecField,
		IndexName:      "_default",
		ExtraParams:    indexParams,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, integration.SparseFloatVecField)

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

	params := integration.GetSearchParams(indexType, metricType)
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.SparseFloatVecField, schemapb.DataType_SparseFloatVector, nil, metricType, params, nq, 0, topk, roundDecimal)

	replaceQuery := func(vecs *schemapb.SparseFloatArray) {
		values := make([][]byte, 0, 1)
		bs, err := proto.Marshal(vecs)
		if err != nil {
			panic(err)
		}
		values = append(values, bs)

		plg := &commonpb.PlaceholderGroup{
			Placeholders: []*commonpb.PlaceholderValue{
				{
					Tag:    "$0",
					Type:   commonpb.PlaceholderType_SparseFloatVector,
					Values: values,
				},
			},
		}
		plgBs, err := proto.Marshal(plg)
		if err != nil {
			panic(err)
		}
		searchReq.PlaceholderGroup = plgBs
	}

	sparseVecs := integration.GenerateSparseFloatArray(nq)

	// negative column index
	oldIdx := typeutil.SparseFloatRowIndexAt(sparseVecs.Contents[0], 0)
	var newIdx int32 = -10
	binary.LittleEndian.PutUint32(sparseVecs.Contents[0][0:], uint32(newIdx))
	replaceQuery(sparseVecs)
	searchResult, err := c.Proxy.Search(ctx, searchReq)
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
	binary.LittleEndian.PutUint32(sparseVecs.Contents[0][0:], oldIdx)

	// of each row, length of indices and data must equal
	sparseVecs.Contents[0] = append(sparseVecs.Contents[0], make([]byte, 4)...)
	replaceQuery(sparseVecs)
	searchResult, err = c.Proxy.Search(ctx, searchReq)
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
	sparseVecs.Contents[0] = sparseVecs.Contents[0][:len(sparseVecs.Contents[0])-4]

	// empty row is not allowed
	sparseVecs.Contents[0] = []byte{}
	replaceQuery(sparseVecs)
	searchResult, err = c.Proxy.Search(ctx, searchReq)
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())

	// column index in the same row must be ordered
	sparseVecs.Contents[0] = make([]byte, 16)
	typeutil.SparseFloatRowSetAt(sparseVecs.Contents[0], 0, 20, 0.1)
	typeutil.SparseFloatRowSetAt(sparseVecs.Contents[0], 1, 10, 0.2)
	replaceQuery(sparseVecs)
	searchResult, err = c.Proxy.Search(ctx, searchReq)
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
}

func TestSparse(t *testing.T) {
	suite.Run(t, new(SparseTestSuite))
}
