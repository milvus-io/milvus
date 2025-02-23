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

package materializedview

import (
	"context"
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
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type MaterializedViewTestSuite struct {
	integration.MiniClusterSuite

	isPartitionKeyEnable      bool
	partitionKeyFieldDataType schemapb.DataType
}

// func (s *MaterializedViewTestSuite) SetupTest() {
// 	s.T().Log("Setup in mv")
// }

func (s *MaterializedViewTestSuite) run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dim                   = 128
		dbName                = ""
		rowNum                = 1000
		partitionKeyFieldName = "pid"
	)

	collectionName := "IntegrationTestMaterializedView" + funcutil.GenRandomStr()
	schema := integration.ConstructSchema(collectionName, dim, false)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:        102,
		Name:           partitionKeyFieldName,
		Description:    "",
		DataType:       s.partitionKeyFieldDataType,
		TypeParams:     []*commonpb.KeyValuePair{{Key: "max_length", Value: "100"}},
		IndexParams:    nil,
		IsPartitionKey: s.isPartitionKeyEnable,
	})
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.NoError(merr.Error(createCollectionStatus))

	pkFieldData := integration.NewInt64FieldData(integration.Int64Field, rowNum)
	vecFieldData := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	var partitionKeyFieldData *schemapb.FieldData
	switch s.partitionKeyFieldDataType {
	case schemapb.DataType_Int64:
		partitionKeyFieldData = integration.NewInt64SameFieldData(partitionKeyFieldName, rowNum, 0)
	case schemapb.DataType_VarChar:
		partitionKeyFieldData = integration.NewVarCharSameFieldData(partitionKeyFieldName, rowNum, "a")
	default:
		s.FailNow("unsupported partition key field data type")
	}
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.Proxy.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkFieldData, vecFieldData, partitionKeyFieldData},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(insertResult.GetStatus()))

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

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.L2),
	})
	s.NoError(err)
	s.NoError(merr.Error(createIndexStatus))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := c.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.NoError(merr.Error(loadStatus))
	s.WaitForLoad(ctx, collectionName)

	{
		var expr string

		switch s.partitionKeyFieldDataType {
		case schemapb.DataType_Int64:
			expr = partitionKeyFieldName + " == 0"
		case schemapb.DataType_VarChar:
			expr = partitionKeyFieldName + " == \"a\""
		default:
			s.FailNow("unsupported partition key field data type")
		}

		nq := 1
		topk := 10
		roundDecimal := -1

		params := integration.GetSearchParams(integration.IndexHNSW, metric.L2)
		searchReq := integration.ConstructSearchRequest("", collectionName, expr,
			integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

		searchResult, err := c.Proxy.Search(ctx, searchReq)
		s.NoError(err)
		s.NoError(merr.Error(searchResult.GetStatus()))
		s.Equal(topk, len(searchResult.GetResults().GetScores()))
	}

	status, err := s.Cluster.Proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.NoError(merr.Error(status))
}

func (s *MaterializedViewTestSuite) TestPartitionKeyDisabledInt64() {
	s.isPartitionKeyEnable = false
	s.partitionKeyFieldDataType = schemapb.DataType_Int64
	s.run()
}

func (s *MaterializedViewTestSuite) TestMvInt64() {
	s.isPartitionKeyEnable = true
	s.partitionKeyFieldDataType = schemapb.DataType_Int64
	s.run()
}

func (s *MaterializedViewTestSuite) TestPartitionKeyDisabledVarChar() {
	s.isPartitionKeyEnable = false
	s.partitionKeyFieldDataType = schemapb.DataType_VarChar
	s.run()
}

func (s *MaterializedViewTestSuite) TestMvVarChar() {
	s.isPartitionKeyEnable = true
	s.partitionKeyFieldDataType = schemapb.DataType_VarChar
	s.run()
}

func TestMaterializedViewEnabled(t *testing.T) {
	paramtable.Init()
	paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("true")
	defer paramtable.Get().CommonCfg.EnableMaterializedView.SwapTempValue("false")
	suite.Run(t, new(MaterializedViewTestSuite))
}
