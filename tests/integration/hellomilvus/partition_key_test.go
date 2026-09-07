/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hellomilvus

import (
	"context"
	"fmt"

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
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *HelloMilvusSuite) TestPartitionKey() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 1000
	)

	collectionName := "TestPartitionKey" + funcutil.GenRandomStr()
	schema := integration.ConstructSchema(collectionName, dim, false)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:        102,
		Name:           "pid",
		Description:    "",
		DataType:       schemapb.DataType_Int64,
		TypeParams:     nil,
		IndexParams:    nil,
		IsPartitionKey: true,
	})
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	if createCollectionStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createCollectionStatus fail reason", zap.String("reason", createCollectionStatus.GetReason()))
	}
	s.Equal(createCollectionStatus.GetErrorCode(), commonpb.ErrorCode_Success)

	{
		pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, 0)
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
		partitionKeyColumn := integration.NewInt64SameFieldData("pid", rowNum, 1)
		hashKeys := integration.GenerateHashKeys(rowNum)
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{pkColumn, fVecColumn, partitionKeyColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	}

	{
		pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, rowNum)
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
		partitionKeyColumn := integration.NewInt64SameFieldData("pid", rowNum, 10)
		hashKeys := integration.GenerateHashKeys(rowNum)
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{pkColumn, fVecColumn, partitionKeyColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	}

	{
		pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, rowNum*2)
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
		partitionKeyColumn := integration.NewInt64SameFieldData("pid", rowNum, 100)
		hashKeys := integration.GenerateHashKeys(rowNum)
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{pkColumn, fVecColumn, partitionKeyColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.Equal(insertResult.GetStatus().GetErrorCode(), commonpb.ErrorCode_Success)
	}

	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
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

	s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)
	segments, err := c.ShowSegments(collectionName)
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}

	// create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	if createIndexStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("createIndexStatus fail reason", zap.String("reason", createIndexStatus.GetReason()))
	}
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	if loadStatus.GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("loadStatus fail reason", zap.String("reason", loadStatus.GetReason()))
	}
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	{
		// search without partition key
		expr := fmt.Sprintf("%s > 0", integration.Int64Field)
		nq := 10
		topk := 10
		roundDecimal := -1

		params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
		searchReq := integration.ConstructSearchRequest("", collectionName, expr,
			integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

		searchResult, err := c.MilvusClient.Search(ctx, searchReq)

		if searchResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("searchResult fail reason", zap.String("reason", searchResult.GetStatus().GetReason()))
		}
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
	}

	{
		// search with partition key
		expr := fmt.Sprintf("%s > 0 && pid == 1", integration.Int64Field)
		nq := 10
		topk := 10
		roundDecimal := -1

		params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
		searchReq := integration.ConstructSearchRequest("", collectionName, expr,
			integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

		searchResult, err := c.MilvusClient.Search(ctx, searchReq)

		if searchResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("searchResult fail reason", zap.String("reason", searchResult.GetStatus().GetReason()))
		}
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
	}

	{
		queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Expr:           "",
			OutputFields:   []string{"count(*)"},
		})
		if queryResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("searchResult fail reason", zap.String("reason", queryResult.GetStatus().GetReason()))
		}
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, queryResult.GetStatus().GetErrorCode())
	}

	{
		queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Expr:           "pid == 1",
			OutputFields:   []string{"count(*)"},
		})
		if queryResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("searchResult fail reason", zap.String("reason", queryResult.GetStatus().GetReason()))
		}
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, queryResult.GetStatus().GetErrorCode())
	}

	{
		deleteResult, err := c.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Expr:           integration.Int64Field + " < 1000",
		})
		if deleteResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("deleteResult fail reason", zap.String("reason", deleteResult.GetStatus().GetReason()))
		}
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, deleteResult.GetStatus().GetErrorCode())
	}

	{
		deleteResult, err := c.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Expr:           integration.Int64Field + " < 2000 && pid == 10",
		})
		if deleteResult.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			log.Warn("deleteResult fail reason", zap.String("reason", deleteResult.GetStatus().GetReason()))
		}
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, deleteResult.GetStatus().GetErrorCode())
	}
}

// TestPartitionKeyIsolation verifies that partition key isolation mode works
// correctly with various filter expressions: equality (==), IN list, and OR
// of equalities on the partition key field.
func (s *HelloMilvusSuite) TestPartitionKeyIsolation() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	const (
		dim    = 128
		dbName = ""
		rowNum = 1000
	)

	collectionName := "TestPartitionKeyIsolation" + funcutil.GenRandomStr()
	schema := integration.ConstructSchema(collectionName, dim, false)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:        102,
		Name:           "pid",
		DataType:       schemapb.DataType_Int64,
		IsPartitionKey: true,
	})
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	// Create collection
	createStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createStatus.GetErrorCode())

	// Enable partition key isolation
	alterStatus, err := c.MilvusClient.AlterCollection(ctx, &milvuspb.AlterCollectionRequest{
		CollectionName: collectionName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.PartitionKeyIsolationKey, Value: "true"},
		},
	})
	s.NoError(err)
	s.True(merr.Ok(alterStatus))

	// Insert 3 batches with different pid values: 1, 10, 100
	for _, pid := range []int64{1, 10, 100} {
		pkColumn := integration.NewInt64FieldDataWithStart(integration.Int64Field, rowNum, (pid-1)*int64(rowNum))
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
		partitionKeyColumn := integration.NewInt64SameFieldData("pid", rowNum, pid)
		hashKeys := integration.GenerateHashKeys(rowNum)
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{pkColumn, fVecColumn, partitionKeyColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, insertResult.GetStatus().GetErrorCode())
	}

	// Flush
	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
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
	s.WaitForFlush(ctx, ids, flushTs, dbName, collectionName)

	// Create index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// Load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	// Helper: query with count(*) and return the count
	queryCount := func(expr string) int64 {
		queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Expr:           expr,
			OutputFields:   []string{"count(*)"},
		})
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, queryResult.GetStatus().GetErrorCode())
		return queryResult.GetFieldsData()[0].GetScalars().GetLongData().GetData()[0]
	}

	// Helper: query that should fail with an error
	queryExpectError := func(expr string) {
		queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			Expr:           expr,
			OutputFields:   []string{"count(*)"},
		})
		s.NoError(err)
		s.NotEqual(commonpb.ErrorCode_Success, queryResult.GetStatus().GetErrorCode(),
			"expr %q should fail under partition key isolation", expr)
		log.Info("partition key isolation: correctly rejected",
			zap.String("expr", expr),
			zap.String("reason", queryResult.GetStatus().GetReason()))
	}

	// Helper: search that should fail with an error
	searchExpectError := func(expr string) {
		nq := 10
		topk := 10
		roundDecimal := -1
		params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
		searchReq := integration.ConstructSearchRequest("", collectionName, expr,
			integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

		searchResult, err := c.MilvusClient.Search(ctx, searchReq)
		s.NoError(err)
		s.NotEqual(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode(),
			"search with expr %q should fail under partition key isolation", expr)
		log.Info("partition key isolation: search correctly rejected",
			zap.String("expr", expr),
			zap.String("reason", searchResult.GetStatus().GetReason()))
	}

	// ── Valid expressions (only == on partition key) ──

	// Test 1: pid == 1 (single equality — the only supported form)
	count := queryCount("pid == 1")
	s.Equal(int64(rowNum), count, "pid == 1 should return %d rows", rowNum)
	log.Info("partition key isolation: pid == 1", zap.Int64("count", count))

	// Test 2: pid == 1 && additional filter (AND with equality)
	count = queryCount(fmt.Sprintf("pid == 1 && %s >= 0", integration.Int64Field))
	s.Equal(int64(rowNum), count, "pid == 1 with AND filter should return %d rows", rowNum)
	log.Info("partition key isolation: pid == 1 && pk >= 0", zap.Int64("count", count))

	// Test 3: search with pid == 1 (valid)
	{
		expr := "pid == 1"
		nq := 10
		topk := 10
		roundDecimal := -1
		params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
		searchReq := integration.ConstructSearchRequest("", collectionName, expr,
			integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

		searchResult, err := c.MilvusClient.Search(ctx, searchReq)
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
		log.Info("partition key isolation: search with pid == 1",
			zap.Int("numResults", len(searchResult.GetResults().GetScores())))
	}

	// ── Invalid expressions (IN and OR are rejected under isolation) ──

	// Test 4: pid in [1, 10] — rejected (IN not supported)
	queryExpectError("pid in [1, 10]")

	// Test 5: pid == 1 || pid == 10 — rejected (OR not supported;
	// rewriter may merge to IN, which is also rejected)
	queryExpectError("pid == 1 || pid == 10")

	// Test 6: pid in [1, 10, 100] — rejected
	queryExpectError("pid in [1, 10, 100]")

	// Test 7: pid == 1 || pid == 10 || pid == 100 — rejected
	queryExpectError("pid == 1 || pid == 10 || pid == 100")

	// Test 8: search with pid in [1, 10] — rejected
	searchExpectError("pid in [1, 10]")

	// Test 9: search with pid == 1 || pid == 10 — rejected
	searchExpectError("pid == 1 || pid == 10")

	// ── Edge cases ──

	// Test 10: pid == 1 && pid in [1, 10] — rejected (contains IN on partition key)
	queryExpectError("pid == 1 && pid in [1, 10]")

	// Test 11: pid == 1 && pid == 1 — redundant equality, should still be valid
	count = queryCount("pid == 1 && pid == 1")
	s.Equal(int64(rowNum), count, "pid == 1 && pid == 1 should return %d rows", rowNum)
	log.Info("partition key isolation: pid == 1 && pid == 1", zap.Int64("count", count))

	// Test 12: no partition key filter at all — rejected under isolation
	queryExpectError(fmt.Sprintf("%s >= 0", integration.Int64Field))

	// Test 13: search without partition key filter — rejected under isolation
	searchExpectError(fmt.Sprintf("%s >= 0", integration.Int64Field))

	// ── Non-partition-key expressions should not be affected when pid is given ──

	// Test 14: pid == 1 && pk IN list — IN on non-partition-key field is fine
	count = queryCount("pid == 1 && int64Field in [0, 1, 2, 3, 4]")
	s.Equal(int64(5), count, "pid == 1 && int64Field in [0..4] should return 5 rows")
	log.Info("partition key isolation: pid == 1 && pk IN list", zap.Int64("count", count))

	// Test 15: pid == 1 && pk OR conditions — OR on non-partition-key field is fine
	count = queryCount("pid == 1 && (int64Field == 0 || int64Field == 1)")
	s.Equal(int64(2), count, "pid == 1 && (pk==0 || pk==1) should return 2 rows")
	log.Info("partition key isolation: pid == 1 && pk OR", zap.Int64("count", count))

	// Test 16: pid == 1 && complex non-pk expression (range + IN)
	count = queryCount("pid == 1 && int64Field >= 0 && int64Field < 500")
	s.Equal(int64(500), count, "pid == 1 && pk range [0,500) should return 500 rows")
	log.Info("partition key isolation: pid == 1 && pk range", zap.Int64("count", count))

	// Test 17: pid == 1 && non-pk NOT IN
	count = queryCount("pid == 1 && int64Field not in [0, 1, 2]")
	s.Equal(int64(rowNum-3), count, "pid == 1 && pk not in [0,1,2] should return %d rows", rowNum-3)
	log.Info("partition key isolation: pid == 1 && pk NOT IN", zap.Int64("count", count))

	// Test 18: search with pid == 1 && non-pk complex filter — should succeed
	{
		expr := "pid == 1 && int64Field >= 0"
		nq := 10
		topk := 10
		roundDecimal := -1
		params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
		searchReq := integration.ConstructSearchRequest("", collectionName, expr,
			integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.L2, params, nq, dim, topk, roundDecimal)

		searchResult, err := c.MilvusClient.Search(ctx, searchReq)
		s.NoError(err)
		s.Equal(commonpb.ErrorCode_Success, searchResult.GetStatus().GetErrorCode())
		log.Info("partition key isolation: search with pid == 1 && non-pk filter",
			zap.Int("numResults", len(searchResult.GetResults().GetScores())))
	}
}
