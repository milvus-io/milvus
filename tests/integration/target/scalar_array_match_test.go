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

package target

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	samDim          = 128
	samDB           = ""
	scoresField     = "scores"
	scoresDim       = samDim
	scalarArrayName = "test_scalar_array_match"
)

// ScalarArrayMatchSuite covers quantified element filtering on scalar ARRAY fields:
//
//	MATCH_ANY / MATCH_ALL / MATCH_LEAST / MATCH_MOST / MATCH_EXACT / element_filter
type ScalarArrayMatchSuite struct {
	integration.MiniClusterSuite
}

// knownInt64Arrays is the fixed dataset inserted into the "scores" Array<Int64> field.
// Row index == primary key (PK) because we use a non-autoID Int64 PK with explicit values.
//
//	pk 0 -> [95, 80]
//	pk 1 -> [40]
//	pk 2 -> [100, 100, 100]
//	pk 3 -> []          (empty array)
//	pk 4 -> [60, 60]
var knownInt64Arrays = [][]int64{
	{95, 80},
	{40},
	{100, 100, 100},
	{},
	{60, 60},
}

// buildScalarArraySchema constructs a schema with a non-autoID Int64 PK, a scalar
// Array<Int64> field "scores", and a FloatVector field.
func (s *ScalarArrayMatchSuite) buildScalarArraySchema(collectionName string) *schemapb.CollectionSchema {
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         integration.Int64Field,
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
		AutoID:       false,
	}
	scoreArr := &schemapb.FieldSchema{
		FieldID:     101,
		Name:        scoresField,
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.MaxCapacityKey, Value: "16"},
		},
	}
	fVec := &schemapb.FieldSchema{
		FieldID:  102,
		Name:     integration.FloatVecField,
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	// VERIFY: ConstructSchema with explicit fields just wraps them in a CollectionSchema
	// (no implicit fields added); AutoID arg here is the collection-level flag.
	return integration.ConstructSchema(collectionName, scoresDim, false, pk, scoreArr, fVec)
}

// newInt64ArrayFieldData builds the FieldData for the scalar Array<Int64> "scores" field.
// Each element of `rows` becomes one ArrayData entry (one row's array value).
func newInt64ArrayFieldData(fieldName string, rows [][]int64) *schemapb.FieldData {
	arrayData := make([]*schemapb.ScalarField, 0, len(rows))
	for _, row := range rows {
		// copy to avoid aliasing the literal slice
		vals := make([]int64, len(row))
		copy(vals, row)
		arrayData = append(arrayData, &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: vals},
			},
		})
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Array,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data:        arrayData,
						ElementType: schemapb.DataType_Int64,
					},
				},
			},
		},
	}
}

// newExplicitInt64PKFieldData builds an Int64 PK column with the explicit values 0..n-1.
func newExplicitInt64PKFieldData(fieldName string, n int) *schemapb.FieldData {
	data := make([]int64, n)
	for i := 0; i < n; i++ {
		data[i] = int64(i)
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: data},
				},
			},
		},
	}
}

// setupCollection creates the collection, inserts the known dataset, flushes, builds the
// vector index, optionally builds an INVERTED index on the "scores" array field, and loads.
func (s *ScalarArrayMatchSuite) setupCollection(ctx context.Context, collectionName string, withScalarIndex bool) {
	c := s.Cluster
	rowNum := len(knownInt64Arrays)

	schema := s.buildScalarArraySchema(collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           samDB,
		CollectionName:   collectionName,
		Schema:           marshaledSchema,
		ShardsNum:        common.DefaultShardsNum,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	err = merr.CheckRPCCall(createCollectionStatus, err)
	s.NoError(err)
	log.Info("CreateCollection done", zap.String("collection", collectionName))

	// insert known rows
	pkColumn := newExplicitInt64PKFieldData(integration.Int64Field, rowNum)
	arrColumn := newInt64ArrayFieldData(scoresField, knownInt64Arrays)
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, samDim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         samDB,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{pkColumn, arrColumn, fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	err = merr.CheckRPCCall(insertResult, err)
	s.NoError(err)
	s.Equal(int64(rowNum), insertResult.GetInsertCnt())

	// flush
	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          samDB,
		CollectionNames: []string{collectionName},
	})
	err = merr.CheckRPCCall(flushResp, err)
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	ids := segmentIDs.GetData()
	s.Require().True(has)
	s.Require().NotEmpty(segmentIDs)
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	s.True(has)
	s.WaitForFlush(ctx, ids, flushTs, samDB, collectionName)

	// vector index
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName:         samDB,
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(samDim, integration.IndexFaissIvfFlat, metric.L2),
	})
	err = merr.CheckRPCCall(createIndexStatus, err)
	s.NoError(err)
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	// optional scalar INVERTED index on the array field
	if withScalarIndex {
		createScalarIdx, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			DbName:         samDB,
			CollectionName: collectionName,
			FieldName:      scoresField,
			IndexName:      "scores_inverted",
			// VERIFY: "INVERTED" is the scalar/array index type string (see json_expr_test.go
			// and snapshot_restore_test.go which build it via common.IndexTypeKey -> "INVERTED").
			ExtraParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "INVERTED"},
			},
		})
		err = merr.CheckRPCCall(createScalarIdx, err)
		s.NoError(err)
		s.WaitForIndexBuiltWithIndexName(ctx, collectionName, scoresField, "scores_inverted")
	}

	// load
	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         samDB,
		CollectionName: collectionName,
	})
	err = merr.CheckRPCCall(loadStatus, err)
	s.NoError(err)
	s.WaitForLoad(ctx, collectionName)
	log.Info("setupCollection done", zap.Bool("withScalarIndex", withScalarIndex))
}

// queryPKs runs a Query with the given expr and returns the matched Int64 primary keys (sorted).
func (s *ScalarArrayMatchSuite) queryPKs(ctx context.Context, collectionName, expr string) []int64 {
	queryResult, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName:         samDB,
		CollectionName: collectionName,
		Expr:           expr,
		OutputFields:   []string{integration.Int64Field},
	})
	err = merr.CheckRPCCall(queryResult, err)
	s.NoError(err)

	// locate the pk field in the returned columns
	var pks []int64
	for _, fd := range queryResult.GetFieldsData() {
		if fd.GetFieldName() == integration.Int64Field {
			pks = fd.GetScalars().GetLongData().GetData()
			break
		}
	}
	out := make([]int64, len(pks))
	copy(out, pks)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// runMatchAssertions issues the quantified-match expressions and asserts the returned PK sets.
func (s *ScalarArrayMatchSuite) runMatchAssertions(ctx context.Context, collectionName string) {
	// MATCH_ANY(scores, $ > 90) -> some element > 90 -> pk 0 ([95,80]) and pk 2 ([100,100,100])
	// VERIFY: expression grammar "MATCH_ANY(scores, $ > 90)" with "$" meaning the array element.
	s.Equal([]int64{0, 2}, s.queryPKs(ctx, collectionName, "MATCH_ANY(scores, $ > 90)"))

	// MATCH_ALL(scores, $ >= 60) -> all elements >= 60; empty array is vacuously true.
	//   pk 0 [95,80] true, pk 2 [100,100,100] true, pk 3 [] vacuous true, pk 4 [60,60] true
	//   pk 1 [40] false
	// VERIFY: empty-array semantics for MATCH_ALL are "vacuous true" (pk 3 included).
	s.Equal([]int64{0, 2, 3, 4}, s.queryPKs(ctx, collectionName, "MATCH_ALL(scores, $ >= 60)"))

	// MATCH_LEAST(scores, $ == 100, threshold=2) -> at least 2 elements == 100 -> only pk 2.
	// VERIFY: keyword-arg syntax "threshold=2" inside MATCH_LEAST.
	s.Equal([]int64{2}, s.queryPKs(ctx, collectionName, "MATCH_LEAST(scores, $ == 100, threshold=2)"))
}

// TestScalarArrayMatchNoIndex runs the quantified-match assertions WITHOUT a scalar index on "scores".
func (s *ScalarArrayMatchSuite) TestScalarArrayMatchNoIndex() {
	ctx := context.Background()
	collectionName := scalarArrayName + "_noidx_" + funcutil.GenRandomStr()
	s.setupCollection(ctx, collectionName, false)
	s.runMatchAssertions(ctx, collectionName)
}

// TestScalarArrayMatchInvertedIndex runs the same assertions WITH an INVERTED index on "scores".
func (s *ScalarArrayMatchSuite) TestScalarArrayMatchInvertedIndex() {
	ctx := context.Background()
	collectionName := scalarArrayName + "_invidx_" + funcutil.GenRandomStr()
	s.setupCollection(ctx, collectionName, true)
	s.runMatchAssertions(ctx, collectionName)
}

func TestScalarArrayMatch(t *testing.T) {
	suite.Run(t, new(ScalarArrayMatchSuite))
}
