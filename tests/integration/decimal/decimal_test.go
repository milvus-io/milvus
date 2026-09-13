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

// Package decimal covers the end-to-end scenarios listed as "required before
// merge, not yet implemented" in docs/design-docs/design_docs/20260718-decimal_type.md.
// Each test exercises the real insert -> ... -> output round trip through the
// Go SDK's raw gRPC client (MiniCluster), not just the unit-level codec.
package decimal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/parameterutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	decimalDim       = 8
	decimalIDField   = "id"
	decimalVecField  = "vec"
	decimalPriceFld  = "price"
	decimalPrecision = int64(10)
	decimalScale     = int64(2)
)

type DecimalSuite struct {
	integration.MiniClusterSuite
}

// ---- schema / field-data helpers -----------------------------------------

func decimalTypeParams(precision, scale int64) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{Key: common.PrecisionKey, Value: fmt.Sprintf("%d", precision)},
		{Key: common.ScaleKey, Value: fmt.Sprintf("%d", scale)},
	}
}

// newDecimalSchema builds pk(int64) + vec(float vector, decimalDim) + a Decimal
// price field. defaultUnscaled, when non-nil, sets FieldSchema.DefaultValue in
// the canonical wire form, matching how the proxy/rootcoord decode it.
func newDecimalSchema(collection string, precision, scale int64, nullable bool, defaultUnscaled *int64) *schemapb.CollectionSchema {
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         decimalIDField,
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	}
	vec := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     decimalVecField,
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: fmt.Sprintf("%d", decimalDim)},
		},
	}
	price := &schemapb.FieldSchema{
		FieldID:    102,
		Name:       decimalPriceFld,
		DataType:   schemapb.DataType_Decimal,
		TypeParams: decimalTypeParams(precision, scale),
		Nullable:   nullable,
	}
	if defaultUnscaled != nil {
		price.DefaultValue = &schemapb.ValueField{
			Data: &schemapb.ValueField_BytesData{
				BytesData: parameterutil.EncodeUnscaledBytes(*defaultUnscaled),
			},
		}
	}
	return &schemapb.CollectionSchema{
		Name:   collection,
		Fields: []*schemapb.FieldSchema{pk, vec, price},
	}
}

// newDecimalFieldData builds a fully-populated (no nulls) Decimal column from
// unscaled int64 values, using the same codec production code uses.
func newDecimalFieldData(fieldName string, unscaled []int64) *schemapb.FieldData {
	data := make([][]byte, len(unscaled))
	for i, v := range unscaled {
		data[i] = parameterutil.EncodeUnscaledBytes(v)
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Decimal,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BytesData{
					BytesData: &schemapb.BytesArray{Data: data},
				},
			},
		},
	}
}

// newNullableDecimalFieldData mirrors newDecimalFieldData but marks positions
// where valid[i] is false as null: an empty-bytes placeholder plus valid_data,
// exactly as the SDK/proxy null-expansion path produces.
func newNullableDecimalFieldData(fieldName string, unscaled []int64, valid []bool) *schemapb.FieldData {
	data := make([][]byte, len(unscaled))
	for i, v := range unscaled {
		if !valid[i] {
			data[i] = []byte{}
			continue
		}
		data[i] = parameterutil.EncodeUnscaledBytes(v)
	}
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_Decimal,
		FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BytesData{
					BytesData: &schemapb.BytesArray{Data: data},
				},
			},
		},
	}
	typeutil.SetFieldDataValidData(fd, valid)
	return fd
}

// decodeDecimalFieldData decodes a returned Decimal column back to unscaled
// int64 plus its per-row validity, using the field's own valid_data (wherever
// it now lives — see the design doc's valid_data-migration section) rather
// than assuming a fixed length or location.
func (s *DecimalSuite) decodeDecimalFieldData(fd *schemapb.FieldData) ([]int64, []bool) {
	raw := fd.GetScalars().GetBytesData().GetData()
	valid := typeutil.GetFieldDataValidData(fd)
	out := make([]int64, len(raw))
	for i, b := range raw {
		if len(valid) == len(raw) && !valid[i] {
			continue
		}
		v, err := parameterutil.DecodeUnscaledBytes(b)
		s.Require().NoError(err, "decoding returned decimal bytes must never fail")
		out[i] = v
	}
	return out, valid
}

func getFieldData(fieldName string, fds []*schemapb.FieldData) *schemapb.FieldData {
	for _, fd := range fds {
		if fd.GetFieldName() == fieldName {
			return fd
		}
	}
	return nil
}

// createCollection creates the collection from schema and returns it ready to
// have data inserted; it does not flush, index, or load.
func (s *DecimalSuite) createCollection(ctx context.Context, schema *schemapb.CollectionSchema) {
	c := s.Cluster
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: schema.GetName(),
		Schema:         marshaled,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
}

// buildIndexAndLoad creates a flat vector index (so growing/sealed data is
// searchable) plus, when withScalarIndex is true, an explicit STL_SORT index
// on the Decimal field, then loads the collection.
func (s *DecimalSuite) buildIndexAndLoad(ctx context.Context, collection string, withScalarIndex bool) {
	c := s.Cluster
	idxStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collection,
		FieldName:      decimalVecField,
		IndexName:      "_default_vec",
		ExtraParams:    integration.ConstructIndexParam(decimalDim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(merr.CheckRPCCall(idxStatus, err))
	s.WaitForIndexBuiltWithIndexName(ctx, collection, decimalVecField, "_default_vec")

	if withScalarIndex {
		scalarStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			CollectionName: collection,
			FieldName:      decimalPriceFld,
			IndexName:      "_default_price",
			ExtraParams:    []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "STL_SORT"}},
		})
		s.Require().NoError(merr.CheckRPCCall(scalarStatus, err))
		s.WaitForIndexBuiltWithIndexName(ctx, collection, decimalPriceFld, "_default_price")
	}

	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(loadStatus, err))
	s.WaitForLoad(ctx, collection)
}

func (s *DecimalSuite) flushAndWait(ctx context.Context, collection string) {
	c := s.Cluster
	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{collection}})
	s.Require().NoError(err)
	segIDs := flushResp.GetCollSegIDs()[collection].GetData()
	flushTs := flushResp.GetCollFlushTs()[collection]
	s.WaitForFlush(ctx, segIDs, flushTs, "", collection)
}

func (s *DecimalSuite) dropCollection(ctx context.Context, collection string) {
	c := s.Cluster
	_, _ = c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: collection})
	status, err := c.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: collection})
	s.NoError(merr.CheckRPCCall(status, err))
}

// ---- Scenario 1: insert + immediate growing-segment query/search ---------

func (s *DecimalSuite) TestGrowingSegmentInsertAndQuery() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.Cluster

	const rowNum = 30
	collection := "TestDecimalGrowing_" + funcutil.GenRandomStr()
	s.createCollection(ctx, newDecimalSchema(collection, decimalPrecision, decimalScale, false, nil))
	s.buildIndexAndLoad(ctx, collection, false)
	defer s.dropCollection(ctx, collection)

	unscaled := make([]int64, rowNum)
	for i := range unscaled {
		unscaled[i] = int64(1000 + i) // 10.00, 10.01, ...
	}
	pk := integration.NewInt64FieldDataWithStart(decimalIDField, rowNum, 0)
	vec := integration.NewFloatVectorFieldData(decimalVecField, rowNum, decimalDim)
	price := newDecimalFieldData(decimalPriceFld, unscaled)

	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pk, vec, price},
		HashKeys:       integration.GenerateHashKeys(rowNum),
		NumRows:        uint32(rowNum),
	})
	s.Require().NoError(merr.CheckRPCCall(insertResult, err))

	// No flush: this must be served entirely out of the growing segment.
	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             fmt.Sprintf("%s >= 0", decimalIDField),
		OutputFields:     []string{decimalPriceFld},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryResult, err))

	got, valid := s.decodeDecimalFieldData(getFieldData(decimalPriceFld, queryResult.GetFieldsData()))
	s.Len(got, rowNum)
	gotSet := map[int64]bool{}
	for i, v := range got {
		s.True(len(valid) == 0 || valid[i], "no nulls inserted, none expected")
		gotSet[v] = true
	}
	for _, v := range unscaled {
		s.True(gotSet[v], "unscaled value %d must round-trip out of the growing segment", v)
	}
}

// ---- Scenario 2: nullable and default-value rows --------------------------

func (s *DecimalSuite) TestNullableAndDefaultValue() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.Cluster

	const rowNum = 40
	defaultUnscaled := int64(500) // 5.00
	collection := "TestDecimalNullable_" + funcutil.GenRandomStr()
	s.createCollection(ctx, newDecimalSchema(collection, decimalPrecision, decimalScale, true, &defaultUnscaled))
	s.buildIndexAndLoad(ctx, collection, false)
	defer s.dropCollection(ctx, collection)

	unscaled := make([]int64, rowNum)
	valid := make([]bool, rowNum)
	for i := range unscaled {
		unscaled[i] = int64(2000 + i)
		valid[i] = i%2 == 0 // even rows carry a real value, odd rows are null
	}
	pk := integration.NewInt64FieldDataWithStart(decimalIDField, rowNum, 0)
	vec := integration.NewFloatVectorFieldData(decimalVecField, rowNum, decimalDim)
	price := newNullableDecimalFieldData(decimalPriceFld, unscaled, valid)

	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pk, vec, price},
		HashKeys:       integration.GenerateHashKeys(rowNum),
		NumRows:        uint32(rowNum),
	})
	s.Require().NoError(merr.CheckRPCCall(insertResult, err))
	s.flushAndWait(ctx, collection)

	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             fmt.Sprintf("%s >= 0", decimalIDField),
		OutputFields:     []string{decimalPriceFld},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryResult, err))

	fd := getFieldData(decimalPriceFld, queryResult.GetFieldsData())
	gotValid := typeutil.GetFieldDataValidData(fd)
	s.Require().Len(gotValid, rowNum, "valid_data must be present and cover every row post-flush")
	nullCount := 0
	for _, v := range gotValid {
		if !v {
			nullCount++
		}
	}
	s.Equal(rowNum/2, nullCount, "exactly the odd-indexed rows must be null")

	// Missing-field-on-insert default-value path: a bare pk+vec insert (no
	// price column at all) must backfill defaultUnscaled for every row.
	const defaultRowNum = 5
	pk2 := integration.NewInt64FieldDataWithStart(decimalIDField, defaultRowNum, int64(rowNum))
	vec2 := integration.NewFloatVectorFieldData(decimalVecField, defaultRowNum, decimalDim)
	insertResult2, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pk2, vec2},
		HashKeys:       integration.GenerateHashKeys(defaultRowNum),
		NumRows:        uint32(defaultRowNum),
	})
	s.Require().NoError(merr.CheckRPCCall(insertResult2, err))
	s.flushAndWait(ctx, collection)

	queryResult2, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             fmt.Sprintf("%s >= %d", decimalIDField, rowNum),
		OutputFields:     []string{decimalPriceFld},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryResult2, err))
	got2, valid2 := s.decodeDecimalFieldData(getFieldData(decimalPriceFld, queryResult2.GetFieldsData()))
	s.Require().Len(got2, defaultRowNum)
	for i, v := range got2 {
		s.True(len(valid2) == 0 || valid2[i], "defaulted rows are not null")
		s.Equal(defaultUnscaled, v, "missing price must backfill to the schema default")
	}
}

// ---- Scenario 3: literal and template filters ------------------------------

func (s *DecimalSuite) TestFiltersLiteralAndTemplate() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.Cluster

	const rowNum = 50
	collection := "TestDecimalFilters_" + funcutil.GenRandomStr()
	s.createCollection(ctx, newDecimalSchema(collection, decimalPrecision, decimalScale, false, nil))
	s.buildIndexAndLoad(ctx, collection, false)
	defer s.dropCollection(ctx, collection)

	// unscaled[i] = 1000 + 100*i -> "10.00", "11.00", ..., "59.00"
	unscaled := make([]int64, rowNum)
	for i := range unscaled {
		unscaled[i] = int64(1000 + 100*i)
	}
	pk := integration.NewInt64FieldDataWithStart(decimalIDField, rowNum, 0)
	vec := integration.NewFloatVectorFieldData(decimalVecField, rowNum, decimalDim)
	price := newDecimalFieldData(decimalPriceFld, unscaled)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pk, vec, price},
		HashKeys:       integration.GenerateHashKeys(rowNum),
		NumRows:        uint32(rowNum),
	})
	s.Require().NoError(merr.CheckRPCCall(insertResult, err))
	s.flushAndWait(ctx, collection)

	query := func(expr string, tmpl map[string]*schemapb.TemplateValue) []int64 {
		resp, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
			CollectionName:     collection,
			Expr:               expr,
			OutputFields:       []string{decimalPriceFld},
			ExprTemplateValues: tmpl,
			ConsistencyLevel:   commonpb.ConsistencyLevel_Strong,
		})
		s.Require().NoError(merr.CheckRPCCall(resp, err))
		got, _ := s.decodeDecimalFieldData(getFieldData(decimalPriceFld, resp.GetFieldsData()))
		return got
	}

	// Comparison: strictly greater than 30.00 -> unscaled 3100..5900 (29 rows).
	greater := query(fmt.Sprintf("%s > 30.00", decimalPriceFld), nil)
	s.Len(greater, 29)
	for _, v := range greater {
		s.Greater(v, int64(3000))
	}

	// Range: 20.00 <= price <= 25.00 -> unscaled 2000,2100,...,2500 (6 rows).
	rangeRes := query(fmt.Sprintf("%s >= 20.00 and %s <= 25.00", decimalPriceFld, decimalPriceFld), nil)
	s.Len(rangeRes, 6)

	// IN list.
	inRes := query(fmt.Sprintf("%s in [10.00, 15.00, 59.00]", decimalPriceFld), nil)
	s.Len(inRes, 2) // 15.00 (unscaled 1500) isn't one of our generated values; 10.00 and 59.00 are.

	// Add/Sub arithmetic against a literal: price - 5 > 50 selects unscaled
	// price - 500 > 5000 i.e. price > 55.00 -> unscaled 5600..5900 (4 rows).
	arith := query(fmt.Sprintf("%s - 5 > 50", decimalPriceFld), nil)
	s.Len(arith, 4)

	// Template filter: same "> 30.00" comparison via a template value carrying
	// the exact source text, matching how literal fixup re-derives from text.
	tmplRes := query(fmt.Sprintf("%s > {threshold}", decimalPriceFld), map[string]*schemapb.TemplateValue{
		"threshold": {Val: &schemapb.TemplateValue_StringVal{StringVal: "30.00"}},
	})
	s.Len(tmplRes, 29, "template-substituted decimal literal must filter identically to the inline literal")
}

// ---- Scenario 4: multi-segment search output ------------------------------

func (s *DecimalSuite) TestMultiSegmentSearch() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	c := s.Cluster

	collection := "TestDecimalMultiSegment_" + funcutil.GenRandomStr()
	s.createCollection(ctx, newDecimalSchema(collection, decimalPrecision, decimalScale, false, nil))
	defer s.dropCollection(ctx, collection)

	revert := s.Cluster.MustModifyMilvusConfig(map[string]string{
		paramtable.Get().DataCoordCfg.SegmentMaxSize.Key: "1", // MB; forces many small segments
	})
	defer revert()

	const rowNum = 200
	const batches = 4
	batchSize := rowNum / batches
	allUnscaled := make([]int64, 0, rowNum)
	for b := 0; b < batches; b++ {
		unscaled := make([]int64, batchSize)
		for i := range unscaled {
			unscaled[i] = int64(b*batchSize + i)
		}
		allUnscaled = append(allUnscaled, unscaled...)
		pk := integration.NewInt64FieldDataWithStart(decimalIDField, batchSize, int64(b*batchSize))
		vec := integration.NewFloatVectorFieldData(decimalVecField, batchSize, decimalDim)
		price := newDecimalFieldData(decimalPriceFld, unscaled)
		insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			CollectionName: collection,
			FieldsData:     []*schemapb.FieldData{pk, vec, price},
			HashKeys:       integration.GenerateHashKeys(batchSize),
			NumRows:        uint32(batchSize),
		})
		s.Require().NoError(merr.CheckRPCCall(insertResult, err))
		s.flushAndWait(ctx, collection) // one flush per batch -> multiple sealed segments
	}

	segments, err := c.ShowSegments(collection)
	s.Require().NoError(err)
	s.Require().Greater(len(segments), 1, "test setup must actually produce multiple segments")
	for _, seg := range segments {
		mlog.Info(context.TODO(), "multi-segment test segment", mlog.String("segment", seg.String()))
	}

	s.buildIndexAndLoad(ctx, collection, false)

	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             fmt.Sprintf("%s >= 0", decimalIDField),
		OutputFields:     []string{decimalPriceFld},
		QueryParams:      []*commonpb.KeyValuePair{{Key: "limit", Value: fmt.Sprintf("%d", rowNum+10)}},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryResult, err))
	got, _ := s.decodeDecimalFieldData(getFieldData(decimalPriceFld, queryResult.GetFieldsData()))
	s.Len(got, rowNum, "result merged across all segments must total every inserted row")
	gotSet := map[int64]bool{}
	for _, v := range got {
		gotSet[v] = true
	}
	for _, v := range allUnscaled {
		s.True(gotSet[v], "value %d from one of the segments must survive multi-segment merge", v)
	}
}

// ---- Scenario 5: flush and sealed-segment reload ---------------------------

func (s *DecimalSuite) TestFlushAndSealedReload() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.Cluster

	const rowNum = 25
	collection := "TestDecimalFlushReload_" + funcutil.GenRandomStr()
	s.createCollection(ctx, newDecimalSchema(collection, decimalPrecision, decimalScale, false, nil))
	s.buildIndexAndLoad(ctx, collection, false)
	defer s.dropCollection(ctx, collection)

	unscaled := make([]int64, rowNum)
	for i := range unscaled {
		unscaled[i] = int64(-1234500 + i) // exercise negative values too
	}
	pk := integration.NewInt64FieldDataWithStart(decimalIDField, rowNum, 0)
	vec := integration.NewFloatVectorFieldData(decimalVecField, rowNum, decimalDim)
	price := newDecimalFieldData(decimalPriceFld, unscaled)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pk, vec, price},
		HashKeys:       integration.GenerateHashKeys(rowNum),
		NumRows:        uint32(rowNum),
	})
	s.Require().NoError(merr.CheckRPCCall(insertResult, err))

	queryBefore, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             fmt.Sprintf("%s >= 0", decimalIDField),
		OutputFields:     []string{decimalPriceFld},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryBefore, err))
	gotBefore, _ := s.decodeDecimalFieldData(getFieldData(decimalPriceFld, queryBefore.GetFieldsData()))

	// This also exercises "Storage V3 growing-source flush" (scenario 7): the
	// rows above only ever existed in the growing segment before this flush,
	// so this Flush call is exactly a growing-source flush through whichever
	// storage version the cluster is configured for. This test does not force
	// a specific storage version — no config knob for that was confirmed
	// during test-writing, so it validates correctness for the cluster's
	// default rather than gambling on an unverified key.
	s.flushAndWait(ctx, collection)

	segments, err := c.ShowSegments(collection)
	s.Require().NoError(err)
	s.Require().NotEmpty(segments)
	for _, seg := range segments {
		mlog.Info(context.TODO(), "flush/reload test segment", mlog.String("segment", seg.String()), mlog.String("storageVersion", fmt.Sprintf("%v", seg.GetStorageVersion())))
	}

	queryAfter, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             fmt.Sprintf("%s >= 0", decimalIDField),
		OutputFields:     []string{decimalPriceFld},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryAfter, err))
	gotAfter, _ := s.decodeDecimalFieldData(getFieldData(decimalPriceFld, queryAfter.GetFieldsData()))

	s.ElementsMatch(gotBefore, gotAfter, "the same logical values must round-trip identically before (growing) and after (sealed) flush")
}

// ---- Scenario 6: STL_SORT indexed retrieval --------------------------------

func (s *DecimalSuite) TestSTLSortIndexedRetrieval() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.Cluster

	const rowNum = 60
	collection := "TestDecimalSTLSort_" + funcutil.GenRandomStr()
	s.createCollection(ctx, newDecimalSchema(collection, decimalPrecision, decimalScale, false, nil))
	defer s.dropCollection(ctx, collection)

	unscaled := make([]int64, rowNum)
	for i := range unscaled {
		unscaled[i] = int64(i * 10) // 0.00, 0.10, 0.20, ...
	}
	pk := integration.NewInt64FieldDataWithStart(decimalIDField, rowNum, 0)
	vec := integration.NewFloatVectorFieldData(decimalVecField, rowNum, decimalDim)
	price := newDecimalFieldData(decimalPriceFld, unscaled)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pk, vec, price},
		HashKeys:       integration.GenerateHashKeys(rowNum),
		NumRows:        uint32(rowNum),
	})
	s.Require().NoError(merr.CheckRPCCall(insertResult, err))
	s.flushAndWait(ctx, collection)

	// withScalarIndex=true builds an explicit STL_SORT index on the Decimal
	// field itself, on top of the vector index every other test also needs.
	s.buildIndexAndLoad(ctx, collection, true)

	desc, err := c.MilvusClient.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
		CollectionName: collection,
		FieldName:      decimalPriceFld,
	})
	s.Require().NoError(merr.CheckRPCCall(desc, err))
	s.Require().NotEmpty(desc.GetIndexDescriptions(), "STL_SORT index must actually exist on the Decimal field")

	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             fmt.Sprintf("%s >= 1.00 and %s <= 2.00", decimalPriceFld, decimalPriceFld),
		OutputFields:     []string{decimalPriceFld},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryResult, err))
	got, _ := s.decodeDecimalFieldData(getFieldData(decimalPriceFld, queryResult.GetFieldsData()))
	s.Len(got, 11) // 1.00, 1.10, ..., 2.00
	for _, v := range got {
		s.GreaterOrEqual(v, int64(100))
		s.LessOrEqual(v, int64(200))
	}
}

// ---- Scenario 8: compaction and restart ------------------------------------

func (s *DecimalSuite) TestCompactionAndRestart() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	c := s.Cluster

	const rowNum = 40
	collection := "TestDecimalCompactRestart_" + funcutil.GenRandomStr()
	s.createCollection(ctx, newDecimalSchema(collection, decimalPrecision, decimalScale, false, nil))
	s.buildIndexAndLoad(ctx, collection, false)
	defer s.dropCollection(ctx, collection)

	unscaled := make([]int64, rowNum)
	for i := range unscaled {
		unscaled[i] = int64(9000000 + i)
	}
	pk := integration.NewInt64FieldDataWithStart(decimalIDField, rowNum, 0)
	vec := integration.NewFloatVectorFieldData(decimalVecField, rowNum, decimalDim)
	price := newDecimalFieldData(decimalPriceFld, unscaled)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pk, vec, price},
		HashKeys:       integration.GenerateHashKeys(rowNum),
		NumRows:        uint32(rowNum),
	})
	s.Require().NoError(merr.CheckRPCCall(insertResult, err))
	s.flushAndWait(ctx, collection)

	desCollResp, err := c.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(desCollResp, err))
	compactResp, err := c.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{CollectionID: desCollResp.GetCollectionID()})
	s.Require().NoError(merr.CheckRPCCall(compactResp, err))
	s.Eventually(func() bool {
		resp, err := c.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{CompactionID: compactResp.GetCompactionID()})
		return err == nil && resp.GetState() == commonpb.CompactionState_Completed
	}, 3*time.Minute, 3*time.Second)

	// Restart the query nodes so the reload path is exercised, not just an
	// in-memory cache still holding the pre-compaction result.
	for _, qn := range c.GetAllQueryNodes() {
		qn.Stop()
	}
	c.AddQueryNode()

	s.Eventually(func() bool {
		loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: collection})
		return err == nil && merr.Ok(loadStatus)
	}, 2*time.Minute, 3*time.Second)
	s.WaitForLoad(ctx, collection)

	queryResult, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             fmt.Sprintf("%s >= 0", decimalIDField),
		OutputFields:     []string{decimalPriceFld},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryResult, err))
	got, _ := s.decodeDecimalFieldData(getFieldData(decimalPriceFld, queryResult.GetFieldsData()))
	s.Len(got, rowNum, "every row must survive compaction + query-node restart")
	gotSet := map[int64]bool{}
	for _, v := range got {
		gotSet[v] = true
	}
	for _, v := range unscaled {
		s.True(gotSet[v], "value %d must survive compaction + restart intact", v)
	}
}

// ---- Scenario 9: add-field schema evolution --------------------------------

func (s *DecimalSuite) TestSchemaEvolutionAddField() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.Cluster

	const rowNum = 20
	collection := "TestDecimalAddField_" + funcutil.GenRandomStr()

	// Start WITHOUT the Decimal field at all.
	base := &schemapb.CollectionSchema{
		Name: collection,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: decimalIDField, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: decimalVecField, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
				{Key: common.DimKey, Value: fmt.Sprintf("%d", decimalDim)},
			}},
		},
	}
	s.createCollection(ctx, base)
	defer s.dropCollection(ctx, collection)

	pk := integration.NewInt64FieldDataWithStart(decimalIDField, rowNum, 0)
	vec := integration.NewFloatVectorFieldData(decimalVecField, rowNum, decimalDim)
	insertResult, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pk, vec},
		HashKeys:       integration.GenerateHashKeys(rowNum),
		NumRows:        uint32(rowNum),
	})
	s.Require().NoError(merr.CheckRPCCall(insertResult, err))
	s.flushAndWait(ctx, collection)

	// Add a nullable Decimal field with a default, backfilling every
	// pre-existing row via SegmentInterface.cpp's default-value path (see
	// design doc: this is a whole-column backfill, not a per-row null
	// distinction).
	defaultUnscaled := int64(0)
	newField := &schemapb.FieldSchema{
		Name:       decimalPriceFld,
		DataType:   schemapb.DataType_Decimal,
		TypeParams: decimalTypeParams(decimalPrecision, decimalScale),
		Nullable:   true,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_BytesData{BytesData: parameterutil.EncodeUnscaledBytes(defaultUnscaled)},
		},
	}
	fieldBytes, err := proto.Marshal(newField)
	s.Require().NoError(err)
	addStatus, err := c.MilvusClient.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{
		CollectionName: collection,
		Schema:         fieldBytes,
	})
	s.Require().NoError(merr.CheckRPCCall(addStatus, err))

	desc, err := c.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: collection})
	s.Require().NoError(merr.CheckRPCCall(desc, err))
	found := false
	for _, f := range desc.GetSchema().GetFields() {
		if f.GetName() == decimalPriceFld {
			found = true
		}
	}
	s.True(found, "AddCollectionField must make the new Decimal field visible in the schema")

	s.buildIndexAndLoad(ctx, collection, false)

	// Pre-existing rows must read back with the backfilled default.
	queryOld, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             fmt.Sprintf("%s < %d", decimalIDField, rowNum),
		OutputFields:     []string{decimalPriceFld},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryOld, err))
	gotOld, validOld := s.decodeDecimalFieldData(getFieldData(decimalPriceFld, queryOld.GetFieldsData()))
	s.Require().Len(gotOld, rowNum)
	for i, v := range gotOld {
		s.True(len(validOld) == 0 || validOld[i])
		s.Equal(defaultUnscaled, v)
	}

	// New rows can now carry a real Decimal value.
	newUnscaled := []int64{12345}
	pk2 := integration.NewInt64FieldDataWithStart(decimalIDField, 1, int64(rowNum))
	vec2 := integration.NewFloatVectorFieldData(decimalVecField, 1, decimalDim)
	price2 := newDecimalFieldData(decimalPriceFld, newUnscaled)
	insertResult2, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: collection,
		FieldsData:     []*schemapb.FieldData{pk2, vec2, price2},
		HashKeys:       integration.GenerateHashKeys(1),
		NumRows:        1,
	})
	s.Require().NoError(merr.CheckRPCCall(insertResult2, err))

	queryNew, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName:   collection,
		Expr:             fmt.Sprintf("%s == %d", decimalIDField, rowNum),
		OutputFields:     []string{decimalPriceFld},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(queryNew, err))
	gotNew, _ := s.decodeDecimalFieldData(getFieldData(decimalPriceFld, queryNew.GetFieldsData()))
	s.Require().Len(gotNew, 1)
	s.Equal(newUnscaled[0], gotNew[0])
}

// ---- Scenario 10: precision/scale/rounding/overflow boundaries ------------

// TestPrecisionScaleBoundaries mirrors the design doc's worked-examples table,
// but through the real insert/schema-creation path instead of unit-testing
// the validator function directly.
func (s *DecimalSuite) TestPrecisionScaleBoundaries() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.Cluster

	// Schema-creation-time rejections.
	negScale := "TestDecimalNegScale_" + funcutil.GenRandomStr()
	schema := newDecimalSchema(negScale, 5, -1, false, nil)
	marshaled, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: negScale, Schema: marshaled, ShardsNum: common.DefaultShardsNum,
	})
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, status.GetErrorCode(), "negative scale must be rejected at schema-creation time")

	tooWide := "TestDecimalTooWidePrecision_" + funcutil.GenRandomStr()
	schema2 := newDecimalSchema(tooWide, 19, 0, false, nil)
	marshaled2, err := proto.Marshal(schema2)
	s.Require().NoError(err)
	status2, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: tooWide, Schema: marshaled2, ShardsNum: common.DefaultShardsNum,
	})
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, status2.GetErrorCode(), "precision > MaxDecimalPrecision (18) must be rejected — the Decimal128 tier isn't implemented")

	// Insert-time boundary: |unscaled| <= 10^precision - 1 for DECIMAL(5, 2)
	// (i.e. magnitude <= 999.99) must succeed; one past it must fail.
	boundary := "TestDecimalInsertBoundary_" + funcutil.GenRandomStr()
	s.createCollection(ctx, newDecimalSchema(boundary, 5, 2, false, nil))
	s.buildIndexAndLoad(ctx, boundary, false)
	defer s.dropCollection(ctx, boundary)

	maxUnscaled := parameterutil.MaxUnscaledValue(5) // 99999 -> 999.99
	pkOK := integration.NewInt64FieldDataWithStart(decimalIDField, 1, 0)
	vecOK := integration.NewFloatVectorFieldData(decimalVecField, 1, decimalDim)
	priceOK := newDecimalFieldData(decimalPriceFld, []int64{maxUnscaled})
	insertOK, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: boundary,
		FieldsData:     []*schemapb.FieldData{pkOK, vecOK, priceOK},
		HashKeys:       integration.GenerateHashKeys(1),
		NumRows:        1,
	})
	s.NoError(err)
	s.Equal(commonpb.ErrorCode_Success, insertOK.GetStatus().GetErrorCode(), "the exact max representable value for DECIMAL(5,2) must be accepted")

	pkBad := integration.NewInt64FieldDataWithStart(decimalIDField, 1, 1)
	vecBad := integration.NewFloatVectorFieldData(decimalVecField, 1, decimalDim)
	priceBad := newDecimalFieldData(decimalPriceFld, []int64{maxUnscaled + 1})
	insertBad, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: boundary,
		FieldsData:     []*schemapb.FieldData{pkBad, vecBad, priceBad},
		HashKeys:       integration.GenerateHashKeys(1),
		NumRows:        1,
	})
	s.NoError(err)
	s.NotEqual(commonpb.ErrorCode_Success, insertBad.GetStatus().GetErrorCode(), "one past the max representable value for DECIMAL(5,2) must be rejected, not silently truncated")
}

func TestDecimal(t *testing.T) {
	suite.Run(t, new(DecimalSuite))
}
