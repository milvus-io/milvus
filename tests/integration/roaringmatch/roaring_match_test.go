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

package roaringmatch

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

// RoaringMatchTestSuite verifies roaring_match end to end.
//
// The decisive difference from the bloom_match suite is that roaring_match is
// EXACT, so these assertions are set equality rather than "superset of the true
// members". A false positive is a failure here, not tolerated noise — which is
// what makes this suite able to catch a mis-decoded bitmap, a wrong high/low
// split, or a sign-extension bug that bloom_match's superset assertions would
// silently absorb.
type RoaringMatchTestSuite struct {
	integration.MiniClusterSuite

	dbName        string
	dim           int
	rowNum        int
	creatorDomain int // creatorId of row i is (i % creatorDomain) - negativeShift
}

const (
	creatorIDField   = "creatorId"   // INT64
	creatorID8Field  = "creatorId8"  // INT8
	creatorID16Field = "creatorId16" // INT16
	creatorID32Field = "creatorId32" // INT32
)

// intWidthFields carry the same creatorId value narrowed to each width. Every
// width widens back through its two's-complement bits, so one int64 bitmap
// probes all four — and a sign-extension bug shows up as a miss on the narrow
// fields only.
var intWidthFields = []string{creatorID8Field, creatorID16Field, creatorID32Field, creatorIDField}

// negativeShift pushes half the id domain below zero. Negative ids are the
// highest-risk part of this feature: they map to the top of the uint64 key
// space (INT8(-1) -> 0xffffffffffffffff), so they land in a different Roaring
// high container than the positive ids. A build/probe pair that zero-extended
// instead would still pass every all-positive test.
const negativeShift = 50

func (s *RoaringMatchTestSuite) SetupSuite() {
	// BITMAP's per-row Reverse_Lookup is only cheap with the offset cache, which
	// the index-only fallback needs (default off). Must be set before start.
	s.WithMilvusConfig(paramtable.Get().QueryNodeCfg.IndexOffsetCacheEnabled.Key, "true")
	s.MiniClusterSuite.SetupSuite()
	s.dbName = ""
	s.dim = 128
	s.rowNum = 2000
	s.creatorDomain = 100
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newInt64FieldData(name string, data []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
		}},
	}
}

func newIntNFieldData(name string, dt schemapb.DataType, data []int32) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      dt,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}},
		}},
	}
}

func (s *RoaringMatchTestSuite) bitmapBlob(vals []int64) []byte {
	blob, err := milvusclient.NewRoaringBitmapBlob(vals)
	s.Require().NoError(err)
	return []byte(blob)
}

func rbParam(blob []byte) map[string]*schemapb.TemplateValue {
	return map[string]*schemapb.TemplateValue{
		"rb": {Val: &schemapb.TemplateValue_BytesVal{BytesVal: blob}},
	}
}

func (s *RoaringMatchTestSuite) query(collectionName, expr string, outputFields []string, tmpl map[string]*schemapb.TemplateValue) *milvuspb.QueryResults {
	// Strong consistency: several of these assertions read back rows written or
	// deleted moments earlier, and an eventually-consistent read would make the
	// exactness checks flaky rather than meaningful.
	res, err := s.Cluster.MilvusClient.Query(context.Background(), &milvuspb.QueryRequest{
		DbName:             s.dbName,
		CollectionName:     collectionName,
		Expr:               expr,
		OutputFields:       outputFields,
		QueryParams:        []*commonpb.KeyValuePair{{Key: "limit", Value: "16384"}},
		ExprTemplateValues: tmpl,
		ConsistencyLevel:   commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(err)
	return res
}

func (s *RoaringMatchTestSuite) search(collectionName, expr string, topK int, outputFields []string, tmpl map[string]*schemapb.TemplateValue) *milvuspb.SearchResults {
	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	req := integration.ConstructSearchRequest(s.dbName, collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, outputFields,
		metric.L2, params, 1, s.dim, topK, -1)
	req.ExprTemplateValues = tmpl
	req.ConsistencyLevel = commonpb.ConsistencyLevel_Strong
	res, err := s.Cluster.MilvusClient.Search(context.Background(), req)
	s.Require().NoError(err)
	return res
}

func queryInt64Field(res *milvuspb.QueryResults, name string) []int64 {
	for _, fd := range res.GetFieldsData() {
		if fd.GetFieldName() == name {
			return fd.GetScalars().GetLongData().GetData()
		}
	}
	return nil
}

func queryIntNField(res *milvuspb.QueryResults, name string) []int32 {
	for _, fd := range res.GetFieldsData() {
		if fd.GetFieldName() == name {
			return fd.GetScalars().GetIntData().GetData()
		}
	}
	return nil
}

func searchInt64Field(res *milvuspb.SearchResults, name string) []int64 {
	for _, fd := range res.GetResults().GetFieldsData() {
		if fd.GetFieldName() == name {
			return fd.GetScalars().GetLongData().GetData()
		}
	}
	return nil
}

func sortedUnique(vals []int64) []int64 {
	seen := make(map[int64]struct{}, len(vals))
	out := make([]int64, 0, len(vals))
	for _, v := range vals {
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// creatorFor is the single source of truth for row i's id, so every expected
// set below is computed rather than hard-coded.
func (s *RoaringMatchTestSuite) creatorFor(i int) int64 {
	return int64(i%s.creatorDomain) - negativeShift
}

func (s *RoaringMatchTestSuite) setupCollection(collectionName string) []int64 {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	schema := integration.ConstructSchema(collectionName, s.dim, true,
		&schemapb.FieldSchema{
			FieldID: 100, Name: integration.Int64Field, IsPrimaryKey: true,
			DataType: schemapb.DataType_Int64, AutoID: true,
		},
		&schemapb.FieldSchema{FieldID: 101, Name: creatorIDField, DataType: schemapb.DataType_Int64},
		&schemapb.FieldSchema{
			FieldID: 102, Name: integration.FloatVecField, DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprintf("%d", s.dim)}},
		},
		&schemapb.FieldSchema{FieldID: 103, Name: creatorID8Field, DataType: schemapb.DataType_Int8},
		&schemapb.FieldSchema{FieldID: 104, Name: creatorID16Field, DataType: schemapb.DataType_Int16},
		&schemapb.FieldSchema{FieldID: 105, Name: creatorID32Field, DataType: schemapb.DataType_Int32},
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createResp, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(createResp))

	creators := make([]int64, s.rowNum)
	creators32 := make([]int32, s.rowNum)
	for i := 0; i < s.rowNum; i++ {
		creators[i] = s.creatorFor(i)
		creators32[i] = int32(creators[i])
	}

	fVec := integration.NewFloatVectorFieldData(integration.FloatVecField, s.rowNum, s.dim)
	insertResp, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
		FieldsData: []*schemapb.FieldData{
			fVec,
			newInt64FieldData(creatorIDField, creators),
			newIntNFieldData(creatorID8Field, schemapb.DataType_Int8, creators32),
			newIntNFieldData(creatorID16Field, schemapb.DataType_Int16, creators32),
			newIntNFieldData(creatorID32Field, schemapb.DataType_Int32, creators32),
		},
		HashKeys: integration.GenerateHashKeys(s.rowNum),
		NumRows:  uint32(s.rowNum),
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(insertResp.GetStatus()))

	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          s.dbName,
		CollectionNames: []string{collectionName},
	})
	s.Require().NoError(err)
	segIDs := flushResp.GetCollSegIDs()[collectionName].GetData()
	flushTs := flushResp.GetCollFlushTs()[collectionName]
	s.WaitForFlush(ctx, segIDs, flushTs, s.dbName, collectionName)

	createIndexResp, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(s.dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(createIndexResp))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	loadResp, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(loadResp))
	s.WaitForLoad(ctx, collectionName)

	return creators
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

// TestExactMembershipAllIntWidths is the core guarantee: the set of ids
// roaring_match selects equals the requested set exactly — no false positives
// (which separates it from bloom_match) and no false negatives.
//
// The member set deliberately straddles zero. A build or probe that
// zero-extended a narrow negative instead of sign-extending it would place it
// in a different Roaring high container and show up here as a missing id on
// creatorId8/16/32 while creatorId (already 64-bit) still passed.
func (s *RoaringMatchTestSuite) TestExactMembershipAllIntWidths() {
	collectionName := "test_roaring_exact_" + funcutil.GenRandomStr()
	creators := s.setupCollection(collectionName)
	present := sortedUnique(creators)

	// Half negative, half positive, plus ids that are NOT in the collection so a
	// filter that ignored the bitmap and returned everything would fail.
	members := []int64{-negativeShift, -30, -1, 0, 1, 17, 49, 1 << 20, -(1 << 20)}
	blob := s.bitmapBlob(members)

	expected := make([]int64, 0, len(members))
	inCollection := make(map[int64]struct{}, len(present))
	for _, v := range present {
		inCollection[v] = struct{}{}
	}
	for _, m := range members {
		if _, ok := inCollection[m]; ok {
			expected = append(expected, m)
		}
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })
	s.Require().NotEmpty(expected, "test setup must select at least one present id")

	for _, field := range intWidthFields {
		res := s.query(collectionName,
			fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", field),
			[]string{creatorIDField}, rbParam(blob))
		s.Require().NoError(merr.Error(res.GetStatus()), "field %s", field)

		got := sortedUnique(queryInt64Field(res, creatorIDField))
		s.Equal(expected, got,
			"field %s: roaring_match must select exactly the requested ids that exist", field)
	}
}

// TestNotRoaringMatchIsExactComplement pins the negated form. Because the
// filter is exact, `not roaring_match` must return precisely the non-NULL rows
// whose id is absent from the bitmap — an exactness property bloom_match cannot
// offer, and the reason roaring_match is allowed in delete.
func (s *RoaringMatchTestSuite) TestNotRoaringMatchIsExactComplement() {
	collectionName := "test_roaring_not_" + funcutil.GenRandomStr()
	creators := s.setupCollection(collectionName)
	present := sortedUnique(creators)

	members := []int64{-negativeShift, -1, 0, 7}
	blob := s.bitmapBlob(members)
	memberSet := make(map[int64]struct{}, len(members))
	for _, m := range members {
		memberSet[m] = struct{}{}
	}

	expected := make([]int64, 0, len(present))
	for _, v := range present {
		if _, isMember := memberSet[v]; !isMember {
			expected = append(expected, v)
		}
	}

	res := s.query(collectionName,
		fmt.Sprintf("not membership_match(%s, {rb}, type=roaring)", creatorIDField),
		[]string{creatorIDField}, rbParam(blob))
	s.Require().NoError(merr.Error(res.GetStatus()))
	s.Equal(expected, sortedUnique(queryInt64Field(res, creatorIDField)),
		"not roaring_match must be the exact complement over non-NULL rows")
}

// TestEmptyBitmap: an empty set matches nothing and its negation matches every
// non-NULL row. Worth pinning because an empty portable body is a distinct
// encoding path (zero high containers) that a length check could mis-handle.
func (s *RoaringMatchTestSuite) TestEmptyBitmap() {
	collectionName := "test_roaring_empty_" + funcutil.GenRandomStr()
	creators := s.setupCollection(collectionName)
	blob := s.bitmapBlob(nil)

	res := s.query(collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", creatorIDField),
		[]string{creatorIDField}, rbParam(blob))
	s.Require().NoError(merr.Error(res.GetStatus()))
	s.Empty(queryInt64Field(res, creatorIDField), "an empty bitmap must match nothing")

	resNot := s.query(collectionName,
		fmt.Sprintf("not membership_match(%s, {rb}, type=roaring)", creatorIDField),
		[]string{creatorIDField}, rbParam(blob))
	s.Require().NoError(merr.Error(resNot.GetStatus()))
	s.Equal(sortedUnique(creators), sortedUnique(queryInt64Field(resNot, creatorIDField)),
		"negating an empty bitmap must match every non-NULL row")
}

// TestNarrowWidthValuesRoundTrip probes each narrow field with a bitmap built
// from that width's own values, and reads the narrow column back. This is the
// direct check that the value segcore recovers is the value the client encoded.
func (s *RoaringMatchTestSuite) TestNarrowWidthValuesRoundTrip() {
	collectionName := "test_roaring_narrow_" + funcutil.GenRandomStr()
	s.setupCollection(collectionName)

	members := []int64{-negativeShift, -2, 0, 3}
	blob := s.bitmapBlob(members)
	want := make([]int32, 0, len(members))
	for _, m := range members {
		want = append(want, int32(m))
	}
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })

	for _, field := range []string{creatorID8Field, creatorID16Field, creatorID32Field} {
		res := s.query(collectionName,
			fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", field),
			[]string{field}, rbParam(blob))
		s.Require().NoError(merr.Error(res.GetStatus()), "field %s", field)

		seen := make(map[int32]struct{})
		for _, v := range queryIntNField(res, field) {
			seen[v] = struct{}{}
		}
		got := make([]int32, 0, len(seen))
		for v := range seen {
			got = append(got, v)
		}
		sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
		s.Equal(want, got, "field %s must round-trip its own narrow values", field)
	}
}

// TestVectorAndHybridSearchApplyExactFilter complements the deterministic
// Query assertions above by exercising both vector-search request paths. ANN
// recall is intentionally not asserted here; every returned row must belong to
// the exact member set, which proves neither path silently drops the filter.
func (s *RoaringMatchTestSuite) TestVectorAndHybridSearchApplyExactFilter() {
	collectionName := "test_roaring_vector_" + funcutil.GenRandomStr()
	s.setupCollection(collectionName)

	members := make([]int64, 0, negativeShift)
	memberSet := make(map[int64]struct{}, negativeShift)
	for value := int64(-negativeShift); value < 0; value++ {
		members = append(members, value)
		memberSet[value] = struct{}{}
	}
	blob := s.bitmapBlob(members)
	expr := fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", creatorIDField)
	assertFiltered := func(result *milvuspb.SearchResults, path string) {
		s.Require().NoError(merr.Error(result.GetStatus()), path)
		values := searchInt64Field(result, creatorIDField)
		s.Require().NotEmpty(values, "%s must return filtered rows", path)
		for _, value := range values {
			_, ok := memberSet[value]
			s.True(ok, "%s returned non-member creatorId %d", path, value)
		}
	}

	assertFiltered(s.search(collectionName, expr, 100,
		[]string{creatorIDField}, rbParam(blob)), "Search")

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	requests := make([]*milvuspb.SearchRequest, 2)
	for i := range requests {
		requests[i] = integration.ConstructSearchRequest(s.dbName, collectionName, expr,
			integration.FloatVecField, schemapb.DataType_FloatVector, nil,
			metric.L2, params, 1, s.dim, 100, -1)
		requests[i].ExprTemplateValues = rbParam(blob)
		requests[i].ConsistencyLevel = commonpb.ConsistencyLevel_Strong
	}
	hybrid, err := s.Cluster.MilvusClient.HybridSearch(context.Background(), &milvuspb.HybridSearchRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
		Requests:       requests,
		OutputFields:   []string{creatorIDField},
		RankParams: []*commonpb.KeyValuePair{
			{Key: "strategy", Value: "rrf"},
			{Key: "params", Value: `{"k":60}`},
			{Key: "limit", Value: "100"},
			{Key: "round_decimal", Value: "-1"},
		},
	})
	s.Require().NoError(err)
	assertFiltered(hybrid, "HybridSearch")
}

// TestRejectsInvalidInput covers what must fail at the proxy rather than reach
// a QueryNode: a non-blob argument, a structurally broken envelope, and an
// unsupported field type.
func (s *RoaringMatchTestSuite) TestRejectsInvalidInput() {
	collectionName := "test_roaring_reject_" + funcutil.GenRandomStr()
	s.setupCollection(collectionName)

	literal := s.query(collectionName,
		fmt.Sprintf("membership_match(%s, [1, 2, 3], type=roaring)", creatorIDField), nil, nil)
	s.False(merr.Ok(literal.GetStatus()), "a literal array argument must be rejected")

	garbage := s.query(collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", creatorIDField), nil,
		rbParam([]byte("not-an-mrb1-blob")))
	s.False(merr.Ok(garbage.GetStatus()), "a malformed blob must be rejected")

	// Truncating a valid blob keeps the magic and version but breaks the
	// declared body length — the check that a naive magic-only validator misses.
	valid := s.bitmapBlob([]int64{1, 2, 3})
	truncated := s.query(collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", creatorIDField), nil,
		rbParam(valid[:len(valid)-1]))
	s.False(merr.Ok(truncated.GetStatus()), "a truncated blob must be rejected")

	onVector := s.query(collectionName,
		fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", integration.FloatVecField), nil, rbParam(valid))
	s.False(merr.Ok(onVector.GetStatus()), "roaring_match on a vector field must be rejected")
}

// TestDeleteWithRoaringMatch is the capability bloom_match does not have.
// bloom_match is rejected in delete because a false positive would remove rows
// outside the caller's set; roaring_match is exact, so it is allowed — and this
// asserts it deletes exactly the requested ids and nothing else.
func (s *RoaringMatchTestSuite) TestDeleteWithRoaringMatch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := "test_roaring_delete_" + funcutil.GenRandomStr()
	creators := s.setupCollection(collectionName)
	present := sortedUnique(creators)

	victims := []int64{-negativeShift, -1, 0, 5}
	blob := s.bitmapBlob(victims)
	victimSet := make(map[int64]struct{}, len(victims))
	for _, v := range victims {
		victimSet[v] = struct{}{}
	}
	survivors := make([]int64, 0, len(present))
	for _, v := range present {
		if _, dead := victimSet[v]; !dead {
			survivors = append(survivors, v)
		}
	}

	delResp, err := s.Cluster.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
		DbName:             s.dbName,
		CollectionName:     collectionName,
		Expr:               fmt.Sprintf("membership_match(%s, {rb}, type=roaring)", creatorIDField),
		ExprTemplateValues: rbParam(blob),
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(delResp.GetStatus()),
		"roaring_match is exact and must be accepted in delete")

	rowsPerCreator := s.rowNum / s.creatorDomain
	s.Require().EqualValues(len(victims)*rowsPerCreator, delResp.GetDeleteCnt(),
		"delete must report exactly the victim rows")

	res := s.query(collectionName, fmt.Sprintf("%s >= %d", creatorIDField, -negativeShift-1),
		[]string{creatorIDField}, nil)
	s.Require().NoError(merr.Error(res.GetStatus()))

	remaining := queryInt64Field(res, creatorIDField)
	s.Equal(survivors, sortedUnique(remaining),
		"delete must remove exactly the ids in the bitmap")

	// The set comparison above only proves which ids survive, not how many rows
	// each kept: wrongly deleting 19 of a survivor's 20 rows still leaves the id
	// present and would pass. roaring_match is allowed in delete precisely
	// because it is exact, so the row counts are the property worth asserting.
	s.Require().Len(remaining, s.rowNum-len(victims)*rowsPerCreator,
		"delete must not remove any row outside the bitmap")
	counts := make(map[int64]int, len(survivors))
	for _, v := range remaining {
		counts[v]++
	}
	for _, v := range survivors {
		s.Equalf(rowsPerCreator, counts[v],
			"creatorId %d must keep all %d rows", v, rowsPerCreator)
	}
	for _, v := range victims {
		s.Zerof(counts[v], "creatorId %d must be fully deleted", v)
	}
}

func TestRoaringMatch(t *testing.T) {
	suite.Run(t, new(RoaringMatchTestSuite))
}
