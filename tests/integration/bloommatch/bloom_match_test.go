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

package bloommatch

import (
	"context"
	"fmt"
	"strings"
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

// BloomMatchTestSuite verifies the bloom_match approximate membership filter end to end.
//
// Data is fully controlled: creatorId of row i is i % creatorDomain, so membership of
// every row against any set is computable, making the assertions deterministic despite
// bloom_match's probabilistic false positives.
type BloomMatchTestSuite struct {
	integration.MiniClusterSuite

	dbName        string
	dim           int
	rowNum        int
	creatorDomain int // creatorId values range [0, creatorDomain)
}

const (
	creatorIDField    = "creatorId"   // INT64
	creatorID8Field   = "creatorId8"  // INT8
	creatorID16Field  = "creatorId16" // INT16
	creatorID32Field  = "creatorId32" // INT32
	creatorIDStrField = "creatorIdStr"
	metaJSONField     = "meta"
)

// intWidthFields are the integer fields (each carrying the same creatorId value,
// narrowed to its width) that the scalar-index matrix exercises — every width
// widens to int64 for hashing, so the same int64 blob probes all of them.
var intWidthFields = []string{creatorID8Field, creatorID16Field, creatorID32Field, creatorIDField}

func (s *BloomMatchTestSuite) SetupSuite() {
	// BITMAP's per-row Reverse_Lookup is only cheap with the offset cache, and the
	// index-only fallback exercised by the scalar-index matrix (BITMAP round) needs
	// it (default is off). Harmless for the other tests, whose fields keep their
	// raw data. Must be set before the cluster starts.
	s.WithMilvusConfig(paramtable.Get().QueryNodeCfg.IndexOffsetCacheEnabled.Key, "true")
	s.MiniClusterSuite.SetupSuite()
	s.dbName = ""
	s.dim = 128
	s.rowNum = 2000
	s.creatorDomain = 100
}

func newInt64FieldData(name string, data []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
			},
		},
	}
}

// newIntNFieldData builds INT8/INT16/INT32 scalar field data (all stored as an
// int32 IntArray in the proto; only the declared Type differs).
func newIntNFieldData(name string, dt schemapb.DataType, data []int32) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      dt,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: data}},
			},
		},
	}
}

func newVarcharFieldData(name string, data []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: data}},
			},
		},
	}
}

func newJSONFieldData(name string, rows []string) *schemapb.FieldData {
	data := make([][]byte, len(rows))
	for i, r := range rows {
		data[i] = []byte(r)
	}
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: data}},
			},
		},
	}
}

// jsonRowsFor derives the JSON column from the creator ids with three
// deliberate encodings, so the JSON-path test exercises the strictly-typed
// value dispatch end to end:
//   - i%11 == 0: the "creator" key is MISSING (never matches, either polarity)
//   - i%3  == 0: creator encoded as a FLOAT literal (7.0) — strictly typed:
//     must NOT match an int64 member 7 (deliberate divergence from exact
//     `in`, which unifies 7.0 == 7)
//   - otherwise: plain int literal
func jsonRowsFor(creators []int64) []string {
	rows := make([]string, len(creators))
	for i, c := range creators {
		switch {
		case i%11 == 0:
			rows[i] = `{"other": 1}`
		case i%3 == 0:
			rows[i] = fmt.Sprintf(`{"creator": %d.0}`, c)
		default:
			rows[i] = fmt.Sprintf(`{"creator": %d}`, c)
		}
	}
	return rows
}

// int64LiteralList renders {0,1,...} as a bloom_match literal array argument.
func int64LiteralList(vals []int64) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = fmt.Sprintf("%d", v)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func stringLiteralList(vals []string) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = fmt.Sprintf("%q", v)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// bloomBlobInt64 builds a client-side SBBF blob from int64 members via the
// USER-FACING SDK builder (milvusclient.NewBloomFilterBlob) — the exact call an
// application makes — so this E2E exercises the real client path. fpr 0.001 and the
// member set are byte-identical to the low-level sbbf.NewBuilder output (golden
// vectors guarantee reproducibility).
func (s *BloomMatchTestSuite) bloomBlobInt64(vals []int64) []byte {
	blob, err := milvusclient.NewBloomFilterBlob(vals, 0.001)
	s.Require().NoError(err)
	return blob
}

func (s *BloomMatchTestSuite) bloomBlobStr(vals []string) []byte {
	blob, err := milvusclient.NewBloomFilterBlob(vals, 0.001)
	s.Require().NoError(err)
	return blob
}

// bfParam wraps a pre-built blob as a raw bytes template value under key "bf",
// matching bloom_match(field, {bf}).
func bfParam(blob []byte) map[string]*schemapb.TemplateValue {
	return map[string]*schemapb.TemplateValue{
		"bf": {Val: &schemapb.TemplateValue_BytesVal{BytesVal: blob}},
	}
}

// setupCollection creates a collection with PK(int64,autoID) + creatorId in every
// integer width (int8/16/32/64) + creatorIdStr(varchar) + meta(JSON) + float
// vector, inserts rowNum controlled rows, flushes, indexes and loads. Every
// creatorId field carries the same value (i % creatorDomain, which fits int8
// since creatorDomain <= 128), so one int64 blob probes all widths. Returns the
// assigned creatorId per row (== i % creatorDomain) and its varchar form.
func (s *BloomMatchTestSuite) setupCollection(collectionName string) (creators []int64, creatorsStr []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := s.Cluster

	schema := integration.ConstructSchema(collectionName, s.dim, true,
		&schemapb.FieldSchema{
			FieldID: 100, Name: integration.Int64Field, IsPrimaryKey: true,
			DataType: schemapb.DataType_Int64, AutoID: true,
		},
		&schemapb.FieldSchema{
			FieldID: 101, Name: creatorIDField, DataType: schemapb.DataType_Int64,
		},
		&schemapb.FieldSchema{
			FieldID: 102, Name: creatorIDStrField, DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
		},
		&schemapb.FieldSchema{
			FieldID: 103, Name: integration.FloatVecField, DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: fmt.Sprintf("%d", s.dim)}},
		},
		&schemapb.FieldSchema{
			FieldID: 104, Name: metaJSONField, DataType: schemapb.DataType_JSON,
		},
		&schemapb.FieldSchema{
			FieldID: 105, Name: creatorID8Field, DataType: schemapb.DataType_Int8,
		},
		&schemapb.FieldSchema{
			FieldID: 106, Name: creatorID16Field, DataType: schemapb.DataType_Int16,
		},
		&schemapb.FieldSchema{
			FieldID: 107, Name: creatorID32Field, DataType: schemapb.DataType_Int32,
		},
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

	creators = make([]int64, s.rowNum)
	creatorsStr = make([]string, s.rowNum)
	creators32 := make([]int32, s.rowNum)
	for i := 0; i < s.rowNum; i++ {
		creators[i] = int64(i % s.creatorDomain)
		creators32[i] = int32(i % s.creatorDomain)
		creatorsStr[i] = fmt.Sprintf("c%d", i%s.creatorDomain)
	}

	fVec := integration.NewFloatVectorFieldData(integration.FloatVecField, s.rowNum, s.dim)
	insertResp, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
		FieldsData: []*schemapb.FieldData{
			fVec,
			newInt64FieldData(creatorIDField, creators),
			newVarcharFieldData(creatorIDStrField, creatorsStr),
			newJSONFieldData(metaJSONField, jsonRowsFor(creators)),
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

	return creators, creatorsStr
}

// insertGrowing inserts rows WITHOUT flushing, so they stay in growing segments.
// Used to exercise the sealed+growing path in one query (setupCollection flushes
// its batch into sealed segments; this batch lands in growing).
func (s *BloomMatchTestSuite) insertGrowing(collectionName string, creators []int64, creatorsStr []string) {
	n := len(creators)
	// The narrow-int fields are required by the schema but never queried on the
	// growing rows (this path exercises the int64/varchar fields); clamp into the
	// int8 range so the insert is valid even when creators hold out-of-int8 ids.
	creators32 := make([]int32, n)
	for i, c := range creators {
		creators32[i] = int32(c % 128)
	}
	fVec := integration.NewFloatVectorFieldData(integration.FloatVecField, n, s.dim)
	insertResp, err := s.Cluster.MilvusClient.Insert(context.Background(), &milvuspb.InsertRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
		FieldsData: []*schemapb.FieldData{
			fVec,
			newInt64FieldData(creatorIDField, creators),
			newVarcharFieldData(creatorIDStrField, creatorsStr),
			newJSONFieldData(metaJSONField, jsonRowsFor(creators)),
			newIntNFieldData(creatorID8Field, schemapb.DataType_Int8, creators32),
			newIntNFieldData(creatorID16Field, schemapb.DataType_Int16, creators32),
			newIntNFieldData(creatorID32Field, schemapb.DataType_Int32, creators32),
		},
		HashKeys: integration.GenerateHashKeys(n),
		NumRows:  uint32(n),
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(insertResp.GetStatus()))
}

// scalarIndexSpec is one scalar index to build on a field.
type scalarIndexSpec struct {
	field     string
	indexName string
	params    []*commonpb.KeyValuePair
}

// buildScalarIndexesAndReload releases the already-loaded collection, builds every
// given scalar index, waits for each, and reloads once — so the sealed segments
// are reloaded WITH the indexes materialized. This drives the load paths that drop
// a field's raw column when its index reports has_raw_data (BITMAP/STL_SORT/
// HYBRID/TRIE), and builds JSON path indexes.
func (s *BloomMatchTestSuite) buildScalarIndexesAndReload(collectionName string, specs []scalarIndexSpec) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := s.Cluster

	relStatus, err := c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(relStatus))

	for _, spec := range specs {
		idxResp, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			DbName:         s.dbName,
			CollectionName: collectionName,
			FieldName:      spec.field,
			IndexName:      spec.indexName,
			ExtraParams:    spec.params,
		})
		s.Require().NoError(err)
		s.Require().NoErrorf(merr.Error(idxResp), "create index %s on %s", spec.indexName, spec.field)
	}
	for _, spec := range specs {
		s.WaitForIndexBuiltWithIndexName(ctx, collectionName, spec.field, spec.indexName)
	}

	loadResp, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         s.dbName,
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.Error(loadResp))
	s.WaitForLoad(ctx, collectionName)
}

// scalarIndexParams builds CreateIndex params for a plain scalar index type.
func scalarIndexParams(indexType string) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: indexType}}
}

// assertBloomSupersetInt runs a scalar QUERY on an integer field and asserts
// bloom_match(field, {bf}) returns a superset of exact `in` — zero row-level false
// negatives — regardless of whether the field's raw column is present or was
// dropped in favor of an index reverse-lookup. desc labels the failure.
func (s *BloomMatchTestSuite) assertBloomSupersetInt(collectionName, field string, memberSet []int64, blob []byte, desc string) {
	exactRes := s.query(collectionName, fmt.Sprintf("%s in %s", field, int64LiteralList(memberSet)), []string{integration.Int64Field}, nil)
	bloomRes := s.query(collectionName, fmt.Sprintf("bloom_match(%s, {bf})", field), []string{integration.Int64Field}, bfParam(blob))

	exactPKs := queryInt64Field(exactRes, integration.Int64Field)
	bloomPKs := queryInt64Field(bloomRes, integration.Int64Field)
	s.Require().NotEmptyf(exactPKs, "%s: exact `in` returned no rows", desc)
	s.Require().NotEmptyf(bloomPKs, "%s: bloom_match returned no rows", desc)

	bloomSet := int64Set(bloomPKs)
	missing := 0
	for _, pk := range exactPKs {
		if _, ok := bloomSet[pk]; !ok {
			missing++
		}
	}
	s.Equalf(0, missing, "%s: bloom_match dropped %d/%d true member rows (false negatives)", desc, missing, len(exactPKs))
}

// assertBloomSupersetStr is assertBloomSupersetInt for a varchar field.
func (s *BloomMatchTestSuite) assertBloomSupersetStr(collectionName, field string, memberSet []string, blob []byte, desc string) {
	exactRes := s.query(collectionName, fmt.Sprintf("%s in %s", field, stringLiteralList(memberSet)), []string{integration.Int64Field}, nil)
	bloomRes := s.query(collectionName, fmt.Sprintf("bloom_match(%s, {bf})", field), []string{integration.Int64Field}, bfParam(blob))

	exactPKs := queryInt64Field(exactRes, integration.Int64Field)
	bloomPKs := queryInt64Field(bloomRes, integration.Int64Field)
	s.Require().NotEmptyf(exactPKs, "%s: exact `in` returned no rows", desc)
	s.Require().NotEmptyf(bloomPKs, "%s: bloom_match returned no rows", desc)

	bloomSet := int64Set(bloomPKs)
	missing := 0
	for _, pk := range exactPKs {
		if _, ok := bloomSet[pk]; !ok {
			missing++
		}
	}
	s.Equalf(0, missing, "%s: bloom_match(varchar) dropped %d/%d true member rows (false negatives)", desc, missing, len(exactPKs))
}

func (s *BloomMatchTestSuite) search(collectionName, expr string, topK int, outputFields []string, tmpl map[string]*schemapb.TemplateValue) *milvuspb.SearchResults {
	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.L2)
	req := integration.ConstructSearchRequest(s.dbName, collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, outputFields, metric.L2, params, 1, s.dim, topK, -1)
	req.ExprTemplateValues = tmpl
	res, err := s.Cluster.MilvusClient.Search(context.Background(), req)
	s.Require().NoError(err)
	return res
}

func resultPKs(res *milvuspb.SearchResults) []int64 {
	return res.GetResults().GetIds().GetIntId().GetData()
}

// query runs a scalar filter query (no vector search / ANN). It returns ALL rows
// matching expr up to a high limit, so correctness assertions built on it are
// deterministic — free of ANN recall and topK-nearest displacement. Strong
// consistency guarantees just-inserted (unflushed, growing) rows are visible.
func (s *BloomMatchTestSuite) query(collectionName, expr string, outputFields []string, tmpl map[string]*schemapb.TemplateValue) *milvuspb.QueryResults {
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

func queryInt64Field(res *milvuspb.QueryResults, name string) []int64 {
	for _, fd := range res.GetFieldsData() {
		if fd.GetFieldName() == name {
			return fd.GetScalars().GetLongData().GetData()
		}
	}
	return nil
}

func queryStringField(res *milvuspb.QueryResults, name string) []string {
	for _, fd := range res.GetFieldsData() {
		if fd.GetFieldName() == name {
			return fd.GetScalars().GetStringData().GetData()
		}
	}
	return nil
}

func int64Set(vals []int64) map[int64]struct{} {
	m := make(map[int64]struct{}, len(vals))
	for _, v := range vals {
		m[v] = struct{}{}
	}
	return m
}

// TestQueryIncludeZeroFalseNegatives is the hard correctness gate: using a scalar
// QUERY (no vector search), the set of rows bloom_match returns must be a SUPERSET of
// the rows exact `in` returns — every true member passes bloom_match. Because query has
// no ANN recall and no topK-nearest truncation, this deterministically proves zero row-
// level false negatives (a member being dropped would be a bug, not an ANN artifact).
func (s *BloomMatchTestSuite) TestQueryIncludeZeroFalseNegatives() {
	collectionName := "test_bloom_match_qincl_" + funcutil.GenRandomStr()
	s.setupCollection(collectionName)

	memberSet := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	lit := int64LiteralList(memberSet)
	blob := s.bloomBlobInt64(memberSet)

	exactRes := s.query(collectionName, fmt.Sprintf("%s in %s", creatorIDField, lit), []string{integration.Int64Field}, nil)
	bloomRes := s.query(collectionName, fmt.Sprintf("bloom_match(%s, {bf})", creatorIDField), []string{integration.Int64Field, creatorIDField}, bfParam(blob))

	exactPKs := queryInt64Field(exactRes, integration.Int64Field)
	bloomPKs := queryInt64Field(bloomRes, integration.Int64Field)
	s.Require().NotEmpty(exactPKs)
	s.Require().NotEmpty(bloomPKs)

	bloomSet := int64Set(bloomPKs)
	missing := 0
	for _, pk := range exactPKs {
		if _, ok := bloomSet[pk]; !ok {
			missing++
		}
	}
	s.Equalf(0, missing, "bloom_match dropped %d/%d true member rows (row-level false negatives)", missing, len(exactPKs))

	// false-positive-rate sanity at fpr=0.001: bloom returns very few out-of-set rows.
	creators := queryInt64Field(bloomRes, creatorIDField)
	fp := 0
	for _, cv := range creators {
		if cv >= 20 {
			fp++
		}
	}
	if len(creators) > 0 {
		s.Lessf(float64(fp)/float64(len(creators)), 0.05,
			"bloom_match false-positive fraction too high: %d/%d", fp, len(creators))
	}
}

// TestQueryJsonPathStrictTypedMembership runs bloom_match on a JSON path
// (meta["creator"]) with three deliberate row encodings (plain int, float
// literal 7.0, missing key — see jsonRowsFor). Deterministic gates:
//  1. every INT-ENCODED member row that has the key is returned (zero false
//     negatives within the strictly-typed domain), and FLOAT-ENCODED member
//     rows are NOT returned (strict typing);
//  2. the divergence from exact `in` is pinned: `in` unifies 7.0 == 7, so the
//     rows `in` returns but bloom does not are exactly the float-encoded
//     member rows;
//  3. false-positive fraction stays small (fpr sanity).
func (s *BloomMatchTestSuite) TestQueryJsonPathStrictTypedMembership() {
	collectionName := "test_bloom_match_json_" + funcutil.GenRandomStr()
	creators, _ := s.setupCollection(collectionName)

	memberSet := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	members := int64Set(memberSet)
	blob := s.bloomBlobInt64(memberSet)

	// Expected member hits: INT-ENCODED member rows that carry the "creator"
	// key. Rows i%11==0 lack the key; rows i%3==0 (and i%11!=0) are
	// float-encoded and must NOT match (strict typing).
	expectedMemberRows := 0
	expectedFloatEncodedMemberRows := 0
	for i, c := range creators {
		if _, ok := members[c]; !ok {
			continue
		}
		if i%11 == 0 {
			continue
		}
		if i%3 == 0 {
			expectedFloatEncodedMemberRows++
			continue
		}
		expectedMemberRows++
	}
	s.Require().Positive(expectedMemberRows)
	// The float-encoded case must actually be present in the data, otherwise
	// this test silently stops covering the strict-typing divergence.
	s.Require().Positive(expectedFloatEncodedMemberRows)

	bloomRes := s.query(collectionName,
		fmt.Sprintf(`bloom_match(%s["creator"], {bf})`, metaJSONField),
		[]string{integration.Int64Field, creatorIDField}, bfParam(blob))
	bloomPKs := queryInt64Field(bloomRes, integration.Int64Field)
	bloomCreators := queryInt64Field(bloomRes, creatorIDField)
	s.Require().NotEmpty(bloomPKs)
	s.Require().Equal(len(bloomPKs), len(bloomCreators))

	// Gate 1: count of returned member-valued rows equals the number of
	// INT-ENCODED member rows exactly. If a float-encoded 7.0 row slipped in,
	// the count would exceed the expectation; if an int-encoded member row
	// were dropped, it would fall short. (creatorId mirrors the JSON
	// "creator" value, so membership per returned row is computable without
	// parsing JSON.)
	memberRows := 0
	for _, cv := range bloomCreators {
		if _, ok := members[cv]; ok {
			memberRows++
		}
	}
	s.Equalf(expectedMemberRows, memberRows,
		"bloom_match(json path) returned %d member rows, want %d int-encoded (float-encoded rows must not match: %d)",
		memberRows, expectedMemberRows, expectedFloatEncodedMemberRows)

	// Gate 2: pin the deliberate divergence from exact `in`. Exact `in`
	// unifies 7.0 == 7 (default Milvus JSON numeric semantics), so it returns
	// the float-encoded member rows too; bloom_match, strictly typed, does
	// not. The difference must be exactly those rows.
	exactRes := s.query(collectionName,
		fmt.Sprintf(`%s["creator"] in %s`, metaJSONField, int64LiteralList(memberSet)),
		[]string{integration.Int64Field}, nil)
	exactPKs := queryInt64Field(exactRes, integration.Int64Field)
	bloomSet := int64Set(bloomPKs)
	missing := 0
	for _, pk := range exactPKs {
		if _, ok := bloomSet[pk]; !ok {
			missing++
		}
	}
	s.Equalf(expectedFloatEncodedMemberRows, missing,
		"rows returned by exact `in` but not bloom_match must be exactly the float-encoded member rows: got %d, want %d",
		missing, expectedFloatEncodedMemberRows)

	// Gate 3: FP sanity at fpr=0.001.
	fp := 0
	for _, cv := range bloomCreators {
		if _, ok := members[cv]; !ok {
			fp++
		}
	}
	if len(bloomCreators) > 0 {
		s.Lessf(float64(fp)/float64(len(bloomCreators)), 0.05,
			"bloom_match(json path) false-positive fraction too high: %d/%d", fp, len(bloomCreators))
	}
}

// assertJSONPathStrictMembership runs bloom_match on meta["creator"] and asserts
// it returns exactly the int-encoded member rows: rows i%11==0 lack the key and
// rows i%3==0 are float-encoded (strict typing → no match against an int64 member).
// Reaching the assertions at all proves the raw JSON column is loaded — bloom_match
// has no JSON reverse-lookup, so a dropped raw column would fail the query with the
// segcore "raw field data is not loaded" error.
func (s *BloomMatchTestSuite) assertJSONPathStrictMembership(collectionName string, creators []int64, desc string) {
	memberSet := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	members := int64Set(memberSet)
	blob := s.bloomBlobInt64(memberSet)

	expectedMemberRows := 0
	for i, c := range creators {
		if _, ok := members[c]; !ok {
			continue
		}
		if i%11 == 0 || i%3 == 0 {
			continue
		}
		expectedMemberRows++
	}
	s.Require().Positivef(expectedMemberRows, "%s: no int-encoded member rows in fixture", desc)

	bloomRes := s.query(collectionName,
		fmt.Sprintf(`bloom_match(%s["creator"], {bf})`, metaJSONField),
		[]string{integration.Int64Field, creatorIDField}, bfParam(blob))
	bloomCreators := queryInt64Field(bloomRes, creatorIDField)
	s.Require().NotEmptyf(bloomCreators, "%s: bloom_match(json path) returned no rows", desc)

	memberRows := 0
	for _, cv := range bloomCreators {
		if _, ok := members[cv]; ok {
			memberRows++
		}
	}
	s.Equalf(expectedMemberRows, memberRows,
		"%s: bloom_match(json path) returned %d member rows, want %d int-encoded — raw JSON must stay loaded and strict typing preserved",
		desc, memberRows, expectedMemberRows)
}

// TestQueryScalarIndexTypeMatrix is the correctness matrix: every supported scalar
// data type (int8/16/32/64, varchar) × every scalar index type valid for it. For
// the has_raw_data indexes (STL_SORT/BITMAP/HYBRID/TRIE) the loader drops the raw
// column, so bloom_match must recover each value through the index's per-row
// Reverse_Lookup (ExecVisitorImplForIndex; BITMAP relies on the offset cache set in
// SetupSuite); INVERTED keeps the raw column and uses the data path. Every case
// must be correct — bloom_match ⊇ exact `in`, zero row-level false negatives. One
// int64 blob probes every integer width (all widen to int64 before hashing).
func (s *BloomMatchTestSuite) TestQueryScalarIndexTypeMatrix() {
	intMembers := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	intBlob := s.bloomBlobInt64(intMembers)
	strMembers := []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}
	strBlob := s.bloomBlobStr(strMembers)

	rounds := []struct {
		indexType string
		onInts    bool
		onVarchar bool
	}{
		{"STL_SORT", true, true},
		{"BITMAP", true, true},
		{"INVERTED", true, true},
		{"HYBRID", true, true},
		{"TRIE", false, true}, // varchar only
	}
	for _, r := range rounds {
		s.Run(r.indexType, func() {
			collectionName := "test_bloom_match_idx_" + strings.ToLower(r.indexType) + "_" + funcutil.GenRandomStr()
			s.setupCollection(collectionName)

			var specs []scalarIndexSpec
			if r.onInts {
				for _, f := range intWidthFields {
					specs = append(specs, scalarIndexSpec{field: f, indexName: "idx_" + f, params: scalarIndexParams(r.indexType)})
				}
			}
			if r.onVarchar {
				specs = append(specs, scalarIndexSpec{field: creatorIDStrField, indexName: "idx_" + creatorIDStrField, params: scalarIndexParams(r.indexType)})
			}
			s.buildScalarIndexesAndReload(collectionName, specs)

			if r.onInts {
				for _, f := range intWidthFields {
					s.assertBloomSupersetInt(collectionName, f, intMembers, intBlob, r.indexType+"/"+f)
				}
			}
			if r.onVarchar {
				s.assertBloomSupersetStr(collectionName, creatorIDStrField, strMembers, strBlob, r.indexType+"/varchar")
			}
		})
	}
}

// TestQueryJsonPathIndexTypeMatrix covers bloom_match on a JSON path with a JSON
// path index of each valid type loaded. A JSON path index never drops the raw JSON
// column (bloom_match reads raw JSON per row), so the probe must keep working with
// strict typing intact. HYBRID is the strong case (internally has_raw_data=true),
// yet the raw JSON survives — the direct refutation of the "index drops raw JSON,
// bloom fails after seal" concern.
func (s *BloomMatchTestSuite) TestQueryJsonPathIndexTypeMatrix() {
	for _, indexType := range []string{"INVERTED", "HYBRID"} {
		s.Run(indexType, func() {
			collectionName := "test_bloom_match_jsonidx_" + strings.ToLower(indexType) + "_" + funcutil.GenRandomStr()
			creators, _ := s.setupCollection(collectionName)
			s.buildScalarIndexesAndReload(collectionName, []scalarIndexSpec{{
				field:     metaJSONField,
				indexName: "idx_meta_creator",
				params: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: indexType},
					{Key: "json_path", Value: fmt.Sprintf(`%s["creator"]`, metaJSONField)},
					{Key: "json_cast_type", Value: "double"},
				},
			}})
			s.assertJSONPathStrictMembership(collectionName, creators, indexType)
		})
	}
}

// TestSearchRecallWithinThreshold exercises the ANN path (IVF index): a filtered vector
// search with bloom_match should recall almost all of the members that the exact `in`
// filtered search returns. Because these are two independent APPROXIMATE searches
// (differing filter selectivity → different graph traversal, plus topK-nearest
// displacement by false positives), an exact superset is NOT guaranteed here, so we
// assert a recall threshold rather than zero misses. The deterministic zero-false-
// negative guarantee lives in the query-based test above.
func (s *BloomMatchTestSuite) TestSearchRecallWithinThreshold() {
	collectionName := "test_bloom_match_recall_" + funcutil.GenRandomStr()
	s.setupCollection(collectionName)

	memberSet := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	lit := int64LiteralList(memberSet)
	blob := s.bloomBlobInt64(memberSet)
	topK := s.rowNum // large topK to minimize truncation; remaining diff is ANN recall

	exactRes := s.search(collectionName, fmt.Sprintf("%s in %s", creatorIDField, lit), topK, []string{creatorIDField}, nil)
	s.Require().NoError(merr.Error(exactRes.GetStatus()))
	bloomRes := s.search(collectionName, fmt.Sprintf("bloom_match(%s, {bf})", creatorIDField), topK, []string{creatorIDField}, bfParam(blob))
	s.Require().NoError(merr.Error(bloomRes.GetStatus()))

	exactPKs := resultPKs(exactRes)
	s.Require().NotEmpty(exactPKs)
	bloomSet := int64Set(resultPKs(bloomRes))
	hit := 0
	for _, pk := range exactPKs {
		if _, ok := bloomSet[pk]; ok {
			hit++
		}
	}
	recall := float64(hit) / float64(len(exactPKs))
	s.GreaterOrEqualf(recall, 0.85, "bloom_match search recall vs exact IN too low: %.3f (%d/%d)", recall, hit, len(exactPKs))
}

// TestQueryExcludeNoTrueMemberLeaks asserts, via a deterministic scalar query, that
// `not bloom_match` never returns a true member: bloom has no false negatives, so every
// member is flagged in and excluded by the negation.
func (s *BloomMatchTestSuite) TestQueryExcludeNoTrueMemberLeaks() {
	collectionName := "test_bloom_match_qexcl_" + funcutil.GenRandomStr()
	s.setupCollection(collectionName)

	memberSet := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	blob := s.bloomBlobInt64(memberSet)

	res := s.query(collectionName, fmt.Sprintf("not bloom_match(%s, {bf})", creatorIDField), []string{creatorIDField}, bfParam(blob))
	creators := queryInt64Field(res, creatorIDField)
	s.Require().NotEmpty(creators)
	for _, cv := range creators {
		s.GreaterOrEqualf(cv, int64(20), "not bloom_match leaked a true member creatorId=%d", cv)
	}
}

// TestQueryVarcharZeroFalseNegatives is the varchar hard gate: deterministic query,
// bloom_match ⊇ exact `in`, zero row-level false negatives.
func (s *BloomMatchTestSuite) TestQueryVarcharZeroFalseNegatives() {
	collectionName := "test_bloom_match_qstr_" + funcutil.GenRandomStr()
	s.setupCollection(collectionName)

	memberSet := []string{"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"}
	lit := stringLiteralList(memberSet)
	blob := s.bloomBlobStr(memberSet)

	exactRes := s.query(collectionName, fmt.Sprintf("%s in %s", creatorIDStrField, lit), []string{integration.Int64Field}, nil)
	bloomRes := s.query(collectionName, fmt.Sprintf("bloom_match(%s, {bf})", creatorIDStrField), []string{integration.Int64Field}, bfParam(blob))

	exactPKs := queryInt64Field(exactRes, integration.Int64Field)
	bloomPKs := queryInt64Field(bloomRes, integration.Int64Field)
	s.Require().NotEmpty(exactPKs)
	s.Require().NotEmpty(bloomPKs)

	bloomSet := int64Set(bloomPKs)
	missing := 0
	for _, pk := range exactPKs {
		if _, ok := bloomSet[pk]; !ok {
			missing++
		}
	}
	s.Equalf(0, missing, "bloom_match(varchar) dropped %d/%d true member rows (false negatives)", missing, len(exactPKs))
}

// TestQueryGrowingAndSealedBothMatched proves bloom_match evaluates BOTH sealed and
// growing segments in a single query (the PR promises sealed/growing consistency, but
// setupCollection flushes everything into sealed). setupCollection's batch (creatorId
// 0..99) is flushed → sealed; a second batch on a DISJOINT creatorId range [500,510)
// is inserted without flushing → growing. A blob spanning a sealed member (0..4) and a
// growing-only member (500..504) must return rows from both partitions — a returned
// creatorId>=500 can only come from a growing segment.
//
// It runs the same sealed/growing check on the int64 field AND the varchar field.
// The varchar leg is the point: it is the only place that exercises the one
// growing-specific branch in Eval() — a non-mmap growing segment routes VARCHAR
// through ExecVisitorImpl<std::string>, while sealed / mmap use std::string_view.
func (s *BloomMatchTestSuite) TestQueryGrowingAndSealedBothMatched() {
	collectionName := "test_bloom_match_growing_" + funcutil.GenRandomStr()
	s.setupCollection(collectionName)

	const growingN = 200
	growingCreators := make([]int64, growingN)
	growingCreatorsStr := make([]string, growingN)
	for i := 0; i < growingN; i++ {
		growingCreators[i] = int64(500 + i%10) // 500..509, disjoint from sealed 0..99
		growingCreatorsStr[i] = fmt.Sprintf("g%d", i%10)
	}
	s.insertGrowing(collectionName, growingCreators, growingCreatorsStr)

	// Blob spans a sealed member range (0..4) and a growing-only range (500..504).
	memberSet := []int64{0, 1, 2, 3, 4, 500, 501, 502, 503, 504}
	blob := s.bloomBlobInt64(memberSet)

	res := s.query(collectionName, fmt.Sprintf("bloom_match(%s, {bf})", creatorIDField),
		[]string{creatorIDField}, bfParam(blob))
	creators := queryInt64Field(res, creatorIDField)
	s.Require().NotEmpty(creators)

	sawSealed, sawGrowing := false, false
	for _, cv := range creators {
		if cv >= 0 && cv <= 4 {
			sawSealed = true
		}
		if cv >= 500 && cv <= 504 {
			sawGrowing = true
		}
	}
	s.True(sawSealed, "bloom_match missed sealed-segment members")
	s.True(sawGrowing, "bloom_match missed growing-segment members (growing segment not probed)")

	// Same sealed/growing split on the VARCHAR field, to cover the growing-only
	// ExecVisitorImpl<std::string> dispatch. Sealed values are "c0".."c99",
	// growing-only are "g0".."g9"; the blob spans sealed "c0".."c4" and
	// growing-only "g0".."g4". A returned value with a "g" prefix can only come
	// from a growing segment.
	strBlob := s.bloomBlobStr([]string{"c0", "c1", "c2", "c3", "c4", "g0", "g1", "g2", "g3", "g4"})
	strRes := s.query(collectionName, fmt.Sprintf("bloom_match(%s, {bf})", creatorIDStrField),
		[]string{creatorIDStrField}, bfParam(strBlob))
	strVals := queryStringField(strRes, creatorIDStrField)
	s.Require().NotEmpty(strVals)

	sawSealedStr, sawGrowingStr := false, false
	for _, sv := range strVals {
		switch {
		case len(sv) >= 1 && sv[0] == 'c':
			sawSealedStr = true
		case len(sv) >= 1 && sv[0] == 'g':
			sawGrowingStr = true
		}
	}
	s.True(sawSealedStr, "bloom_match(varchar) missed sealed-segment members")
	s.True(sawGrowingStr, "bloom_match(varchar) missed growing-segment members (growing segment not probed)")
}

// TestBlobArgAndErrors verifies the reshaped API: the second argument must be a client
// pre-built blob passed as a {template} bytes param. A valid blob succeeds; a malformed
// blob, a wrong field type, or a non-template argument are all rejected with an error
// status (never silently unfiltered).
func (s *BloomMatchTestSuite) TestBlobArgAndErrors() {
	collectionName := "test_bloom_match_err_" + funcutil.GenRandomStr()
	s.setupCollection(collectionName)

	blob := s.bloomBlobInt64([]int64{0, 1, 2})

	// a valid client-built blob passed as a template param succeeds
	okRes := s.search(collectionName, fmt.Sprintf("bloom_match(%s, {bf})", creatorIDField), 10, []string{creatorIDField}, bfParam(blob))
	s.NoError(merr.Error(okRes.GetStatus()))

	// bloom_match on a float vector field must be rejected
	badField := s.search(collectionName, fmt.Sprintf("bloom_match(%s, {bf})", integration.FloatVecField), 10, nil, bfParam(blob))
	s.False(merr.Ok(badField.GetStatus()), "bloom_match on vector field should be rejected")

	// a malformed blob (not a valid MBF1 envelope) must be rejected, never silently unfiltered
	badBlob := s.search(collectionName, fmt.Sprintf("bloom_match(%s, {bf})", creatorIDField), 10, nil, bfParam([]byte("not-a-real-blob")))
	s.False(merr.Ok(badBlob.GetStatus()), "malformed blob should be rejected")

	// a literal-array argument (no client build / not a template) is rejected — the blob
	// must arrive as a {template} bytes param, not be constructed proxy-side.
	badLiteral := s.search(collectionName, fmt.Sprintf("bloom_match(%s, [1, 2, 3])", creatorIDField), 10, nil, nil)
	s.False(merr.Ok(badLiteral.GetStatus()), "literal-array argument should be rejected")
}

func TestBloomMatch(t *testing.T) {
	suite.Run(t, new(BloomMatchTestSuite))
}
