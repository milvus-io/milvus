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

package elementnullable

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

const (
	addedVec        = "added_vectors"
	searchIterV2Key = "search_iter_v2"
	vsLimit         = 50 // larger than every element and row count below
)

// vsElem is one struct element. A nil pointer is a null element value. The
// query is e0, so the inner product of an element vector is its first
// coordinate; every vector of a field has a distinct one, which makes the
// score of a hit identify the element it came from.
type vsElem struct {
	nullable *float32
	plain    float32
	added    *float32
	ints     *int64
}

type vsRow struct {
	pk        int64
	embedding float32
	null      bool // the whole struct is null
	elems     []vsElem
}

func f32(v float32) *float32 { return &v }

func i64(v int64) *int64 { return &v }

func el(nullable *float32, plain float32, ints *int64) vsElem {
	return vsElem{nullable: nullable, plain: plain, ints: ints}
}

func elAdded(nullable *float32, plain float32, ints *int64, added *float32) vsElem {
	return vsElem{nullable: nullable, plain: plain, ints: ints, added: added}
}

// Each batch has rows with some null element vectors, all-null element
// vectors, an empty struct array and a null struct.
var (
	vsSealedRows = []vsRow{
		{pk: 1, embedding: 0.11, elems: []vsElem{el(f32(0.90), 0.30, i64(1)), el(nil, 0.80, nil), el(f32(0.50), 0.10, i64(3))}},
		{pk: 2, embedding: 0.12, elems: []vsElem{el(nil, 0.70, nil), el(nil, 0.20, i64(2))}},
		{pk: 3, embedding: 0.13},
		{pk: 4, embedding: 0.14, null: true},
		{pk: 5, embedding: 0.15, elems: []vsElem{el(f32(0.60), 0.62, i64(4)), el(f32(0.95), 0.05, nil), el(nil, 0.90, i64(6)), el(f32(0.40), 0.15, i64(7))}},
		{pk: 6, embedding: 0.16, elems: []vsElem{el(f32(0.85), 0.35, i64(8))}},
	}
	vsGrowingRows = []vsRow{
		{pk: 11, embedding: 0.21, elems: []vsElem{el(nil, 0.45, i64(9)), el(f32(0.97), 0.25, nil)}},
		{pk: 12, embedding: 0.22, elems: []vsElem{el(nil, 0.55, nil)}},
		{pk: 13, embedding: 0.23},
		{pk: 14, embedding: 0.24, null: true},
		{pk: 15, embedding: 0.25, elems: []vsElem{el(f32(0.70), 0.75, i64(5)), el(f32(0.30), 0.40, nil), el(nil, 0.65, i64(10))}},
	}
	// Written after added_vectors is added; the rows above stay null in it.
	vsAddedSealedRows = []vsRow{
		{pk: 21, embedding: 0.31, elems: []vsElem{
			elAdded(f32(0.33), 0.66, i64(11), nil), elAdded(nil, 0.12, nil, f32(0.81)), elAdded(f32(0.44), 0.88, i64(12), f32(0.29)),
		}},
		{pk: 22, embedding: 0.32, elems: []vsElem{elAdded(f32(0.57), 0.27, nil, nil), elAdded(nil, 0.37, i64(13), nil)}},
		{pk: 23, embedding: 0.33},
		{pk: 24, embedding: 0.34, null: true},
	}
	vsAddedGrowingRows = []vsRow{
		{pk: 31, embedding: 0.41, elems: []vsElem{elAdded(nil, 0.47, i64(14), f32(0.93)), elAdded(f32(0.77), 0.17, nil, nil)}},
		{pk: 32, embedding: 0.42, elems: []vsElem{
			elAdded(f32(0.22), 0.58, i64(15), f32(0.64)), elAdded(f32(0.83), 0.08, i64(16), f32(0.18)), elAdded(nil, 0.98, nil, nil),
		}},
		{pk: 33, embedding: 0.43, null: true},
	}
)

func (e vsElem) score(field string) (float32, bool) {
	switch field {
	case nullVec:
		if e.nullable == nil {
			return 0, false
		}
		return *e.nullable, true
	case plainVec:
		return e.plain, true
	case addedVec:
		if e.added == nil {
			return 0, false
		}
		return *e.added, true
	}
	panic("unknown vector sub-field " + field)
}

// vsVector puts the score in the first dimension; the rest only makes the
// vectors distinct.
func vsVector(score float32, pk int64, offset int) []float32 {
	return []float32{score, float32(pk) / 100, float32(offset) / 10, 0, 0, 0, 0, 0}
}

func vsAnns(field string) string {
	return typeutil.ConcatStructFieldName(structName, field)
}

func vsSchema(name string) *schemapb.CollectionSchema {
	vectorParams := []*commonpb.KeyValuePair{kv(common.DimKey, strconv.Itoa(dim)), kv(common.MaxCapacityKey, "8")}
	return &schemapb.CollectionSchema{
		Name: name,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{kv(common.DimKey, strconv.Itoa(dim))},
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 102, Name: structName, Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID: 103, Name: nullVec, DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true, TypeParams: vectorParams,
				},
				{
					FieldID: 104, Name: plainVec, DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: vectorParams,
				},
				{
					FieldID: 105, Name: intsName, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
					Nullable: true, ElementNullable: true, TypeParams: []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8")},
				},
			},
		}},
	}
}

// vsFields builds an insert payload. Children are compact: a null struct row
// has no entry, and element-nullable children carry only their valid values.
func vsFields(rows []vsRow, withAdded bool) []*schemapb.FieldData {
	pks := make([]int64, 0, len(rows))
	embeddings := make([]float32, 0, len(rows)*dim)
	structValid := make([]bool, 0, len(rows))
	var nullableRows, plainRows, addedRows []*schemapb.VectorField
	var intRows []*schemapb.ScalarField
	for _, row := range rows {
		pks = append(pks, row.pk)
		embeddings = append(embeddings, vsVector(row.embedding, row.pk, 0)...)
		structValid = append(structValid, !row.null)
		if row.null {
			continue
		}
		var nullableData, plainData, addedData []float32
		var nullableValid, addedValid, intValid []bool
		var ints []int64
		for j, e := range row.elems {
			if e.nullable != nil {
				nullableData = append(nullableData, vsVector(*e.nullable, row.pk, j)...)
			}
			nullableValid = append(nullableValid, e.nullable != nil)
			plainData = append(plainData, vsVector(e.plain, row.pk, j)...)
			if e.added != nil {
				addedData = append(addedData, vsVector(*e.added, row.pk, j)...)
			}
			addedValid = append(addedValid, e.added != nil)
			if e.ints != nil {
				ints = append(ints, *e.ints)
			}
			intValid = append(intValid, e.ints != nil)
		}
		nullableRows = append(nullableRows, vectorRow(nullableData, nullableValid...))
		plainRows = append(plainRows, vectorRow(plainData))
		addedRows = append(addedRows, vectorRow(addedData, addedValid...))
		intRows = append(intRows, longRow(ints, intValid...))
	}
	children := []*schemapb.FieldData{
		vectorField(nullVec, 103, nullableRows...),
		vectorField(plainVec, 104, plainRows...),
		scalarField(intsName, 105, intRows...),
	}
	if withAdded {
		children = append(children, vectorField(addedVec, 0, addedRows...))
	}
	for _, child := range children {
		typeutil.SetFieldDataValidData(child, structValid)
	}
	return []*schemapb.FieldData{
		{
			FieldName: "pk", FieldId: 100, Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
			}},
		},
		{
			FieldName: "embedding", FieldId: 101, Type: schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: embeddings}},
			}},
		},
		{
			FieldName: structName, FieldId: 102, Type: schemapb.DataType_ArrayOfStruct,
			Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: children}},
		},
	}
}

type vsHit struct {
	pk, offset int64
	score      float32
}

type vsModel struct {
	rows  []vsRow
	added bool
}

// hits is the exact element-level answer: every valid element vector of field
// in rows that pass row, whose element passes elem, best score first.
func (m *vsModel) hits(field string, row func(vsRow) bool, elem func(vsElem) bool) []vsHit {
	var hits []vsHit
	for _, r := range m.rows {
		if r.null || (row != nil && !row(r)) {
			continue
		}
		for j, e := range r.elems {
			score, ok := e.score(field)
			if ok && (elem == nil || elem(e)) {
				hits = append(hits, vsHit{pk: r.pk, offset: int64(j), score: score})
			}
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].score > hits[j].score })
	return hits
}

// groups is the exact strict group-by-pk answer for hits in score order.
func vsGroups(hits []vsHit, groupSize, topk int) []vsHit {
	var order []int64
	groups := make(map[int64][]vsHit)
	for _, hit := range hits {
		group, ok := groups[hit.pk]
		if !ok {
			if len(order) == topk {
				continue
			}
			order = append(order, hit.pk)
		}
		if len(group) < groupSize {
			groups[hit.pk] = append(group, hit)
		}
	}
	var out []vsHit
	for _, pk := range order {
		out = append(out, groups[pk]...)
	}
	return out
}

func vsAggregate(scores []float32, strategy string, k int) float32 {
	sorted := append([]float32(nil), scores...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] > sorted[j] })
	if strategy == "topk_sum" || strategy == "topk_avg" {
		sorted = sorted[:min(k, len(sorted))]
	}
	sum := float32(0)
	for _, score := range sorted {
		sum += score
	}
	switch strategy {
	case "max":
		return sorted[0]
	case "sum", "topk_sum":
		return sum
	default: // avg, topk_avg
		return sum / float32(len(sorted))
	}
}

func vsPlaceholder(elementLevel bool) []byte {
	query := make([]byte, dim*4)
	binary.LittleEndian.PutUint32(query, math.Float32bits(1))
	group := &commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{
		Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{query}, ElementLevel: elementLevel,
	}}}
	data, err := proto.Marshal(group)
	if err != nil {
		panic(err)
	}
	return data
}

func vsSearchRequest(name, annsField, expr string, elementLevel bool, topk int, params map[string]any, extra ...*commonpb.KeyValuePair) *milvuspb.SearchRequest {
	if params == nil {
		params = map[string]any{"ef": 64}
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}
	return &milvuspb.SearchRequest{
		CollectionName: name,
		Dsl:            expr,
		DslType:        commonpb.DslType_BoolExprV1,
		SearchInput:    &milvuspb.SearchRequest_PlaceholderGroup{PlaceholderGroup: vsPlaceholder(elementLevel)},
		SearchParams: append([]*commonpb.KeyValuePair{
			kv(common.MetricTypeKey, metric.IP),
			kv(common.ParamsKey, string(encoded)),
			kv(integration.AnnsFieldKey, annsField),
			kv(integration.TopKKey, strconv.Itoa(topk)),
			kv(integration.RoundDecimalKey, "-1"),
		}, extra...),
		Nq: 1,
	}
}

func vsHits(data *schemapb.SearchResultData) []vsHit {
	ids := data.GetIds().GetIntId().GetData()
	offsets := data.GetElementIndices().GetData()
	hits := make([]vsHit, len(ids))
	for i, pk := range ids {
		hits[i] = vsHit{pk: pk, offset: -1, score: data.GetScores()[i]}
		if i < len(offsets) {
			hits[i].offset = offsets[i]
		}
	}
	return hits
}

func vsKeys(hits []vsHit) []string {
	keys := make([]string, len(hits))
	for i, hit := range hits {
		keys[i] = fmt.Sprintf("%d/%d", hit.pk, hit.offset)
	}
	return keys
}

func (s *ElementNullableSuite) vsAssertHits(want, got []vsHit, msg string) {
	s.Require().Equal(vsKeys(want), vsKeys(got), msg)
	for i := range want {
		s.InDelta(want[i].score, got[i].score, 1e-5, "%s: hit %d", msg, i)
	}
}

// vsAssertRowScores checks a row-level result: unique pks, no element offsets,
// scores in descending order and equal to want.
func (s *ElementNullableSuite) vsAssertRowScores(want map[int64]float32, data *schemapb.SearchResultData, msg string) {
	s.Empty(data.GetElementIndices().GetData(), msg)
	got := make(map[int64]float32)
	for i, hit := range vsHits(data) {
		s.NotContains(got, hit.pk, "%s: duplicate pk", msg)
		got[hit.pk] = hit.score
		if i > 0 {
			s.LessOrEqual(hit.score, data.GetScores()[i-1]+1e-6, "%s: scores are not descending", msg)
		}
	}
	s.Require().Len(got, len(want), "%s: got %v, want %v", msg, got, want)
	for pk, score := range want {
		s.Require().Contains(got, pk, msg)
		s.InDelta(score, got[pk], 1e-5, "%s: pk %d", msg, pk)
	}
}

func (s *ElementNullableSuite) vsSearch(ctx context.Context, request *milvuspb.SearchRequest) *milvuspb.SearchResults {
	result, err := s.Cluster.MilvusClient.Search(ctx, request)
	s.Require().NoError(merr.CheckRPCCall(result, err), "%s %s", request.GetDsl(), request.GetSearchParams())
	return result
}

func vsHybridRequest(name string, weights []float64, subs ...*milvuspb.SearchRequest) *milvuspb.HybridSearchRequest {
	params, err := json.Marshal(map[string]any{proxy.WeightsParamsKey: weights, proxy.NormScoreKey: false})
	if err != nil {
		panic(err)
	}
	return &milvuspb.HybridSearchRequest{
		CollectionName: name,
		Requests:       subs,
		RankParams: []*commonpb.KeyValuePair{
			kv(proxy.RankTypeKey, "weighted"),
			kv(proxy.ParamsKey, string(params)),
			kv(proxy.LimitKey, strconv.Itoa(vsLimit)),
			kv(proxy.RoundDecimalKey, "-1"),
		},
	}
}

func (s *ElementNullableSuite) vsHybrid(ctx context.Context, request *milvuspb.HybridSearchRequest, msg string) *schemapb.SearchResultData {
	result, err := s.Cluster.MilvusClient.HybridSearch(ctx, request)
	s.Require().NoError(merr.CheckRPCCall(result, err), msg)
	return result.GetResults()
}

func (s *ElementNullableSuite) vsInsert(ctx context.Context, name string, model *vsModel, rows []vsRow) {
	result, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name, FieldsData: vsFields(rows, model.added), NumRows: uint32(len(rows)),
		HashKeys: integration.GenerateHashKeys(len(rows)),
	})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	model.rows = append(model.rows, rows...)
}

func (s *ElementNullableSuite) vsFlush(ctx context.Context, name string) {
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(flush.GetStatus(), err))
	segmentIDs := flush.GetFlushCollSegIDs()[name].GetData()
	s.Require().NotEmpty(segmentIDs)
	s.WaitForFlush(ctx, segmentIDs, flush.GetCollFlushTs()[name], "", name)
}

func (s *ElementNullableSuite) vsCreateIndex(ctx context.Context, name, field string) {
	status, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: field, IndexName: "idx_" + field,
		ExtraParams: integration.ConstructIndexParam(dim, integration.IndexHNSW, metric.IP),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err), field)
}

// vsWaitIndexed waits until every flushed segment, flushedRows rows in all, has
// a finished index on field.
func (s *ElementNullableSuite) vsWaitIndexed(ctx context.Context, name, field string, flushedRows int64) {
	failReason := ""
	s.Require().Eventually(func() bool {
		resp, err := s.Cluster.MilvusClient.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			CollectionName: name, FieldName: field,
		})
		if merr.CheckRPCCall(resp.GetStatus(), err) != nil {
			return false
		}
		for _, desc := range resp.GetIndexDescriptions() {
			if desc.GetFieldName() != field {
				continue
			}
			if desc.GetState() == commonpb.IndexState_Failed {
				failReason = desc.GetIndexStateFailReason()
				return true
			}
			return desc.GetState() == commonpb.IndexState_Finished && desc.GetPendingIndexRows() == 0 &&
				desc.GetTotalRows() == flushedRows
		}
		return false
	}, 5*time.Minute, 200*time.Millisecond, field)
	s.Require().Empty(failReason, "index on %s failed", field)
}

func (s *ElementNullableSuite) vsLoad(ctx context.Context, name string) {
	status, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
}

func (s *ElementNullableSuite) vsAddVectorField(ctx context.Context, name string) {
	describe := func() int32 {
		resp, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
		if merr.CheckRPCCall(resp.GetStatus(), err) != nil {
			return -1
		}
		return resp.GetSchema().GetVersion()
	}
	before := describe()
	s.Require().GreaterOrEqual(before, int32(0))
	resp, err := s.Cluster.MilvusClient.AlterCollectionSchema(ctx, &milvuspb.AlterCollectionSchemaRequest{
		CollectionName: name,
		Action: &milvuspb.AlterCollectionSchemaRequest_Action{Op: &milvuspb.AlterCollectionSchemaRequest_Action_AddRequest{
			AddRequest: &milvuspb.AlterCollectionSchemaRequest_AddRequest{
				FieldInfos: []*milvuspb.AlterCollectionSchemaRequest_FieldInfo{{StructPath: structName, FieldSchema: &schemapb.FieldSchema{
					Name: addedVec, DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
					ElementNullable: true,
					TypeParams:      []*commonpb.KeyValuePair{kv(common.DimKey, strconv.Itoa(dim)), kv(common.MaxCapacityKey, "8")},
				}}},
			},
		}},
	})
	s.Require().NoError(merr.CheckRPCCall(resp.GetAlterStatus(), err))
	s.Require().Eventually(func() bool { return describe() > before }, 30*time.Second, 100*time.Millisecond)
}

// TestElementLevelVectorSearch runs element-level ANN on element-nullable
// vector sub-fields end to end: group by pk, hybrid search with and without
// element_scope, range search, search iterator v2 and element_filter, first on
// a sealed (indexed) plus a growing (brute force) segment, then again after a
// vector sub-field is added to the struct.
func (s *ElementNullableSuite) TestElementLevelVectorSearch() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	// Index the small sealed segments for real; below this threshold datacoord
	// skips the build and sealed search falls back to brute force.
	revert := s.Cluster.MustModifyMilvusConfig(map[string]string{"indexCoord.segment.minSegmentNumRowsToEnableIndex": "1"})
	defer revert()

	name := "w8i_search_" + funcutil.RandomString(8)
	s.Require().NoError(merr.Error(s.create(ctx, vsSchema(name))))
	fields := []string{"embedding", vsAnns(nullVec), vsAnns(plainVec)}
	for _, field := range fields {
		s.vsCreateIndex(ctx, name, field)
	}
	model := &vsModel{}
	s.vsInsert(ctx, name, model, vsSealedRows)
	s.vsFlush(ctx, name)
	for _, field := range fields {
		s.vsWaitIndexed(ctx, name, field, int64(len(vsSealedRows)))
	}
	s.vsLoad(ctx, name)
	s.vsInsert(ctx, name, model, vsGrowingRows)
	s.Run("before_add", func() { s.vsRunChecks(ctx, name, model) })

	s.vsAddVectorField(ctx, name)
	model.added = true
	s.vsInsert(ctx, name, model, vsAddedSealedRows)
	fields = append(fields, vsAnns(addedVec))
	s.vsCreateIndex(ctx, name, vsAnns(addedVec))
	s.vsFlush(ctx, name)
	flushed := int64(len(vsSealedRows) + len(vsGrowingRows) + len(vsAddedSealedRows))
	for _, field := range fields {
		s.vsWaitIndexed(ctx, name, field, flushed)
	}
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.vsLoad(ctx, name)
	s.vsInsert(ctx, name, model, vsAddedGrowingRows)
	s.Run("after_add", func() { s.vsRunChecks(ctx, name, model) })
}

func (s *ElementNullableSuite) vsRunChecks(ctx context.Context, name string, model *vsModel) {
	vectors := []string{nullVec}
	if model.added {
		vectors = append(vectors, addedVec)
	}
	for _, field := range vectors {
		s.Run(field+"/plain", func() { s.vsCheckPlain(ctx, name, model, field) })
		s.Run(field+"/group_by_pk", func() { s.vsCheckGroupBy(ctx, name, model, field) })
		s.Run(field+"/range", func() { s.vsCheckRange(ctx, name, model, field) })
		s.Run(field+"/iterator_v2", func() { s.vsCheckIterator(ctx, name, model, field) })
	}
	s.Run("element_filter", func() { s.vsCheckFilters(ctx, name, model) })
	s.Run("hybrid", func() { s.vsCheckHybrid(ctx, name, model) })
}

// vsCheckPlain: null elements never hit and offsets are logical (a null
// element keeps its position), on the sealed index and growing brute force.
func (s *ElementNullableSuite) vsCheckPlain(ctx context.Context, name string, model *vsModel, field string) {
	want := model.hits(field, nil, nil)
	s.Require().NotEmpty(want)
	for _, topk := range []int{vsLimit, 3} {
		result := s.vsSearch(ctx, vsSearchRequest(name, vsAnns(field), "", true, topk, nil))
		s.vsAssertHits(want[:min(topk, len(want))], vsHits(result.GetResults()), fmt.Sprintf("%s topk %d", field, topk))
	}
}

// vsCheckGroupBy: element-level ANN grouped by pk. Each group holds elements
// of one row only, at most group_size of them, none null, at logical offsets.
// strict_group_size returns the exact answer; without it a group may stop
// early, so its elements must be the best ones of that row.
func (s *ElementNullableSuite) vsCheckGroupBy(ctx context.Context, name string, model *vsModel, field string) {
	const groupSize = 2
	all := model.hits(field, nil, nil)
	for _, strict := range []bool{true, false} {
		for _, topk := range []int{2, 10} {
			msg := fmt.Sprintf("%s strict=%v topk=%d", field, strict, topk)
			result := s.vsSearch(ctx, vsSearchRequest(name, vsAnns(field), "", true, topk, nil,
				kv(proxy.GroupByFieldKey, "pk"), kv(proxy.GroupSizeKey, strconv.Itoa(groupSize)),
				kv(proxy.StrictGroupSize, strconv.FormatBool(strict))))
			data := result.GetResults()
			got := vsHits(data)
			groupValues := data.GetGroupByFieldValue()
			if groupValues == nil && len(data.GetGroupByFieldValues()) > 0 {
				groupValues = data.GetGroupByFieldValues()[0]
			}
			values := groupValues.GetScalars().GetLongData().GetData()
			s.Require().Len(values, len(got), msg)
			for i, hit := range got {
				s.Require().Equal(hit.pk, values[i], "%s: group value is not the row pk", msg)
			}

			want := vsGroups(all, groupSize, topk)
			if strict {
				s.vsAssertHits(want, got, msg)
				continue
			}
			byPK := make(map[int64][]vsHit)
			var order, wantOrder []int64
			for i, hit := range got {
				if i == 0 || got[i-1].pk != hit.pk {
					s.Require().NotContains(byPK, hit.pk, "%s: elements of pk %d are not contiguous", msg, hit.pk)
					order = append(order, hit.pk)
				}
				byPK[hit.pk] = append(byPK[hit.pk], hit)
			}
			for i, hit := range want {
				if i == 0 || want[i-1].pk != hit.pk {
					wantOrder = append(wantOrder, hit.pk)
				}
			}
			s.Require().Equal(wantOrder, order, msg)
			for _, pk := range order {
				rowHits := model.hits(field, func(r vsRow) bool { return r.pk == pk }, nil)
				s.Require().LessOrEqual(len(byPK[pk]), groupSize, msg)
				s.vsAssertHits(rowHits[:len(byPK[pk])], byPK[pk], fmt.Sprintf("%s pk %d", msg, pk))
			}
		}
	}
}

var vsRanges = map[string][2]float32{nullVec: {0.45, 0.92}, addedVec: {0.25, 0.90}}

// vsCheckRange: IP range search keeps radius < score <= range_filter.
func (s *ElementNullableSuite) vsCheckRange(ctx context.Context, name string, model *vsModel, field string) {
	bounds := vsRanges[field]
	var want []vsHit
	for _, hit := range model.hits(field, nil, nil) {
		if hit.score > bounds[0] && hit.score <= bounds[1] {
			want = append(want, hit)
		}
	}
	s.Require().NotEmpty(want)
	params := map[string]any{"ef": 64, "radius": bounds[0], "range_filter": bounds[1]}
	result := s.vsSearch(ctx, vsSearchRequest(name, vsAnns(field), "", true, vsLimit, params))
	s.vsAssertHits(want, vsHits(result.GetResults()), field+" range")
}

// vsCheckIterator: search iterator v2 in batches of 2 returns every valid
// element once, in score order, and then an empty batch.
func (s *ElementNullableSuite) vsCheckIterator(ctx context.Context, name string, model *vsModel, field string) {
	const batchSize = 2
	want := model.hits(field, nil, nil)
	var got []vsHit
	token, lastBound := "", ""
	var sessionTs uint64
	for round := 0; ; round++ {
		s.Require().LessOrEqual(round, len(want)/batchSize+2, "iterator does not terminate")
		extra := []*commonpb.KeyValuePair{
			kv(proxy.IteratorField, "true"), kv(searchIterV2Key, "true"),
			kv(proxy.SearchIterBatchSizeKey, strconv.Itoa(batchSize)),
		}
		if token != "" {
			extra = append(extra, kv(proxy.SearchIterIdKey, token), kv(proxy.SearchIterLastBoundKey, lastBound))
		}
		request := vsSearchRequest(name, vsAnns(field), "", true, batchSize, nil, extra...)
		request.GuaranteeTimestamp = sessionTs
		result := s.vsSearch(ctx, request)
		if round == 0 {
			sessionTs = result.GetSessionTs()
		}
		batch := vsHits(result.GetResults())
		s.Require().LessOrEqual(len(batch), batchSize)
		iterator := result.GetResults().GetSearchIteratorV2Results()
		s.Require().NotNil(iterator)
		token = iterator.GetToken()
		lastBound = strconv.FormatFloat(float64(iterator.GetLastBound()), 'g', -1, 32)
		if len(batch) == 0 {
			break
		}
		got = append(got, batch...)
	}
	s.vsAssertHits(want, got, field+" iterator")
}

type vsFilterCase struct {
	field, expr string
	row         func(vsRow) bool
	elem        func(vsElem) bool
}

func vsIntsIs(pred func(int64) bool) func(vsElem) bool {
	return func(e vsElem) bool { return e.ints != nil && pred(*e.ints) }
}

// vsCheckFilters: element_filter (and MATCH_*) on element-nullable scalar and
// vector sub-fields combined with element-level ANN. A predicate on a null
// value is UNKNOWN and selects nothing; the result is the intersection with
// the valid elements of the searched vector.
func (s *ElementNullableSuite) vsCheckFilters(ctx context.Context, name string, model *vsModel) {
	cases := []vsFilterCase{
		{nullVec, "element_filter(items, $[ints] > 3)", nil, vsIntsIs(func(v int64) bool { return v > 3 })},
		{nullVec, "element_filter(items, !($[ints] > 3))", nil, vsIntsIs(func(v int64) bool { return v <= 3 })},
		{nullVec, "element_filter(items, $[ints] IS NULL)", nil, func(e vsElem) bool { return e.ints == nil }},
		{nullVec, "element_filter(items, $[ints] IS NOT NULL && $[ints] < 8)", nil, vsIntsIs(func(v int64) bool { return v < 8 })},
		{nullVec, "element_filter(items, $[ints] != 1 || $[ints] IS NULL)", nil, func(e vsElem) bool { return e.ints == nil || *e.ints != 1 }},
		{nullVec, "element_filter(items, $[nullable_vectors] IS NULL)", nil, func(e vsElem) bool { return e.nullable == nil }},
		{nullVec, "element_filter(items, $[nullable_vectors] IS NOT NULL)", nil, nil},
		{plainVec, "element_filter(items, $[nullable_vectors] IS NULL)", nil, func(e vsElem) bool { return e.nullable == nil }},
		{
			plainVec, "element_filter(items, $[nullable_vectors] IS NOT NULL && $[ints] IS NULL)", nil,
			func(e vsElem) bool { return e.nullable != nil && e.ints == nil },
		},
		{
			nullVec, "pk > 5 && element_filter(items, $[ints] >= 4 || $[ints] IS NULL)", func(r vsRow) bool { return r.pk > 5 },
			func(e vsElem) bool { return e.ints == nil || *e.ints >= 4 },
		},
		{nullVec, "MATCH_ANY(items, $[ints] IS NULL)", func(r vsRow) bool {
			for _, e := range r.elems {
				if e.ints == nil {
					return true
				}
			}
			return false
		}, nil},
		{plainVec, "MATCH_ALL(items, $[nullable_vectors] IS NOT NULL)", func(r vsRow) bool {
			for _, e := range r.elems {
				if e.nullable == nil {
					return false
				}
			}
			return true
		}, nil},
	}
	if model.added {
		cases = append(cases,
			vsFilterCase{addedVec, "element_filter(items, $[added_vectors] IS NULL)", nil, func(e vsElem) bool { return e.added == nil }},
			vsFilterCase{plainVec, "element_filter(items, $[added_vectors] IS NOT NULL)", nil, func(e vsElem) bool { return e.added != nil }},
			vsFilterCase{nullVec, "element_filter(items, $[added_vectors] IS NOT NULL)", nil, func(e vsElem) bool { return e.added != nil }},
			vsFilterCase{addedVec, "element_filter(items, $[nullable_vectors] IS NULL)", nil, func(e vsElem) bool { return e.nullable == nil }},
			vsFilterCase{addedVec, "element_filter(items, $[ints] IS NULL)", nil, func(e vsElem) bool { return e.ints == nil }},
		)
	}
	for _, tc := range cases {
		want := model.hits(tc.field, tc.row, tc.elem)
		for _, hint := range []string{"", "iterative_filter"} {
			var extra []*commonpb.KeyValuePair
			if hint != "" {
				extra = append(extra, kv(common.HintsKey, hint))
			}
			result := s.vsSearch(ctx, vsSearchRequest(name, vsAnns(tc.field), tc.expr, true, vsLimit, nil, extra...))
			s.vsAssertHits(want, vsHits(result.GetResults()), fmt.Sprintf("%s on %s hints=%q", tc.expr, tc.field, hint))
		}
	}
}

// vsCheckHybrid covers the two hybrid shapes:
//   - element-level sub-searches on the same struct keep element granularity
//     and are fused per (pk, offset); element_scope is rejected there;
//   - mixed with a top-level vector, each element-level sub-search is first
//     reduced to rows by element_scope (default max), then fused.
//
// The weighted ranker sums raw IP scores; weight 0 on the top-level vector
// keeps every row in the result without changing its score. Null elements
// must not count in sum / avg / topk_* aggregates.
func (s *ElementNullableSuite) vsCheckHybrid(ctx context.Context, name string, model *vsModel) {
	pairs := [][2]string{{nullVec, plainVec}}
	if model.added {
		pairs = append(pairs, [2]string{addedVec, nullVec})
	}
	elementSub := func(field string, params map[string]any) *milvuspb.SearchRequest {
		return vsSearchRequest(name, vsAnns(field), "", true, vsLimit, params)
	}
	topLevelSub := vsSearchRequest(name, "embedding", "", false, vsLimit, nil)

	for _, pair := range pairs {
		msg := fmt.Sprintf("same struct %v", pair)
		want := make(map[string]float32)
		var keys []string
		for _, field := range pair {
			for _, hit := range model.hits(field, nil, nil) {
				key := vsKeys([]vsHit{hit})[0]
				if _, ok := want[key]; !ok {
					keys = append(keys, key)
				}
				want[key] += hit.score
			}
		}
		data := s.vsHybrid(ctx, vsHybridRequest(name, []float64{1, 1}, elementSub(pair[0], nil), elementSub(pair[1], nil)), msg)
		got := vsHits(data)
		s.Require().Len(data.GetElementIndices().GetData(), len(got), msg)
		s.Require().ElementsMatch(keys, vsKeys(got), msg)
		for i, hit := range got {
			s.InDelta(want[vsKeys([]vsHit{hit})[0]], hit.score, 1e-5, "%s: %d/%d", msg, hit.pk, hit.offset)
			if i > 0 {
				s.LessOrEqual(hit.score, got[i-1].score+1e-6, msg)
			}
		}

		scoped := map[string]any{"ef": 64, "element_scope": map[string]any{"collapse": map[string]any{"strategy": "max"}}}
		result, err := s.Cluster.MilvusClient.HybridSearch(ctx, vsHybridRequest(name, []float64{1, 1},
			elementSub(pair[0], scoped), elementSub(pair[1], nil)))
		s.Require().NoError(err)
		s.Require().ErrorContains(merr.Error(result.GetStatus()), "is not allowed for same-struct element-level hybrid search", msg)
	}

	rowScores := func(fields []string, strategy string, k int) map[int64]float32 {
		want := make(map[int64]float32)
		for _, row := range model.rows {
			want[row.pk] = 0
			if row.null {
				continue
			}
			for _, field := range fields {
				var scores []float32
				for _, e := range row.elems {
					if score, ok := e.score(field); ok {
						scores = append(scores, score)
					}
				}
				if len(scores) > 0 {
					want[row.pk] += vsAggregate(scores, strategy, k)
				}
			}
		}
		return want
	}
	for _, pair := range pairs {
		// Default element_scope (max) with one element-level sub-search.
		data := s.vsHybrid(ctx, vsHybridRequest(name, []float64{1, 0}, elementSub(pair[0], nil), topLevelSub), pair[0]+" + embedding")
		s.vsAssertRowScores(rowScores(pair[:1], "max", 0), data, pair[0]+" + embedding")

		for _, strategy := range []string{"max", "sum", "avg", "topk_sum", "topk_avg"} {
			collapse := map[string]any{"strategy": strategy}
			if strategy == "topk_sum" || strategy == "topk_avg" {
				collapse["topk"] = 2
			}
			params := map[string]any{"ef": 64, "element_scope": map[string]any{"collapse": collapse}}
			msg := fmt.Sprintf("%v + embedding element_scope %s", pair, strategy)
			data := s.vsHybrid(ctx, vsHybridRequest(name, []float64{1, 1, 0},
				elementSub(pair[0], params), elementSub(pair[1], params), topLevelSub), msg)
			s.vsAssertRowScores(rowScores(pair[:], strategy, 2), data, msg)
		}
	}
}
