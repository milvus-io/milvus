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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (s *ElementNullableSuite) TestArrayTextMatchLifecycle() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	restore := s.Cluster.MustModifyMilvusConfig(map[string]string{
		"common.enableMatchConstantPredicate":               "true",
		"dataCoord.compaction.enableAutoCompaction":         "false",
		"indexCoord.segment.minSegmentNumRowsToEnableIndex": "1",
	})
	defer restore()
	name := "array_text_" + funcutil.RandomString(8)
	varcharArray := func(id int64, name string, match, nullableElements bool) *schemapb.FieldSchema {
		opts := []*commonpb.KeyValuePair{kv(common.MaxCapacityKey, "8"), kv(common.MaxLengthKey, "64")}
		if match {
			opts = append(opts, kv(common.EnableAnalyzerKey, "true"), kv(common.EnableMatchKey, "true"))
		}
		return &schemapb.FieldSchema{FieldID: id, Name: name, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar, Nullable: true, ElementNullable: nullableElements, TypeParams: opts}
	}
	schema := &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "embedding", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{kv(common.DimKey, "8")}},
		varcharArray(102, "tags", true, false),
	}, StructArrayFields: []*schemapb.StructArrayFieldSchema{{
		FieldID: 103, Name: "reviews", Nullable: true, Fields: []*schemapb.FieldSchema{
			varcharArray(104, "body", true, true), varcharArray(105, "lang", false, false),
			{FieldID: 106, Name: "vec", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector,
				Nullable: true, TypeParams: []*commonpb.KeyValuePair{kv(common.DimKey, "8"), kv(common.MaxCapacityKey, "8")}},
		},
	}}}
	s.Require().NoError(merr.Error(s.create(ctx, schema)))
	defer func() {
		resp, err := s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{CollectionName: name})
		s.NoError(merr.CheckRPCCall(resp, err))
	}()
	s.vsCreateIndex(ctx, name, "embedding")
	s.vsCreateIndex(ctx, name, "reviews[vec]")
	s.vsLoad(ctx, name)

	insert := func(pks []int64, tags, body, lang []*schemapb.ScalarField, valid []bool) {
		var embedding []float32
		for _, pk := range pks {
			embedding = append(embedding, vector(float32(pk))...)
		}
		var reviewVectors []*schemapb.VectorField
		for i, row := range body {
			if row.ValidData == nil {
				for range row.GetStringData().GetData() {
					row.ValidData = append(row.ValidData, true)
				}
			}
			var values []float32
			for j := range row.ValidData {
				values = append(values, vsVector(float32(i+j+1), int64(i), j)...)
			}
			reviewVectors = append(reviewVectors, vectorRow(values))
		}
		vecData := vectorField("vec", 106, reviewVectors...)
		tagData, bodyData, langData := scalarField("tags", 102, tags...), scalarField("body", 104, body...), scalarField("lang", 105, lang...)
		for _, field := range []*schemapb.FieldData{tagData, bodyData, langData, vecData} {
			typeutil.SetFieldDataValidData(field, valid)
		}
		fields := []*schemapb.FieldData{
			{FieldName: "pk", FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}}}}},
			{FieldName: "embedding", FieldId: 101, Type: schemapb.DataType_FloatVector, Field: &schemapb.FieldData_Vectors{Vectors: vectorRow(embedding)}},
			tagData,
			{FieldName: "reviews", FieldId: 103, Type: schemapb.DataType_ArrayOfStruct, Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{bodyData, langData, vecData}}}},
		}
		s.Require().NoError(s.insert(ctx, name, fields))
	}
	query := func(t *testing.T, expr string, ignoreGrowing bool) ([]int64, []*milvuspb.ElementIndices) {
		t.Helper()
		req := &milvuspb.QueryRequest{CollectionName: name, Expr: expr, OutputFields: []string{"pk"}}
		if ignoreGrowing {
			req.QueryParams = []*commonpb.KeyValuePair{kv("ignore_growing", "true")}
		}
		resp, err := s.Cluster.MilvusClient.Query(ctx, req)
		require.NoError(t, merr.CheckRPCCall(resp, err), expr)
		if len(resp.GetFieldsData()) == 0 {
			return nil, nil
		}
		return resp.GetFieldsData()[0].GetScalars().GetLongData().GetData(), resp.GetElementIndices()
	}
	check := func(t *testing.T, lastPK int64, ignoreGrowing bool) {
		for _, tc := range []struct {
			expr string
			ids  []int64
		}{
			{"pk > 0", []int64{1, 2, 3, 4, 5, 6}},
			{`MATCH_ANY(tags, TEXT_MATCH($, "york"))`, []int64{1, 2, 4, 6}},
			{`MATCH_ANY(tags, PHRASE_MATCH($, "new york", 99))`, []int64{2, 4}},
			{`MATCH_ALL(tags, TEXT_MATCH($, "york"))`, []int64{2, 3, 6}},
			{`MATCH_EXACT(tags, TEXT_MATCH($, "new york", minimum_should_match=2), threshold=2)`, []int64{2}},
			{`NOT MATCH_ANY(tags, PHRASE_MATCH($, "new york"))`, []int64{1, 3, 6}},
			{`MATCH_ANY(reviews, TEXT_MATCH($[body], "york"))`, []int64{1, 2, 4, 6}},
			{`MATCH_ANY(reviews, PHRASE_MATCH($[body], "new york", 99))`, []int64{1, 4, 6}},
			{`MATCH_LEAST(reviews, PHRASE_MATCH($[body], "new york"), threshold=1)`, []int64{1, 4, 6}},
			{`MATCH_MOST(reviews, TEXT_MATCH($[body], "york"), threshold=0)`, []int64{3}},
			{`MATCH_ANY(reviews, PHRASE_MATCH($[body], "new york") && $[lang] == "en")`, []int64{4}},
		} {
			var want []int64
			for _, id := range tc.ids {
				if id <= lastPK {
					want = append(want, id)
				}
			}
			got, _ := query(t, tc.expr, ignoreGrowing)
			require.ElementsMatch(t, want, got, tc.expr)
		}
		type hit struct{ pk, offset int64 }
		for _, tc := range []struct {
			expr string
			hits []hit
		}{
			{`ELEMENT_FILTER(reviews, TEXT_MATCH($[body], "new york"))`, []hit{{1, 0}, {2, 0}, {2, 1}, {4, 0}, {6, 1}}},
			{`ELEMENT_FILTER(reviews, PHRASE_MATCH($[body], "new york", 99))`, []hit{{1, 0}, {4, 0}, {6, 1}}},
			{`pk >= 2 && ELEMENT_FILTER(reviews, TEXT_MATCH($[body], "new york", minimum_should_match=2) && $[lang] == "en")`, []hit{{4, 0}}},
			{`MATCH_ANY(tags, PHRASE_MATCH($, "new york")) && ELEMENT_FILTER(reviews, PHRASE_MATCH($[body], "new york"))`, []hit{{4, 0}}},
			{`ELEMENT_FILTER(reviews, NOT TEXT_MATCH($[body], "york"))`, []hit{{1, 1}, {2, 0}}},
			{`ELEMENT_FILTER(reviews, true || $[lang] == "en")`, []hit{{1, 0}, {1, 1}, {2, 0}, {2, 1}, {4, 0}, {6, 0}, {6, 1}}},
			{`ELEMENT_FILTER(reviews, false && $[lang] == "en")`, nil},
			{`pk >= 4 && ELEMENT_FILTER(reviews, true || $[lang] == "en")`, []hit{{4, 0}, {6, 0}, {6, 1}}},
			{`ELEMENT_FILTER(reviews, $[body] == "new" && $[body] == "york")`, nil},
			{`ELEMENT_FILTER(reviews, NOT ($[body] == "new" && $[body] == "york"))`, []hit{{1, 0}, {1, 1}, {2, 0}, {2, 1}, {4, 0}, {6, 1}}},
		} {
			var want []hit
			for _, h := range tc.hits {
				if h.pk <= lastPK {
					want = append(want, h)
				}
			}
			pks, indices := query(t, tc.expr, ignoreGrowing)
			require.Len(t, indices, len(pks), tc.expr)
			var got []hit
			for i, pk := range pks {
				for _, offset := range indices[i].GetIndices().GetData() {
					got = append(got, hit{pk, offset})
				}
			}
			require.ElementsMatch(t, want, got, tc.expr)

			for _, hint := range []string{"", "iterative_filter"} {
				var params []*commonpb.KeyValuePair
				if hint != "" {
					params = append(params, kv(common.HintsKey, hint))
				}
				if ignoreGrowing {
					params = append(params, kv("ignore_growing", "true"))
				}
				request := vsSearchRequest(name, "reviews[vec]", tc.expr, true, vsLimit, nil, params...)
				resp, err := s.Cluster.MilvusClient.Search(ctx, request)
				require.NoError(t, merr.CheckRPCCall(resp, err), "%s hint=%q", tc.expr, hint)
				var found []hit
				for _, h := range vsHits(resp.GetResults()) {
					found = append(found, hit{h.pk, h.offset})
				}
				require.ElementsMatch(t, want, found, "%s hint=%q", tc.expr, hint)
			}
		}
	}
	insert([]int64{1, 2, 3},
		[]*schemapb.ScalarField{stringRow("new", "york"), stringRow("new york", "new york"), stringRow()},
		[]*schemapb.ScalarField{stringRow("new york", "plain"), stringRow("new", "york"), stringRow()},
		[]*schemapb.ScalarField{stringRow("fr", "en"), stringRow("en", "en"), stringRow()}, []bool{true, true, true})
	s.Run("growing", func() {
		check(s.T(), 3, false)
		ids, _ := query(s.T(), "pk > 0", true)
		s.Empty(ids, "growing rows must disappear with ignore_growing")
	})
	s.vsFlush(ctx, name)
	nullAndPhrase := stringRow("new york")
	nullAndPhrase.ValidData = []bool{false, true}
	insert([]int64{4, 5, 6},
		[]*schemapb.ScalarField{stringRow("plain", "new york"), stringRow("york")},
		[]*schemapb.ScalarField{stringRow("new york"), nullAndPhrase},
		[]*schemapb.ScalarField{stringRow("en"), stringRow("en", "fr")}, []bool{true, false, true})
	s.Run("second_batch_growing", func() {
		check(s.T(), 6, false)
		ids, _ := query(s.T(), "pk >= 4", true)
		s.Empty(ids, "the second batch must still be growing")
	})
	s.flushAndReload(ctx, name)
	desc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(desc, err))
	helper, err := typeutil.CreateSchemaHelper(desc.GetSchema())
	s.Require().NoError(err)
	tagSchema, err := helper.GetFieldFromName("tags")
	s.Require().NoError(err)
	bodySchema, err := helper.GetFieldFromName("body")
	s.Require().NoError(err)
	// A flush can expose the original segment before sort compaction publishes its
	// indexed replacement. Wait for the same IDs to be loaded and indexed.
	waitForIndexedSegments := func(count int, expected ...int64) []int64 {
		var serving []int64
		s.Require().Eventually(func() bool {
			info, err := s.Cluster.MilvusClient.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{CollectionName: name})
			if merr.CheckRPCCall(info, err) != nil {
				return false
			}
			loaded := typeutil.NewSet[int64]()
			for _, seg := range info.GetInfos() {
				if seg.GetState() == commonpb.SegmentState_Sealed || seg.GetState() == commonpb.SegmentState_Flushed {
					loaded.Insert(seg.GetSegmentID())
				}
			}
			if loaded.Len() != count || !loaded.Contain(expected...) {
				return false
			}
			segments, err := s.Cluster.ShowSegments(name)
			if err != nil {
				return false
			}
			indexed := typeutil.NewSet[int64]()
			for _, seg := range segments {
				stats := seg.GetTextStatsLogs()
				if len(stats[tagSchema.GetFieldID()].GetFiles()) > 0 && len(stats[bodySchema.GetFieldID()].GetFiles()) > 0 {
					indexed.Insert(seg.GetID())
				}
			}
			serving = loaded.Collect()
			return indexed.Contain(serving...)
		}, 5*time.Minute, time.Second, "expected %d loaded sealed segments with persisted text indexes on both fields, target IDs %v", count, expected)
		return serving
	}
	before := waitForIndexedSegments(2)
	s.Run("sealed", func() { check(s.T(), 6, true) })

	compaction, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{CollectionID: desc.GetCollectionID()})
	s.Require().NoError(merr.CheckRPCCall(compaction, err))
	id := compaction.GetCompactionID()
	s.Require().Greater(compaction.GetCompactionPlanCount(), int32(0))
	s.Require().Eventually(func() bool {
		res, err := s.Cluster.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{CompactionID: id})
		return merr.CheckRPCCall(res, err) == nil && res.GetState() == commonpb.CompactionState_Completed
	}, 5*time.Minute, time.Second)
	plans, err := s.Cluster.MilvusClient.GetCompactionStateWithPlans(ctx, &milvuspb.GetCompactionPlansRequest{CompactionID: id})
	s.Require().NoError(merr.CheckRPCCall(plans, err))
	var sources, targets []int64
	for _, plan := range plans.GetMergeInfos() {
		sources = append(sources, plan.GetSources()...)
		targets = append(targets, plan.GetTargets()...)
	}
	s.Require().ElementsMatch(before, sources)
	s.Require().NotEmpty(targets)
	for _, target := range targets {
		s.Require().NotContains(sources, target)
	}
	s.T().Logf("compaction %d rewrote segments %v to %v", id, sources, targets)
	waitForIndexedSegments(len(targets), targets...)
	s.Run("compacted", func() { check(s.T(), 6, true) })
}
