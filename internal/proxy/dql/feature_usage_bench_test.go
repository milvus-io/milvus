// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The design doc's cost rule is "one branch and one atomic add per counted
// feature". These benchmarks measure where that claim is checkable: the
// per-request mark-and-flush the search path pays, the per-subrequest search
// shape counters, the one hook that parses JSON (legacy norm_score), the
// expression walk, which is the one counted class whose cost is more than a
// branch, and parseSearchInfo as a guard that the parser stays free of
// counting. Run:
//
//	go test ./internal/proxy/dql/ -run XXX -bench 'FeatureUsage' -benchtime 200000x
func benchSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "bench",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "meta", DataType: schemapb.DataType_JSON},
			{FieldID: 102, Name: "tag", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "128"}}},
			{FieldID: 103, Name: testFloatVecField, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
		},
	}
}

// BenchmarkFeatureUsageSearchFlush measures what a search request pays for the
// counters end to end: marking the features it used into the task's set, then
// flushing the set once when PreExecute returns.
//
// The set is what makes the number a count of requests: a hybrid search parses
// every sub-request and Proxy.Search can run the task again for the
// un-optimized retry and the recall-evaluation ground truth, and none of those
// may count twice. direct_hits is the alternative that cannot dedupe, for
// comparison; empty_request is a request that used no counted feature at all,
// which is the floor every search pays.
func BenchmarkFeatureUsageSearchFlush(b *testing.B) {
	was := featureusage.Enabled()
	b.Cleanup(func() { featureusage.SetEnabled(was) })
	featureusage.SetEnabled(true)

	marks := []featureusage.Feature{
		featureusage.FeatureConsistencyStrong,
		featureusage.FeatureGroupByField,
		featureusage.FeatureOutputVectorField,
		featureusage.FeatureIgnoreGrowing,
		featureusage.FeatureLike,
		featureusage.FeatureHints,
	}

	b.Run("marked_and_flushed", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var set featureusage.FeatureSet
			for _, f := range marks {
				set.Set(f)
			}
			set.HitAll()
		}
	})
	b.Run("empty_request", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var set featureusage.FeatureSet
			set.HitAll()
		}
	})
	b.Run("direct_hits", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, f := range marks {
				featureusage.Hit(f)
			}
		}
	})
}

// BenchmarkFeatureUsageSearchUnits measures the per-subrequest counters one
// plain search adds: the ef, nprobe and limit buckets read from the params
// JSON, the retrieval kind and nq bucket, then the tally flush. Five counters
// move per search, so the flush pays five atomic adds.
func BenchmarkFeatureUsageSearchUnits(b *testing.B) {
	was := featureusage.Enabled()
	b.Cleanup(func() { featureusage.SetEnabled(was) })
	featureusage.SetEnabled(true)
	schema := benchSchema()

	b.Run("one_subrequest", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			var units featureusage.Tally
			collectSearchParamUnits(`{"nprobe": 16}`, 10, &units)
			collectSubSearchUnits(schema, 103, commonpb.PlaceholderType_FloatVector, 1, &units)
			units.HitAll()
		}
	})
	b.Run("counting_off", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			collectSearchParamUnits(`{"nprobe": 16}`, 10, nil)
			collectSubSearchUnits(schema, 103, commonpb.PlaceholderType_FloatVector, 1, nil)
		}
	})
}

// BenchmarkFeatureUsageLegacyNormScore measures the one hook that parses: the
// legacy rank_params carry norm_score inside a JSON object. with_key is a
// request that asks for normalization and pays the unmarshal; without_key is
// every other legacy hybrid search, which the substring check short-circuits.
func BenchmarkFeatureUsageLegacyNormScore(b *testing.B) {
	was := featureusage.Enabled()
	b.Cleanup(func() { featureusage.SetEnabled(was) })
	featureusage.SetEnabled(true)

	for _, tc := range []struct {
		name   string
		params string
	}{
		{"without_key", `{"weights": [0.1, 0.9]}`},
		{"with_key", `{"weights": [0.1, 0.9], "norm_score": true}`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			kvs := []*commonpb.KeyValuePair{
				{Key: RankTypeKey, Value: "weighted"},
				{Key: ParamsKey, Value: tc.params},
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var set featureusage.FeatureSet
				collectLegacyRankStrategy(kvs, &set)
			}
		})
	}
}

// BenchmarkFeatureUsageParseSearchInfo measures parseSearchInfo with the
// counters on and off. The parse itself no longer counts -- the hooks moved to
// the search task so one user request counts once -- so this is now a guard
// that the parser stayed free of them, not a measure of the counters' cost.
// BenchmarkFeatureUsageSearchFlush is where that cost shows up.
func BenchmarkFeatureUsageParseSearchInfo(b *testing.B) {
	schema := benchSchema()
	params := getValidSearchParams()
	was := featureusage.Enabled()
	b.Cleanup(func() { featureusage.SetEnabled(was) })

	for _, tc := range []struct {
		name    string
		enabled bool
	}{{"counters_off", false}, {"counters_on", true}} {
		b.Run(tc.name, func(b *testing.B) {
			featureusage.SetEnabled(tc.enabled)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := parseSearchInfo(params, schema, nil, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFeatureUsageExprWalk measures the one walk over a parsed predicate
// that replaces per-node counting, on a predicate with many terms. The parse
// itself is outside the timed loop: the request already paid for it.
func BenchmarkFeatureUsageExprWalk(b *testing.B) {
	schema := benchSchema()
	helper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		b.Fatal(err)
	}
	for _, terms := range []int{1, 10, 100} {
		expr := buildWideExpr(terms)
		plan, err := planparserv2.ParseExpr(helper, expr, nil)
		if err != nil {
			b.Fatalf("%d terms: %v", terms, err)
		}
		b.Run(fmt.Sprintf("terms_%d", terms), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var set featureusage.FeatureSet
				featureusage.CollectExprFeatures(plan, &set)
			}
		})
	}
}

// buildWideExpr builds a predicate with n OR-ed terms, mixing the expression
// kinds the walk has to recognize, so the cost measured is the walk's own and
// not one branch repeated.
func buildWideExpr(n int) string {
	terms := make([]string, 0, n)
	for i := 0; i < n; i++ {
		switch i % 4 {
		case 0:
			terms = append(terms, fmt.Sprintf("id > %d", i))
		case 1:
			terms = append(terms, fmt.Sprintf("tag like \"p%d%%\"", i))
		case 2:
			terms = append(terms, fmt.Sprintf("meta[\"k%d\"] == %d", i, i))
		default:
			terms = append(terms, fmt.Sprintf("json_contains(meta[\"a%d\"], %d)", i, i))
		}
	}
	return strings.Join(terms, " or ")
}
