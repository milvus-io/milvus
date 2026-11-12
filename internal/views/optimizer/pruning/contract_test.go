package pruning

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Exercise the real expression parser and selection entry point together. A
// predicate involving an unsupported field must never discard possible matches.
func TestScalarPruningPredicateContract(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64, IsClusteringKey: true},
		{FieldID: 102, Name: "other", DataType: schemapb.DataType_Int64},
	}}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	stats := map[int64]*storage.PartitionStatsSnapshot{2: {SegmentStats: map[int64]storage.SegmentStats{}}}
	for id, lo := range map[int64]int64{10: 0, 11: 10, 12: 20} {
		stats[2].SegmentStats[id] = storage.SegmentStats{FieldStats: []storage.FieldStats{{FieldID: 101, Type: schemapb.DataType_Int64, Min: storage.NewInt64FieldValue(lo), Max: storage.NewInt64FieldValue(lo + 5)}}}
	}
	candidates := map[int64][]int64{2: {10, 11, 12, 99}, 3: {30}}
	for _, tc := range []struct {
		expr     string
		excluded []int64
	}{
		{"age == 12", []int64{10, 12}},
		{"age < 10", []int64{11, 12}},
		{"age <= 10", []int64{12}},
		{"age > 15", []int64{10, 11}},
		{"age >= 15", []int64{10}},
		{"10 <= age <= 15", []int64{10, 12}},
		{"age in [2,12]", []int64{12}},
		{"age != 12", nil},
		{"not (age in [2,12])", nil},
		{"other == 12", nil},
		{"other in [2,12]", nil},
		{"10 <= other <= 15", nil},
		{"age == 12 and other == 1", []int64{10, 12}},
		{"age == 12 or other == 1", nil},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			plan, err := planparserv2.CreateRetrievePlan(helper, tc.expr, nil)
			require.NoError(t, err)
			data, err := proto.Marshal(plan)
			require.NoError(t, err)
			for _, search := range []bool{false, true} {
				var sr *internalpb.SearchRequest
				var qr *internalpb.RetrieveRequest
				if search {
					sr = &internalpb.SearchRequest{SerializedExprPlan: data}
				} else {
					qr = &internalpb.RetrieveRequest{SerializedExprPlan: data}
				}
				excluded, _ := Exclude(t.Context(), schema, stats, candidates, sr, qr, 0.5)
				want := make(map[int64]struct{}, len(tc.excluded))
				for _, id := range tc.excluded {
					want[id] = struct{}{}
				}
				require.Equal(t, want, excluded)
			}
		})
	}
	for _, data := range [][]byte{nil, {0xff}} {
		excluded, flags := Exclude(t.Context(), schema, stats, candidates, nil, &internalpb.RetrieveRequest{SerializedExprPlan: data}, 0.5)
		require.Empty(t, excluded)
		require.Equal(t, NoPruningFlags, flags)
	}
}

func TestVectorPruningConservativeInputs(t *testing.T) {
	paramtable.Init()
	cfg := paramtable.Get()
	key := cfg.CommonCfg.EnableVectorClusteringKey.Key
	old := cfg.CommonCfg.EnableVectorClusteringKey.GetValue()
	require.NoError(t, cfg.Save(key, "true"))
	t.Cleanup(func() { require.NoError(t, cfg.Save(key, old)) })
	field := &schemapb.FieldSchema{FieldID: 101, IsClusteringKey: true, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	holder := &commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 8)}}}}
	data, err := proto.Marshal(holder)
	require.NoError(t, err)
	req := &internalpb.SearchRequest{FieldId: 101, MetricType: "L2", Topk: 1, PlaceholderGroup: data}
	stats := map[int64]*storage.PartitionStatsSnapshot{2: {SegmentStats: make(map[int64]storage.SegmentStats)}}
	for id, value := range map[int64]float32{10: 0, 11: 10} {
		stats[2].SegmentStats[id] = storage.SegmentStats{
			NumRows: 10,
			FieldStats: []storage.FieldStats{{
				FieldID: 101, Type: schemapb.DataType_FloatVector,
				Centroids: []storage.VectorFieldValue{&storage.FloatVectorFieldValue{Value: []float32{value, value}}},
			}},
		}
	}
	for _, tc := range []struct {
		name   string
		change func(*schemapb.FieldSchema, *internalpb.SearchRequest)
	}{
		{"unsupported metric", func(_ *schemapb.FieldSchema, r *internalpb.SearchRequest) { r.MetricType = "HAMMING" }},
		{"malformed placeholder", func(_ *schemapb.FieldSchema, r *internalpb.SearchRequest) { r.PlaceholderGroup = []byte{0xff} }},
		{"empty placeholder", func(_ *schemapb.FieldSchema, r *internalpb.SearchRequest) { r.PlaceholderGroup = nil }},
		{"missing dimension", func(f *schemapb.FieldSchema, _ *internalpb.SearchRequest) { f.TypeParams = nil }},
		{"invalid dimension", func(f *schemapb.FieldSchema, _ *internalpb.SearchRequest) { f.TypeParams[0].Value = "invalid" }},
		{"zero dimension", func(f *schemapb.FieldSchema, _ *internalpb.SearchRequest) { f.TypeParams[0].Value = "0" }},
		{"wrong vector size", func(f *schemapb.FieldSchema, _ *internalpb.SearchRequest) { f.TypeParams[0].Value = "3" }},
		{"wrong vector type", func(f *schemapb.FieldSchema, _ *internalpb.SearchRequest) {
			f.DataType = schemapb.DataType_BinaryVector
		}},
		{"no values", func(_ *schemapb.FieldSchema, r *internalpb.SearchRequest) {
			h := proto.Clone(holder).(*commonpb.PlaceholderGroup)
			h.Placeholders[0].Values = nil
			r.PlaceholderGroup, err = proto.Marshal(h)
			require.NoError(t, err)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := proto.Clone(schema).(*schemapb.CollectionSchema)
			r := proto.Clone(req).(*internalpb.SearchRequest)
			tc.change(s.Fields[0], r)
			excluded, flags := Exclude(t.Context(), s, stats, map[int64][]int64{2: {10, 11}}, r, nil, 0.5)
			require.Empty(t, excluded)
			require.Equal(t, NoPruningFlags, flags)
		})
	}
	// An oversized candidate budget must retain all segments for every metric.
	for _, metric := range []string{"IP", "COSINE", "L2"} {
		req.MetricType = metric
		excluded, _ := Exclude(t.Context(), schema, stats, map[int64][]int64{2: {10, 11}}, req, nil, 10)
		require.Empty(t, excluded, "a budget exceeding the partition size keeps every candidate")
	}
}
