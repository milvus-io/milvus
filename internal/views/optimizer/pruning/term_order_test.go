package pruning

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// Internal PK reads build plans without the expression rewriter. Pruning must
// not depend on the order of the caller's IDs or change that order itself.
func TestRequeryPruningPreservesUnsortedMatches(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		field                  *schemapb.FieldSchema
		ids                    *schemapb.IDs
		min, max               storage.ScalarFieldValue
		outsideMin, outsideMax storage.ScalarFieldValue
	}{
		{
			"int64", &schemapb.FieldSchema{FieldID: 100, IsPrimaryKey: true, IsClusteringKey: true, DataType: schemapb.DataType_Int64},
			&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{100, 5, 5, -10}}}}, storage.NewInt64FieldValue(1), storage.NewInt64FieldValue(10), storage.NewInt64FieldValue(20), storage.NewInt64FieldValue(30),
		},
		{
			"varchar", &schemapb.FieldSchema{FieldID: 100, IsPrimaryKey: true, IsClusteringKey: true, DataType: schemapb.DataType_VarChar},
			&schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"z", "c", "c", "a"}}}}, storage.NewVarCharFieldValue("b"), storage.NewVarCharFieldValue("d"), storage.NewVarCharFieldValue("e"), storage.NewVarCharFieldValue("f"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{tc.field}}
			plan := planparserv2.CreateRequeryPlan(tc.field, tc.ids)
			original := proto.Clone(plan)
			data, err := proto.Marshal(plan)
			require.NoError(t, err)
			stats := map[int64]*storage.PartitionStatsSnapshot{1: {SegmentStats: map[int64]storage.SegmentStats{
				10: {FieldStats: []storage.FieldStats{{FieldID: 100, Type: tc.field.GetDataType(), Min: tc.min, Max: tc.max}}},
				11: {FieldStats: []storage.FieldStats{{FieldID: 100, Type: tc.field.GetDataType(), Min: tc.outsideMin, Max: tc.outsideMax}}},
			}}}
			excluded, _ := Exclude(t.Context(), schema, stats, map[int64][]int64{1: {10, 11, 12}}, nil, &internalpb.RetrieveRequest{SerializedExprPlan: data}, 0.5)
			require.Equal(t, map[int64]struct{}{11: {}}, excluded, "matching and uncovered segments remain; disjoint segment is still pruned")
			// The parser also accepts a caller-owned TermExpr directly. Sorting its
			// converted values must not reorder the protobuf's values.
			_, err = ParseExpr(plan.GetQuery().GetPredicates(), NewParseContext(100, tc.field.GetDataType()))
			require.NoError(t, err)
			require.True(t, proto.Equal(original, plan))
		})
	}
}

func TestScalarPruningSelectsCompatibleFieldStatistics(t *testing.T) {
	field := &schemapb.FieldSchema{FieldID: 100, IsPrimaryKey: true, IsClusteringKey: true, DataType: schemapb.DataType_Int64}
	plan := planparserv2.CreateRequeryPlan(field, &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{5}}}})
	data, err := proto.Marshal(plan)
	require.NoError(t, err)
	valid := storage.FieldStats{FieldID: 100, Type: schemapb.DataType_Int64, Min: storage.NewInt64FieldValue(20), Max: storage.NewInt64FieldValue(30)}
	for _, tc := range []struct {
		name   string
		fields []storage.FieldStats
		prune  bool
	}{
		{"matching field second", []storage.FieldStats{{FieldID: 101, Type: schemapb.DataType_Int64, Min: storage.NewInt64FieldValue(1), Max: storage.NewInt64FieldValue(10)}, valid}, true},
		{"wrong type", []storage.FieldStats{{FieldID: 100, Type: schemapb.DataType_VarChar, Min: storage.NewVarCharFieldValue("a"), Max: storage.NewVarCharFieldValue("z")}}, false},
		{"missing minimum", []storage.FieldStats{{FieldID: 100, Type: schemapb.DataType_Int64, Max: valid.Max}}, false},
		{"missing maximum", []storage.FieldStats{{FieldID: 100, Type: schemapb.DataType_Int64, Min: valid.Min}}, false},
		{"no matching field", []storage.FieldStats{{FieldID: 101, Type: schemapb.DataType_Int64, Min: valid.Min, Max: valid.Max}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stats := map[int64]*storage.PartitionStatsSnapshot{1: {SegmentStats: map[int64]storage.SegmentStats{10: {FieldStats: tc.fields}}}}
			excluded, _ := Exclude(t.Context(), &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}, stats, map[int64][]int64{1: {10}}, nil, &internalpb.RetrieveRequest{SerializedExprPlan: data}, 0.5)
			_, pruned := excluded[10]
			require.Equal(t, tc.prune, pruned)
		})
	}
}
