package loadmgr

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestSchemaDeltaPreservesLoadingIntent(t *testing.T) {
	old := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}, {FieldID: 102}, {FieldID: 104, IsDynamic: true}}}
	target := &schemapb.CollectionSchema{
		Fields:            []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}, {FieldID: 103}, {FieldID: 104, IsDynamic: true}, {FieldID: 105}},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 106, Fields: []*schemapb.FieldSchema{{FieldID: 107}, {FieldID: 108}}}},
		Functions:         []*schemapb.FunctionSchema{{InputFieldIds: []int64{100}, OutputFieldIds: []int64{105}}},
	}
	for _, tc := range []struct {
		name        string
		hints, want []int64
	}{
		{"empty follows schema", nil, nil},
		{"explicit old schema", []int64{100, 101, 102, 104}, []int64{100, 101, 104}},
		{"unhinted function input", []int64{101, 102}, []int64{101}},
		{"dynamic field not hinted", []int64{100, 102}, []int64{100}},
		{"dynamic field hinted", []int64{100, 104}, []int64{100, 104}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &LoadConfig{CollectionID: 1, RequiredFields: RequiredFieldsForSchema(old)}
			for _, id := range tc.hints {
				cfg.LoadFields = append(cfg.LoadFields, &messagespb.LoadFieldConfig{FieldId: id})
			}
			original := cfg.Clone()
			next := reconcileSchemaFields(cfg, old, target, []*messagespb.LoadFieldConfig{{FieldId: 105, IndexId: 900}})
			var hints []int64
			for _, f := range next.LoadFields {
				hints = append(hints, f.FieldId)
			}
			require.Equal(t, tc.want, hints)
			var required []int64
			for _, f := range next.RequiredFields {
				required = append(required, f.GetFieldId())
			}
			require.Equal(t, []int64{100, 101, 103, 104, 105, 107, 108}, required)
			require.Equal(t, original, cfg, "old views retain immutable requirements")
			require.True(t, equalSchemaLoadIntent(next, reconcileSchemaFields(next, old, target, nil)), "replay is idempotent")
			wire, err := proto.Marshal(next.toCollectionLoadInfoProto())
			require.NoError(t, err)
			var persisted querypb.CollectionLoadInfo
			require.NoError(t, proto.Unmarshal(wire, &persisted))
			recovered := buildFromPersisted(&persisted, nil, nil)
			requireFieldConfigsEqual(t, next.RequiredFields, recovered.RequiredFields)
			requireFieldConfigsEqual(t, next.LoadFields, recovered.LoadFields)
		})
	}
}

func TestSchemaDeltaNormalizesLegacyRequiredFields(t *testing.T) {
	old := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101, IsDynamic: true}}}
	target := proto.Clone(old).(*schemapb.CollectionSchema)
	target.Fields = append(target.Fields, &schemapb.FieldSchema{FieldID: 102})
	next := reconcileSchemaFields(&LoadConfig{LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}}}, old, target, nil)
	require.Equal(t, RequiredFieldsForSchema(target), next.RequiredFields)
	require.Equal(t, []*messagespb.LoadFieldConfig{{FieldId: 100}}, next.LoadFields)
}

func requireFieldConfigsEqual(t *testing.T, expected, actual []*messagespb.LoadFieldConfig) {
	t.Helper()
	require.True(t, slices.EqualFunc(expected, actual, func(a, b *messagespb.LoadFieldConfig) bool { return proto.Equal(a, b) }), "expected %v, actual %v", expected, actual)
}
