package loadmgr

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

func TestSchemaDeltaPreservesLoadingIntent(t *testing.T) {
	old := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}, {FieldID: 102}}}
	target := &schemapb.CollectionSchema{
		Fields:            []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}, {FieldID: 103}, {FieldID: 104, IsDynamic: true}, {FieldID: 105}},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 106, Fields: []*schemapb.FieldSchema{{FieldID: 107}, {FieldID: 108}}}},
		Functions:         []*schemapb.FunctionSchema{{InputFieldIds: []int64{100}, OutputFieldIds: []int64{105}}},
	}
	for _, tc := range []struct {
		name     string
		all      *bool
		skip     bool
		fields   []int64
		expected []int64
	}{
		{"full", proto.Bool(true), false, []int64{100, 101, 102}, []int64{100, 101, 103, 104, 105, 107, 108}},
		{"full skip dynamic", proto.Bool(true), true, []int64{100, 101, 102}, []int64{100, 101, 103, 105, 107, 108}},
		{"explicit all old fields", proto.Bool(false), false, []int64{100, 101, 102}, []int64{100, 101, 105}},
		{"partial missing input", proto.Bool(false), false, []int64{101, 102}, []int64{101}},
		{"legacy full", nil, false, []int64{100, 101, 102}, []int64{100, 101, 103, 104, 105, 107, 108}},
		{"legacy partial", nil, false, []int64{100, 102}, []int64{100, 105}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &LoadConfig{CollectionID: 1, LoadAllFields: tc.all, SkipLoadDynamicField: tc.skip}
			for _, id := range tc.fields {
				cfg.LoadFields = append(cfg.LoadFields, &messagespb.LoadFieldConfig{FieldId: id})
			}
			original := cfg.Clone()
			next := reconcileSchemaFields(cfg, old, target, []*messagespb.LoadFieldConfig{{FieldId: 105, IndexId: 900}})
			ids := make([]int64, 0, len(next.LoadFields))
			for _, field := range next.LoadFields {
				ids = append(ids, field.FieldId)
				if field.FieldId == 105 {
					require.EqualValues(t, 900, field.IndexId)
				}
			}
			require.Equal(t, tc.expected, ids)
			require.Equal(t, original, cfg, "published config is immutable")
			require.True(t, equalSchemaLoadIntent(next, reconcileSchemaFields(next, old, target, []*messagespb.LoadFieldConfig{{FieldId: 105, IndexId: 900}})), "callback replay is idempotent")
			recovered := buildFromPersisted(next.toCollectionLoadInfoProto(), nil, nil)
			require.Equal(t, next.LoadAllFields, recovered.LoadAllFields)
			require.Equal(t, next.SkipLoadDynamicField, recovered.SkipLoadDynamicField)
		})
	}
}

func TestSchemaDeltaLegacySkipDynamic(t *testing.T) {
	old := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101, IsDynamic: true}}}
	target := proto.Clone(old).(*schemapb.CollectionSchema)
	target.Fields = append(target.Fields, &schemapb.FieldSchema{FieldID: 102})
	next := reconcileSchemaFields(&LoadConfig{LoadFields: []*messagespb.LoadFieldConfig{{FieldId: 100}}}, old, target, nil)
	require.True(t, *next.LoadAllFields)
	require.True(t, next.SkipLoadDynamicField)
	require.Equal(t, []*messagespb.LoadFieldConfig{{FieldId: 100}, {FieldId: 102}}, next.LoadFields)
}
