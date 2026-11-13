package loadmgr

import (
	"context"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSchemaDeltaPreservesLoadingIntent(t *testing.T) {
	old := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}, {FieldID: 102}, {FieldID: 104, IsDynamic: true}}}
	target := &schemapb.CollectionSchema{
		Fields:            []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}, {FieldID: 103}, {FieldID: 104, IsDynamic: true}, {FieldID: 105}},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 106, Fields: []*schemapb.FieldSchema{{FieldID: 107}, {FieldID: 108, TypeParams: []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "true"}}}}}},
		Functions:         []*schemapb.FunctionSchema{{InputFieldIds: []int64{100}, OutputFieldIds: []int64{105}}},
	}
	for _, tc := range []struct {
		name        string
		hints, want []int64
	}{
		{"empty retains old fields on demand", nil, []int64{103, 105, 107}},
		{"explicit old schema", []int64{100, 101, 102, 104}, []int64{100, 101, 103, 104, 105, 107}},
		{"unhinted function input", []int64{101, 102}, []int64{101, 103, 105, 107}},
		{"dynamic field not hinted", []int64{100, 102}, []int64{100, 103, 105, 107}},
		{"dynamic field hinted", []int64{100, 104}, []int64{100, 103, 104, 105, 107}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &LoadConfig{CollectionID: 1, RequiredFields: RequiredFieldsForSchema(old)}
			for _, id := range tc.hints {
				cfg.LoadFields = append(cfg.LoadFields, &viewpb.LoadFieldConfig{FieldId: id})
			}
			original := cfg.Clone()
			next := reconcileSchemaFields(cfg, old, target, []*viewpb.LoadFieldConfig{{FieldId: 105, IndexIds: []int64{900}}})
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
			wire, err := proto.Marshal(next.toCollectionLoadConfigProto())
			require.NoError(t, err)
			var persisted viewpb.CollectionLoadConfig
			require.NoError(t, proto.Unmarshal(wire, &persisted))
			recovered := buildFromPersisted(&persisted)
			requireFieldConfigsEqual(t, next.RequiredFields, recovered.RequiredFields)
			requireFieldConfigsEqual(t, next.LoadFields, recovered.LoadFields)
		})
	}
}

func TestSchemaDeltaAddsDefaultPreloads(t *testing.T) {
	old := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101, IsDynamic: true}}}
	target := proto.Clone(old).(*schemapb.CollectionSchema)
	target.Fields = append(target.Fields, &schemapb.FieldSchema{FieldID: 102})
	next := reconcileSchemaFields(&LoadConfig{RequiredFields: RequiredFieldsForSchema(old), LoadFields: []*viewpb.LoadFieldConfig{{FieldId: 100}}}, old, target, nil)
	requireFieldConfigsEqual(t, RequiredFieldsForSchema(target), next.RequiredFields)
	requireFieldConfigsEqual(t, []*viewpb.LoadFieldConfig{{FieldId: 100}, {FieldId: 102}}, next.LoadFields)
}

func requireFieldConfigsEqual(t *testing.T, expected, actual []*viewpb.LoadFieldConfig) {
	t.Helper()
	require.True(t, slices.EqualFunc(expected, actual, func(a, b *viewpb.LoadFieldConfig) bool { return proto.Equal(a, b) }), "expected %v, actual %v", expected, actual)
}

func TestApplySchemaPublishesOnlyCommittedRequirements(t *testing.T) {
	store, hooks := newTestStore(t)
	old := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}}}
	target := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}}}
	cfg := &LoadConfig{CollectionID: 1, RequiredFields: RequiredFieldsForSchema(old)}
	require.NoError(t, store.Put(t.Context(), cfg))
	first := store.GetConfigVersion(1)
	hooks.beforeSave = func(*viewpb.CollectionLoadConfig) error { return context.Canceled }
	require.ErrorIs(t, store.ApplySchema(t.Context(), 1, old, target, nil), context.Canceled)
	require.Equal(t, first, store.GetConfigVersion(1))
	hooks.beforeSave = nil
	require.NoError(t, store.ApplySchema(t.Context(), 1, old, target, nil))
	second := store.GetConfigVersion(1)
	require.Greater(t, second, first)
	require.NoError(t, store.ApplySchema(t.Context(), 1, old, target, nil))
	require.Equal(t, second, store.GetConfigVersion(1))
	previous, err := store.GetLoadInfo(t.Context(), 1, first)
	require.NoError(t, err)
	requireFieldConfigsEqual(t, RequiredFieldsForSchema(old), previous.RequiredFields)
	store.StopPlanning(1)
	require.NoError(t, store.ApplySchema(t.Context(), 1, old, target, nil))
	require.False(t, store.Contains(1))
}

func TestLoadScopesMaterializeSchemaDefaults(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0},
			{FieldID: 1},
			{FieldID: 100},
			{FieldID: 101, TypeParams: []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "true"}}},
			{FieldID: 102, IsDynamic: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 103, Fields: []*schemapb.FieldSchema{{FieldID: 104}}}},
	}
	require.Equal(t, []int64{100, 102, 104}, PreloadFieldIDsForSchema(schema, nil))
	require.Equal(t, []int64{101}, PreloadFieldIDsForSchema(schema, []int64{101}), "explicit selection overrides default skip_load")
	require.Equal(t, []int64{100, 104}, PreloadFieldIDsForSchema(schema, []int64{104, 100, 100}), "construct a subset in schema order without duplicates")
	requireFieldConfigsEqual(t, []*viewpb.LoadFieldConfig{{FieldId: 100}, {FieldId: 101}, {FieldId: 102}, {FieldId: 104}}, RequiredFieldsForSchema(schema))
}

func TestLoadScopesEmptySurvivesPublicationAndRecovery(t *testing.T) {
	deps := versionedTestDependencies(t)
	store, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	require.NoError(t, store.Put(t.Context(), &LoadConfig{CollectionID: 1}))
	version := store.GetConfigVersion(1)
	recovered, err := RecoverLoadConfigStore(t.Context(), deps)
	require.NoError(t, err)
	info, err := recovered.GetLoadInfo(t.Context(), 1, version)
	require.NoError(t, err)
	require.Empty(t, info.RequiredFields)
	require.Empty(t, info.LoadFields)
}
