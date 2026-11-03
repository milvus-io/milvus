package dml

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestUpsertReadSchemaDuringPublication(t *testing.T) {
	base := &schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
	}}
	for _, test := range []struct {
		name    string
		partial bool
		change  func(*schemapb.CollectionSchema)
		blocked bool
	}{
		{"new function output", true, func(s *schemapb.CollectionSchema) {
			s.Fields = append(s.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "sparse", IsFunctionOutput: true, DataType: schemapb.DataType_SparseFloatVector})
		}, false},
		{"new inherited column", true, func(s *schemapb.CollectionSchema) {
			s.Fields = append(s.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "new", Nullable: true, DataType: schemapb.DataType_Int64})
		}, true},
		{"same name different identity", true, func(s *schemapb.CollectionSchema) { s.Fields[1].FieldID = 102 }, true},
		{"dynamic enabled", true, func(s *schemapb.CollectionSchema) {
			s.EnableDynamicField = true
			s.Fields = append(s.Fields, &schemapb.FieldSchema{FieldID: 104, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true})
		}, true},
		{"struct array added", true, func(s *schemapb.CollectionSchema) {
			s.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 105, Name: "items", Fields: []*schemapb.FieldSchema{{FieldID: 106, Name: "value", DataType: schemapb.DataType_Int64}}}}
		}, true},
		{"removed column", true, func(s *schemapb.CollectionSchema) { s.Fields = s.Fields[:1] }, true},
		{"full lookup independent of new column", false, func(s *schemapb.CollectionSchema) {
			s.Fields = append(s.Fields, &schemapb.FieldSchema{FieldID: 102, Name: "new", Nullable: true, DataType: schemapb.DataType_Int64})
		}, false},
		{"full lookup changed identity", false, func(s *schemapb.CollectionSchema) { s.Fields[0].FieldID = 103 }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			task := createTestUpdateTask()
			write := proto.Clone(base).(*schemapb.CollectionSchema)
			write.Version = 2
			test.change(write)
			task.schema = mustNewSchemaInfo(write)
			task.req.PartialUpdate = test.partial
			published := mustNewSchemaInfo(base)
			lookup := mockey.Mock((*metacache.MetaCache).GetCollectionInfo).Return(&metacache.CollectionInfo{CollID: task.collectionID, Schema: published}, nil).Build()
			defer lookup.UnPatch()
			read, err := task.upsertReadSchema(context.Background(), []string{"id"})
			if test.blocked {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Nil(t, read)
			} else {
				require.NoError(t, err)
				require.Same(t, published, read)
			}
			require.EqualValues(t, 2, task.schema.GetVersion(), "write encoding must stay on the admitted schema")
		})
	}
}

func TestUpsertReadSchemaResolution(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version int32
		wrongID bool
		output  []string
		err     error
		want    error
	}{
		{name: "same version", version: 1},
		{name: "lookup error", err: merr.ErrServiceUnavailable, want: merr.ErrServiceUnavailable},
		{name: "collection replaced", wrongID: true, want: merr.ErrCollectionNotFound},
		{name: "missing lookup dependency", output: []string{"missing"}, want: merr.ErrParameterInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := createTestUpdateTask()
			task.req.PartialUpdate = false
			task.schema = mustNewSchemaInfo(&schemapb.CollectionSchema{Name: "test", Version: 1, Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			}})
			read := proto.Clone(task.schema.CollectionSchema).(*schemapb.CollectionSchema)
			read.Version = tc.version
			info := &metacache.CollectionInfo{CollID: task.collectionID, Schema: mustNewSchemaInfo(read)}
			if tc.wrongID {
				info.CollID++
			}
			patch := mockey.Mock((*metacache.MetaCache).GetCollectionInfo).Return(info, tc.err).Build()
			defer patch.UnPatch()
			got, err := task.upsertReadSchema(t.Context(), tc.output)
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Nil(t, got)
			} else {
				require.NoError(t, err)
				require.Same(t, info.Schema, got)
			}
		})
	}
}
