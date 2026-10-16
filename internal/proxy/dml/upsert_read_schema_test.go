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
