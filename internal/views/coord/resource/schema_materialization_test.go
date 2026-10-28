package resource

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestSchemaRequiresMaterialization(t *testing.T) {
	old := &schemapb.CollectionSchema{Version: 1, Functions: []*schemapb.FunctionSchema{{OutputFieldIds: []int64{102}}}}
	for _, tc := range []struct {
		name   string
		target *schemapb.CollectionSchema
		want   bool
	}{
		{"drop function", &schemapb.CollectionSchema{Version: 2}, false},
		{"ordinary nullable column", &schemapb.CollectionSchema{Version: 2, Fields: []*schemapb.FieldSchema{{FieldID: 103, Nullable: true}}, Functions: old.Functions}, false},
		{"function parameter change", &schemapb.CollectionSchema{Version: 2, Functions: []*schemapb.FunctionSchema{{Name: "changed", OutputFieldIds: []int64{102}}}}, false},
		{"new function output", &schemapb.CollectionSchema{Version: 2, Functions: []*schemapb.FunctionSchema{{OutputFieldIds: []int64{103}}}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, SchemaRequiresMaterialization(old, tc.target)) })
	}
}
