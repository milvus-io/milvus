package resource

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPreparationSchemaSource(t *testing.T) {
	for _, scenario := range []string{"pending", "recovering", "published"} {
		mockey.PatchConvey(scenario, t, func() {
			var target *broadcaster.SchemaTarget
			var targetErr error
			switch scenario {
			case "pending":
				target = &broadcaster.SchemaTarget{Schema: &schemapb.CollectionSchema{Version: 2}}
			case "recovering":
				targetErr = merr.WrapErrServiceNotReadyMsg("callback recovery pending")
			}
			mockey.Mock(broadcast.GetPendingSchemaTarget).Return(target, targetErr).Build()
			reads := 0
			schema, err := PreparationSchema(context.Background(), 100, func(context.Context, int64) (*schemapb.CollectionSchema, error) {
				reads++
				return &schemapb.CollectionSchema{Version: 1}, nil
			})
			if scenario == "recovering" {
				require.ErrorIs(t, err, merr.ErrServiceNotReady)
				require.Nil(t, schema)
			} else {
				require.NoError(t, err)
				if scenario == "pending" {
					require.EqualValues(t, 2, schema.Version)
				} else {
					require.EqualValues(t, 1, schema.Version)
				}
			}
			if scenario == "published" {
				require.Equal(t, 1, reads)
			} else {
				require.Zero(t, reads, "must not infer a preparation schema from old published metadata")
			}
		})
	}
}

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
