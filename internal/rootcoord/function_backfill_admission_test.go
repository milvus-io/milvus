package rootcoord

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestFunctionBackfillAdmissionPrecedesBroadcast(t *testing.T) {
	items := enableFunctionBackfillForTest(t)
	collection := func() *model.Collection {
		return &model.Collection{
			CollectionID: 1, Name: "test",
			Fields: []*model.Field{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{
					FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
					TypeParams: funcutil.Map2KeyValuePair(map[string]string{common.MaxLengthKey: "1024", common.EnableAnalyzerKey: "true"}),
				},
			},
		}
	}
	// A nil broadcaster and uninitialized Core make any allocation/broadcast
	// before rejection fail this test. Direct Coord callers bypass Proxy guards.
	core := &Core{}
	for _, item := range items {
		t.Run(item.Key, func(t *testing.T) {
			require.NoError(t, Params.Save(item.Key, "false"))
			t.Cleanup(func() { Params.Save(item.Key, "true") })
			err := core.broadcastAlterCollectionSchemaAdd(t.Context(), nil, collection(), buildAlterSchemaReq("default", "test", "text", "sparse", "bm25"))
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.ErrorContains(t, err, item.Key)
		})
	}
	t.Run("external data", func(t *testing.T) {
		coll := collection()
		coll.ExternalSource = "s3://bucket/data"
		coll.Fields[0].ExternalField = "id"
		coll.Fields[1].ExternalField = "text"
		err := core.broadcastAlterCollectionSchemaAdd(t.Context(), nil, coll, buildAlterSchemaReq("default", "test", "text", "sparse", "bm25"))
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorContains(t, err, "external collection")
	})
}

func enableFunctionBackfillForTest(t *testing.T) []*paramtable.ParamItem {
	t.Helper()
	paramtable.Init()
	items := []*paramtable.ParamItem{
		&Params.CommonCfg.UseLoonFFI,
		&Params.DataCoordCfg.BumpSchemaVersionCompactionEnabled,
		&Params.DataCoordCfg.StorageVersionCompactionEnabled,
	}
	for _, item := range items {
		previous := item.GetValue()
		require.NoError(t, Params.Save(item.Key, "true"))
		t.Cleanup(func() { Params.Save(item.Key, previous) })
	}
	return items
}
