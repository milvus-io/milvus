package datacoord

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
)

func TestPrepareDataViewSchema(t *testing.T) {
	m := meta.NewEmptyTestMeta()
	m.DataViewManager = dataview.NewManager(datacoordkv.NewCatalog(meta.NewMetaMemoryKV(), "", ""), nil)
	_, err := m.DataViewManager.OnCreateCollection(t.Context(), dataview.CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	s := &Server{meta: m, dataViewManager: m.DataViewManager}
	s.UpdateStateCode(commonpb.StateCode_Initializing)
	published, target := &schemapb.CollectionSchema{Version: 1}, &schemapb.CollectionSchema{Version: 2}
	require.Error(t, s.PrepareDataViewSchema(t.Context(), 1, published, target))
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	require.NoError(t, s.PrepareDataViewSchema(t.Context(), 1, published, target))
	derived := &schemapb.CollectionSchema{Version: 3, Functions: []*schemapb.FunctionSchema{{OutputFieldIds: []int64{102}}}}
	require.NoError(t, s.PrepareDataViewSchema(t.Context(), 1, target, derived), "empty collection needs no physical materialization")
	ref, err := m.DataViewManager.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer ref.Deref()
	require.EqualValues(t, 3, ref.DataView().GetSchemaVersion())
}
