package datacoord

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/dataview"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCompactionSchemaPublication(t *testing.T) {
	m := newEmptyTestMeta()
	m.dataViewManager = dataview.NewManager(datacoordkv.NewCatalog(NewMetaMemoryKV(), "", ""), nil)
	m.metaStore.PutCollection(&collectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{Version: 1}})
	_, err := m.dataViewManager.OnCreateCollection(t.Context(), dataview.CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	target := &schemapb.CollectionSchema{Version: 2}
	require.ErrorIs(t, m.checkCompactionSchemaPublication(t.Context(), 1, target), merr.ErrServiceNotReady)
	derived := &schemapb.CollectionSchema{Version: 2, Functions: []*schemapb.FunctionSchema{{OutputFieldIds: []int64{102}}}}
	require.NoError(t, m.checkCompactionSchemaPublication(t.Context(), 1, derived), "materialization must make progress before DDL publication")
	_, err = m.dataViewManager.AdvanceSchema(t.Context(), 1, 2, nil)
	require.NoError(t, err)
	require.NoError(t, m.checkCompactionSchemaPublication(t.Context(), 1, target))
	require.NoError(t, m.checkCompactionSchemaPublication(t.Context(), 1, &schemapb.CollectionSchema{Version: 1}), "old jobs retain their captured schema")
}

func TestPrepareDataViewSchema(t *testing.T) {
	m := newEmptyTestMeta()
	require.NoError(t, m.checkCompactionSchemaPublication(t.Context(), 1, &schemapb.CollectionSchema{Version: 1}))
	m.dataViewManager = dataview.NewManager(datacoordkv.NewCatalog(NewMetaMemoryKV(), "", ""), nil)
	require.ErrorIs(t, m.checkCompactionSchemaPublication(t.Context(), 1, &schemapb.CollectionSchema{Version: 1}), merr.ErrServiceNotReady)
	_, err := m.dataViewManager.OnCreateCollection(t.Context(), dataview.CreateCollectionDataViewEvent{CollectionID: 1, SchemaVersion: 1, VChannels: []string{"v1"}})
	require.NoError(t, err)
	s := &Server{meta: m, dataViewManager: m.dataViewManager}
	s.UpdateStateCode(commonpb.StateCode_Initializing)
	published, target := &schemapb.CollectionSchema{Version: 1}, &schemapb.CollectionSchema{Version: 2}
	require.Error(t, s.PrepareDataViewSchema(t.Context(), 1, published, target))
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	require.NoError(t, s.PrepareDataViewSchema(t.Context(), 1, published, target))
	derived := &schemapb.CollectionSchema{Version: 3, Functions: []*schemapb.FunctionSchema{{OutputFieldIds: []int64{102}}}}
	require.NoError(t, s.PrepareDataViewSchema(t.Context(), 1, target, derived), "empty collection needs no physical materialization")
	ref, err := m.dataViewManager.Latest(t.Context(), 1)
	require.NoError(t, err)
	defer ref.Deref()
	require.EqualValues(t, 3, ref.DataView().GetSchemaVersion())
}
