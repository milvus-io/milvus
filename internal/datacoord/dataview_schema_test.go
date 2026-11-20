package datacoord

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	datacoordkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

func TestMaterializedSchemaRejectsIncompleteStreamingProjection(t *testing.T) {
	s, req := newStreamingFlushTest(t)
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	status, err := s.SaveBinlogPaths(t.Context(), req)
	require.NoError(t, err)
	require.NotNil(t, status)
	published := s.meta.GetCollection(1).Schema
	target := &schemapb.CollectionSchema{Version: 1, Functions: []*schemapb.FunctionSchema{{OutputFieldIds: []int64{102}}}}
	require.ErrorIs(t, s.PrepareDataViewSchema(t.Context(), 1, published, target), merr.ErrServiceNotReady,
		"an empty sealed projection must not hide durable pre-ALTER growing rows")
	ref, err := s.dataViewManager.Latest(t.Context(), 1)
	require.NoError(t, err)
	require.Zero(t, ref.DataView().GetSchemaVersion())
	ref.Deref()
	// A logical NULL/default change needs no historical computation.
	require.NoError(t, s.PrepareDataViewSchema(t.Context(), 1, published, &schemapb.CollectionSchema{Version: 1}))
	require.Equal(t, model.SegmentChangeStateStaged, s.meta.GetSegmentChangeGroup(t.Context(), 1, 1).State)
	// An explicit owning Drop removes the obligation without publishing rows
	// into the new schema or requiring the missing indexes to finish.
	drop := &datapb.SaveBinlogPathsRequest{
		Base: req.Base, SegmentID: req.SegmentID, CollectionID: req.CollectionID,
		PartitionID: req.PartitionID, Channel: req.Channel, Flushed: true, Dropped: true,
	}
	drop.Base.Properties[dataview.FlushGroupRetire] = "1"
	status, err = s.SaveBinlogPaths(t.Context(), drop)
	require.NoError(t, err)
	require.NotNil(t, status)
	require.False(t, s.meta.HasPendingStreamingSchema(1, 2))
	target.Version = 2
	require.NoError(t, s.PrepareDataViewSchema(t.Context(), 1, published, target), "durable retirement removes the historical materialization obligation")
}
