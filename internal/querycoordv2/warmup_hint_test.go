package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr/loadmgrtest"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func enableWarmupHintGate(t *testing.T, enabled string) {
	paramtable.Init()
	key := paramtable.Get().QueryCoordCfg.EnableLoadCollectionWarmupHint.Key
	require.NoError(t, paramtable.Get().Save(key, enabled))
	t.Cleanup(func() { _ = paramtable.Get().Reset(key) })
}

func TestLoadCollectionWarmupHintDoesNotAllocateIdentity(t *testing.T) {
	enableWarmupHintGate(t, "true")
	server := &Server{idAllocator: func() (int64, error) { t.Fatal("warmup must not allocate an identity"); return 0, nil }}
	coll := &metacache.CollectionInfo{Schema: &schemapb.CollectionSchema{}}
	for range 3 {
		require.NoError(t, server.validateLoadCollectionWarmupHint(&querypb.LoadCollectionRequest{WarmupHint: true}, coll))
		require.NoError(t, server.validateLoadCollectionWarmupHint(&querypb.LoadCollectionRequest{}, coll))
	}
	external := &metacache.CollectionInfo{Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "f", ExternalField: "col"}}}}
	require.ErrorIs(t, server.validateLoadCollectionWarmupHint(&querypb.LoadCollectionRequest{WarmupHint: true}, external), merr.ErrParameterInvalid)
}

func TestWarmupHintAdmissionRequiresGate(t *testing.T) {
	enableWarmupHintGate(t, "false")
	server := &Server{}
	coll := &metacache.CollectionInfo{Schema: &schemapb.CollectionSchema{}}
	require.ErrorIs(t, server.validateLoadCollectionWarmupHint(&querypb.LoadCollectionRequest{WarmupHint: true}, coll), merr.ErrServiceUnimplemented)
	require.NoError(t, server.validateLoadCollectionWarmupHint(&querypb.LoadCollectionRequest{}, coll))
}

func TestWarmupHintWarmsLoadedCollectionInPlace(t *testing.T) {
	enableWarmupHintGate(t, "true")
	initStreamingSystem()
	ctx := context.Background()
	collectionID := int64(100)
	store, err := loadmgr.RecoverLoadConfigStore(ctx, loadmgrtest.Dependencies(t))
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, &loadmgr.LoadConfig{
		DbID:                     1,
		CollectionID:             collectionID,
		PartitionIDs:             []int64{10, 20},
		LoadFields:               []*viewpb.LoadFieldConfig{{FieldId: 101}},
		UserSpecifiedReplicaMode: true,
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 1001, ResourceGroup: "rg1"},
			{ReplicaID: 1002, ResourceGroup: "rg2"},
		},
	}))
	server := &Server{
		meta:          newViewMeta(&metacache.CollectionInfo{ID: collectionID, Schema: &schemapb.CollectionSchema{}}),
		qviewsRuntime: &qviewsRuntime{loadConfigStore: store, readyChanges: readiness.NewNotifications(), warmClosed: true},
		idAllocator:   func() (int64, error) { return 200, nil },
	}
	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Close().Return().Once()
	bc := mock_broadcaster.NewMockBroadcaster(t)
	bc.EXPECT().WithResourceKeys(mock.Anything, mock.Anything, mock.Anything).Return(bapi, nil).Once()
	bc.EXPECT().Close().Return().Maybe()
	broadcast.ResetBroadcaster()
	broadcast.Register(bc)
	t.Cleanup(broadcast.ResetBroadcaster)

	// The proxy lists the fields to load even when the request names none. The
	// existing partition and field configuration must not be restated.
	require.NoError(t, server.broadcastAlterLoadConfigCollectionV2ForLoadCollection(ctx, &querypb.LoadCollectionRequest{
		CollectionID:  collectionID,
		LoadFields:    []int64{100},
		LoadAllFields: true,
		WarmupHint:    true,
	}))
	bapi.AssertNotCalled(t, "Broadcast", mock.Anything, mock.Anything)
	require.Equal(t, []int64{10, 20}, store.GetConfig(collectionID).PartitionIDs)
	require.Len(t, store.GetConfig(collectionID).Replicas, 2)

	for _, req := range []*querypb.LoadCollectionRequest{
		{LoadAllFields: true},
		{LoadAllFields: true, WarmupHint: true, ReplicaNumber: 2},
		{LoadAllFields: true, WarmupHint: true, ResourceGroups: []string{"rg1"}},
		{LoadAllFields: false, WarmupHint: true, LoadFields: []int64{100}},
		{LoadAllFields: true, WarmupHint: true, SkipLoadDynamicField: true},
	} {
		require.False(t, req.RequestsInPlaceWarmup(), "a load that names load options restates the configuration")
	}
}

func TestWarmupHintFirstLoadChecksDefaultFieldIndexes(t *testing.T) {
	enableWarmupHintGate(t, "true")
	server, _, _ := newLoadConfigQViewsServer(t)
	server.qviewsRuntime.warmClosed = true
	t.Cleanup(server.qviewsRuntime.readyChanges.Close)
	coll := testDescribeCollection(100, []string{"v0"})
	coll.Schema.Fields = []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector},
	}
	putTestCollection(server, coll, 10)
	mix := mocks.NewMixCoord(t)
	server.mixCoord = mix
	indexReq := &indexpb.DescribeIndexRequest{CollectionID: coll.GetCollectionID()}
	indexes := &indexpb.DescribeIndexResponse{Status: merr.Success()}
	mix.EXPECT().DescribeIndex(mock.Anything, indexReq).Return(indexes, nil).Twice()
	req := &querypb.LoadCollectionRequest{CollectionID: coll.GetCollectionID(), LoadAllFields: true, WarmupHint: true}
	err := server.broadcastAlterLoadConfigCollectionV2ForLoadCollection(t.Context(), req)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.ErrorContains(t, err, "vec")
	require.Nil(t, server.qviewsRuntime.loadConfigStore.GetConfig(coll.GetCollectionID()))

	var captured message.BroadcastMutableMessage
	registerCaptureBroadcast(t, func(msg message.BroadcastMutableMessage) { captured = msg })
	indexes.IndexInfos = []*indexpb.IndexInfo{{FieldID: 101, IndexID: 201}}
	require.NoError(t, server.broadcastAlterLoadConfigCollectionV2ForLoadCollection(t.Context(), req))
	header := message.MustAsBroadcastAlterLoadConfigMessageV2(captured).Header()
	require.Nil(t, header.ProtoReflect().Descriptor().Fields().ByName("warmup_epoch"))
	require.Len(t, header.GetLoadFields(), 2)
	require.EqualValues(t, 100, header.GetLoadFields()[0].GetFieldId())
	require.EqualValues(t, 101, header.GetLoadFields()[1].GetFieldId())
	require.EqualValues(t, 201, header.GetLoadFields()[1].GetIndexId())
}
