package querycoordv2

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
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

func TestAdmitLoadCollectionWarmupHint(t *testing.T) {
	enableWarmupHintGate(t, "true")
	ctx := context.Background()
	nextID := int64(100)
	server := &Server{idAllocator: func() (int64, error) { nextID++; return nextID, nil }}
	coll := &metacache.CollectionInfo{Schema: &schemapb.CollectionSchema{}}
	ordinary := &querypb.LoadCollectionRequest{CollectionID: 1}
	hinted := &querypb.LoadCollectionRequest{CollectionID: 1, WarmupHint: true}

	epoch, err := server.admitLoadCollectionWarmupHint(ctx, ordinary, coll, nil)
	require.NoError(t, err)
	require.Zero(t, epoch)
	epoch, err = server.admitLoadCollectionWarmupHint(ctx, ordinary, coll, &loadmgr.LoadConfig{WarmupEpoch: 42})
	require.NoError(t, err)
	require.EqualValues(t, 42, epoch, "a load without the hint keeps the current epoch")

	// Every request with a warmup hint gets a newer epoch, loaded or not.
	epoch, err = server.admitLoadCollectionWarmupHint(ctx, hinted, coll, nil)
	require.NoError(t, err)
	require.EqualValues(t, 101, epoch)
	epoch, err = server.admitLoadCollectionWarmupHint(ctx, hinted, coll, &loadmgr.LoadConfig{WarmupEpoch: 101})
	require.NoError(t, err)
	require.EqualValues(t, 102, epoch)
	epoch, err = server.admitLoadCollectionWarmupHint(ctx, hinted, coll, &loadmgr.LoadConfig{})
	require.NoError(t, err)
	require.EqualValues(t, 103, epoch, "a warmup hint warms a collection loaded without it in place")

	external := &metacache.CollectionInfo{Schema: &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{Name: "f", ExternalField: "col"}},
	}}
	_, err = server.admitLoadCollectionWarmupHint(ctx, hinted, external, nil)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	server.idAllocator = func() (int64, error) { return 0, errors.New("allocator unavailable") }
	_, err = server.admitLoadCollectionWarmupHint(ctx, hinted, coll, nil)
	require.ErrorContains(t, err, "allocator unavailable")
}

func TestWarmupHintAdmissionRequiresGate(t *testing.T) {
	enableWarmupHintGate(t, "false")
	server := &Server{idAllocator: func() (int64, error) { return 1, nil }}
	coll := &metacache.CollectionInfo{Schema: &schemapb.CollectionSchema{}}
	_, err := server.admitLoadCollectionWarmupHint(context.Background(), &querypb.LoadCollectionRequest{WarmupHint: true}, coll, nil)
	require.ErrorIs(t, err, merr.ErrServiceUnimplemented)
	epoch, err := server.admitLoadCollectionWarmupHint(context.Background(), &querypb.LoadCollectionRequest{}, coll, &loadmgr.LoadConfig{WarmupEpoch: 7})
	require.NoError(t, err)
	require.EqualValues(t, 7, epoch, "a closed gate keeps existing hints")
}

func TestWarmupHintWarmsLoadedCollectionInPlace(t *testing.T) {
	enableWarmupHintGate(t, "true")
	ctx := context.Background()
	server, _, _ := newLoadConfigQViewsServer(t)
	collectionID := int64(100)
	// The collection has a partition the load config leaves out, so restating
	// the load would add it.
	putTestCollection(server, testDescribeCollection(collectionID, []string{"v0"}), 10, 20, 30)
	store := server.qviewsRuntime.loadConfigStore
	require.NoError(t, store.Put(ctx, &loadmgr.LoadConfig{
		DbID:                     1,
		CollectionID:             collectionID,
		PartitionIDs:             []int64{10, 20},
		LoadFields:               []*viewpb.LoadFieldConfig{{FieldId: 100}},
		UserSpecifiedReplicaMode: true,
		WarmupEpoch:              7,
		Replicas: []*loadmgr.ReplicaAssignment{
			{ReplicaID: 1001, ResourceGroup: "rg1"},
			{ReplicaID: 1002, ResourceGroup: "rg2"},
		},
	}))
	server.idAllocator = func() (int64, error) { return 200, nil }
	var captured message.BroadcastMutableMessage
	registerCaptureBroadcast(t, func(msg message.BroadcastMutableMessage) {
		captured = msg
	})

	// The proxy lists the fields to load even when the request names none.
	require.NoError(t, server.broadcastAlterLoadConfigCollectionV2ForLoadCollection(ctx, &querypb.LoadCollectionRequest{
		CollectionID:  collectionID,
		LoadFields:    []int64{100},
		LoadAllFields: true,
		WarmupHint:    true,
	}))
	want := loadConfigIntoAlterLoadConfigHeader(store.GetConfig(collectionID))
	want.WarmupEpoch = 200
	require.True(t, proto.Equal(want, message.MustAsBroadcastAlterLoadConfigMessageV2(captured).Header()),
		"partitions, fields, replicas and modes are kept; only the epoch advances")

	for _, req := range []*querypb.LoadCollectionRequest{
		{LoadAllFields: true},
		{LoadAllFields: true, WarmupHint: true, ReplicaNumber: 2},
		{LoadAllFields: true, WarmupHint: true, ResourceGroups: []string{"rg1"}},
		{LoadAllFields: false, WarmupHint: true, LoadFields: []int64{100}},
		{LoadAllFields: true, WarmupHint: true, SkipLoadDynamicField: true},
	} {
		require.False(t, warmsInPlace(req), "a load that names load options restates the configuration")
	}
}
