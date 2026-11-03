package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	mixcoordclient "github.com/milvus-io/milvus/internal/distributed/mixcoord/client"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestSchemaMetadataAlignmentPreservesWALSemantics(t *testing.T) {
	for _, ttlPresent := range []bool{false, true} {
		current := legacySchemaMigrationVChannel().CollectionInfo.Schemas[2].Schema
		current.Properties = []*commonpb.KeyValuePair{{Key: common.MaxFieldIDKey, Value: "122"}}
		if ttlPresent {
			current.Properties = append(current.Properties,
				&commonpb.KeyValuePair{Key: common.CollectionTTLConfigKey, Value: "30"},
				&commonpb.KeyValuePair{Key: common.CollectionTTLFieldKey, Value: "expiration"})
		}
		before := proto.Clone(current)
		metadata := &schemapb.CollectionSchema{
			Name: "renamed", DbName: "moved", Description: "new description",
			ExternalSource: "source", ExternalSpec: "spec", Version: 1,
			Properties: []*commonpb.KeyValuePair{
				{Key: common.TimezoneKey, Value: "Asia/Shanghai"},
				{Key: common.CollectionTTLConfigKey, Value: "60"},
				{Key: common.CollectionTTLFieldKey, Value: "old_expiration"},
				{Key: common.CollectionOnTruncatingKey, Value: "1"},
				{Key: common.MaxFieldIDKey, Value: "121"},
			},
		}
		got := schemaWithCoordinatorMetadata(current, metadata)
		require.Equal(t, current.Version, got.Version)
		require.Equal(t, current.Fields, got.Fields)
		require.Equal(t, metadata.Name, got.Name)
		require.Equal(t, metadata.DbName, got.DbName)
		require.Equal(t, metadata.Description, got.Description)
		require.Equal(t, metadata.ExternalSource, got.ExternalSource)
		require.Equal(t, metadata.ExternalSpec, got.ExternalSpec)
		props := common.KeyValuePairs(got.Properties).ToMap()
		require.Equal(t, "Asia/Shanghai", props[common.TimezoneKey])
		require.Equal(t, "122", props[common.MaxFieldIDKey])
		require.NotContains(t, props, common.CollectionOnTruncatingKey)
		if ttlPresent {
			require.Equal(t, "30", props[common.CollectionTTLConfigKey])
			require.Equal(t, "expiration", props[common.CollectionTTLFieldKey])
		} else {
			require.NotContains(t, props, common.CollectionTTLConfigKey)
			require.NotContains(t, props, common.CollectionTTLFieldKey)
		}
		got.Fields[0].Name = "caller mutation"
		require.True(t, proto.Equal(before, current))
	}
}

func TestSchemaMetadataAlignmentPublishesMagicAfterPersistence(t *testing.T) {
	ctx := context.Background()
	catalog := &legacyMigrationCatalog{}
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
	store := newTestRecoveryStorage(t, &utility.WALCheckpoint{
		MessageID: walimplstest.NewTestMessageID(90), TimeTick: 900, Term: 7,
		Magic: utility.RecoveryMagicRecoveryStorageV2,
	})
	meta := legacySchemaMigrationVChannel()
	before := proto.Clone(meta)
	second := proto.Clone(meta).(*streamingpb.VChannelMeta)
	second.Vchannel = "v2"
	dropped := proto.Clone(meta).(*streamingpb.VChannelMeta)
	dropped.Vchannel = "dropped"
	dropped.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	dropped.CollectionInfo.CollectionId = 11
	vchannels := map[string]*streamingpb.VChannelMeta{"v1": meta, "v2": second, "dropped": dropped}
	calls := 0
	describe := mockey.Mock((*recoveryStorageImpl).describeCollectionSchema).To(func(_ *recoveryStorageImpl, _ context.Context, id int64) (*schemapb.CollectionSchema, error) {
		calls++
		require.EqualValues(t, 10, id)
		return &schemapb.CollectionSchema{Name: "renamed", Version: 1}, nil
	}).Build()
	defer describe.UnPatch()
	failSave := true
	var persisted *streamingpb.VChannelMeta
	var persistedCheckpoint *streamingpb.WALCheckpoint
	save := mockey.Mock((*legacyMigrationCatalog).SaveRecoverySnapshot).To(func(_ *legacyMigrationCatalog, _ context.Context, _ string, snapshot *metastore.WALRecoverySnapshot) error {
		// Capture owned values inside the callback, not the patched argument.
		persisted = proto.Clone(snapshot.VChannels["v1"]).(*streamingpb.VChannelMeta)
		persistedCheckpoint = proto.Clone(snapshot.ConsumeCheckpoint).(*streamingpb.WALCheckpoint)
		require.Len(t, snapshot.VChannels, 2)
		require.EqualValues(t, utility.RecoveryMagicSchemaMetadataAligned, persistedCheckpoint.RecoveryMagic)
		require.EqualValues(t, utility.RecoveryMagicRecoveryStorageV2, store.checkpoint.Magic)
		if failSave {
			return merr.WrapErrServiceUnavailableMsg("injected save failure")
		}
		return nil
	}).Build()
	defer save.UnPatch()
	require.Error(t, store.alignCollectionSchemaMetadata(ctx, vchannels))
	require.Equal(t, 1, calls, "describe once per collection, not per shard")
	require.True(t, proto.Equal(before, vchannels["v1"]))
	require.EqualValues(t, utility.RecoveryMagicRecoveryStorageV2, store.checkpoint.Magic)
	failSave = false
	require.NoError(t, store.alignCollectionSchemaMetadata(ctx, vchannels))
	require.Equal(t, 2, calls)
	require.True(t, proto.Equal(persisted, vchannels["v1"]))
	require.Equal(t, "renamed", persisted.CollectionInfo.Schemas[2].Schema.Name)
	require.EqualValues(t, 2, persisted.CollectionInfo.Schemas[2].Schema.Version)
	require.True(t, proto.Equal(meta.CollectionInfo.Schemas[0], persisted.CollectionInfo.Schemas[0]))
	require.True(t, proto.Equal(before, meta), "migration replaces rather than mutates borrowed metadata")
	require.EqualValues(t, 900, persistedCheckpoint.TimeTick)
	require.EqualValues(t, 7, persistedCheckpoint.Term)
	require.EqualValues(t, utility.RecoveryMagicSchemaMetadataAligned, store.getCompletedCheckpoint().Magic)
	// A completed migration is not re-run after loading the persisted marker.
	restart := newTestRecoveryStorage(t, utility.NewWALCheckpointFromProto(persistedCheckpoint))
	require.NoError(t, restart.alignCollectionSchemaMetadata(ctx, map[string]*streamingpb.VChannelMeta{"v1": persisted}))
	require.Equal(t, 2, calls)
	restart.pchannelControl.CheckpointTimeTick++
	require.EqualValues(t, utility.RecoveryMagicSchemaMetadataAligned, restart.consumeDirtySnapshot().Checkpoint.Magic)
}

func TestSchemaMetadataAlignmentDescribeFailures(t *testing.T) {
	for _, missing := range []bool{false, true} {
		ctx := context.Background()
		catalog := &legacyMigrationCatalog{}
		resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
		store := newTestRecoveryStorage(t, &utility.WALCheckpoint{
			MessageID: walimplstest.NewTestMessageID(90), TimeTick: 900, Magic: utility.RecoveryMagicRecoveryStorageV2,
		})
		meta := legacySchemaMigrationVChannel()
		describe := mockey.Mock((*recoveryStorageImpl).describeCollectionSchema).To(func(_ *recoveryStorageImpl, _ context.Context, _ int64) (*schemapb.CollectionSchema, error) {
			if missing {
				return nil, merr.WrapErrCollectionNotFound("dropped")
			}
			return nil, merr.WrapErrServiceUnavailableMsg("coordinator unavailable")
		}).Build()
		saves := 0
		save := mockey.Mock((*legacyMigrationCatalog).SaveRecoverySnapshot).To(func(_ *legacyMigrationCatalog, _ context.Context, _ string, snapshot *metastore.WALRecoverySnapshot) error {
			saves++
			require.Empty(t, snapshot.VChannels)
			return nil
		}).Build()
		err := store.alignCollectionSchemaMetadata(ctx, map[string]*streamingpb.VChannelMeta{"v1": meta})
		if missing {
			require.NoError(t, err)
			require.Equal(t, 1, saves)
		} else {
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.Zero(t, saves)
			require.EqualValues(t, utility.RecoveryMagicRecoveryStorageV2, store.checkpoint.Magic)
		}
		describe.UnPatch()
		save.UnPatch()
	}
}

func TestSchemaMetadataAlignmentDescribeRPC(t *testing.T) {
	for _, tc := range []struct {
		name      string
		response  *milvuspb.DescribeCollectionResponse
		rpcError  error
		wantError error
	}{
		{name: "success", response: &milvuspb.DescribeCollectionResponse{Status: merr.Success(), Schema: &schemapb.CollectionSchema{Name: "current"}}},
		{name: "missing schema", response: &milvuspb.DescribeCollectionResponse{Status: merr.Success()}, wantError: merr.ErrServiceInternal},
		{name: "not found", response: &milvuspb.DescribeCollectionResponse{Status: merr.Status(merr.WrapErrCollectionNotFound("gone"))}, wantError: merr.ErrCollectionNotFound},
		{name: "transport failure", rpcError: merr.WrapErrServiceUnavailableMsg("injected transport failure"), wantError: merr.ErrServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			future := syncutil.NewFuture[types.MixCoordClient]()
			future.Set(&mixcoordclient.Client{})
			resource.InitForTest(t, resource.OptMixCoordClient(future))
			patch := mockey.Mock((*mixcoordclient.Client).DescribeCollectionInternal).To(func(_ *mixcoordclient.Client, _ context.Context, request *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
				require.EqualValues(t, 42, request.CollectionID)
				require.Equal(t, commonpb.MsgType_DescribeCollection, request.GetBase().GetMsgType())
				return tc.response, tc.rpcError
			}).Build()
			defer patch.UnPatch()
			schema, err := (&recoveryStorageImpl{}).describeCollectionSchema(context.Background(), 42)
			if tc.wantError == nil {
				require.NoError(t, err)
				require.Equal(t, "current", schema.Name)
			} else {
				require.ErrorIs(t, err, tc.wantError)
			}
		})
	}
	future := syncutil.NewFuture[types.MixCoordClient]()
	resource.InitForTest(t, resource.OptMixCoordClient(future))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := (&recoveryStorageImpl{}).describeCollectionSchema(ctx, 42)
	require.ErrorIs(t, err, context.Canceled)
}
