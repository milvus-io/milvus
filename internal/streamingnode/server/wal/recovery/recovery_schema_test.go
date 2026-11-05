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
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestSchemaAlignmentPublishesMagicAfterPersistence(t *testing.T) {
	for _, tc := range []struct{ name, beforeTTL, afterTTL string }{
		{"add TTL", "", "60"}, {"change TTL", "30", "60"}, {"remove TTL", "30", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			catalog := &legacyMigrationCatalog{}
			resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
			store := newTestRecoveryStorage(t, &utility.WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(90), TimeTick: 900, Term: 7,
				Magic: utility.RecoveryMagicRecoveryStorageV2,
			})
			meta := legacySchemaMigrationVChannel()
			if tc.beforeTTL != "" {
				meta.CollectionInfo.Schemas[1].Schema.Properties = []*commonpb.KeyValuePair{{Key: common.CollectionTTLConfigKey, Value: tc.beforeTTL}}
			}
			coordinatorSchema := &schemapb.CollectionSchema{
				Name: "renamed", DbName: "moved", Description: "current", Version: 1,
				ExternalSource: "source", ExternalSpec: "spec", EnableDynamicField: true,
				Fields:     []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
				Properties: []*commonpb.KeyValuePair{{Key: common.CollectionOnTruncatingKey, Value: "1"}},
			}
			if tc.afterTTL != "" {
				coordinatorSchema.Properties = append(coordinatorSchema.Properties, &commonpb.KeyValuePair{Key: common.CollectionTTLConfigKey, Value: tc.afterTTL})
			}
			expectedSchema := proto.Clone(coordinatorSchema).(*schemapb.CollectionSchema)
			expectedSchema.Properties = expectedSchema.Properties[1:]
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
				return coordinatorSchema, nil
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
			require.Error(t, store.alignCollectionSchema(ctx, vchannels))
			require.Equal(t, 1, calls, "describe once per collection, not per shard")
			require.True(t, proto.Equal(before, vchannels["v1"]))
			require.EqualValues(t, utility.RecoveryMagicRecoveryStorageV2, store.checkpoint.Magic)
			failSave = false
			require.NoError(t, store.alignCollectionSchema(ctx, vchannels))
			require.Equal(t, 2, calls)
			require.True(t, proto.Equal(persisted, vchannels["v1"]))
			require.True(t, proto.Equal(expectedSchema, persisted.CollectionInfo.Schemas[1].Schema), "the full Coord schema replaces the matching retained version")
			require.Len(t, coordinatorSchema.Properties, len(expectedSchema.Properties)+1, "alignment must not mutate the shared Describe response")
			require.NotSame(t, coordinatorSchema.Fields[0], persisted.CollectionInfo.Schemas[1].Schema.Fields[0])
			require.True(t, proto.Equal(meta.CollectionInfo.Schemas[0], persisted.CollectionInfo.Schemas[0]))
			require.True(t, proto.Equal(meta.CollectionInfo.Schemas[2], persisted.CollectionInfo.Schemas[2]), "a newer WAL version must remain intact")
			require.True(t, proto.Equal(before, meta), "migration replaces rather than mutates borrowed metadata")
			require.EqualValues(t, 900, persistedCheckpoint.TimeTick)
			require.EqualValues(t, 7, persistedCheckpoint.Term)
			require.EqualValues(t, utility.RecoveryMagicSchemaMetadataAligned, store.getCompletedCheckpoint().Magic)
			// A completed migration is not re-run after loading the persisted marker.
			restart := newTestRecoveryStorage(t, utility.NewWALCheckpointFromProto(persistedCheckpoint))
			require.NoError(t, restart.alignCollectionSchema(ctx, map[string]*streamingpb.VChannelMeta{"v1": persisted}))
			require.Equal(t, 2, calls)
			restart.pchannelControl.CheckpointTimeTick++
			require.EqualValues(t, utility.RecoveryMagicSchemaMetadataAligned, restart.consumeDirtySnapshot().Checkpoint.Magic)
		})
	}
}

func TestSchemaAlignmentPreservesSegmentVersions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version int32
		retired bool
	}{
		{name: "version zero", version: 0},
		{name: "WAL ahead of Coord", version: 1},
		{name: "same latest version", version: 2},
		{name: "Coord ahead of snapshot", version: 3},
		{name: "retired matching version", version: 1, retired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := &legacyMigrationCatalog{}
			resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))
			store := newTestRecoveryStorage(t, &utility.WALCheckpoint{
				MessageID: walimplstest.NewTestMessageID(90), TimeTick: 900, Magic: utility.RecoveryMagicRecoveryStorageV2,
			})
			meta := legacySchemaMigrationVChannel()
			coordinatorSchema := proto.Clone(meta.CollectionInfo.Schemas[min(int(tc.version), 2)].Schema).(*schemapb.CollectionSchema)
			coordinatorSchema.Version = tc.version
			coordinatorSchema.Description = "aligned metadata"
			if tc.retired {
				meta.CollectionInfo.Schemas[1].State = streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_DROPPED
				meta.CollectionInfo.Schemas[1].Schema = nil
			}
			before := proto.Clone(meta).(*streamingpb.VChannelMeta)
			describe := mockey.Mock((*recoveryStorageImpl).describeCollectionSchema).Return(coordinatorSchema, nil).Build()
			defer describe.UnPatch()
			save := mockey.Mock((*legacyMigrationCatalog).SaveRecoverySnapshot).To(func(_ *legacyMigrationCatalog, _ context.Context, _ string, snapshot *metastore.WALRecoverySnapshot) error {
				if tc.version > 2 || tc.retired {
					require.Empty(t, snapshot.VChannels, "a missing version must not overwrite another version")
				}
				return nil
			}).Build()
			defer save.UnPatch()
			metas := map[string]*streamingpb.VChannelMeta{"v1": meta}
			require.NoError(t, store.alignCollectionSchema(context.Background(), metas))
			require.EqualValues(t, utility.RecoveryMagicSchemaMetadataAligned, store.checkpoint.Magic)
			for i, entry := range metas["v1"].CollectionInfo.Schemas {
				require.Equal(t, before.CollectionInfo.Schemas[i].CheckpointTimeTick, entry.CheckpointTimeTick)
				if int32(i) == tc.version && !tc.retired {
					require.True(t, proto.Equal(coordinatorSchema, entry.Schema))
				} else {
					require.True(t, proto.Equal(before.CollectionInfo.Schemas[i], entry))
				}
			}
			// Exercise real module restoration after alignment. Both growing and
			// legacy sealed assignments must retain their version-2 encoding schema.
			var restored []*schemapb.CollectionSchema
			var original func(*streamingpb.SegmentAssignmentMeta, *schemapb.CollectionSchema, segment.ViewConfig) *segment.SegmentView
			patch := mockey.Mock(segment.NewSegmentViewFromMetaWithConfig).Origin(&original).
				To(func(meta *streamingpb.SegmentAssignmentMeta, schema *schemapb.CollectionSchema, config segment.ViewConfig) *segment.SegmentView {
					require.NotNil(t, schema)
					restored = append(restored, proto.Clone(schema).(*schemapb.CollectionSchema))
					return original(meta, schema, config)
				}).Build()
			defer patch.UnPatch()
			_, err := vchannel.NewModule(vchannel.ModuleConfig{
				PChannel: "p", VChannel: "v1", VChannelMeta: metas["v1"],
				Segments: map[int64]*streamingpb.SegmentAssignmentMeta{
					1: {
						CollectionId: 10, PartitionId: 20, SegmentId: 1, Vchannel: "v1", SchemaVersion: 2,
						State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
						Stat:  &streamingpb.SegmentAssignmentStat{CreateSegmentTimeTick: 210},
					},
					2: {
						CollectionId: 10, PartitionId: 20, SegmentId: 2, Vchannel: "v1", SchemaVersion: 2,
						State: streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_SEALED,
					},
				},
			})
			require.NoError(t, err)
			require.Len(t, restored, 2)
			for _, schema := range restored {
				require.True(t, proto.Equal(metas["v1"].CollectionInfo.Schemas[2].Schema, schema))
			}
		})
	}
}

func TestSchemaAlignmentDescribeFailures(t *testing.T) {
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
		err := store.alignCollectionSchema(ctx, map[string]*streamingpb.VChannelMeta{"v1": meta})
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

func TestSchemaAlignmentDescribeRPC(t *testing.T) {
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
