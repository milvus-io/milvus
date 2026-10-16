package adaptor

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type schemaShardManager struct{ shards.ShardManager }

func (*schemaShardManager) GetWriteSchema(int64, string) (*streamingpb.CollectionSchemaOfVChannel, error) {
	panic("unpatched")
}

func TestWALSchemaSelectors(t *testing.T) {
	mockey.PatchConvey("write admission and historical read lookup have independent frontiers", t, func() {
		channel := types.PChannelInfo{AccessMode: types.AccessModeRW}
		mockey.Mock((*analyzerWALTest).Channel).To(func(*analyzerWALTest) types.PChannelInfo { return channel }).Build()
		mockey.Mock((*schemaShardManager).GetWriteSchema).To(func(_ *schemaShardManager, collectionID int64, vchannel string) (*streamingpb.CollectionSchemaOfVChannel, error) {
			if collectionID == 2 {
				require.Equal(t, "p_2v0", vchannel)
				return nil, status.NewUnrecoverableError("collection does not exist")
			}
			if collectionID == 3 {
				require.Equal(t, "p_3v0", vchannel)
				return nil, status.NewInner("write schema is not ready")
			}
			require.EqualValues(t, 1, collectionID)
			require.Contains(t, []string{"p_1v0", "p_1v1"}, vchannel)
			return &streamingpb.CollectionSchemaOfVChannel{Schema: &schemapb.CollectionSchema{Version: 2}, CheckpointTimeTick: 30}, nil
		}).Build()
		manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{PChannel: "p", VChannelMetas: map[string]*streamingpb.VChannelMeta{"p_1v0": {
			Vchannel: "p_1v0", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1, Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{Schema: &schemapb.CollectionSchema{Version: 0}, CheckpointTimeTick: 10, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL},
				{Schema: &schemapb.CollectionSchema{Version: 1}, CheckpointTimeTick: 20, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL},
			}},
		}}})
		require.NoError(t, err)
		defer manager.Close()
		w := &walAdaptorImpl{roWALAdaptorImpl: &roWALAdaptorImpl{lifetime: typeutil.NewLifetime()}, rwWALImpls: &analyzerWALTest{}, viewResourceManager: manager, param: &interceptors.InterceptorBuildParam{ShardManager: &schemaShardManager{}}}
		req := &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: "p_1v0", Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true}}
		resp, err := w.GetCollectionSchema(context.Background(), req)
		require.NoError(t, err)
		require.EqualValues(t, 2, resp.Schema.Version)
		require.EqualValues(t, 30, resp.EffectiveTimeTick)
		req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: 0}
		resp, err = w.GetCollectionSchema(context.Background(), req)
		require.NoError(t, err)
		require.Zero(t, resp.Schema.Version)
		require.EqualValues(t, 10, resp.EffectiveTimeTick)
		req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: 2}
		_, err = w.GetCollectionSchema(context.Background(), req)
		require.Error(t, err)
		require.False(t, status.AsStreamingError(err).IsUnrecoverable(), "write schema visibility does not imply observer visibility")
		for _, tc := range []struct {
			vchannel string
			terminal bool
		}{
			{vchannel: "p_1v1"},                 // Write admission exists, observer has not caught up.
			{vchannel: "p_2v0", terminal: true}, // Unknown or fully GC'd collection.
			{vchannel: "p_3v0"},                 // Write schema is still recovering.
		} {
			req.Vchannel = tc.vchannel
			resp, err = w.GetCollectionSchema(context.Background(), req)
			require.Nil(t, resp, tc.vchannel)
			require.Error(t, err, tc.vchannel)
			require.Equal(t, tc.terminal, status.AsStreamingError(err).IsUnrecoverable(), tc.vchannel)
		}
		req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true}
		w.param.ShardManager = nil
		_, err = w.GetCollectionSchema(context.Background(), req)
		require.True(t, status.AsStreamingError(err).IsUnrecoverable())
		channel.AccessMode = types.AccessModeRO
		_, err = w.GetCollectionSchema(context.Background(), req)
		require.True(t, status.AsStreamingError(err).IsUnrecoverable())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = w.GetCollectionSchema(ctx, req)
		require.ErrorIs(t, err, context.Canceled)
		w.lifetime.SetState(typeutil.LifetimeStateStopped)
		_, err = w.GetCollectionSchema(context.Background(), req)
		require.True(t, status.AsStreamingError(err).IsOnShutdown())
	})
}
