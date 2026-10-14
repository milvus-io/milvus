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
		mockey.Mock((*schemaShardManager).GetWriteSchema).Return(&streamingpb.CollectionSchemaOfVChannel{Schema: &schemapb.CollectionSchema{Version: 2}, CheckpointTimeTick: 30}, nil).Build()
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
		req := &streamingpb.StreamingNodeGetCollectionSchemaRequest{CollectionId: 1, Vchannel: "p_1v0", Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true}}
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
		req.Vchannel = "absent"
		_, err = w.GetCollectionSchema(context.Background(), req)
		require.Error(t, err)
		require.False(t, status.AsStreamingError(err).IsUnrecoverable())
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
