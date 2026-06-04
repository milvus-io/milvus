package adaptor_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	_ "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestWALIdempotencyAppend(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.EtcdCfg.RootPath.Key, fmt.Sprintf("idempotency-wal-%d", time.Now().UnixNano()))
	params.Save(params.MinioCfg.RootPath.Key, t.TempDir())
	params.Save(params.StreamingCfg.WALWriteAheadBufferKeepalive.Key, "500ms")
	params.Save(params.StreamingCfg.WALWriteAheadBufferCapacity.Key, "10k")
	message.RegisterDefaultWALName(message.WALNameTest)
	defer func() {
		params.Reset(params.EtcdCfg.RootPath.Key)
		params.Reset(params.MinioCfg.RootPath.Key)
	}()

	initIdempotencyResourceForTest(t)

	openerBuilder := registry.MustGetBuilder(
		message.WALNameTest,
		idempotency.NewInterceptorBuilder(),
		timetick.NewInterceptorBuilder(),
	)
	opener, err := openerBuilder.Build()
	require.NoError(t, err)
	defer opener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	channel := types.PChannelInfo{
		Name: fmt.Sprintf("idempotency-wal-pchannel-%d", time.Now().UnixNano()),
		Term: 1,
	}
	rwWAL, err := opener.Open(ctx, &wal.OpenOption{
		Channel:        channel,
		DisableFlusher: true,
	})
	require.NoError(t, err)

	first, err := rwWAL.Append(ctx, newIdempotencyWALAppendMessage("key-1"))
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NotNil(t, first.MessageID)
	require.NotZero(t, first.TimeTick)

	duplicate, err := rwWAL.Append(ctx, newIdempotencyWALAppendMessage("key-1"))
	require.NoError(t, err)
	require.NotNil(t, duplicate)
	require.True(t, first.MessageID.EQ(duplicate.MessageID))
	require.Equal(t, first.TimeTick, duplicate.TimeTick)
	require.True(t, first.LastConfirmedMessageID.EQ(duplicate.LastConfirmedMessageID))

	second, err := rwWAL.Append(ctx, newIdempotencyWALAppendMessage("key-2"))
	require.NoError(t, err)
	require.NotNil(t, second)
	require.False(t, first.MessageID.EQ(second.MessageID))
	require.Greater(t, second.TimeTick, first.TimeTick)

	rwWAL.Close()

	recoveredWAL, err := opener.Open(ctx, &wal.OpenOption{
		Channel:        channel,
		DisableFlusher: true,
	})
	require.NoError(t, err)
	defer recoveredWAL.Close()

	recoveredDuplicate, err := recoveredWAL.Append(ctx, newIdempotencyWALAppendMessage("key-1"))
	require.NoError(t, err)
	require.True(t, first.MessageID.EQ(recoveredDuplicate.MessageID))
	require.Equal(t, first.TimeTick, recoveredDuplicate.TimeTick)

	recoveredSecondDuplicate, err := recoveredWAL.Append(ctx, newIdempotencyWALAppendMessage("key-2"))
	require.NoError(t, err)
	require.True(t, second.MessageID.EQ(recoveredSecondDuplicate.MessageID))
	require.Equal(t, second.TimeTick, recoveredSecondDuplicate.TimeTick)
}

func initIdempotencyResourceForTest(t *testing.T) {
	var consumeCheckpoint *streamingpb.WALCheckpoint
	vchannels := make(map[string]*streamingpb.VChannelMeta)
	idempotencyWindowMetas := make(map[string]*streamingpb.VChannelWindowMeta)
	var pchannelWindowMeta *streamingpb.PChannelWindowMeta

	rc := mocks.NewMockMixCoordClient(t)
	tso := atomic.Uint64{}
	tso.Store(1000)
	rc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
		start := tso.Add(uint64(req.Count)) - uint64(req.Count)
		return &rootcoordpb.AllocTimestampResponse{
			Status:    merr.Success(),
			Timestamp: start,
			Count:     req.Count,
		}, nil
	}).Maybe()
	rc.EXPECT().GetPChannelInfo(mock.Anything, mock.Anything).Return(&rootcoordpb.GetPChannelInfoResponse{
		Status: merr.Success(),
		Collections: []*rootcoordpb.CollectionInfoOnPChannel{
			{
				CollectionId: 1,
				Partitions: []*rootcoordpb.PartitionInfoOnPChannel{
					{PartitionId: 1},
				},
				Vchannel: "v1",
				State:    etcdpb.CollectionState_CollectionCreated,
			},
		},
	}, nil).Maybe()
	rc.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: req.CollectionID,
			Schema:       &schemapb.CollectionSchema{Name: "idempotency_test_collection"},
		}, nil
	}).Maybe()

	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) (*streamingpb.WALCheckpoint, error) {
		if consumeCheckpoint == nil {
			return nil, nil
		}
		return proto.Clone(consumeCheckpoint).(*streamingpb.WALCheckpoint), nil
	})
	catalog.EXPECT().SaveConsumeCheckpoint(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, checkpoint *streamingpb.WALCheckpoint) error {
		consumeCheckpoint = proto.Clone(checkpoint).(*streamingpb.WALCheckpoint)
		return nil
	}).Maybe()
	catalog.EXPECT().ListSegmentAssignment(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveSegmentAssignments(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().ListVChannel(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) ([]*streamingpb.VChannelMeta, error) {
		values := make([]*streamingpb.VChannelMeta, 0, len(vchannels))
		for _, meta := range vchannels {
			values = append(values, proto.Clone(meta).(*streamingpb.VChannelMeta))
		}
		return values, nil
	}).Maybe()
	catalog.EXPECT().SaveVChannels(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, saved map[string]*streamingpb.VChannelMeta) error {
		for key, meta := range saved {
			vchannels[key] = proto.Clone(meta).(*streamingpb.VChannelMeta)
		}
		return nil
	}).Maybe()
	catalog.EXPECT().ListVChannelWindowMetas(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string) ([]*streamingpb.VChannelWindowMeta, error) {
		values := make([]*streamingpb.VChannelWindowMeta, 0, len(idempotencyWindowMetas))
		for _, meta := range idempotencyWindowMetas {
			values = append(values, proto.Clone(meta).(*streamingpb.VChannelWindowMeta))
		}
		return values, nil
	}).Maybe()
	catalog.EXPECT().SaveVChannelWindowMetas(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, viewType string, saved map[string]*streamingpb.VChannelWindowMeta) error {
		for key, meta := range saved {
			idempotencyWindowMetas[key] = proto.Clone(meta).(*streamingpb.VChannelWindowMeta)
		}
		return nil
	}).Maybe()
	catalog.EXPECT().GetPChannelWindowMeta(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) (*streamingpb.PChannelWindowMeta, error) {
		if pchannelWindowMeta == nil {
			return nil, nil
		}
		return proto.Clone(pchannelWindowMeta).(*streamingpb.PChannelWindowMeta), nil
	}).Maybe()
	catalog.EXPECT().SavePChannelWindowMeta(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, meta *streamingpb.PChannelWindowMeta) error {
		pchannelWindowMeta = proto.Clone(meta).(*streamingpb.PChannelWindowMeta)
		return nil
	}).Maybe()
	catalog.EXPECT().GetSalvageCheckpoint(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveSalvageCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	fMixCoordClient := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixCoordClient.Set(rc)
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(paramtable.Get().MinioCfg.RootPath.GetValue()))
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixCoordClient),
		resource.OptStreamingNodeCatalog(catalog),
		resource.OptChunkManager(chunkManager),
	)
}

func newIdempotencyWALAppendMessage(key string) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{CollectionId: 1, IdempotencyKey: proto.String(key)}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()
}
