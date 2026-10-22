package transformlog_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	resumable "github.com/milvus-io/milvus/internal/distributed/streaming/internal/transformlog"
	remote "github.com/milvus-io/milvus/internal/streamingnode/client/handler/transformlog"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// An opt-in live-cluster check. Set the endpoint, namespace and collection input
// explicitly; ordinary unit runs must never attach to a developer's cluster.
func TestLiveTransformRPC(t *testing.T) {
	endpoint := os.Getenv("TRANSFORM_LIVE_PROXY")
	if endpoint == "" {
		t.Skip("requires an explicitly selected live cluster")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	root := os.Getenv("TRANSFORM_LIVE_ROOT")
	require.NotEmpty(t, root)
	vchannel, pchannel := os.Getenv("TRANSFORM_LIVE_VCHANNEL"), os.Getenv("TRANSFORM_LIVE_PCHANNEL")
	require.NotEmpty(t, vchannel)
	require.NotEmpty(t, pchannel)
	start, err := strconv.ParseUint(os.Getenv("TRANSFORM_LIVE_START"), 10, 64)
	require.NoError(t, err)
	deleted, err := strconv.ParseUint(os.Getenv("TRANSFORM_LIVE_DELETE"), 10, 64)
	require.NoError(t, err)
	etcd, err := clientv3.New(clientv3.Config{Endpoints: []string{os.Getenv("TRANSFORM_LIVE_ETCD")}, DialTimeout: 5 * time.Second})
	require.NoError(t, err)
	defer etcd.Close()
	connections := make(chan *grpc.ClientConn, 8)
	stream := resumable.NewResumableStream(ctx, pchannel, func(ctx context.Context, channel string) (wal.TransformLogStream, error) {
		result, err := etcd.Get(ctx, root+"/meta/streamingcoord-meta/pchannel/"+channel)
		if err != nil {
			return nil, err
		}
		require.Len(t, result.Kvs, 1)
		meta := &streamingpb.PChannelMeta{}
		require.NoError(t, proto.Unmarshal(result.Kvs[0].Value, meta))
		conn, err := grpc.NewClient(meta.GetNode().GetAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStreamInterceptor(interceptor.NewStreamingServiceStreamClientInterceptor()))
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() { _ = conn.Close() })
		physical, err := remote.CreateEventStream(ctx, &remote.EventStreamOptions{Assignment: &types.PChannelInfoAssigned{Channel: types.PChannelInfo{Name: channel, Term: meta.GetChannel().GetTerm()}, Node: types.StreamingNodeInfo{ServerID: meta.GetNode().GetServerId()}}}, streamingpb.NewStreamingNodeHandlerServiceClient(conn))
		if err != nil {
			_ = conn.Close()
			return nil, err
		}
		connections <- conn
		return physical, nil
	})
	defer stream.Close()
	handler := newRecordingHandler()
	sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: vchannel, StartAfterTimeTick: start, Handler: handler})
	require.NoError(t, err)
	defer sub.Close()
	event := recvStreamEvent(t, handler.events)
	require.NotNil(t, event.Entry)
	require.Equal(t, deleted, event.Entry.GetTimeTick())
	require.NotNil(t, event.Entry.GetDelete())
	event = recvStreamEvent(t, handler.events)
	require.NotNil(t, event.SyncUp)
	require.GreaterOrEqual(t, event.SyncUp.TimeTick, deleted)
	cursor := event.SyncUp.TimeTick
	first := <-connections
	require.NoError(t, first.Close())
	select {
	case <-connections:
	case <-ctx.Done():
		t.Fatal("live stream did not reconnect")
	}
	proxy, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer proxy.Close()
	result, err := milvuspb.NewMilvusServiceClient(proxy).Delete(ctx, &milvuspb.DeleteRequest{DbName: "default", CollectionName: os.Getenv("TRANSFORM_LIVE_COLLECTION"), Expr: "id in [51]"})
	require.NoError(t, err)
	require.NoError(t, merr.Error(result.GetStatus()))
	for {
		event = recvStreamEvent(t, handler.events)
		if event.Entry == nil {
			require.Greater(t, event.SyncUp.TimeTick, cursor)
			cursor = event.SyncUp.TimeTick
			continue
		}
		require.Equal(t, result.GetTimestamp(), event.Entry.GetTimeTick())
		break
	}
	for event.SyncUp == nil || event.SyncUp.TimeTick < result.GetTimestamp() {
		event = recvStreamEvent(t, handler.events)
		require.Nil(t, event.Entry, "no duplicate Delete")
	}
	require.NoError(t, sub.Close())
	require.NoError(t, stream.Close())
	t.Logf("live RPC: %s, exclusive start %d, Delete %d, resume %d, new Delete %d", vchannel, start, deleted, cursor, result.GetTimestamp())
}
