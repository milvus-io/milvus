package adaptor_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/service"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type schemaRPCManager struct{ walmanager.Manager }

func (*schemaRPCManager) GetAvailableWAL(types.PChannelInfo) (wal.WAL, error) {
	panic("patch with mockey")
}

// The service and schema providers are real: CREATE/ALTER go through the WAL
// interceptors and recovery observer, and reads cross the native gRPC transport.
func TestCollectionSchemaRPCWithWAL(t *testing.T) {
	walimplstest.Reset()
	initResourceForTest(t)
	message.RegisterDefaultWALName(message.WALNameTest)
	opener, err := registry.MustGetBuilder(message.WALNameTest,
		redo.NewInterceptorBuilder(), lock.NewInterceptorBuilder(),
		replicate.NewInterceptorBuilder(), timetick.NewInterceptorBuilder(), shard.NewInterceptorBuilder(),
	).Build()
	require.NoError(t, err)
	defer opener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	channel := types.PChannelInfo{Name: "schema-rpc", Term: 1, AccessMode: types.AccessModeRW}
	w, err := opener.Open(ctx, &wal.OpenOption{Channel: channel, DisableFlusher: true})
	require.NoError(t, err)
	defer w.Close()
	manager := mockey.Mock((*schemaRPCManager).GetAvailableWAL).To(func(_ *schemaRPCManager, requested types.PChannelInfo) (wal.WAL, error) {
		if requested.Term != channel.Term {
			return nil, status.NewUnmatchedChannelTerm(channel.Name, requested.Term, channel.Term)
		}
		return w, nil
	}).Build()
	defer manager.UnPatch()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.NewServiceUnaryServerInterceptor()))
	streamingpb.RegisterStreamingNodeHandlerServiceServer(server, service.NewHandlerService(&schemaRPCManager{}))
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///schema-wal", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptor.NewServiceUnaryClientInterceptor()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	defer conn.Close()
	client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
	const vchannel = "schema-rpc_1v0"
	created, err := w.Append(ctx, message.NewCreateCollectionMessageBuilderV1().WithVChannel(vchannel).
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{2}}).
		WithBody(&msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: "schema_rpc", Version: 0}}).MustBuildMutable())
	require.NoError(t, err)
	req := &streamingpb.StreamingNodeGetCollectionSchemaRequest{
		Vchannel: vchannel,
		Pchannel: types.NewProtoFromPChannelInfo(channel),
		Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true},
	}
	current, err := client.GetCollectionSchema(ctx, req)
	require.NoError(t, err)
	require.Zero(t, current.Schema.Version)
	require.Equal(t, created.TimeTick, current.EffectiveTimeTick)
	req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: 0}
	require.Eventually(t, func() bool {
		exact, err := client.GetCollectionSchema(ctx, req)
		return err == nil && exact.Schema.Version == 0 && exact.EffectiveTimeTick == created.TimeTick
	}, 5*time.Second, time.Millisecond)
	altered, err := w.Append(ctx, message.NewAlterCollectionMessageBuilderV2().WithVChannel(vchannel).
		WithHeader(&message.AlterCollectionMessageHeader{CollectionId: 1, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}}}).
		WithBody(&message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{Schema: &schemapb.CollectionSchema{Name: "schema_rpc", Version: 1}}}).MustBuildMutable())
	require.NoError(t, err)
	req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true}
	current, err = client.GetCollectionSchema(ctx, req)
	require.NoError(t, err)
	require.EqualValues(t, 1, current.Schema.Version)
	require.Equal(t, altered.TimeTick, current.EffectiveTimeTick)
	req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: 1}
	require.Eventually(t, func() bool {
		exact, err := client.GetCollectionSchema(ctx, req)
		return err == nil && exact.Schema.Version == 1 && exact.EffectiveTimeTick == altered.TimeTick
	}, 5*time.Second, time.Millisecond)
	req.Pchannel.Term++
	_, err = client.GetCollectionSchema(ctx, req)
	require.True(t, status.AsStreamingError(err).IsWrongStreamingNode())
}
