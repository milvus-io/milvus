package service

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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type schemaWAL struct{ wal.WAL }

func (*schemaWAL) GetCollectionSchema(context.Context, *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
	panic("unpatched")
}

func TestSchemaNativeTransport(t *testing.T) {
	mockey.PatchConvey("exact schema selectors and native errors", t, func() {
		var ownershipErr error
		var provider wal.WAL = &schemaWAL{}
		mockey.Mock((*analyzerManager).GetAvailableWAL).To(func(_ *analyzerManager, channel types.PChannelInfo) (wal.WAL, error) {
			require.Equal(t, "p", channel.Name)
			require.EqualValues(t, 2, channel.Term)
			return provider, ownershipErr
		}).Build()
		calls := mockey.Mock((*schemaWAL).GetCollectionSchema).To(func(_ *schemaWAL, _ context.Context, req *streamingpb.StreamingNodeGetCollectionSchemaRequest) (*streamingpb.StreamingNodeGetCollectionSchemaResponse, error) {
			version := req.GetSchemaVersion()
			if req.GetCurrentWrite() {
				version = 5
			}
			return &streamingpb.StreamingNodeGetCollectionSchemaResponse{Schema: &schemapb.CollectionSchema{Version: version}, EffectiveTimeTick: 100}, nil
		}).Build()
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.NewStreamingServiceUnaryServerInterceptor()))
		streamingpb.RegisterStreamingNodeHandlerServiceServer(server, NewHandlerService(&analyzerManager{}))
		go server.Serve(listener)
		defer server.Stop()
		conn, err := grpc.NewClient("passthrough:///schema", grpc.WithUnaryInterceptor(interceptor.NewStreamingServiceUnaryClientInterceptor()), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		defer conn.Close()
		client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req := &streamingpb.StreamingNodeGetCollectionSchemaRequest{Vchannel: "p_1v0", Pchannel: &streamingpb.PChannelInfo{Name: "p", Term: 2}, Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: 0}}
		resp, err := client.GetCollectionSchema(ctx, req)
		require.NoError(t, err)
		require.Zero(t, resp.Schema.Version)
		req.Selector = &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{CurrentWrite: true}
		resp, err = client.GetCollectionSchema(ctx, req)
		require.NoError(t, err)
		require.EqualValues(t, 5, resp.Schema.Version)
		ownershipErr = status.NewUnmatchedChannelTerm("p", 2, 3)
		resp, err = client.GetCollectionSchema(ctx, req)
		require.Nil(t, resp)
		require.True(t, status.AsStreamingError(err).IsWrongStreamingNode())
		require.Equal(t, 2, calls.Times())
		ownershipErr = nil
		provider = &fieldAnalyzerWAL{}
		resp, err = client.GetCollectionSchema(ctx, req)
		require.Nil(t, resp)
		require.True(t, status.AsStreamingError(err).IsUnrecoverable())
		canceled, stop := context.WithCancel(ctx)
		stop()
		_, err = NewHandlerService(nil).GetCollectionSchema(canceled, req)
		require.ErrorIs(t, err, context.Canceled)
		for _, invalid := range []*streamingpb.StreamingNodeGetCollectionSchemaRequest{
			{},
			{Vchannel: "p_invalid", Pchannel: req.Pchannel, Selector: req.Selector},
			{Vchannel: "p_0v0", Pchannel: req.Pchannel, Selector: req.Selector},
			{Vchannel: "p_1v0"},
			{Vchannel: "p_1v0", Pchannel: &streamingpb.PChannelInfo{Name: "other"}},
			{Vchannel: "p_1v0", Pchannel: req.Pchannel},
			{Vchannel: "p_1v0", Pchannel: req.Pchannel, Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_CurrentWrite{}},
			{Vchannel: "p_1v0", Pchannel: req.Pchannel, Selector: &streamingpb.StreamingNodeGetCollectionSchemaRequest_SchemaVersion{SchemaVersion: -1}},
		} {
			resp, err = client.GetCollectionSchema(ctx, invalid)
			require.Nil(t, resp)
			require.True(t, status.AsStreamingError(err).IsInvalidArgument(), "%v", err)
		}
		require.Equal(t, 2, calls.Times())
	})
}
