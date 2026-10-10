package service

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func inlineAnalyzerRequest(params string) *streamingpb.StreamingNodeRunAnalyzerRequest {
	return &streamingpb.StreamingNodeRunAnalyzerRequest{
		Placeholder: [][]byte{[]byte("Hello world"), []byte("")}, WithDetail: true, WithHash: true,
		Source: &streamingpb.StreamingNodeRunAnalyzerRequest_InlineAnalyzer{InlineAnalyzer: &streamingpb.StreamingInlineAnalyzer{AnalyzerParams: params}},
	}
}

func TestAnalyzerNativeTransport(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.NewStreamingServiceUnaryServerInterceptor()))
	handler := NewHandlerService(nil) // Inline analyzers do not require any WAL or QN.
	streamingpb.RegisterStreamingNodeHandlerServiceServer(server, handler)
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///analyzer", grpc.WithUnaryInterceptor(interceptor.NewStreamingServiceUnaryClientInterceptor()), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	defer conn.Close()
	client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.RunAnalyzer(ctx, inlineAnalyzerRequest(`{"tokenizer":"standard"}`))
	require.NoError(t, err)
	require.Len(t, resp.Results, 2)
	require.Equal(t, "Hello", resp.Results[0].Tokens[0].Token)
	require.Equal(t, int64(0), resp.Results[0].Tokens[0].StartOffset)
	require.NotZero(t, resp.Results[0].Tokens[0].Hash)
	require.Empty(t, resp.Results[1].Tokens)
	for _, req := range []*streamingpb.StreamingNodeRunAnalyzerRequest{{}, inlineAnalyzerRequest(`{"tokenizer":"not_a_tokenizer"}`)} {
		resp, err = client.RunAnalyzer(ctx, req)
		require.Error(t, err)
		require.Nil(t, resp)
		require.True(t, status.AsStreamingError(err).IsInvalidArgument(), "%v", err)
	}
}

type analyzerManager struct{ walmanager.Manager }

func (*analyzerManager) GetAvailableWAL(types.PChannelInfo) (wal.WAL, error) {
	panic("unpatched")
}

type fieldAnalyzerWAL struct{ wal.WAL }

func (*fieldAnalyzerWAL) RunAnalyzer(context.Context, *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
	panic("unpatched")
}

func TestAnalyzerFieldNativeTransport(t *testing.T) {
	mockey.PatchConvey("field ownership and native error projection", t, func() {
		version := int32(3)
		req := &streamingpb.StreamingNodeRunAnalyzerRequest{Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{
			CollectionId: 7, Vchannel: "p_7v0", FieldId: 101, SchemaVersion: &version,
			Pchannel: &streamingpb.PChannelInfo{Name: "p", Term: 2},
		}}}
		var ownershipErr, executionErr error
		mockey.Mock((*analyzerManager).GetAvailableWAL).To(func(_ *analyzerManager, channel types.PChannelInfo) (wal.WAL, error) {
			require.Equal(t, "p", channel.Name)
			require.Equal(t, int64(2), channel.Term)
			return &fieldAnalyzerWAL{}, ownershipErr
		}).Build()
		execute := mockey.Mock((*fieldAnalyzerWAL).RunAnalyzer).To(func(_ *fieldAnalyzerWAL, _ context.Context, got *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
			require.Equal(t, int64(7), got.GetFieldAnalyzer().GetCollectionId())
			if executionErr != nil {
				return nil, executionErr
			}
			return &streamingpb.StreamingNodeRunAnalyzerResponse{}, nil
		}).Build()
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.NewStreamingServiceUnaryServerInterceptor()))
		streamingpb.RegisterStreamingNodeHandlerServiceServer(server, NewHandlerService(&analyzerManager{}))
		go server.Serve(listener)
		defer server.Stop()
		conn, err := grpc.NewClient("passthrough:///field-analyzer", grpc.WithUnaryInterceptor(interceptor.NewStreamingServiceUnaryClientInterceptor()), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		defer conn.Close()
		client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = client.RunAnalyzer(ctx, req)
		require.NoError(t, err)
		ownershipErr = status.NewUnmatchedChannelTerm("p", 2, 3)
		_, err = client.RunAnalyzer(ctx, req)
		require.True(t, status.AsStreamingError(err).IsWrongStreamingNode())
		require.Equal(t, 1, execute.Times())
		ownershipErr = nil
		executionErr = merr.ErrCollectionSchemaVersionNotReady
		_, err = client.RunAnalyzer(ctx, req)
		require.True(t, status.AsStreamingError(err).IsSchemaVersionMismatch())
		req.GetFieldAnalyzer().Pchannel.Name = "other"
		_, err = client.RunAnalyzer(ctx, req)
		require.True(t, status.AsStreamingError(err).IsInvalidArgument())
		require.Equal(t, 2, execute.Times())
	})
}

func TestAnalyzerGracefulStopWaitsForInline(t *testing.T) {
	mockey.PatchConvey("gRPC owns inline request shutdown", t, func() {
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		mockey.Mock(analyzer.Run).To(func(context.Context, string, [][]byte, bool, bool) ([]*milvuspb.AnalyzerResult, error) {
			close(entered)
			<-release
			return nil, nil
		}).Build()
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer()
		streamingpb.RegisterStreamingNodeHandlerServiceServer(server, NewHandlerService(nil))
		go server.Serve(listener)
		defer server.Stop()
		conn, err := grpc.NewClient("passthrough:///inline-shutdown", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := streamingpb.NewStreamingNodeHandlerServiceClient(conn).RunAnalyzer(ctx, inlineAnalyzerRequest("{}"))
			done <- err
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		stopped := make(chan struct{})
		go func() { server.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
			t.Fatal("stopped with an inline request in flight")
		case <-time.After(50 * time.Millisecond):
		}
		once.Do(func() { close(release) })
		require.NoError(t, <-done)
		select {
		case <-stopped:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	})
}
