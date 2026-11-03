package handler

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestHandlerMessageSizeFailureStopsRetry(t *testing.T) {
	for _, scenario := range []string{"client send", "server receive", "server send", "client receive", "decompressed receive"} {
		mockey.PatchConvey(scenario, t, func() {
			var served atomic.Int32
			mockey.Mock(streamingpb.UnimplementedStreamingNodeHandlerServiceServer.RunAnalyzer).To(func(_ streamingpb.UnimplementedStreamingNodeHandlerServiceServer, _ context.Context, _ *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				served.Add(1)
				return &streamingpb.StreamingNodeRunAnalyzerResponse{Result: &streamingpb.StreamingNodeRunAnalyzerResponse_Tokenize{Tokenize: &streamingpb.TokenizeResult{Results: []*milvuspb.AnalyzerResult{{Tokens: []*milvuspb.AnalyzerToken{{Token: strings.Repeat("z", 1024)}}}}}}}, nil
			}).Build()
			listener := bufconn.Listen(16 * 1024)
			serverOptions := []grpc.ServerOption{grpc.UnaryInterceptor(interceptor.NewServiceUnaryServerInterceptor())}
			if scenario == "server receive" {
				serverOptions = append(serverOptions, grpc.MaxRecvMsgSize(256))
			}
			if scenario == "server send" {
				serverOptions = append(serverOptions, grpc.MaxSendMsgSize(256))
			}
			server := grpc.NewServer(serverOptions...)
			streamingpb.RegisterStreamingNodeHandlerServiceServer(server, streamingpb.UnimplementedStreamingNodeHandlerServiceServer{})
			go server.Serve(listener)
			defer server.Stop()
			conn, err := grpc.NewClient("passthrough:///size-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(interceptor.NewServiceUnaryClientInterceptor()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			require.NoError(t, err)
			defer conn.Close()
			rpc := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
			request := &streamingpb.StreamingNodeRunAnalyzerRequest{}
			var opts []grpc.CallOption
			if scenario == "client send" || scenario == "server receive" {
				request.Placeholder = [][]byte{[]byte(strings.Repeat("x", 1024))}
			}
			if scenario == "client send" {
				opts = append(opts, grpc.MaxCallSendMsgSize(256))
			}
			if scenario == "client receive" || scenario == "decompressed receive" {
				opts = append(opts, grpc.MaxCallRecvMsgSize(256))
			}
			if scenario == "decompressed receive" {
				opts = append(opts, grpc.UseCompressor("gzip"))
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			attempts := 0
			hc := &handlerClientImpl{}
			_, err = hc.createHandlerAfterStreamingNodeReady(ctx, mlog.With(), "", func(ctx context.Context, _ *types.PChannelInfoAssigned) (any, error) {
				attempts++
				return rpc.RunAnalyzer(ctx, request, opts...)
			})
			require.Equal(t, codes.ResourceExhausted, grpcstatus.Code(err), "%v", err)
			require.Equal(t, 1, attempts, "a deterministic transport failure must not re-execute")
			if scenario == "client send" || scenario == "server receive" {
				require.Zero(t, served.Load())
			} else {
				require.EqualValues(t, 1, served.Load())
			}
		})
	}
}

func TestHandlerResourcePressureStillRetries(t *testing.T) {
	for _, failure := range []error{
		grpcstatus.Error(codes.ResourceExhausted, "server temporarily busy"),
		status.ConvertStreamingError("RunAnalyzer", status.NewGRPCStatusFromStreamingError(status.NewRateLimitRejected("slow down")).Err()),
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			attempts := 0
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			hc := &handlerClientImpl{}
			result, err := hc.createHandlerAfterStreamingNodeReady(ctx, mlog.With(), "", func(context.Context, *types.PChannelInfoAssigned) (any, error) {
				attempts++
				if attempts == 1 {
					return nil, failure
				}
				return "ready", nil
			})
			require.NoError(t, err)
			require.Equal(t, "ready", result)
			require.Equal(t, 2, attempts)
		})
	}
}
