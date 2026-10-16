package service

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/fileresource"
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
	resp, err = client.RunAnalyzer(ctx, &streamingpb.StreamingNodeRunAnalyzerRequest{})
	require.Error(t, err)
	require.Nil(t, resp)
	require.True(t, status.AsStreamingError(err).IsInvalidArgument(), "%v", err)
	for _, params := range []string{`{"tokenizer":"not_a_tokenizer"}`, `{"tokenizer":`, `[]`} {
		resp, err = client.RunAnalyzer(ctx, inlineAnalyzerRequest(params))
		require.NoError(t, err)
		require.Error(t, merr.Error(resp.GetStatus()))
		_, analyzerErr := analyzer.Run(ctx, params, [][]byte{[]byte("Hello")}, false, false)
		require.True(t, proto.Equal(merr.Status(analyzerErr), resp.GetStatus()), "%s: %v", params, resp.GetStatus())
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
	mockey.PatchConvey("field ownership and analyzer response status", t, func() {
		req := &streamingpb.StreamingNodeRunAnalyzerRequest{Source: &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{
			CollectionId: 7, Vchannel: "p_7v0", FieldId: 101,
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
			return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Status(executionErr)}, nil
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
		executionErr = merr.ErrServiceInternal
		resp, err := client.RunAnalyzer(ctx, req)
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceInternal)
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

func TestValidateAnalyzerPreparesResources(t *testing.T) {
	mockey.PatchConvey("prepare before validating, deduplicate references", t, func() {
		prepared := false
		var prepareErr error
		prepare := mockey.Mock(fileresource.Prepare).To(func(context.Context) error {
			prepared = true
			return prepareErr
		}).Build()
		native := mockey.Mock(analyzer.ValidateAnalyzer).To(func(string, string) ([]int64, error) {
			require.True(t, prepared)
			return []int64{10, 20, 10}, nil
		}).Build()
		handler := NewHandlerService(nil)
		req := &streamingpb.StreamingNodeValidateAnalyzerRequest{AnalyzerInfos: []*streamingpb.StreamingAnalyzerInfo{{Params: "{}"}, {Params: "{}"}}}
		resp, err := handler.ValidateAnalyzer(context.Background(), req)
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{10, 20}, resp.ResourceIds)
		require.Equal(t, 2, native.Times())
		prepareErr = merr.ErrServiceUnavailable
		resp, err = handler.ValidateAnalyzer(context.Background(), req)
		require.ErrorIs(t, err, prepareErr)
		require.Nil(t, resp)
		require.Equal(t, 2, native.Times())
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = handler.ValidateAnalyzer(ctx, req)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 2, prepare.Times())
	})
}

func TestValidateAnalyzerNativeTransport(t *testing.T) {
	mockey.PatchConvey("native validation failures use response Status", t, func() {
		mockey.Mock(fileresource.Prepare).Return(nil).Build()
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.NewStreamingServiceUnaryServerInterceptor()))
		streamingpb.RegisterStreamingNodeHandlerServiceServer(server, NewHandlerService(nil))
		go server.Serve(listener)
		defer server.Stop()
		conn, err := grpc.NewClient("passthrough:///validate-analyzer", grpc.WithUnaryInterceptor(interceptor.NewStreamingServiceUnaryClientInterceptor()), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		defer conn.Close()
		client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, params := range []string{
			`{"tokenizer":"standard","filter":[{}]}`,
			`{"tokenizer":"standard","filter":[{"type":1}]}`,
			`{"tokenizer":"standard","filter":[{"type":"unknown_filter"}]}`,
			`{"tokenizer":"standard","filter":[{"type":"length"}]}`,
			`{"tokenizer":"standard","filter":[{"type":"length","max":"10"}]}`,
			`{"tokenizer":"standard","filter":[{"type":"pinyin","keep_original":"yes"}]}`,
			`{"tokenizer":"standard","filter":[{"type":"pinyin","keep_full_pinyin":"yes"}]}`,
			`{"tokenizer":"standard","filter":[{"type":"pinyin","keep_joined_full_pinyin":"yes"}]}`,
			`{"tokenizer":"standard","filter":[{"type":"pinyin","keep_separate_first_letter":"yes"}]}`,
			`{"tokenizer":"standard","filter":[{"type":"regex"}]}`,
			`{"tokenizer":"standard","filter":[{"type":"regex","expr":1}]}`,
			`{"tokenizer":"standard","filter":[{"type":"decompounder","word_list":"word"}]}`,
			`{"tokenizer":"standard","filter":[{"type":"decompounder","word_list":[1]}]}`,
			`{"tokenizer":"standard","filter":[{"type":"decompounder","word_list":[]}]}`,
			`{"tokenizer":"standard","filter":[{"type":"stop","stop_words":"word"}]}`,
			`{"tokenizer":"standard","filter":[{"type":"stop","stop_words":[1]}]}`,
			`{"tokenizer":"standard","filter":[{"type":"synonym","synonyms":[1]}]}`,
			`{"tokenizer":{"type":"lang_ident","identifier":{"type":"whatlang","confidence":"high"}}}`,
			`{"tokenizer":"standard"}`,
			`{"tokenizer":"not_a_tokenizer"}`,
			`{"type":"klingon"}`,
			`{"tokenizer":"standard","filter":[{"type":"stemmer","language":"xx"}]}`,
			`{"tokenizer":"standard","filter":["unknown_filter"]}`,
			`{"tokenizer":"standard","unknown_option":true}`,
			`{"tokenizer":`,
			`[]`,
			`null`,
		} {
			resp, err := client.ValidateAnalyzer(ctx, &streamingpb.StreamingNodeValidateAnalyzerRequest{AnalyzerInfos: []*streamingpb.StreamingAnalyzerInfo{{Params: params, Field: "body"}}})
			if params == `{"tokenizer":"standard"}` {
				require.NoError(t, err)
				require.Empty(t, resp.ResourceIds)
			} else {
				require.NoError(t, err)
				require.Equal(t, merr.InputError, merr.GetErrorType(merr.Error(resp.GetStatus())), "%s: %v", params, resp.GetStatus())
			}
		}
		var nativeErr error
		mockey.Mock(analyzer.ValidateAnalyzer).To(func(string, string) ([]int64, error) {
			return nil, nativeErr
		}).Build()
		for _, failure := range []error{merr.ErrServiceUnavailable, merr.ErrServiceInternal} {
			nativeErr = failure
			resp, err := client.ValidateAnalyzer(ctx, &streamingpb.StreamingNodeValidateAnalyzerRequest{AnalyzerInfos: []*streamingpb.StreamingAnalyzerInfo{{Params: "{}"}}})
			require.NoError(t, err)
			require.Equal(t, merr.Code(failure), resp.GetStatus().GetCode())
			require.Equal(t, merr.IsRetryableErr(failure), resp.GetStatus().GetRetriable())
			require.Equal(t, merr.SystemError, merr.GetErrorType(merr.Error(resp.GetStatus())))
		}
	})
}

func TestAnalyzerPreparationErrorClassification(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		terminal bool
	}{
		{"missing object", merr.WrapErrIoKeyNotFound("dictionary.txt"), true},
		{"missing bucket", merr.Wrap(merr.ErrIoBucketNotFound, "bucket"), true},
		{"permission denied", merr.Wrap(merr.ErrIoPermissionDenied, "dictionary.txt"), true},
		{"not ready", merr.ErrServiceUnavailable, false},
		{"network unavailable", grpcstatus.Error(codes.Unavailable, "connection lost"), false},
		{"fetch timeout", context.DeadlineExceeded, false},
		{"canceled", context.Canceled, false},
	}
	for _, tc := range cases {
		mockey.PatchConvey(tc.name, t, func() {
			mockey.Mock(fileresource.Prepare).Return(tc.err).Build()
			native := mockey.Mock(analyzer.ValidateAnalyzer).To(func(string, string) ([]int64, error) {
				t.Fatal("failed preparation must not enter native validation")
				return nil, nil
			}).Build()
			handler := NewHandlerService(nil)
			_, validateErr := handler.ValidateAnalyzer(t.Context(), &streamingpb.StreamingNodeValidateAnalyzerRequest{})
			require.Error(t, validateErr)
			wireErr := status.AsStreamingError(validateErr)
			require.Equal(t, tc.terminal, wireErr.IsUnrecoverable())
			require.False(t, wireErr.IsInvalidArgument(), "storage state is not an analyzer parameter error")
			if !tc.terminal {
				require.ErrorIs(t, validateErr, tc.err)
			}
			require.Zero(t, native.Times())
		})
	}
}

func TestRunAnalyzerPreparationStatus(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.NewStreamingServiceUnaryServerInterceptor()))
	streamingpb.RegisterStreamingNodeHandlerServiceServer(server, NewHandlerService(nil))
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///analyzer-preparation", grpc.WithUnaryInterceptor(interceptor.NewStreamingServiceUnaryClientInterceptor()), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	defer conn.Close()
	client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
	for _, prepareErr := range []error{
		merr.WrapErrIoKeyNotFound("dictionary.txt"),
		merr.Wrap(merr.ErrIoPermissionDenied, "dictionary.txt"),
		merr.ErrServiceUnavailable,
		context.DeadlineExceeded,
	} {
		mockey.PatchConvey(prepareErr.Error(), t, func() {
			mockey.Mock(fileresource.Prepare).Return(prepareErr).Build()
			native := mockey.Mock(analyzer.Run).Return(nil, nil).Build()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			resp, err := client.RunAnalyzer(ctx, inlineAnalyzerRequest("{}"))
			require.NoError(t, err, "preparation failures must remain business Status across streaming interceptors")
			require.NotNil(t, resp)
			want := merr.Status(merr.Wrap(prepareErr, "prepare analyzer file resources"))
			require.True(t, proto.Equal(want, resp.GetStatus()), "want %v, got %v", want, resp.GetStatus())
			require.Equal(t, merr.Code(prepareErr), resp.GetStatus().GetCode())
			require.Equal(t, merr.IsRetryableErr(prepareErr), resp.GetStatus().GetRetriable())
			require.Equal(t, merr.GetErrorType(prepareErr), merr.GetErrorType(merr.Error(resp.GetStatus())))
			require.Empty(t, resp.GetResults())
			require.Zero(t, native.Times())
		})
	}
}

func TestValidateAnalyzerMissingResourceNativeTransport(t *testing.T) {
	mockey.PatchConvey("missing names are input only against a prepared sync inventory", t, func() {
		previous := fileresource.GlobalFileManager
		defer func() { fileresource.GlobalFileManager = previous }()
		mockey.Mock(fileresource.Prepare).Return(nil).Build()
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.NewStreamingServiceUnaryServerInterceptor()))
		streamingpb.RegisterStreamingNodeHandlerServiceServer(server, NewHandlerService(nil))
		go server.Serve(listener)
		defer server.Stop()
		conn, err := grpc.NewClient("passthrough:///resource-validation", grpc.WithUnaryInterceptor(interceptor.NewStreamingServiceUnaryClientInterceptor()), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		defer conn.Close()
		client := streamingpb.NewStreamingNodeHandlerServiceClient(conn)
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		params := `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"remote","resource_name":"missing_validation_dictionary","file_name":"stop.txt"}}]}`
		req := &streamingpb.StreamingNodeValidateAnalyzerRequest{AnalyzerInfos: []*streamingpb.StreamingAnalyzerInfo{{Params: params}}}
		for _, mode := range []fileresource.Mode{fileresource.SyncMode, fileresource.CloseMode} {
			fileresource.GlobalFileManager = fileresource.NewManager(nil, mode)
			resp, err := client.ValidateAnalyzer(ctx, req)
			require.NoError(t, err)
			require.Error(t, merr.Error(resp.GetStatus()))
			require.Equal(t, mode == fileresource.SyncMode, merr.GetErrorType(merr.Error(resp.GetStatus())) == merr.InputError)
		}
	})
}

func TestValidateAnalyzerMissingFileStatus(t *testing.T) {
	mockey.PatchConvey("missing local file is a validation result, not a transport failure", t, func() {
		mockey.Mock(fileresource.Prepare).Return(nil).Build()
		previous := fileresource.GlobalFileManager
		fileresource.GlobalFileManager = fileresource.NewManager(nil, fileresource.CloseMode)
		defer func() { fileresource.GlobalFileManager = previous }()
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer(grpc.UnaryInterceptor(interceptor.NewStreamingServiceUnaryServerInterceptor()))
		streamingpb.RegisterStreamingNodeHandlerServiceServer(server, NewHandlerService(nil))
		go server.Serve(listener)
		defer server.Stop()
		conn, err := grpc.NewClient("passthrough:///missing-validation-file", grpc.WithUnaryInterceptor(interceptor.NewStreamingServiceUnaryClientInterceptor()), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		defer conn.Close()
		params := `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"local","path":"` + filepath.Join(t.TempDir(), "missing.txt") + `"}}]}`
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		resp, err := streamingpb.NewStreamingNodeHandlerServiceClient(conn).ValidateAnalyzer(ctx, &streamingpb.StreamingNodeValidateAnalyzerRequest{AnalyzerInfos: []*streamingpb.StreamingAnalyzerInfo{{Params: params, Field: "body"}}})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Equal(t, int32(merr.CodePathNotExist), resp.GetStatus().GetCode())
		require.False(t, resp.GetStatus().GetRetriable())
		require.Equal(t, merr.SystemError, merr.GetErrorType(merr.Error(resp.GetStatus())))
		require.Contains(t, resp.GetStatus().GetReason(), "body")
		require.Contains(t, resp.GetStatus().GetReason(), "missing.txt")
		require.Contains(t, resp.GetStatus().GetReason(), "No such file or directory")
		require.Empty(t, resp.GetResourceIds())
	})
}
