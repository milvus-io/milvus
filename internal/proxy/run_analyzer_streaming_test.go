package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Only the method patched by Mockey is used; unexpected calls fail immediately.
type analyzerTestClient struct{ streaming.AnalyzerClient }

func (*analyzerTestClient) RunAnalyzer(context.Context, *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
	panic("unpatched analyzer call")
}

func TestFieldAnalyzerRouting(t *testing.T) {
	paramtable.Init()
	for _, scenario := range []string{"success", "analyzer failure", "transport failure", "drop recreate", "resource group", "collection missing", "field missing", "no channel"} {
		mockey.PatchConvey(scenario, t, func() {
			// No scheduler, TSO allocator, Coordinator or load configuration is needed.
			node := &Proxy{metaCache: &MetaCache{}}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			request := &milvuspb.RunAnalyzerRequest{CollectionName: "c", FieldName: "text", Placeholder: [][]byte{[]byte("hello")}, WithDetail: true, WithHash: true, AnalyzerNames: []string{"default"}}
			mockStreamingAnalyzerClient()
			id := int64(7)
			calls := 0
			lookup := mockey.Mock((*MetaCache).GetCollectionID).To(func(*MetaCache, context.Context, string, string) (int64, error) {
				if scenario == "collection missing" {
					return 0, merr.WrapErrCollectionNotFound("c")
				}
				return id, nil
			}).Build()
			mockey.Mock((*MetaCache).GetCollectionInfo).To(func(_ *MetaCache, _ context.Context, _, name string, collectionID int64) (*collectionInfo, error) {
				require.Empty(t, name)
				require.Equal(t, int64(7), collectionID)
				if scenario == "drop recreate" {
					// The name now resolves to a new collection, but metadata must
					// still be requested using the original ID.
					id = 8
					return nil, merr.WrapErrCollectionNotFound(collectionID)
				}
				info := &collectionInfo{CollID: 7, VChannels: []string{"test_7v0"}, Schema: mustNewSchemaInfo(&schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{{Name: "text", FieldID: 101}}})}
				if scenario == "field missing" {
					info.Schema = mustNewSchemaInfo(&schemapb.CollectionSchema{})
				}
				if scenario == "no channel" {
					info.VChannels = nil
				}
				return info, nil
			}).Build()
			refresh := mockey.Mock((*MetaCache).RemoveCollection).To(func(*MetaCache, context.Context, string, string) {
				t.Fatal("analyzer calls must not refresh Proxy metadata")
			}).Build()
			mockey.Mock((*analyzerTestClient).RunAnalyzer).To(func(_ *analyzerTestClient, _ context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				calls++
				require.Equal(t, request.GetPlaceholder(), req.GetPlaceholder())
				require.True(t, req.GetTokenize().GetWithDetail())
				require.True(t, req.GetTokenize().GetWithHash())
				require.Equal(t, request.GetAnalyzerNames(), req.GetFieldAnalyzer().GetAnalyzerNames())
				require.Equal(t, int64(7), req.GetFieldAnalyzer().GetCollectionId())
				require.Equal(t, "test_7v0", req.GetFieldAnalyzer().GetVchannel())
				require.Equal(t, int64(101), req.GetFieldAnalyzer().GetFieldId())
				if scenario == "transport failure" {
					return nil, status.NewOnShutdownError("closing")
				}
				if scenario == "analyzer failure" {
					return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Status(merr.ErrServiceInternal)}, nil
				}
				return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Success(), Result: &streamingpb.StreamingNodeRunAnalyzerResponse_Tokenize{Tokenize: &streamingpb.TokenizeResult{Results: []*milvuspb.AnalyzerResult{{}}}}}, nil
			}).Build()
			ctx := context.Background()
			if scenario == "resource group" {
				ctx = extension.WithQueryResourceGroup(ctx, "rg")
			}
			resp, err := node.RunAnalyzer(ctx, request)
			require.NoError(t, err)
			require.Equal(t, 1, lookup.Times())
			switch scenario {
			case "analyzer failure":
				require.NoError(t, err)
				require.Equal(t, merr.Status(merr.ErrServiceInternal), resp.GetStatus())
				require.Equal(t, 1, calls)
				require.Zero(t, refresh.Times())
			case "drop recreate", "collection missing":
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrCollectionNotFound)
				require.Zero(t, calls)
			case "field missing":
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrFieldNotFound)
				require.Zero(t, calls)
			case "no channel":
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
				require.Zero(t, calls)
			case "transport failure":
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable)
				require.Equal(t, 1, calls, "Proxy must not wrap AnalyzerClient in another retry loop")
			default:
				require.NoError(t, err)
				require.Len(t, resp.GetResults(), 1)
			}
			require.Zero(t, refresh.Times())
		})
	}
}

type analyzerTestWAL struct{ streaming.WALAccesser }

func (*analyzerTestWAL) AnalyzerClient() streaming.AnalyzerClient { panic("unpatched analyzer client") }

func mockStreamingAnalyzerClient() {
	mockey.Mock(streaming.WAL).Return(&analyzerTestWAL{}).Build()
	mockey.Mock((*analyzerTestWAL).AnalyzerClient).Return(&analyzerTestClient{}).Build()
}

func TestComputePhraseMatchSlopStreaming(t *testing.T) {
	paramtable.Init()
	for _, scenario := range []string{"success", "business", "transport", "canceled", "unimplemented"} {
		mockey.PatchConvey(scenario, t, func() {
			node := &Proxy{} // No Coordinator, collection cache or query scheduler.
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			mockStreamingAnalyzerClient()
			call := mockey.Mock((*analyzerTestClient).RunAnalyzer).To(func(_ *analyzerTestClient, _ context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				require.Equal(t, `{}`, req.GetInlineAnalyzer().GetAnalyzerParams())
				require.Nil(t, req.GetTokenize())
				require.Equal(t, "a b", req.GetPhraseMatch().GetQueryText())
				require.Equal(t, [][]byte{[]byte("a b"), []byte("b a"), []byte("c")}, req.GetPlaceholder())
				switch scenario {
				case "business":
					return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Status(merr.ErrIoPermissionDenied)}, nil
				case "transport":
					return nil, status.NewOnShutdownError("closing")
				case "canceled":
					return nil, context.Canceled
				case "unimplemented":
					return nil, grpcstatus.Error(codes.Unimplemented, "unsupported")
				default:
					return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Success(), Result: &streamingpb.StreamingNodeRunAnalyzerResponse_PhraseMatch{PhraseMatch: &streamingpb.PhraseMatchResult{IsMatch: []bool{true, true, false}, Slops: []int64{0, 2, -1}}}}, nil
				}
			}).Build()
			resp, err := node.ComputePhraseMatchSlop(t.Context(), &milvuspb.ComputePhraseMatchSlopRequest{AnalyzerParams: `{}`, QueryText: "a b", DataTexts: []string{"a b", "b a", "c"}})
			require.NoError(t, err)
			require.Equal(t, 1, call.Times())
			switch scenario {
			case "success":
				require.NoError(t, merr.Error(resp.GetStatus()))
				require.Equal(t, []bool{true, true, false}, resp.GetIsMatch())
				require.Equal(t, []int64{0, 2, -1}, resp.GetSlops())
			case "business":
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrIoPermissionDenied)
			case "transport":
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable)
			case "canceled":
				require.Equal(t, merr.Code(context.Canceled), resp.GetStatus().GetCode())
			case "unimplemented":
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceUnimplemented)
			}
		})
	}
}

func TestRunAnalyzerStreamingErrors(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"canceled", context.Canceled, context.Canceled},
		{"deadline", context.DeadlineExceeded, context.DeadlineExceeded},
		{"unimplemented", status.ConvertStreamingError("RunAnalyzer", grpcstatus.Error(codes.Unimplemented, "older SN")), merr.ErrServiceUnimplemented},
		{"invalid envelope", status.NewInvalidArgument("bad envelope"), merr.ErrParameterInvalid},
		{"shutdown", status.NewOnShutdownError("closing"), merr.ErrServiceUnavailable},
	} {
		mockey.PatchConvey(tc.name, t, func() {
			node := &Proxy{}
			node.UpdateStateCode(commonpb.StateCode_Healthy)
			mockStreamingAnalyzerClient()
			call := mockey.Mock((*analyzerTestClient).RunAnalyzer).Return(nil, tc.err).Build()
			response, err := node.RunAnalyzer(context.Background(), &milvuspb.RunAnalyzerRequest{Placeholder: [][]byte{[]byte("hello")}})
			require.NoError(t, err)
			require.Equal(t, merr.Code(tc.want), response.GetStatus().GetCode())
			require.Equal(t, merr.IsRetryableErr(tc.want), response.GetStatus().GetRetriable())
			require.Equal(t, 1, call.Times())
		})
	}
}
