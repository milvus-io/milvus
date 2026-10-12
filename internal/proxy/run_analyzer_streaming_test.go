package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	snhandler "github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Only the method patched by Mockey is used; unexpected calls fail immediately.
type analyzerTestClient struct{ snhandler.AnalyzerClient }

func (*analyzerTestClient) RunAnalyzer(context.Context, *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
	panic("unpatched analyzer call")
}

func TestFieldAnalyzerRouting(t *testing.T) {
	for _, scenario := range []string{"success", "analyzer failure", "transport failure", "drop recreate", "resource group"} {
		mockey.PatchConvey(scenario, t, func() {
			// No Coordinator client or load configuration is needed.
			task := &RunAnalyzerTask{baseTask: baseTask{MetaCache: &MetaCache{}}, collectionID: 7, RunAnalyzerRequest: &milvuspb.RunAnalyzerRequest{CollectionName: "c", FieldName: "text", Placeholder: [][]byte{[]byte("hello")}}}
			mockStreamingAnalyzerClient()
			id := int64(7)
			if scenario == "drop recreate" {
				id = 8
			}
			calls := 0
			mockey.Mock((*MetaCache).GetCollectionID).To(func(*MetaCache, context.Context, string, string) (int64, error) { return id, nil }).Build()
			mockey.Mock((*MetaCache).GetCollectionInfo).To(func(_ *MetaCache, _ context.Context, _, name string, collectionID int64) (*collectionInfo, error) {
				require.Empty(t, name)
				require.Equal(t, int64(7), collectionID)
				return &collectionInfo{CollID: 7, VChannels: []string{"test_7v0"}, Schema: mustNewSchemaInfo(&schemapb.CollectionSchema{Version: 1, Fields: []*schemapb.FieldSchema{{Name: "text", FieldID: 101}}})}, nil
			}).Build()
			refresh := mockey.Mock((*MetaCache).RemoveCollection).To(func(*MetaCache, context.Context, string, string) {
				t.Fatal("analyzer calls must not refresh Proxy metadata")
			}).Build()
			mockey.Mock((*analyzerTestClient).RunAnalyzer).To(func(_ *analyzerTestClient, _ context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				calls++
				require.Equal(t, int64(7), req.GetFieldAnalyzer().GetCollectionId())
				require.Equal(t, "test_7v0", req.GetFieldAnalyzer().GetVchannel())
				require.Equal(t, int64(101), req.GetFieldAnalyzer().GetFieldId())
				if scenario == "transport failure" {
					return nil, status.NewOnShutdownError("closing")
				}
				if scenario == "analyzer failure" {
					return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Status(merr.ErrServiceInternal)}, nil
				}
				return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Success(), Results: []*milvuspb.AnalyzerResult{{}}}, nil
			}).Build()
			ctx := context.Background()
			if scenario == "resource group" {
				ctx = extension.WithQueryResourceGroup(ctx, "rg")
			}
			resp, err := task.runFieldAnalyzer(ctx)
			switch scenario {
			case "analyzer failure":
				require.NoError(t, err)
				require.Equal(t, merr.Status(merr.ErrServiceInternal), resp.GetStatus())
				require.Equal(t, 1, calls)
				require.Zero(t, refresh.Times())
			case "drop recreate":
				require.ErrorIs(t, err, merr.ErrCollectionNotFound)
				require.Zero(t, calls)
			case "transport failure":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
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

func (*analyzerTestWAL) AnalyzerClient() snhandler.AnalyzerClient { panic("unpatched analyzer client") }

func mockStreamingAnalyzerClient() {
	mockey.Mock(streaming.WAL).Return(&analyzerTestWAL{}).Build()
	mockey.Mock((*analyzerTestWAL).AnalyzerClient).Return(&analyzerTestClient{}).Build()
}
