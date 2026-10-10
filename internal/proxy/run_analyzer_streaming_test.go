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
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Only the method patched by Mockey is used; unexpected calls fail immediately.
type analyzerTestClient struct{ snhandler.AnalyzerClient }

func (*analyzerTestClient) RunAnalyzer(context.Context, *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
	panic("unpatched analyzer call")
}

func TestFieldAnalyzerRoutingAndRefresh(t *testing.T) {
	for _, scenario := range []string{"success", "schema refresh", "schema exhausted", "analyzer failure", "drop recreate", "resource group"} {
		mockey.PatchConvey(scenario, t, func() {
			// No Coordinator client or load configuration is needed.
			task := &RunAnalyzerTask{baseTask: baseTask{MetaCache: &MetaCache{}}, collectionID: 7, RunAnalyzerRequest: &milvuspb.RunAnalyzerRequest{CollectionName: "c", FieldName: "text", Placeholder: [][]byte{[]byte("hello")}}}
			mockStreamingAnalyzerClient()
			id := int64(7)
			version := int32(1)
			calls := 0
			mockey.Mock((*MetaCache).GetCollectionID).To(func(*MetaCache, context.Context, string, string) (int64, error) { return id, nil }).Build()
			mockey.Mock((*MetaCache).GetCollectionInfo).To(func(_ *MetaCache, _ context.Context, _, name string, collectionID int64) (*collectionInfo, error) {
				require.Empty(t, name)
				require.Equal(t, int64(7), collectionID)
				return &collectionInfo{CollID: 7, VChannels: []string{"test_7v0"}, Schema: mustNewSchemaInfo(&schemapb.CollectionSchema{Version: version, Fields: []*schemapb.FieldSchema{{Name: "text", FieldID: int64(100 + version)}}})}, nil
			}).Build()
			refresh := mockey.Mock((*MetaCache).RemoveCollection).To(func(*MetaCache, context.Context, string, string) {
				version = 2
				if scenario == "drop recreate" {
					id = 8
				}
			}).Build()
			mockey.Mock((*analyzerTestClient).RunAnalyzer).To(func(_ *analyzerTestClient, _ context.Context, req *streamingpb.StreamingNodeRunAnalyzerRequest) (*streamingpb.StreamingNodeRunAnalyzerResponse, error) {
				calls++
				require.Equal(t, int64(7), req.GetFieldAnalyzer().GetCollectionId())
				require.Equal(t, "test_7v0", req.GetFieldAnalyzer().GetVchannel())
				require.Equal(t, int64(100+version), req.GetFieldAnalyzer().GetFieldId())
				require.Equal(t, version, req.GetFieldAnalyzer().GetSchemaVersion())
				if scenario == "schema exhausted" || calls == 1 && (scenario == "schema refresh" || scenario == "drop recreate") {
					return &streamingpb.StreamingNodeRunAnalyzerResponse{Status: merr.Status(merr.ErrCollectionSchemaVersionNotReady)}, nil
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
			case "schema exhausted":
				require.NoError(t, err)
				require.Equal(t, merr.Status(merr.ErrCollectionSchemaVersionNotReady), resp.GetStatus())
				require.Equal(t, 3, calls)
				require.Equal(t, 3, refresh.Times())
			case "analyzer failure":
				require.NoError(t, err)
				require.Equal(t, merr.Status(merr.ErrServiceInternal), resp.GetStatus())
				require.Equal(t, 1, calls)
				require.Zero(t, refresh.Times())
			case "drop recreate":
				require.ErrorIs(t, err, merr.ErrCollectionNotFound)
				require.Equal(t, 1, calls)
			default:
				require.NoError(t, err)
				require.Len(t, resp.GetResults(), 1)
			}
			if scenario == "schema refresh" {
				require.Equal(t, 2, calls)
				require.Equal(t, 1, refresh.Times())
			}
		})
	}
}

type analyzerTestWAL struct{ streaming.WALAccesser }

func (*analyzerTestWAL) AnalyzerClient() snhandler.AnalyzerClient { panic("unpatched analyzer client") }

func mockStreamingAnalyzerClient() {
	mockey.Mock(streaming.WAL).Return(&analyzerTestWAL{}).Build()
	mockey.Mock((*analyzerTestWAL).AnalyzerClient).Return(&analyzerTestClient{}).Build()
}
