package analyzer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestHighlightOperation(t *testing.T) {
	req := &streamingpb.StreamingNodeRunAnalyzerRequest{
		Placeholder: [][]byte{[]byte("hello"), []byte("世界"), []byte("hello 世界 👋"), []byte("missing"), []byte("hello")},
		Operation: &streamingpb.StreamingNodeRunAnalyzerRequest_Highlight{Highlight: &streamingpb.HighlightOptions{
			SearchTextNum: 1, CorpusTextNum: 3, Queries: []*querypb.HighlightQuery{{}}, Topks: []int64{3},
			Options: &querypb.HighlightOptions{FragmentSize: 100, NumOfFragments: 5},
		}},
	}
	for _, scenario := range []string{"success", "native failure", "cancel", "malformed results", "bad shape"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			bindings, released := 0, false
			resp := RunOperation(ctx, req, func(use func(BatchRun, []string) error) error {
				bindings++
				defer func() { released = true }()
				return use(func(texts [][]byte, detail, hash bool, names []string) ([]*milvuspb.AnalyzerResult, error) {
					require.True(t, detail)
					require.False(t, hash)
					if scenario == "native failure" {
						return nil, merr.ErrIoPermissionDenied
					}
					if scenario == "malformed results" {
						return nil, nil
					}
					results, err := Run(ctx, `{"tokenizer":"standard"}`, texts, detail, hash)
					if scenario == "cancel" {
						cancel()
					}
					if scenario == "bad shape" {
						results = append(results, &milvuspb.AnalyzerResult{})
					}
					return results, err
				}, nil)
			})
			require.Equal(t, 1, bindings)
			require.True(t, released)
			if scenario != "success" {
				require.Error(t, merr.Error(resp.GetStatus()))
				require.Nil(t, resp.GetResult())
				return
			}
			require.NoError(t, merr.Error(resp.GetStatus()))
			require.Nil(t, resp.GetTokenize())
			results := resp.GetHighlight().GetResults()
			require.Len(t, results, 3)
			require.Equal(t, []int64{0, 5, 6, 8}, results[0].GetFragments()[0].GetOffsets())
			require.Empty(t, results[1].GetFragments())
			require.Equal(t, []int64{0, 5}, results[2].GetFragments()[0].GetOffsets())
		})
	}
}
