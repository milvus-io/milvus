package analyzer

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/analyzer/interfaces"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type runTestAnalyzer struct{ Analyzer }

func (*runTestAnalyzer) NewTokenStream(string) (interfaces.TokenStream, error) { panic("unpatched") }
func (*runTestAnalyzer) Destroy()                                              { panic("unpatched") }

func TestRunAnalyzerResources(t *testing.T) {
	for _, detail := range []bool{false, true} {
		results, err := Run(context.Background(), `{"tokenizer":"standard"}`, [][]byte{[]byte("Hello world"), {}}, detail, true)
		require.NoError(t, err)
		require.Equal(t, "Hello", results[0].Tokens[0].Token)
		require.NotZero(t, results[0].Tokens[0].Hash)
		require.Empty(t, results[1].Tokens)
	}
	_, err := Run(context.Background(), `{"tokenizer":"unknown"}`, nil, false, false)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = Run(ctx, "{}", nil, false, false)
	require.ErrorIs(t, err, context.Canceled)
	for _, cancelAfterCreate := range []bool{false, true} {
		mockey.PatchConvey("resources released on failure", t, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mockey.Mock(NewAnalyzer).To(func(string, string) (Analyzer, error) {
				if cancelAfterCreate {
					cancel()
				}
				return &runTestAnalyzer{}, nil
			}).Build()
			destroy := mockey.Mock((*runTestAnalyzer).Destroy).Return().Build()
			stream := mockey.Mock((*runTestAnalyzer).NewTokenStream).Return(nil, merr.ErrServiceUnavailable).Build()
			_, err := Run(ctx, "{}", [][]byte{[]byte("hello")}, false, false)
			require.Error(t, err)
			require.Equal(t, 1, destroy.Times())
			if cancelAfterCreate {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, stream.Times())
			}
		})
	}
}
