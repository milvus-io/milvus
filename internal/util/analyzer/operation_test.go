package analyzer

import (
	"context"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/textmatch"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestPhraseMatchNativeParity(t *testing.T) {
	cases := [][2]string{
		{"hello world", "hello world"},
		{"hello world", "world hello"},
		{"hello world", "hello there"},
		{"A B", "A C D E B F G A B"},
		{"A B A", "A B A"},
		{"A A", "A"},
		{"A C", "A B C"},
		{"I like swimming and skiing", "Skiing and swimming are sports that I like the most."},
		{"中文 测试", "测试 的 中文"},
		{"the", "hello"},
		{"hello", ""},
	}
	rng := rand.New(rand.NewSource(42))
	words := []string{"a", "b", "c", "d", "the"}
	text := func(size int) string {
		tokens := make([]string, size)
		for i := range tokens {
			tokens[i] = words[rng.Intn(len(words))]
		}
		return strings.Join(tokens, " ")
	}
	for i := 0; i < 100; i++ {
		cases = append(cases, [2]string{text(1 + rng.Intn(5)), text(rng.Intn(20))})
	}
	for _, params := range []string{
		`{"tokenizer":"standard"}`,
		`{"tokenizer":"whitespace"}`,
		`{"tokenizer":"jieba"}`,
		`{"tokenizer":"standard","filter":["lowercase",{"type":"stop","stop_words":["the"]}]}`,
	} {
		for _, pair := range cases {
			results, err := Run(t.Context(), params, [][]byte{[]byte(pair[0]), []byte(pair[1])}, true, false)
			require.NoError(t, err)
			got := phraseMatchSlop(results[0].GetTokens(), results[1].GetTokens())
			want, nativeErr := textmatch.ComputePhraseMatchSlop(params, pair[0], pair[1])
			if nativeErr != nil {
				require.Equal(t, unmatchedPhraseSlop, got, "%s %v", params, pair)
			} else {
				require.Equal(t, int64(want), got, "%s %v", params, pair)
			}
		}
	}
}

func TestAnalyzerOperation(t *testing.T) {
	req := &streamingpb.StreamingNodeRunAnalyzerRequest{
		Placeholder: [][]byte{[]byte("a x b"), []byte("missing")},
		Operation:   &streamingpb.StreamingNodeRunAnalyzerRequest_PhraseMatch{PhraseMatch: &streamingpb.PhraseMatchOptions{QueryText: "a b"}},
		Source:      &streamingpb.StreamingNodeRunAnalyzerRequest_FieldAnalyzer{FieldAnalyzer: &streamingpb.StreamingFieldAnalyzer{AnalyzerNames: []string{"en", "zh"}}},
	}
	run := func(texts [][]byte, detail, hash bool, names []string) ([]*milvuspb.AnalyzerResult, error) {
		require.True(t, detail)
		require.False(t, hash)
		require.Equal(t, []string{"en", "en", "zh", "zh"}, names)
		return Run(t.Context(), `{"tokenizer":"standard"}`, texts, detail, hash)
	}
	resp := RunOperation(t.Context(), req, run)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Nil(t, resp.GetTokenize())
	require.Equal(t, []bool{true, false}, resp.GetPhraseMatch().GetIsMatch())
	require.Equal(t, []int64{1, -1}, resp.GetPhraseMatch().GetSlops())
	for _, failure := range []error{merr.ErrIoPermissionDenied, merr.ErrParameterInvalid} {
		resp = RunOperation(t.Context(), req, func([][]byte, bool, bool, []string) ([]*milvuspb.AnalyzerResult, error) { return nil, failure })
		require.ErrorIs(t, merr.Error(resp.GetStatus()), failure)
		require.Nil(t, resp.GetResult())
	}
	resp = RunOperation(t.Context(), req, func([][]byte, bool, bool, []string) ([]*milvuspb.AnalyzerResult, error) { return nil, nil })
	require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceInternal)
	ctx, cancel := context.WithCancel(t.Context())
	resp = RunOperation(ctx, req, func(texts [][]byte, detail, hash bool, names []string) ([]*milvuspb.AnalyzerResult, error) {
		results, err := run(texts, detail, hash, names)
		cancel() // Native execution completed after the caller left.
		return results, err
	})
	require.Equal(t, merr.Code(context.Canceled), resp.GetStatus().GetCode())
	require.Nil(t, resp.GetResult(), "cancellation must not publish partial results")
	req.GetPhraseMatch().QueryText = ""
	resp = RunOperation(t.Context(), req, nil)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.NotNil(t, resp.GetPhraseMatch())
	require.Empty(t, resp.GetPhraseMatch().GetSlops())
	req.GetPhraseMatch().QueryText = "a b"
	req.Placeholder = nil
	resp = RunOperation(t.Context(), req, nil)
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Empty(t, resp.GetPhraseMatch().GetSlops())
	req.Operation = nil
	resp = RunOperation(t.Context(), req, nil)
	require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrParameterInvalid)
	resp = RunOperation(ctx, req, nil)
	require.Equal(t, merr.Code(context.Canceled), resp.GetStatus().GetCode())
}

func TestAnalyzerTokenizeOperation(t *testing.T) {
	req := &streamingpb.StreamingNodeRunAnalyzerRequest{
		Placeholder: [][]byte{[]byte("hello")},
		Operation:   &streamingpb.StreamingNodeRunAnalyzerRequest_Tokenize{Tokenize: &streamingpb.TokenizeOptions{WithDetail: true, WithHash: true}},
	}
	resp := RunOperation(t.Context(), req, func(texts [][]byte, detail, hash bool, names []string) ([]*milvuspb.AnalyzerResult, error) {
		require.True(t, detail)
		require.True(t, hash)
		require.Nil(t, names)
		return Run(t.Context(), `{"tokenizer":"standard"}`, texts, detail, hash)
	})
	require.NoError(t, merr.Error(resp.GetStatus()))
	require.Nil(t, resp.GetPhraseMatch())
	require.Equal(t, "hello", resp.GetTokenize().GetResults()[0].GetTokens()[0].GetToken())
	require.NotZero(t, resp.GetTokenize().GetResults()[0].GetTokens()[0].GetHash())
	resp = RunOperation(t.Context(), req, func([][]byte, bool, bool, []string) ([]*milvuspb.AnalyzerResult, error) {
		return nil, merr.ErrIoPermissionDenied
	})
	require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrIoPermissionDenied)
	require.Nil(t, resp.GetResult())
}
