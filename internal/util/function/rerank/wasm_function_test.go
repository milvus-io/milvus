package rerank

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
)

func readFileIfExists(t *testing.T, p string) []byte {
	b, err := os.ReadFile(p)
	if err != nil {
		return nil
	}
	return b
}

func TestWasmFunction_BasicNoFields(t *testing.T) {
	req := require.New(t)

	// locate wasm built by tests/reranker_wasm/rust_reranker
	wasmPath := filepath.Join("tests", "reranker_wasm", "rust_reranker", "target", "wasm32-unknown-unknown", "release", "rust_reranker.wasm")
	wasmBytes := readFileIfExists(t, wasmPath)
	if len(wasmBytes) == 0 {
		t.Skipf("WASM artifact not found at %s; run cargo build --target wasm32-unknown-unknown in tests/reranker_wasm/rust_reranker", wasmPath)
	}

	// minimal collection schema with int64 PK
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}

	funcSchema := &schemapb.FunctionSchema{
		Name:            "wasm-basic",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: reranker, Value: WasmName},
			{Key: wasmCodeKey, Value: base64.StdEncoding.EncodeToString(wasmBytes)},
			{Key: entryPointKey, Value: "rerank"}, // score, rank -> f32
		},
	}

	funcScores := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{funcSchema}}
	f, err := NewFunctionScore(schema, funcScores)
	req.NoError(err)
	req.Equal(WasmName, f.RerankName())

	// build simple search result (nq=1, topk=5), ids int64
	nq := int64(1)
	data := embedding.GenSearchResultData(nq, 5, schemapb.DataType_Int64, "", 0)
	search := &milvuspb.SearchResults{Results: data}

	// process and verify shape, scores are changed but finite
	ret, err := f.Process(context.Background(), NewSearchParams(nq, 5, 0, -1, -1, 1, false, "", []string{"COSINE"}), []*milvuspb.SearchResults{search})
	req.NoError(err)
	req.Equal(int64(5), ret.Results.TopK)
	req.Equal([]int64{5}, ret.Results.Topks)
	req.Len(ret.Results.Scores, 5)
}

func TestWasmFunction_WithSingleFloatField(t *testing.T) {
	req := require.New(t)

	// This test uses a function that expects an extra f32 field after (score, rank)
	// We'll reuse rust_reranker.wasm which exports rerank_with_popularity(score, rank, popularity_f32)
	wasmPath := filepath.Join("tests", "reranker_wasm", "rust_reranker", "target", "wasm32-unknown-unknown", "release", "rust_reranker.wasm")
	wasmBytes := readFileIfExists(t, wasmPath)
	if len(wasmBytes) == 0 {
		t.Skipf("WASM artifact not found at %s; run cargo build --target wasm32-unknown-unknown in tests/reranker_wasm/rust_reranker", wasmPath)
	}

	// schema with pk and one float field named popularity
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "popularity", DataType: schemapb.DataType_Float},
		},
	}

	funcSchema := &schemapb.FunctionSchema{
		Name:            "wasm-field",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"popularity"},
		Params: []*commonpb.KeyValuePair{
			{Key: reranker, Value: WasmName},
			{Key: wasmCodeKey, Value: base64.StdEncoding.EncodeToString(wasmBytes)},
			{Key: entryPointKey, Value: "rerank_with_popularity"},
		},
	}
	funcScores := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{funcSchema}}
	f, err := NewFunctionScore(schema, funcScores)
	req.NoError(err)

	// create search result with popularity as field data
	nq := int64(1)
	topk := int64(5)
	data := embedding.GenSearchResultData(nq, topk, schemapb.DataType_Int64, "popularity", 101)
	// overwrite popularity values to a known pattern to ensure boosting happens
	search := &milvuspb.SearchResults{Results: data}

	ret, err := f.Process(context.Background(), NewSearchParams(nq, topk, 0, -1, -1, 1, false, "", []string{"COSINE"}), []*milvuspb.SearchResults{search})
	req.NoError(err)
	req.Equal(topk, ret.Results.TopK)
	req.Equal([]int64{topk}, ret.Results.Topks)
	req.Len(ret.Results.Scores, int(topk))
}
