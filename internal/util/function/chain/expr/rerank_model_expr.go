/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package expr

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RerankModelExpr implements FunctionExpr for model-based reranking.
// It takes a text column as input, calls an external rerank service per-chunk (per-NQ),
// and outputs a new score column.
//
// Expected inputs (passed from MapOp):
//   - inputs[0]: VarChar column containing document texts
//
// Outputs:
//   - outputs[0]: Float32 score column from the rerank model
type RerankModelExpr struct {
	BaseExpr
	provider rerank.ModelProvider
	queries  []string // one query string per NQ (chunk)
}

// NewRerankModelExpr creates a new RerankModelExpr with the given provider and queries.
func NewRerankModelExpr(provider rerank.ModelProvider, queries []string) (*RerankModelExpr, error) {
	if provider == nil {
		return nil, merr.WrapErrServiceInternal("model: provider is nil")
	}
	if len(queries) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("model: queries must not be empty")
	}
	return &RerankModelExpr{
		BaseExpr: *NewBaseExpr("model", []string{types.StageL2Rerank}),
		provider: provider,
		queries:  queries,
	}, nil
}

// OutputDataTypes returns the data types of output columns.
// RerankModelExpr outputs a single Float32 column (the model score).
func (m *RerankModelExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

// Execute calls the external rerank service for each chunk (NQ) and returns model scores.
func (m *RerankModelExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if len(inputs) != 1 {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("model: expected 1 input column (text), got %d", len(inputs)))
	}

	textCol := inputs[0]
	numChunks := len(textCol.Chunks())

	if len(m.queries) != numChunks {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("model: queries count (%d) != nq count (%d)", len(m.queries), numChunks))
	}

	scoreChunks := make([]arrow.Array, numChunks)
	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		chunk := textCol.Chunk(chunkIdx)
		query := m.queries[chunkIdx]

		scoreArr, err := m.processChunk(ctx, chunk, query)
		if err != nil {
			for i := 0; i < chunkIdx; i++ {
				scoreChunks[i].Release()
			}
			return nil, err
		}
		scoreChunks[chunkIdx] = scoreArr
	}

	result := arrow.NewChunked(arrow.PrimitiveTypes.Float32, scoreChunks)
	for _, chunk := range scoreChunks {
		chunk.Release()
	}

	return []*arrow.Chunked{result}, nil
}

// processChunk extracts texts from a single chunk, calls the provider in batches,
// and returns a Float32 array of scores.
func (m *RerankModelExpr) processChunk(ctx *types.FuncContext, chunk arrow.Array, query string) (arrow.Array, error) {
	stringArr, ok := chunk.(*array.String)
	if !ok {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("model: input column must be String/VarChar, got %T", chunk))
	}

	n := stringArr.Len()
	if n == 0 {
		builder := array.NewFloat32Builder(ctx.Pool())
		defer builder.Release()
		return builder.NewArray(), nil
	}

	// Extract texts, treating nulls as empty strings
	texts := make([]string, n)
	for i := 0; i < n; i++ {
		if stringArr.IsNull(i) {
			texts[i] = ""
		} else {
			texts[i] = stringArr.Value(i)
		}
	}

	// Call provider in batches
	scores, err := m.rerankBatch(ctx.Context(), query, texts)
	if err != nil {
		return nil, err
	}

	// Build Float32 array
	builder := array.NewFloat32Builder(ctx.Pool())
	defer builder.Release()
	builder.AppendValues(scores, nil)

	return builder.NewArray(), nil
}

// rerankBatch calls the provider's rerank API in batches and returns all scores.
func (m *RerankModelExpr) rerankBatch(ctx context.Context, query string, texts []string) ([]float32, error) {
	maxBatch := m.provider.MaxBatch()
	if maxBatch <= 0 {
		maxBatch = len(texts)
	}

	scores := make([]float32, 0, len(texts))
	for i := 0; i < len(texts); i += maxBatch {
		end := i + maxBatch
		if end > len(texts) {
			end = len(texts)
		}
		batchScores, err := m.provider.Rerank(ctx, query, texts[i:end])
		if err != nil {
			return nil, err
		}
		if len(batchScores) != end-i {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("model: rerank service returned %d scores for %d docs", len(batchScores), end-i))
		}
		scores = append(scores, batchScores...)
	}

	return scores, nil
}
