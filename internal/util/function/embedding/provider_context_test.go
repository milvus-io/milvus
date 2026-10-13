// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embedding

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/openai"
)

// This client cancels after a successful batch to exercise the boundary between
// batches independently of cancellation while an HTTP request is in flight.
type cancelingEmbeddingClient struct {
	cancel context.CancelFunc
	calls  int
	ctx    context.Context
}

func (c *cancelingEmbeddingClient) Check() error { return nil }

func (c *cancelingEmbeddingClient) Embedding(ctx context.Context, _ string, texts []string, _ int, _ string, _ int64) (*openai.EmbeddingResponse, error) {
	c.calls++
	c.ctx = ctx
	c.cancel()
	response := &openai.EmbeddingResponse{}
	for range texts {
		response.Data = append(response.Data, openai.EmbeddingData{Embedding: []float32{1, 2}})
	}
	return response, nil
}

func TestOpenAIEmbeddingCancellationBetweenBatches(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &cancelingEmbeddingClient{cancel: cancel}
	provider := &OpenAIEmbeddingProvider{client: client, fieldDim: 2, maxBatch: 1}
	_, err := provider.CallEmbedding(ctx, []string{"a", "b", "c"}, models.InsertMode)
	if !errors.Is(err, context.Canceled) || client.calls != 1 || client.ctx != ctx {
		t.Fatalf("expected caller context and one batch, got err=%v calls=%d ctx=%v", err, client.calls, client.ctx)
	}
}

func TestOpenAIEmbeddingCanceledBeforeFirstBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := &cancelingEmbeddingClient{cancel: cancel}
	provider := &OpenAIEmbeddingProvider{client: client, fieldDim: 2, maxBatch: 1}
	_, err := provider.CallEmbedding(ctx, []string{"a", "b", "c"}, models.InsertMode)
	if !errors.Is(err, context.Canceled) || client.calls != 0 {
		t.Fatalf("expected cancellation without a batch, got err=%v calls=%d", err, client.calls)
	}
}
