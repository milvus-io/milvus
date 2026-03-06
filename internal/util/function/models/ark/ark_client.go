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

package ark

import (
	"context"
	"fmt"

	"github.com/volcengine/volcengine-go-sdk/service/arkruntime"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

// ArkClient wraps the Volcengine Ark SDK client for embedding operations.
type ArkClient struct {
	client *arkruntime.Client
}

// NewArkClient creates a new ArkClient with the given API key and base URL.
// The apiKey is required; if empty, an error is returned.
// The url specifies the Ark API endpoint (e.g., "https://ark.cn-beijing.volces.com/api/v3").
func NewArkClient(apiKey string, url string) (*ArkClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials config or configure the %s environment variable in the Milvus service.", models.ArkAKEnvStr)
	}

	opts := []arkruntime.ConfigOption{
		arkruntime.WithBaseUrl(url),
	}
	client := arkruntime.NewClientWithApiKey(apiKey, opts...)

	return &ArkClient{
		client: client,
	}, nil
}

// Embedding generates embeddings for the given texts using the specified model.
//
// Parameters:
//   - modelName: The Ark model endpoint ID (e.g., "ep-xxx").
//   - texts: The input texts to generate embeddings for.
//   - dim: Output embedding dimension (0 uses model default).
//   - user: Optional end-user identifier for abuse monitoring.
//   - isMultimodal: If true, uses the multimodal embedding API (for vision models).
//
// Returns a slice of embeddings corresponding to each input text.
func (c *ArkClient) Embedding(ctx context.Context, modelName string, texts []string, dim int, user string, isMultimodal bool) ([]model.Embedding, error) {
	if isMultimodal {
		// Note: The Ark SDK's MultiModalEmbeddingRequest does not support a User field,
		// so the user parameter is not used.
		return c.embeddingMultiModal(ctx, modelName, texts, dim)
	}

	req := model.EmbeddingRequestStrings{
		Input:          texts,
		Model:          modelName,
		User:           user,
		EncodingFormat: model.EmbeddingEncodingFormatFloat,
	}

	if dim > 0 {
		req.Dimensions = dim
	}

	res, err := c.client.CreateEmbeddings(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.Data, nil
}

// embeddingMultiModal handles multimodal embedding requests.
func (c *ArkClient) embeddingMultiModal(ctx context.Context, modelName string, texts []string, dim int) ([]model.Embedding, error) {
	out := make([]model.Embedding, len(texts))

	// SDK CreateMultiModalEmbeddings returns a response with a SINGLE Data item (MultimodalEmbedding).
	// It does not appear to support batch return in the current SDK version struct definition.
	// We must iterate and call individually.
	for i, text := range texts {
		txtVal := text
		input := model.MultimodalEmbeddingInput{
			Type: model.MultiModalEmbeddingInputTypeText,
			Text: &txtVal,
		}

		req := model.MultiModalEmbeddingRequest{
			Input: []model.MultimodalEmbeddingInput{input},
			Model: modelName,
		}

		if dim > 0 {
			req.Dimensions = &dim
		}

		res, err := c.client.CreateMultiModalEmbeddings(ctx, req)
		if err != nil {
			// Truncate text for error message to avoid overly long errors
			truncatedText := text
			if len(truncatedText) > 50 {
				truncatedText = truncatedText[:50] + "..."
			}
			return nil, fmt.Errorf("multimodal embedding request failed for text %q: %w", truncatedText, err)
		}

		// res.Data is MultimodalEmbedding (struct), not slice.
		out[i] = model.Embedding{
			Embedding: res.Data.Embedding,
			Index:     i,
		}
	}

	return out, nil
}
