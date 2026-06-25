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

package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	defaultTwelveLabsEmbeddingURL = "https://api.twelvelabs.io/v1.3/embed"
)

// twelveLabsEmbeddingResponse mirrors the JSON returned by the TwelveLabs
// /v1.3/embed endpoint for a text input. Marengo returns a single segment for
// a text query, whose "float" field holds the embedding vector.
//
//	{"model_name":"marengo3.0","text_embedding":{"segments":[{"float":[...]}]}}
type twelveLabsEmbeddingResponse struct {
	ModelName     string `json:"model_name"`
	TextEmbedding struct {
		Segments []struct {
			Float []float32 `json:"float"`
		} `json:"segments"`
	} `json:"text_embedding"`
}

// TwelveLabsEmbeddingProvider calls the TwelveLabs Marengo multimodal embedding
// model. Marengo embeds text, image, audio and video into the same vector
// space, so text embeddings produced here can be searched against video
// embeddings generated out-of-band. The text endpoint accepts a single piece
// of text per request, so a batch is sent as one request per row.
type TwelveLabsEmbeddingProvider struct {
	fieldDim int64

	url       string
	apiKey    string
	modelName string

	maxBatch  int
	timeoutMs int64
	extraInfo *models.ModelExtraInfo
}

func NewTwelveLabsEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (*TwelveLabsEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}

	if fieldSchema.DataType != schemapb.DataType_FloatVector {
		return nil, merr.WrapErrParameterInvalidMsg("TwelveLabs embedding only supports FloatVector output field, but got %s", fieldSchema.DataType.String()) //nolint:staticcheck // starts with proper noun
	}

	var modelName string
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.DimParamKey:
			if _, err = models.ParseAndCheckFieldDim(param.Value, fieldDim, fieldSchema.Name); err != nil {
				return nil, err
			}
		default:
		}
	}
	if modelName == "" {
		return nil, merr.WrapErrParameterMissingMsg("twelvelabs embedding model name is required")
	}

	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.TwelveLabsAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}
	if apiKey == "" {
		return nil, merr.WrapErrParameterInvalidMsg("missing credentials config or configure the %s environment variable in the Milvus service", models.TwelveLabsAKEnvStr)
	}
	if url == "" {
		url = defaultTwelveLabsEmbeddingURL
	}

	timeoutMs := models.ResolveTimeoutMs(functionSchema.Params)

	provider := TwelveLabsEmbeddingProvider{
		fieldDim:  fieldDim,
		url:       url,
		apiKey:    apiKey,
		modelName: modelName,
		maxBatch:  64,
		timeoutMs: timeoutMs,
		extraInfo: extraInfo,
	}
	return &provider, nil
}

func (provider *TwelveLabsEmbeddingProvider) MaxBatch() int {
	return provider.extraInfo.BatchFactor * provider.maxBatch
}

func (provider *TwelveLabsEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

// embed sends a single text to the TwelveLabs embed endpoint and returns its
// embedding vector. The endpoint expects multipart/form-data with model_name
// and text fields and the x-api-key header.
func (provider *TwelveLabsEmbeddingProvider) embed(ctx context.Context, text string) ([]float32, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	if err := writer.WriteField("model_name", provider.modelName); err != nil {
		return nil, err
	}
	if err := writer.WriteField("text", text); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, provider.url, &buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("x-api-key", provider.apiKey)

	resp, err := http.DefaultClient.Do(req) //nolint:gosec // URL is constructed from configured model endpoints, not user input
	if err != nil {
		return nil, merr.WrapErrServiceUnavailable(err.Error(), "call twelvelabs service failed")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, merr.WrapErrServiceUnavailable(err.Error(), "call twelvelabs service failed, read response failed")
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("call twelvelabs service failed, errs:[%s, %s]", resp.Status, body)
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError {
			return nil, merr.WrapErrServiceUnavailable(msg)
		}
		return nil, merr.WrapErrFunctionFailedMsg("%s", msg)
	}

	var res twelveLabsEmbeddingResponse
	if err := json.Unmarshal(body, &res); err != nil {
		return nil, merr.Wrap(err, "call twelvelabs service failed, unmarshal response failed")
	}
	if len(res.TextEmbedding.Segments) == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("twelvelabs returned no text embedding segments")
	}
	return res.TextEmbedding.Segments[0].Float, nil
}

func (provider *TwelveLabsEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, _ models.TextEmbeddingMode) (any, error) {
	data := make([][]float32, 0, len(texts))
	for _, text := range texts {
		emb, err := provider.embed(ctx, text)
		if err != nil {
			return nil, err
		}
		if len(emb) != int(provider.fieldDim) {
			return nil, merr.WrapErrFunctionFailedMsg("the required embedding dim is [%d], but the embedding obtained from the model is [%d]",
				provider.fieldDim, len(emb))
		}
		data = append(data, emb)
	}
	return data, nil
}
