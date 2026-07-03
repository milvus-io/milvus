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
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/huggingface"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type HuggingFaceEmbeddingProvider struct {
	fieldDim int64

	client     *huggingface.Client
	modelName  string
	hfProvider string

	params map[string]any

	maxBatch   int
	timeoutSec int64
	extraInfo  *models.ModelExtraInfo
}

func NewHuggingFaceEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (*HuggingFaceEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	if fieldSchema.DataType != schemapb.DataType_FloatVector {
		return nil, merr.WrapErrParameterInvalidMsg("Hugging Face embedding only supports FloatVector field, got %s", schemapb.DataType_name[int32(fieldSchema.DataType)])
	}

	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.HuggingFaceAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}
	client, err := huggingface.NewClient(apiKey, url)
	if err != nil {
		return nil, err
	}

	var modelName string
	hfProvider := huggingface.DefaultHFProvider
	maxBatch := 128
	hfParams := map[string]any{}

	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.HuggingFaceProviderParamKey:
			hfProvider = param.Value
		case models.NormalizeParamKey:
			normalize, err := strconv.ParseBool(param.Value)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("[%s param's value: %s] is invalid, only supports: [true/false]", models.NormalizeParamKey, param.Value)
			}
			hfParams[models.NormalizeParamKey] = normalize
		case models.TruncateParamKey:
			truncate, err := strconv.ParseBool(param.Value)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("[%s param's value: %s] is invalid, only supports: [true/false]", models.TruncateParamKey, param.Value)
			}
			hfParams[models.TruncateParamKey] = truncate
		case models.TruncationDirectionParamKey:
			hfParams[models.TruncationDirectionParamKey] = param.Value
		case models.HuggingFacePromptNameParamKey:
			hfParams[models.HuggingFacePromptNameParamKey] = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseEmbeddingMaxBatch(param.Value); err != nil {
				return nil, err
			}
		default:
		}
	}
	if modelName == "" {
		return nil, merr.WrapErrParameterMissingMsg("huggingface embedding model name is required")
	}
	if hfProvider == "" {
		return nil, merr.WrapErrParameterInvalidMsg("huggingface embedding hf_provider cannot be empty")
	}
	provider := HuggingFaceEmbeddingProvider{
		client:     client,
		fieldDim:   fieldDim,
		modelName:  modelName,
		hfProvider: hfProvider,
		params:     hfParams,
		maxBatch:   maxBatch,
		timeoutSec: 30,
		extraInfo:  extraInfo,
	}
	return &provider, nil
}

func parseEmbeddingMaxBatch(maxBatch string) (int, error) {
	batch, err := strconv.Atoi(maxBatch)
	if err != nil {
		return -1, merr.WrapErrParameterInvalidMsg("[%s param's value: %s] is not a valid number", models.MaxClientBatchSizeParamKey, maxBatch)
	}
	if batch <= 0 {
		return -1, merr.WrapErrParameterInvalidMsg("[%s param's value: %s] must be greater than 0", models.MaxClientBatchSizeParamKey, maxBatch)
	}
	return batch, nil
}

func (provider *HuggingFaceEmbeddingProvider) MaxBatch() int {
	return provider.extraInfo.BatchFactor * provider.maxBatch
}

func (provider *HuggingFaceEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *HuggingFaceEmbeddingProvider) CallEmbedding(_ context.Context, texts []string, _ models.TextEmbeddingMode) (any, error) {
	data := make([][]float32, 0, len(texts))
	for i := 0; i < len(texts); i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > len(texts) {
			end = len(texts)
		}
		resp, err := provider.client.FeatureExtraction(provider.hfProvider, provider.modelName, texts[i:end], provider.params, provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		embeddings, err := parseFeatureExtractionResponse(*resp, end-i, provider.fieldDim)
		if err != nil {
			return nil, err
		}
		data = append(data, embeddings...)
	}
	return data, nil
}

func parseFeatureExtractionResponse(raw json.RawMessage, expectedRows int, fieldDim int64) ([][]float32, error) {
	var batch [][]float32
	if err := json.Unmarshal(raw, &batch); err != nil || len(batch) == 0 {
		return nil, merr.WrapErrFunctionFailedMsg("unsupported Hugging Face feature-extraction response format")
	}

	if len(batch) != expectedRows {
		return nil, merr.WrapErrFunctionFailedMsg("get embedding failed, the number of texts and embeddings does not match text:[%d], embedding:[%d]", expectedRows, len(batch))
	}
	for _, item := range batch {
		if len(item) != int(fieldDim) {
			return nil, merr.WrapErrFunctionFailedMsg("the required embedding dim is [%d], but the embedding obtained from the model is [%d]", fieldDim, len(item))
		}
	}
	return batch, nil
}
