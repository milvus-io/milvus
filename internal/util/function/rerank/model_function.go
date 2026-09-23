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

package rerank

import (
	"context"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	providerParamName       string = "provider"
	vllmProviderName        string = "vllm"
	teiProviderName         string = "tei"
	siliconflowProviderName string = "siliconflow"
	cohereProviderName      string = "cohere"
	voyageaiProviderName    string = "voyageai"
	aliProviderName         string = "ali"
	zillizProviderName      string = "zilliz"
	huggingFaceProviderName string = "huggingface"
)

func parseMaxBatch(maxBatch string) (int, error) {
	if batch, err := strconv.Atoi(maxBatch); err != nil {
		return -1, merr.WrapErrParameterInvalidMsg("[%s param's value: %s] is not a valid number", models.MaxClientBatchSizeParamKey, maxBatch)
	} else {
		if batch <= 0 {
			return -1, merr.WrapErrParameterInvalidMsg("[%s param's value: %s] must be greater than 0", models.MaxClientBatchSizeParamKey, maxBatch)
		}
		return batch, nil
	}
}

// rerankScoresByIndex maps the results a rerank service returned onto a
// per-document score slice.
//
// A rerank response is a list of (index, score) pairs where index is the position
// of the document in the request. The service may answer in any order — that is
// why every client in models/ sorts the results by that index — and, with
// parameters such as top_n, it may answer for only some of the documents. Storing
// the scores by their position in the response therefore attaches a score to the
// wrong document as soon as the response is incomplete, and leaves 0 for the
// documents that were not answered for, which is a legal score and so is
// invisible to the caller.
//
// Each score is placed at its own index, and a response that does not cover every
// document exactly once is an error — the same contract the embedding providers
// and the huggingface rerank provider already enforce.
func rerankScoresByIndex(docCount int, resultCount int, at func(i int) (int, float32)) ([]float32, error) {
	if resultCount != docCount {
		return nil, merr.WrapErrFunctionFailedMsg("get rerank scores failed, the number of docs and scores does not match docs:[%d], scores:[%d]", docCount, resultCount)
	}
	scores := make([]float32, docCount)
	filled := make([]bool, docCount)
	for i := 0; i < resultCount; i++ {
		idx, score := at(i)
		if idx < 0 || idx >= docCount || filled[idx] {
			return nil, merr.WrapErrFunctionFailedMsg("get rerank scores failed, the rerank service returned an invalid or duplicated result index [%d] for [%d] docs", idx, docCount)
		}
		filled[idx] = true
		scores[idx] = score
	}
	return scores, nil
}

// ModelProvider is the interface for external rerank model services.
type ModelProvider interface {
	Rerank(context.Context, string, []string) ([]float32, error)
	MaxBatch() int
}

type baseProvider struct {
	batchSize int
}

func (provider *baseProvider) MaxBatch() int {
	return provider.batchSize
}

// NewModelProvider creates a ModelProvider from function parameters and extra info.
func NewModelProvider(params []*commonpb.KeyValuePair, extraInfo *models.ModelExtraInfo) (ModelProvider, error) {
	for _, param := range params {
		if strings.ToLower(param.Key) == providerParamName {
			provider := strings.ToLower(param.Value)
			conf := paramtable.Get().FunctionCfg.GetRerankModelProviders(provider)
			if !models.IsEnable(conf) {
				return nil, merr.WrapErrParameterInvalidMsg("rerank provider: [%s] is disabled", provider)
			}
			credentials := credentials.NewCredentials(paramtable.Get().CredentialCfg.GetCredentials())
			switch provider {
			case vllmProviderName:
				return newVllmProvider(params, conf, credentials)
			case teiProviderName:
				return newTeiProvider(params, conf, credentials)
			case siliconflowProviderName:
				return newSiliconflowProvider(params, conf, credentials, extraInfo)
			case cohereProviderName:
				return newCohereProvider(params, conf, credentials, extraInfo)
			case voyageaiProviderName:
				return newVoyageaiProvider(params, conf, credentials, extraInfo)
			case aliProviderName:
				return newAliProvider(params, conf, credentials, extraInfo)
			case zillizProviderName:
				conf := paramtable.Get().FunctionCfg.ZillizProviders.GetValue()
				return newZillizProvider(params, conf, extraInfo)
			case huggingFaceProviderName:
				return newHuggingFaceProvider(params, conf, credentials, extraInfo)
			default:
				return nil, merr.WrapErrParameterInvalidMsg("unknown rerank model provider:%s", param.Value)
			}
		}
	}
	return nil, merr.WrapErrParameterInvalidMsg("lost rerank params:%s ", providerParamName)
}
