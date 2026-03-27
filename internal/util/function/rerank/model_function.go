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
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
)

func parseMaxBatch(maxBatch string) (int, error) {
	if batch, err := strconv.Atoi(maxBatch); err != nil {
		return -1, fmt.Errorf("[%s param's value: %s] is not a valid number", models.MaxClientBatchSizeParamKey, maxBatch)
	} else {
		if batch <= 0 {
			return -1, fmt.Errorf("[%s param's value: %s] must be greater than 0", models.MaxClientBatchSizeParamKey, maxBatch)
		}
		return batch, nil
	}
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
				return nil, fmt.Errorf("rerank provider: [%s] is disabled", provider)
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
			default:
				return nil, fmt.Errorf("unknown rerank model provider:%s", param.Value)
			}
		}
	}
	return nil, fmt.Errorf("lost rerank params:%s ", providerParamName)
}
