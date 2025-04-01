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

package paramtable

import (
	"strings"
)

type functionConfig struct {
	TextEmbeddingEnableVerifiInfoInParams ParamItem  `refreshable:"true"`
	TextEmbeddingProviders                ParamGroup `refreshable:"true"`
}

func (p *functionConfig) init(base *BaseTable) {
	p.TextEmbeddingEnableVerifiInfoInParams = ParamItem{
		Key:          "function.textEmbedding.enableVerifiInfoInParams",
		Version:      "2.6.0",
		DefaultValue: "true",
		Export:       true,
		Doc:          "Controls whether to allow configuration of apikey and model service url on function parameters",
	}
	p.TextEmbeddingEnableVerifiInfoInParams.Init(base.mgr)

	p.TextEmbeddingProviders = ParamGroup{
		KeyPrefix: "function.textEmbedding.providers.",
		Version:   "2.6.0",
		Export:    true,
		DocFunc: func(key string) string {
			switch key {
			case "tei.enable":
				return "Whether to enable TEI model service"
			case "azure_openai.api_key":
				return "Your azure openai embedding url, Default is the official embedding url"
			case "azure_openai.url":
				return "Your azure openai api key"
			case "azure_openai.resource_name":
				return "Your azure openai resource name"
			case "openai.api_key":
				return "Your openai embedding url, Default is the official embedding url"
			case "openai.url":
				return "Your openai api key"
			case "dashscope.api_key":
				return "Your dashscope embedding url, Default is the official embedding url"
			case "dashscope.url":
				return "Your dashscope api key"
			case "cohere.api_key":
				return "Your cohere embedding url, Default is the official embedding url"
			case "cohere.url":
				return "Your cohere api key"
			case "voyageai.api_key":
				return "Your voyageai embedding url, Default is the official embedding url"
			case "voyageai.url":
				return "Your voyageai api key"
			case "siliconflow.url":
				return "Your siliconflow embedding url, Default is the official embedding url"
			case "siliconflow.api_key":
				return "Your siliconflow api key"
			case "bedrock.aws_access_key_id":
				return "Your aws_access_key_id"
			case "bedrock.aws_secret_access_key":
				return "Your aws_secret_access_key"
			case "vertexai.url":
				return "Your VertexAI embedding url"
			case "vertexai.credentials_file_path":
				return "Path to your google application credentials, change the file path to refresh the configuration"
			default:
				return ""
			}
		},
	}
	p.TextEmbeddingProviders.Init(base.mgr)
}

const (
	textEmbeddingKey string = "textEmbedding"
)

func (p *functionConfig) GetTextEmbeddingProviderConfig(providerName string) map[string]string {
	matchedParam := make(map[string]string)

	params := p.TextEmbeddingProviders.GetValue()
	prefix := providerName + "."

	for k, v := range params {
		if strings.HasPrefix(k, prefix) {
			matchedParam[strings.TrimPrefix(k, prefix)] = v
		}
	}
	matchedParam["enableVerifiInfoInParams"] = p.TextEmbeddingEnableVerifiInfoInParams.GetValue()
	return matchedParam
}
