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
	TextEmbeddingProviders ParamGroup `refreshable:"true"`
	RerankModelProviders   ParamGroup `refreshable:"true"`
	LocalResourcePath      ParamItem  `refreshable:"true"`
	LinderaDownloadUrls    ParamGroup `refreshable:"true"`
}

func (p *functionConfig) init(base *BaseTable) {
	p.TextEmbeddingProviders = ParamGroup{
		KeyPrefix: "function.textEmbedding.providers.",
		Version:   "2.6.0",
		Export:    true,
		DocFunc: func(key string) string {
			switch key {
			case "tei.enable":
				return "Whether to enable TEI model service"
			case "tei.credential":
				return "The name in the crendential configuration item"
			case "azure_openai.credential":
				return "The name in the crendential configuration item"
			case "azure_openai.url":
				return "Your azure openai embedding url, Default is the official embedding url"
			case "azure_openai.resource_name":
				return "Your azure openai resource name"
			case "openai.credential":
				return "The name in the crendential configuration item"
			case "openai.url":
				return "Your openai embedding url, Default is the official embedding url"
			case "dashscope.credential":
				return "The name in the crendential configuration item"
			case "dashscope.url":
				return "Your dashscope embedding url, Default is the official embedding url"
			case "cohere.credential":
				return "The name in the crendential configuration item"
			case "cohere.url":
				return "Your cohere embedding url, Default is the official embedding url"
			case "voyageai.credential":
				return "The name in the crendential configuration item"
			case "voyageai.url":
				return "Your voyageai embedding url, Default is the official embedding url"
			case "siliconflow.url":
				return "Your siliconflow embedding url, Default is the official embedding url"
			case "siliconflow.credential":
				return "The name in the crendential configuration item"
			case "bedrock.credential":
				return "The name in the crendential configuration item"
			case "vertexai.url":
				return "Your VertexAI embedding url"
			case "vertexai.credential":
				return "The name in the crendential configuration item"
			default:
				return ""
			}
		},
	}
	p.TextEmbeddingProviders.Init(base.mgr)

	p.RerankModelProviders = ParamGroup{
		KeyPrefix: "function.rerank.model.providers.",
		Version:   "2.6.0",
		Export:    true,
		DocFunc: func(key string) string {
			switch key {
			case "tei.enable":
				return "Whether to enable TEI rerank service"
			case "vllm.enable":
				return "Whether to enable vllm rerank service"
			default:
				return ""
			}
		},
	}
	p.RerankModelProviders.Init(base.mgr)

	p.LocalResourcePath = ParamItem{
		Key:          "function.analyzer.local_resource_path",
		Version:      "2.5.16",
		Export:       true,
		DefaultValue: "/var/lib/milvus/analyzer",
	}
	p.LocalResourcePath.Init(base.mgr)

	p.LinderaDownloadUrls = ParamGroup{
		KeyPrefix: "function.analyzer.lindera.download_urls",
		Version:   "2.5.16",
	}
	p.LinderaDownloadUrls.Init(base.mgr)
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
	return matchedParam
}

func (p *functionConfig) GetRerankModelProviders(providerName string) map[string]string {
	matchedParam := make(map[string]string)

	params := p.RerankModelProviders.GetValue()
	prefix := providerName + "."

	for k, v := range params {
		if strings.HasPrefix(k, prefix) {
			matchedParam[strings.TrimPrefix(k, prefix)] = v
		}
	}
	return matchedParam
}
