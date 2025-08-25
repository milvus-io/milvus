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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFunctionConfig(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.FunctionCfg
	teiConf := cfg.GetTextEmbeddingProviderConfig("tei")
	assert.Equal(t, teiConf["enable"], "true")
	openaiConf := cfg.GetTextEmbeddingProviderConfig("openai")
	assert.Equal(t, openaiConf["credential"], "")
	assert.Equal(t, openaiConf["url"], "")

	keys := []string{
		"tei.enable",
		"tei.credential",
		"azure_openai.credential",
		"azure_openai.url",
		"azure_openai.resource_name",
		"azure_openai.enable",
		"openai.credential",
		"openai.url",
		"openai.enable",
		"dashscope.credential",
		"dashscope.url",
		"dashscope.enable",
		"cohere.credential",
		"cohere.url",
		"cohere.enable",
		"voyageai.credential",
		"voyageai.url",
		"voyageai.enable",
		"siliconflow.url",
		"siliconflow.credential",
		"siliconflow.enable",
		"bedrock.credential",
		"bedrock.enable",
		"vertexai.url",
		"vertexai.credential",
		"vertexai.enable",
	}
	for _, key := range keys {
		assert.True(t, cfg.TextEmbeddingProviders.GetDoc(key) != "")
	}
	assert.True(t, cfg.TextEmbeddingProviders.GetDoc("Unknow") == "")

	keys = []string{
		"tei.enable",
		"tei.credential",
		"vllm.enable",
		"vllm.credential",
		"cohere.credential",
		"cohere.url",
		"cohere.enable",
		"voyageai.credential",
		"voyageai.url",
		"voyageai.enable",
		"siliconflow.url",
		"siliconflow.credential",
		"siliconflow.enable",
	}
	for _, key := range keys {
		assert.True(t, cfg.RerankModelProviders.GetDoc(key) != "")
	}
	assert.True(t, cfg.RerankModelProviders.GetDoc("Unknow") == "")
}
