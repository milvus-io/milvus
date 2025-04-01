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
	notExistProvider := cfg.GetTextEmbeddingProviderConfig("notExist")

	// Only has enableVerifiInfoInParams config
	assert.Equal(t, len(notExistProvider), 1)

	teiConf := cfg.GetTextEmbeddingProviderConfig("tei")
	assert.Equal(t, teiConf["enable"], "true")
	assert.Equal(t, teiConf["enableVerifiInfoInParams"], "true")
	openaiConf := cfg.GetTextEmbeddingProviderConfig("openai")
	assert.Equal(t, openaiConf["api_key"], "")
	assert.Equal(t, openaiConf["url"], "")
	assert.Equal(t, openaiConf["enableVerifiInfoInParams"], "true")

	keys := []string{
		"tei.enable",
		"azure_openai.api_key",
		"azure_openai.url",
		"azure_openai.resource_name",
		"openai.api_key",
		"openai.url",
		"dashscope.api_key",
		"dashscope.url",
		"cohere.api_key",
		"cohere.url",
		"voyageai.api_key",
		"voyageai.url",
		"siliconflow.url",
		"siliconflow.api_key",
		"bedrock.aws_access_key_id",
		"bedrock.aws_secret_access_key",
		"vertexai.url",
		"vertexai.credentials_file_path",
	}
	for _, key := range keys {
		assert.True(t, cfg.TextEmbeddingProviders.GetDoc(key) != "")
	}
}
