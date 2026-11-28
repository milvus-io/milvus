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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		"yc.credential",
		"yc.url",
		"yc.enable",
		"huggingface.credential",
		"huggingface.url",
		"huggingface.enable",
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
		"huggingface.credential",
		"huggingface.url",
		"huggingface.enable",
	}
	for _, key := range keys {
		assert.True(t, cfg.RerankModelProviders.GetDoc(key) != "")
	}
	assert.True(t, cfg.RerankModelProviders.GetDoc("Unknow") == "")

	assert.Equal(t, 5, cfg.GetBatchFactor())
	assert.Equal(t, 8, cfg.GetAnalyzerRunnerConcurrency())

	old := cfg.AnalyzerRunnerConcurrency.SwapTempValue("-1")
	defer cfg.AnalyzerRunnerConcurrency.SwapTempValue(old)
	assert.Equal(t, 1, cfg.GetAnalyzerRunnerConcurrency())

	assert.Equal(t, 30*time.Second, cfg.ModelRequestTimeout.GetAsDurationByParse())
}

func TestFunctionConfigPyUDF(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.FunctionCfg

	assert.False(t, cfg.PyUDFEnabled.GetAsBool())

	oldEnabled := cfg.PyUDFEnabled.SwapTempValue("true")
	defer cfg.PyUDFEnabled.SwapTempValue(oldEnabled)

	assert.True(t, cfg.PyUDFEnabled.GetAsBool())
}

func TestGrepConfigValidationAndRefresh(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	for _, tc := range []struct {
		item        *ParamItem
		fallback    int64
		zeroAllowed bool
	}{
		{&params.FunctionCfg.GrepMaxArgumentBytes, 65536, false},
		{&params.FunctionCfg.GrepMaxRegexBytes, 10485760, false},
		{&params.FunctionCfg.GrepMaxDFACacheBytes, 8388608, false},
		{&params.FunctionCfg.GrepBatchSize, 128, false},
		{&params.FunctionCfg.GrepMaxScanRows, 0, true},
	} {
		t.Run(tc.item.Key, func(t *testing.T) {
			assert.Equal(t, tc.fallback, tc.item.GetAsInt64())
			for _, value := range []string{"", "-1", "bad", "1.5", "9223372036854775808", "-9223372036854775809"} {
				// Populate the typed cache before changing to an invalid value.
				assert.NoError(t, params.Save(tc.item.Key, "7"))
				assert.Equal(t, int64(7), tc.item.GetAsInt64())
				assert.NoError(t, params.Save(tc.item.Key, value))
				assert.Equal(t, tc.fallback, tc.item.GetAsInt64(), value)
				assert.Equal(t, tc.item.DefaultValue, tc.item.GetValue(), value)
				assert.Equal(t, tc.fallback, tc.item.GetAsInt64(), value)
			}
			assert.NoError(t, params.Save(tc.item.Key, "0"))
			zeroExpected := tc.fallback
			if tc.zeroAllowed {
				zeroExpected = 0
			}
			assert.Equal(t, zeroExpected, tc.item.GetAsInt64())
			for _, value := range []int64{1, 11, 9223372036854775807} {
				assert.NoError(t, params.Save(tc.item.Key, strconv.FormatInt(value, 10)))
				assert.Equal(t, value, tc.item.GetAsInt64())
				assert.Equal(t, value, tc.item.GetAsInt64())
			}
		})
	}
}

func TestAnalyzerLocalResourcePath(t *testing.T) {
	for _, tc := range []struct {
		name, root, configured, want string
	}{
		{name: "default", root: defaultLocalStoragePath, want: "/var/lib/milvus/data/analyzer"},
		{name: "custom_storage_root", root: "/srv/milvus/data", want: "/srv/milvus/data/analyzer"},
		{name: "explicit_legacy_path", root: "/srv/milvus/data", configured: "/var/lib/milvus/analyzer", want: "/var/lib/milvus/analyzer"},
		{name: "relative_override", root: defaultLocalStoragePath, configured: "dicts"},
		{name: "filesystem_root_override", root: defaultLocalStoragePath, configured: "/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := NewBaseTable(SkipRemote(true), SkipEnv(true), Files([]string{}))
			t.Cleanup(base.Manager().Close)
			require.NoError(t, base.Save("function.analyzer.local_resource_path", tc.configured))
			var cfg functionConfig
			if tc.want == "" {
				require.Panics(t, func() { cfg.init(base, tc.root) })
				return
			}
			cfg.init(base, tc.root)
			require.Equal(t, tc.want, cfg.LocalResourcePath.GetValue())
		})
	}
}
