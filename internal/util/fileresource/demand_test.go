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

package fileresource

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const remoteDictionaryParams = `{"tokenizer":"standard","filter":[{"type":"stop","stop_words_file":{"type":"remote","resource_name":"dictionary","file_name":"stop.txt"}}]}`

func TestAnalyzerResourceNames(t *testing.T) {
	params := `{"by_field":"language","alias":{"en":"default"},"analyzers":{
		"default":{"tokenizer":{"type":"jieba","dict":["_default_"],"extra_dict_file":
			{"type":"remote","resource_name":"dictionary","file_name":"stop.txt"}}},
		"other":{"tokenizer":"standard","filter":[
			{"type":"stop","stop_words_file":{"type":"remote","resource_name":"synonyms","file_name":"synonyms.txt"}},
			{"type":"stop","stop_words_file":{"type":"local","path":"local.txt"}}]}
	}}`
	require.Equal(t, map[string]struct{}{"dictionary": {}, "synonyms": {}},
		AnalyzerResourceNames([]string{params, remoteDictionaryParams}))
	for _, params := range []string{
		"", "{}", `{"tokenizer":"standard"}`, `{"tokenizer":{"type":"jieba","dict":["default"]}}`,
		`{"file":{"type":"local","path":"a","resource_name":"not-a-reference"}}`, `{"broken":`,
		`{"type":"remote","resource_name":123}`,
	} {
		require.Empty(t, AnalyzerResourceNames([]string{params}), params)
	}
}
