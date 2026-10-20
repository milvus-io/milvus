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
		analyzerResourceNames([]string{params, remoteDictionaryParams}))
	for _, params := range []string{
		"", "{}", `{"tokenizer":"standard"}`, `{"tokenizer":{"type":"jieba","dict":["default"]}}`,
		`{"file":{"type":"local","path":"a","resource_name":"not-a-reference"}}`, `{"broken":`,
		`{"type":"remote","resource_name":123}`,
	} {
		require.Empty(t, analyzerResourceNames([]string{params}), params)
	}
}
