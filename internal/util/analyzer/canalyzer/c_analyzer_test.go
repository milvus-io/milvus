package canalyzer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestAnalyzer(t *testing.T) {
	// use default analyzer.
	{
		m := "{}"
		analyzer, err := NewAnalyzer(m)
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream := analyzer.NewTokenStream("football, basketball, pingpang")
		defer tokenStream.Destroy()

		tokens := []string{}
		for tokenStream.Advance() {
			tokens = append(tokens, tokenStream.Token())
		}
		assert.Equal(t, len(tokens), 3)
	}

	{
		m := ""
		analyzer, err := NewAnalyzer(m)
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream := analyzer.NewTokenStream("football, basketball, pingpang")
		defer tokenStream.Destroy()

		tokens := []string{}
		for tokenStream.Advance() {
			tokens = append(tokens, tokenStream.Token())
		}
		assert.Equal(t, len(tokens), 3)
	}

	// use default tokenizer.
	{
		m := "{\"tokenizer\": \"standard\"}"
		analyzer, err := NewAnalyzer(m)
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream := analyzer.NewTokenStream("football, basketball, pingpang")
		defer tokenStream.Destroy()

		tokens := []string{}
		for tokenStream.Advance() {
			tokens = append(tokens, tokenStream.Token())
		}
		assert.Equal(t, len(tokens), 3)
	}

	// jieba tokenizer.
	{
		m := "{\"tokenizer\": \"jieba\"}"
		analyzer, err := NewAnalyzer(m)
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream := analyzer.NewTokenStream("张华考上了北京大学；李萍进了中等技术学校；我在百货公司当售货员：我们都有光明的前途")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
	}

	// lindera tokenizer.
	{
		m := "{\"tokenizer\": {\"type\":\"lindera\", \"dict_kind\": \"ipadic\"}}"
		tokenizer, err := NewAnalyzer(m)
		require.NoError(t, err)
		defer tokenizer.Destroy()

		tokenStream := tokenizer.NewTokenStream("東京スカイツリーの最寄り駅はとうきょうスカイツリー駅です")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
	}
}

func TestValidateAnalyzer(t *testing.T) {
	// valid analyzer
	{
		m := "{\"tokenizer\": \"standard\"}"
		err := ValidateAnalyzer(m)
		assert.NoError(t, err)
	}

	{
		m := ""
		err := ValidateAnalyzer(m)
		assert.NoError(t, err)
	}

	// invalid tokenizer
	{
		m := "{\"tokenizer\": \"invalid\"}"
		err := ValidateAnalyzer(m)
		assert.Error(t, err)
	}
}

func TestCheckAndFillParams(t *testing.T) {
	paramtable.Init()
	paramtable.Get().SaveGroup(map[string]string{"function.analyzer.lindera.download_urls.ipadic": "/test/url"})

	// normal case
	{
		m := "{\"tokenizer\": {\"type\":\"jieba\"}}"
		_, err := CheckAndFillParams(m)
		assert.NoError(t, err)
	}

	// fill lindera tokenizer download urls and dict local path
	{
		m := "{\"tokenizer\": {\"type\":\"lindera\", \"dict_kind\": \"ipadic\"}}"
		_, err := CheckAndFillParams(m)
		assert.NoError(t, err)
	}

	// error with wrong json
	{
		m := "{invalid json"
		_, err := CheckAndFillParams(m)
		assert.Error(t, err)
	}

	// skip if use default analyzer
	{
		m := "{}"
		_, err := CheckAndFillParams(m)
		assert.NoError(t, err)
	}

	// error tokenizer without type
	{
		m := "{\"tokenizer\": {\"dict_kind\": \"ipadic\"}}"
		_, err := CheckAndFillParams(m)
		assert.Error(t, err)
	}

	// error tokenizer type not string
	{
		m := "{\"tokenizer\": {\"type\": 1, \"dict_kind\": \"ipadic\"}}"
		_, err := CheckAndFillParams(m)
		assert.Error(t, err)
	}

	// error tokenizer params type
	{
		m := "{\"tokenizer\": 1}"
		_, err := CheckAndFillParams(m)
		assert.Error(t, err)
	}

	// error set dict_build_dir by user
	{
		m := "{\"tokenizer\": {\"type\": \"lindera\", \"dict_kind\": \"ipadic\", \"dict_build_dir\": \"/tmp/milvus\"}}"
		_, err := CheckAndFillParams(m)
		assert.Error(t, err)
	}

	// error lindera kind not set
	{
		m := "{\"tokenizer\": {\"type\": \"lindera\"}}"
		_, err := CheckAndFillParams(m)
		assert.Error(t, err)
	}
}
