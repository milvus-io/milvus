package ctokenizer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestTokenizer(t *testing.T) {
	paramtable.Init()

	// default tokenizer.
	{
		m := "{\"tokenizer\": \"standard\"}"
		tokenizer, err := NewTokenizer(m)
		require.NoError(t, err)
		defer tokenizer.Destroy()

		tokenStream := tokenizer.NewTokenStream("football, basketball, pingpang")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
	}

	// jieba tokenizer.
	{
		m := "{\"tokenizer\": \"jieba\"}"
		tokenizer, err := NewTokenizer(m)
		require.NoError(t, err)
		defer tokenizer.Destroy()

		tokenStream := tokenizer.NewTokenStream("张华考上了北京大学；李萍进了中等技术学校；我在百货公司当售货员：我们都有光明的前途")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
	}

	// lindera tokenizer.
	{
		m := "{\"tokenizer\": {\"type\":\"lindera\", \"dict_kind\": \"ipadic\"}}"
		tokenizer, err := NewTokenizer(m)
		require.NoError(t, err)
		defer tokenizer.Destroy()

		tokenStream := tokenizer.NewTokenStream("東京スカイツリーの最寄り駅はとうきょうスカイツリー駅です")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
	}
}

func TestValidateTokenizer(t *testing.T) {
	// valid tokenizer
	{
		m := "{\"tokenizer\": \"standard\"}"
		err := ValidateTokenizer(m)
		assert.NoError(t, err)
	}

	{
		m := ""
		err := ValidateTokenizer(m)
		assert.NoError(t, err)
	}

	// invalid tokenizer
	{
		m := "{\"tokenizer\": \"invalid\"}"
		err := ValidateTokenizer(m)
		assert.Error(t, err)
	}
}

func TestCheckAndFillTokenizerParams(t *testing.T) {
	// fill lindera tokenizer download urls and dict local path
	{
		m := "{\"tokenizer\": {\"type\":\"lindera\", \"dict_kind\": \"ipadic\"}}"
		_, err := CheckAndFillParams(m)
		assert.NoError(t, err)
	}
}
