package ctokenizer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenizer(t *testing.T) {
	// default tokenizer.
	{
		m := make(map[string]string)
		tokenizer, err := NewTokenizer(m)
		assert.NoError(t, err)
		defer tokenizer.Destroy()

		tokenStream := tokenizer.NewTokenStream("football, basketball, pingpang")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
	}

	// jieba tokenizer.
	{
		m := make(map[string]string)
		m["tokenizer"] = "jieba"
		tokenizer, err := NewTokenizer(m)
		assert.NoError(t, err)
		defer tokenizer.Destroy()

		tokenStream := tokenizer.NewTokenStream("张华考上了北京大学；李萍进了中等技术学校；我在百货公司当售货员：我们都有光明的前途")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
	}
}
