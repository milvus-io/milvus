package canalyzer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnalyzer(t *testing.T) {
	// default tokenizer.
	{
		m := "{\"tokenizer\": \"standard\"}"
		analyzer, err := NewAnalyzer(m)
		assert.NoError(t, err)
		defer analyzer.Destroy()

		tokenStream := analyzer.NewTokenStream("football, basketball, pingpang")
		defer tokenStream.Destroy()
		for tokenStream.Advance() {
			fmt.Println(tokenStream.Token())
		}
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
