package ctokenizer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestValidateEmptyTextSchema(t *testing.T) {
	fs := &schemapb.FieldSchema{
		FieldID:    101,
		DataType:   schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{},
	}
	assert.Nil(t, ValidateTextSchema(fs))
}

func TestValidateTextSchema(t *testing.T) {
	tests := []*schemapb.FieldSchema{
		{
			FieldID:  101,
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "enable_match", Value: "true"},
			},
		},
		{
			FieldID:  101,
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "enable_match", Value: "true"},
				{Key: "tokenizer_params", Value: `{"tokenizer": "default"}`},
			},
		},
		{
			FieldID:  101,
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "enable_match", Value: "true"},
				{Key: "tokenizer_params", Value: `{"tokenizer": "jieba"}`},
			},
		},
	}

	for idx, tt := range tests {
		t.Run(fmt.Sprintf("enable_tokenizer not set %d", idx), func(t *testing.T) {
			err := ValidateTextSchema(tt)
			assert.NotNil(t, err)
		})
	}

	for idx, tt := range tests {
		t.Run(fmt.Sprintf("enable_tokenizer set to false %d", idx), func(t *testing.T) {
			tt.TypeParams = append(tt.TypeParams, &commonpb.KeyValuePair{
				Key:   "enable_tokenizer",
				Value: "false",
			})
			err := ValidateTextSchema(tt)
			assert.NotNil(t, err)
		})
	}
	for idx, tt := range tests {
		t.Run(fmt.Sprintf("enable_tokenizer set to true %d", idx), func(t *testing.T) {
			tt.TypeParams[len(tt.TypeParams)-1].Value = "true"
			err := ValidateTextSchema(tt)
			assert.Nil(t, err)
		})
	}
}
