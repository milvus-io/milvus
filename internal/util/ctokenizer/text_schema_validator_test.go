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
	assert.Nil(t, ValidateTextSchema(fs, false))
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
				{Key: "analyzer_params", Value: `{"tokenizer": "standard"}`},
			},
		},
		{
			FieldID:  101,
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "enable_match", Value: "true"},
				{Key: "analyzer_params", Value: `{"tokenizer": "standard"}`},
			},
		},
	}

	for idx, tt := range tests {
		t.Run(fmt.Sprintf("enable_analyzer not set %d", idx), func(t *testing.T) {
			err := ValidateTextSchema(tt, false)
			assert.NotNil(t, err)
		})
	}

	for idx, tt := range tests {
		t.Run(fmt.Sprintf("enable_analyzer set to false %d", idx), func(t *testing.T) {
			tt.TypeParams = append(tt.TypeParams, &commonpb.KeyValuePair{
				Key:   "enable_analyzer",
				Value: "false",
			})
			err := ValidateTextSchema(tt, false)
			assert.NotNil(t, err)
		})
	}
	for idx, tt := range tests {
		t.Run(fmt.Sprintf("enable_analyzer set to true %d", idx), func(t *testing.T) {
			tt.TypeParams[len(tt.TypeParams)-1].Value = "true"
			err := ValidateTextSchema(tt, false)
			assert.Nil(t, err)
		})
	}
}
