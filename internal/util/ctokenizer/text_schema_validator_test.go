package ctokenizer

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestValidateTextSchema(t *testing.T) {
	type args struct {
		fieldSchema *schemapb.FieldSchema
	}
	tests := []struct {
		name     string
		args     args
		errIsNil bool
	}{
		{
			args: args{
				fieldSchema: &schemapb.FieldSchema{
					FieldID:    101,
					TypeParams: []*commonpb.KeyValuePair{},
				},
			},
			errIsNil: true,
		},
		{
			// default
			args: args{
				fieldSchema: &schemapb.FieldSchema{
					FieldID: 101,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "enable_match", Value: "true"},
					},
				},
			},
			errIsNil: true,
		},
		{
			// default
			args: args{
				fieldSchema: &schemapb.FieldSchema{
					FieldID: 101,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "enable_match", Value: "true"},
						{Key: "analyzer_params", Value: `{"tokenizer": "default"}`},
					},
				},
			},
			errIsNil: true,
		},
		{
			// jieba
			args: args{
				fieldSchema: &schemapb.FieldSchema{
					FieldID: 101,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "enable_match", Value: "true"},
						{Key: "analyzer_params", Value: `{"tokenizer": "jieba"}`},
					},
				},
			},
			errIsNil: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateTextSchema(tt.args.fieldSchema)
			if tt.errIsNil {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
			}
		})
	}
}
