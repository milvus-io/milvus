package parameterutil

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/common"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestGetMaxLength(t *testing.T) {
	t.Run("not string type", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Bool,
		}
		_, err := GetMaxLength(f)
		assert.Error(t, err)
	})

	t.Run("max length not found", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
		}
		_, err := GetMaxLength(f)
		assert.Error(t, err)
	})

	t.Run("max length not int", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "not_int_aha",
				},
			},
		}
		_, err := GetMaxLength(f)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "100",
				},
			},
		}
		maxLength, err := GetMaxLength(f)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), maxLength)
	})
}
