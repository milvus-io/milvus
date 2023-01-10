package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
)

func Test_getPrimaryKeysFromExpr(t *testing.T) {
	t.Run("delete on non-pk field", func(t *testing.T) {
		schema := &schemapb.CollectionSchema{
			Name:        "test_delete",
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:      common.StartOfUserFieldID + 1,
					Name:         "non_pk",
					IsPrimaryKey: false,
					DataType:     schemapb.DataType_Int64,
				},
			},
		}

		expr := "non_pk in [1, 2, 3]"

		_, _, err := getPrimaryKeysFromExpr(schema, expr)
		assert.Error(t, err)
	})
}
