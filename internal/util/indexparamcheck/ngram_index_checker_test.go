package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func Test_NgramIndexChecker_CheckTrain(t *testing.T) {
	checker := newNgramIndexChecker()

	t.Run("valid ngram on varchar", func(t *testing.T) {
		params := map[string]string{
			MinGramKey: "2",
			MaxGramKey: "3",
		}
		err := checker.CheckTrain(schemapb.DataType_VarChar, schemapb.DataType_None, params)
		assert.NoError(t, err)
	})

	t.Run("valid ngram on json with varchar cast", func(t *testing.T) {
		testCases := []string{"VARCHAR", "varchar", "VarChar", "  VARCHAR  "}
		for _, castType := range testCases {
			params := map[string]string{
				MinGramKey:             "2",
				MaxGramKey:             "3",
				common.JSONCastTypeKey: castType,
			}
			err := checker.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, params)
			assert.NoError(t, err)
		}
	})

	t.Run("invalid ngram on json without cast type", func(t *testing.T) {
		params := map[string]string{
			MinGramKey: "2",
			MaxGramKey: "3",
		}
		err := checker.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "JSON field with ngram index must specify json_cast_type")
	})

	t.Run("invalid ngram on json with non-varchar cast type", func(t *testing.T) {
		invalidTypes := []string{"INT64", "FLOAT", "BOOL", "ARRAY"}
		for _, castType := range invalidTypes {
			params := map[string]string{
				MinGramKey:             "2",
				MaxGramKey:             "3",
				common.JSONCastTypeKey: castType,
			}
			err := checker.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, params)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "JSON field with ngram index only supports VARCHAR cast type")
			assert.Contains(t, err.Error(), castType)
		}
	})

	t.Run("invalid ngram params", func(t *testing.T) {
		testCases := []struct {
			name   string
			params map[string]string
			errMsg string
		}{
			{
				name:   "missing min_gram",
				params: map[string]string{MaxGramKey: "3"},
				errMsg: "Ngram index must specify both min_gram and max_gram",
			},
			{
				name:   "non-integer min_gram",
				params: map[string]string{MinGramKey: "abc", MaxGramKey: "3"},
				errMsg: "min_gram for Ngram index must be an integer",
			},
			{
				name:   "min_gram > max_gram",
				params: map[string]string{MinGramKey: "5", MaxGramKey: "3"},
				errMsg: "invalid min_gram or max_gram value",
			},
		}

		for _, tc := range testCases {
			err := checker.CheckTrain(schemapb.DataType_VarChar, schemapb.DataType_None, tc.params)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		}
	})
}

func Test_NgramIndexChecker_CheckValidDataType(t *testing.T) {
	checker := newNgramIndexChecker()

	t.Run("valid data types", func(t *testing.T) {
		validTypes := []schemapb.DataType{
			schemapb.DataType_VarChar,
			schemapb.DataType_JSON,
		}
		for _, dtype := range validTypes {
			field := &schemapb.FieldSchema{DataType: dtype}
			err := checker.CheckValidDataType(IndexNGRAM, field)
			assert.NoError(t, err)
		}
	})

	t.Run("invalid data types", func(t *testing.T) {
		invalidTypes := []schemapb.DataType{
			schemapb.DataType_Int64,
			schemapb.DataType_Float,
			schemapb.DataType_Bool,
			schemapb.DataType_Array,
			schemapb.DataType_FloatVector,
		}
		for _, dtype := range invalidTypes {
			field := &schemapb.FieldSchema{DataType: dtype}
			err := checker.CheckValidDataType(IndexNGRAM, field)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "ngram index can only be created on VARCHAR or JSON field")
		}
	})
}
