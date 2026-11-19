package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func Test_NgramIndexChecker_CheckTrain(t *testing.T) {
	checker := newNgramIndexChecker()

	for _, dtype := range []schemapb.DataType{schemapb.DataType_VarChar, schemapb.DataType_Text} {
		t.Run("valid ngram on "+dtype.String(), func(t *testing.T) {
			assert.NoError(t, checker.CheckTrain(dtype, schemapb.DataType_None, map[string]string{MinGramKey: "2", MaxGramKey: "3"}))
		})
	}

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

	for _, dtype := range []schemapb.DataType{schemapb.DataType_VarChar, schemapb.DataType_Text} {
		t.Run("invalid params on "+dtype.String(), func(t *testing.T) {
			cases := []struct {
				name, min, max string
				missing        bool
			}{
				{name: "both missing", missing: true},
				{name: "min missing", max: "3", missing: true},
				{name: "max missing", min: "2", missing: true},
				{name: "min non-integer", min: "abc", max: "3"},
				{name: "max non-integer", min: "2", max: "abc"},
				{name: "min zero", min: "0", max: "3"},
				{name: "max zero", min: "2", max: "0"},
				{name: "min negative", min: "-1", max: "3"},
				{name: "max negative", min: "2", max: "-1"},
				{name: "reversed range", min: "5", max: "3"},
				{name: "min overflow", min: "999999999999999999999999", max: "3"},
				{name: "max overflow", min: "2", max: "999999999999999999999999"},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					params := map[string]string{}
					if tc.min != "" {
						params[MinGramKey] = tc.min
					}
					if tc.max != "" {
						params[MaxGramKey] = tc.max
					}
					err := checker.CheckTrain(dtype, schemapb.DataType_None, params)
					if tc.missing {
						assert.ErrorIs(t, err, merr.ErrParameterMissing)
					} else {
						assert.ErrorIs(t, err, merr.ErrParameterInvalid)
					}
				})
			}
		})
	}
	t.Run("legacy STRING train remains unsupported", func(t *testing.T) {
		err := checker.CheckTrain(schemapb.DataType_String, schemapb.DataType_None, map[string]string{MinGramKey: "2", MaxGramKey: "3"})
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}

func Test_NgramIndexChecker_CheckValidDataType(t *testing.T) {
	checker := newNgramIndexChecker()

	t.Run("valid data types", func(t *testing.T) {
		validTypes := []schemapb.DataType{
			schemapb.DataType_VarChar,
			schemapb.DataType_Text,
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
			assert.Contains(t, err.Error(), "ngram index can only be created on VARCHAR, TEXT or JSON field")
		}
	})
}
