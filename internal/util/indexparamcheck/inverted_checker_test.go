package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func Test_INVERTEDIndexChecker(t *testing.T) {
	c := newINVERTEDChecker()

	assert.NoError(t, c.CheckTrain(schemapb.DataType_Bool, schemapb.DataType_None, map[string]string{}))

	assert.NoError(t, c.CheckValidDataType(IndexINVERTED, &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}))
	assert.NoError(t, c.CheckValidDataType(IndexINVERTED, &schemapb.FieldSchema{DataType: schemapb.DataType_String}))
	assert.NoError(t, c.CheckValidDataType(IndexINVERTED, &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}))
	assert.NoError(t, c.CheckValidDataType(IndexINVERTED, &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}))
	assert.NoError(t, c.CheckValidDataType(IndexINVERTED, &schemapb.FieldSchema{DataType: schemapb.DataType_Float}))
	assert.NoError(t, c.CheckValidDataType(IndexINVERTED, &schemapb.FieldSchema{DataType: schemapb.DataType_Array}))
	assert.NoError(t, c.CheckValidDataType(IndexINVERTED, &schemapb.FieldSchema{DataType: schemapb.DataType_JSON}))

	assert.Error(t, c.CheckValidDataType(IndexINVERTED, &schemapb.FieldSchema{DataType: schemapb.DataType_Geometry}))
	assert.Error(t, c.CheckValidDataType(IndexINVERTED, &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector}))
}

func Test_CheckTrain(t *testing.T) {
	c := newINVERTEDChecker()
	assert.NoError(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{"json_cast_type": "BOOL", "json_path": "json['a']"}))
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{"json_cast_type": "array", "json_path": "json['a']"}))
	assert.Error(t, c.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, map[string]string{"json_cast_type": "abc", "json_path": "json['a']"}))
}

func Test_JSONCastFunctionCheck(t *testing.T) {
	ngramParams := map[string]string{MinGramKey: "2", MaxGramKey: "3"}
	cases := []struct {
		name         string
		checker      IndexChecker
		castType     string
		castFunction string
		extra        map[string]string
		wantErr      bool
	}{
		{"inverted double", newINVERTEDChecker(), "DOUBLE", "STRING_TO_DOUBLE", nil, false},
		{"inverted varchar", newINVERTEDChecker(), "VARCHAR", "STRING_TO_DOUBLE", nil, true},
		{"inverted array double", newINVERTEDChecker(), "ARRAY_DOUBLE", "STRING_TO_DOUBLE", nil, true},
		{"inverted unknown", newINVERTEDChecker(), "DOUBLE", "UNKNOWN_FUNC", nil, true},
		{"stl_sort double", newSTLSORTChecker(), "DOUBLE", "STRING_TO_DOUBLE", nil, false},
		{"stl_sort varchar", newSTLSORTChecker(), "VARCHAR", "STRING_TO_DOUBLE", nil, true},
		{"stl_sort unknown", newSTLSORTChecker(), "DOUBLE", "UNKNOWN_FUNC", nil, true},
		{"bitmap no function", newBITMAPChecker(), "VARCHAR", "", nil, false},
		{"bitmap bool", newBITMAPChecker(), "BOOL", "STRING_TO_DOUBLE", nil, true},
		{"bitmap varchar", newBITMAPChecker(), "VARCHAR", "STRING_TO_DOUBLE", nil, true},
		{"bitmap unknown", newBITMAPChecker(), "VARCHAR", "UNKNOWN_FUNC", nil, true},
		{"hybrid double", newHYBRIDChecker(), "DOUBLE", "STRING_TO_DOUBLE", nil, false},
		{"hybrid varchar", newHYBRIDChecker(), "VARCHAR", "STRING_TO_DOUBLE", nil, true},
		{"hybrid unknown", newHYBRIDChecker(), "BOOL", "UNKNOWN_FUNC", nil, true},
		{"ngram no function", newNgramIndexChecker(), "VARCHAR", "", ngramParams, false},
		{"ngram varchar", newNgramIndexChecker(), "VARCHAR", "STRING_TO_DOUBLE", ngramParams, true},
		{"ngram unknown", newNgramIndexChecker(), "VARCHAR", "UNKNOWN_FUNC", ngramParams, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{"json_cast_type": tc.castType, "json_path": "/a"}
			if tc.castFunction != "" {
				params["json_cast_function"] = tc.castFunction
			}
			for k, v := range tc.extra {
				params[k] = v
			}
			err := tc.checker.CheckTrain(schemapb.DataType_JSON, schemapb.DataType_None, params)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
