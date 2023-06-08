package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/stretchr/testify/assert"
)

func Test_baseChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: L2,
	}
	paramsWithoutDim := map[string]string{
		Metric: L2,
	}
	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{validParams, true},
		{paramsWithoutDim, false},
	}

	c := newBaseChecker()
	for _, test := range cases {
		err := c.CheckTrain(test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_baseChecker_CheckValidDataType(t *testing.T) {
	cases := []struct {
		dType    schemapb.DataType
		errIsNil bool
	}{
		{
			dType:    schemapb.DataType_Bool,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_Int8,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_Int16,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_Int32,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_Int64,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_Float,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_Double,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_String,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_VarChar,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_Array,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_JSON,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_FloatVector,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_BinaryVector,
			errIsNil: true,
		},
	}

	c := newBaseChecker()
	for _, test := range cases {
		err := c.CheckValidDataType(test.dType)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_baseChecker_StaticCheck(t *testing.T) {
	// TODO
	assert.Error(t, newBaseChecker().StaticCheck(nil))
}
