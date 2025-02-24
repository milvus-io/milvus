package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

func Test_baseChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.L2,
	}
	paramsWithoutDim := map[string]string{
		Metric: metric.L2,
	}
	sparseParamsWithoutDim := map[string]string{
		Metric:             metric.IP,
		common.IsSparseKey: "True",
	}
	sparseParamsWrongMetric := map[string]string{
		Metric:             metric.L2,
		common.IsSparseKey: "True",
	}
	badSparseParams := map[string]string{
		Metric:             metric.IP,
		common.IsSparseKey: "ds",
	}
	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{validParams, true},
		{paramsWithoutDim, false},
		{sparseParamsWithoutDim, true},
		{sparseParamsWrongMetric, false},
		{badSparseParams, false},
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("HNSW")
	for _, test := range cases {
		test.params[common.IndexTypeKey] = "HNSW"
		var err error
		if test.params[common.IsSparseKey] == "True" {
			err = c.CheckTrain(schemapb.DataType_SparseFloatVector, test.params)
		} else {
			err = c.CheckTrain(schemapb.DataType_FloatVector, test.params)
		}
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
		fieldSchema := &schemapb.FieldSchema{DataType: test.dType}
		err := c.CheckValidDataType("FLAT", fieldSchema)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_baseChecker_StaticCheck(t *testing.T) {
	// TODO
	assert.Error(t, newBaseChecker().StaticCheck(schemapb.DataType_FloatVector, nil))
}
