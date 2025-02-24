package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

func Test_ivfSQChecker_CheckTrain(t *testing.T) {
	getValidParams := func(withNBits bool) map[string]string {
		validParams := map[string]string{
			DIM:    strconv.Itoa(128),
			NLIST:  strconv.Itoa(100),
			NBITS:  strconv.Itoa(8),
			Metric: metric.L2,
		}
		if withNBits {
			validParams[NBITS] = strconv.Itoa(DefaultNBits)
		}
		return validParams
	}
	validParams := getValidParams(false)
	validParamsWithNBits := getValidParams(true)
	paramsWithInvalidNBits := getValidParams(false)
	paramsWithInvalidNBits[NBITS] = strconv.Itoa(DefaultNBits + 1)

	p1 := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		NBITS:  strconv.Itoa(8),
		Metric: metric.L2,
	}
	p2 := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		NBITS:  strconv.Itoa(8),
		Metric: metric.IP,
	}
	p3 := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		NBITS:  strconv.Itoa(8),
		Metric: metric.COSINE,
	}

	p4 := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		NBITS:  strconv.Itoa(8),
		Metric: metric.HAMMING,
	}
	p5 := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		NBITS:  strconv.Itoa(8),
		Metric: metric.JACCARD,
	}
	p6 := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		NBITS:  strconv.Itoa(8),
		Metric: metric.SUBSTRUCTURE,
	}
	p7 := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		NBITS:  strconv.Itoa(8),
		Metric: metric.SUPERSTRUCTURE,
	}

	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{validParams, true},
		{validParamsWithNBits, true},
		{invalidIVFParamsMin(), false},
		{invalidIVFParamsMax(), false},
		{p1, true},
		{p2, true},
		{p3, true},
		{p4, false},
		{p5, false},
		{p6, false},
		{p7, false},
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("IVF_SQ")
	for _, test := range cases {
		test.params[common.IndexTypeKey] = "IVF_SQ"
		err := c.CheckTrain(schemapb.DataType_FloatVector, test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_ivfSQChecker_CheckValidDataType(t *testing.T) {
	cases := []struct {
		dType    schemapb.DataType
		errIsNil bool
	}{
		{
			dType:    schemapb.DataType_Bool,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_Int8,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_Int16,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_Int32,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_Int64,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_Float,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_Double,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_String,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_VarChar,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_Array,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_JSON,
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_FloatVector,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_BinaryVector,
			errIsNil: false,
		},
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("IVF_SQ")
	for _, test := range cases {
		err := c.CheckValidDataType("IVF_SQ8", &schemapb.FieldSchema{DataType: test.dType})
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
