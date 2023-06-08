package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/stretchr/testify/assert"
)

func Test_binFlatChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: JACCARD,
	}
	paramsWithoutDim := map[string]string{
		Metric: JACCARD,
	}

	p1 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: L2,
	}
	p2 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: IP,
	}
	p3 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: COSINE,
	}

	p4 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: HAMMING,
	}
	p5 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: JACCARD,
	}
	p6 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: TANIMOTO,
	}
	p7 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: SUBSTRUCTURE,
	}
	p8 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: SUPERSTRUCTURE,
	}

	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{validParams, true},
		{paramsWithoutDim, false},
		{p1, false},
		{p2, false},
		{p3, false},
		{p4, true},
		{p5, true},
		{p6, true},
		{p7, true},
		{p8, true},
	}

	c := newBinFlatChecker()
	for _, test := range cases {
		err := c.CheckTrain(test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_binFlatChecker_CheckValidDataType(t *testing.T) {

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
			errIsNil: false,
		},
		{
			dType:    schemapb.DataType_BinaryVector,
			errIsNil: true,
		},
	}

	c := newBinFlatChecker()
	for _, test := range cases {
		err := c.CheckValidDataType(test.dType)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
