package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/stretchr/testify/assert"
)

func Test_binIVFFlatChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: JACCARD,
	}
	paramsWithoutDim := map[string]string{
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: JACCARD,
	}

	invalidParams := copyParams(validParams)
	invalidParams[Metric] = L2

	paramsWithLargeNlist := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(MaxNList + 1),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: JACCARD,
	}

	paramsWithSmallNlist := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(MinNList - 1),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: JACCARD,
	}

	p1 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: L2,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}
	p2 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: IP,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}
	p3 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: COSINE,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}

	p4 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: HAMMING,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}
	p5 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: JACCARD,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}
	p6 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: TANIMOTO,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}

	p7 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: SUBSTRUCTURE,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}
	p8 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: SUPERSTRUCTURE,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}

	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{validParams, true},
		{paramsWithoutDim, false},
		{paramsWithLargeNlist, false},
		{paramsWithSmallNlist, false},
		{invalidParams, false},

		{p1, false},
		{p2, false},
		{p3, false},

		{p4, true},
		{p5, true},
		{p6, true},

		{p7, false},
		{p8, false},
	}

	c := newBinIVFFlatChecker()
	for _, test := range cases {
		err := c.CheckTrain(test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_binIVFFlatChecker_CheckValidDataType(t *testing.T) {

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

	c := newBinIVFFlatChecker()
	for _, test := range cases {
		err := c.CheckValidDataType(test.dType)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
