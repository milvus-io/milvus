package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

func Test_binIVFFlatChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: metric.JACCARD,
	}
	paramsWithoutDim := map[string]string{
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: metric.JACCARD,
	}

	invalidParams := copyParams(validParams)
	invalidParams[Metric] = metric.L2

	paramsWithLargeNlist := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(MaxNList + 1),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: metric.JACCARD,
	}

	paramsWithSmallNlist := map[string]string{
		DIM:    strconv.Itoa(128),
		NLIST:  strconv.Itoa(MinNList - 1),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
		Metric: metric.JACCARD,
	}

	p1 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.L2,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}
	p2 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.IP,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}
	p3 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.COSINE,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}

	p4 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.HAMMING,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}
	p5 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.JACCARD,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}
	p6 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.SUBSTRUCTURE,
		NLIST:  strconv.Itoa(100),
		IVFM:   strconv.Itoa(4),
		NBITS:  strconv.Itoa(8),
	}
	p7 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.SUPERSTRUCTURE,
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
		{p6, false},
		{p7, false},
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("BIN_IVF_FLAT")
	for _, test := range cases {
		test.params[common.IndexTypeKey] = "BIN_IVF_FLAT"
		err := c.CheckTrain(schemapb.DataType_BinaryVector, test.params)
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

	c, _ := GetIndexCheckerMgrInstance().GetChecker("BIN_IVF_FLAT")
	for _, test := range cases {
		fieldSchema := &schemapb.FieldSchema{DataType: test.dType}
		err := c.CheckValidDataType("BIN_IVF_FLAT", fieldSchema)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
