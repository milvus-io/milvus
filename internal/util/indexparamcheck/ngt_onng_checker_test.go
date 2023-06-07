package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func Test_ngtONNGChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:              strconv.Itoa(128),
		EdgeSize:         strconv.Itoa(20),
		OutgoingEdgeSize: strconv.Itoa(5),
		IncomingEdgeSize: strconv.Itoa(40),
		Metric:           L2,
	}

	invalidEdgeSizeParamsMin := copyParams(validParams)
	invalidEdgeSizeParamsMin[EdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidEdgeSizeParamsMax := copyParams(validParams)
	invalidEdgeSizeParamsMax[EdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	invalidOutEdgeSizeParamsMin := copyParams(validParams)
	invalidOutEdgeSizeParamsMin[OutgoingEdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidOutEdgeSizeParamsMax := copyParams(validParams)
	invalidOutEdgeSizeParamsMax[OutgoingEdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	invalidInEdgeSizeParamsMin := copyParams(validParams)
	invalidInEdgeSizeParamsMin[IncomingEdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidInEdgeSizeParamsMax := copyParams(validParams)
	invalidInEdgeSizeParamsMax[IncomingEdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	p1 := map[string]string{
		DIM:              strconv.Itoa(128),
		EdgeSize:         strconv.Itoa(20),
		OutgoingEdgeSize: strconv.Itoa(5),
		IncomingEdgeSize: strconv.Itoa(40),
		Metric:           L2,
	}
	p2 := map[string]string{
		DIM:              strconv.Itoa(128),
		EdgeSize:         strconv.Itoa(20),
		OutgoingEdgeSize: strconv.Itoa(5),
		IncomingEdgeSize: strconv.Itoa(40),
		Metric:           IP,
	}

	//p3 := map[string]string{
	//	DIM:              strconv.Itoa(128),
	//	EdgeSize:         strconv.Itoa(20),
	//	OutgoingEdgeSize: strconv.Itoa(5),
	//	IncomingEdgeSize: strconv.Itoa(40),
	//	Metric:           COSINE,
	//}

	p4 := map[string]string{
		DIM:              strconv.Itoa(128),
		EdgeSize:         strconv.Itoa(20),
		OutgoingEdgeSize: strconv.Itoa(5),
		IncomingEdgeSize: strconv.Itoa(40),
		Metric:           HAMMING,
	}
	p5 := map[string]string{
		DIM:              strconv.Itoa(128),
		EdgeSize:         strconv.Itoa(20),
		OutgoingEdgeSize: strconv.Itoa(5),
		IncomingEdgeSize: strconv.Itoa(40),
		Metric:           JACCARD,
	}
	p6 := map[string]string{
		DIM:              strconv.Itoa(128),
		EdgeSize:         strconv.Itoa(20),
		OutgoingEdgeSize: strconv.Itoa(5),
		IncomingEdgeSize: strconv.Itoa(40),
		Metric:           TANIMOTO,
	}
	p7 := map[string]string{
		DIM:              strconv.Itoa(128),
		EdgeSize:         strconv.Itoa(20),
		OutgoingEdgeSize: strconv.Itoa(5),
		IncomingEdgeSize: strconv.Itoa(40),
		Metric:           SUBSTRUCTURE,
	}
	p8 := map[string]string{
		DIM:              strconv.Itoa(128),
		EdgeSize:         strconv.Itoa(20),
		OutgoingEdgeSize: strconv.Itoa(5),
		IncomingEdgeSize: strconv.Itoa(40),
		Metric:           SUPERSTRUCTURE,
	}

	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{validParams, true},
		{invalidEdgeSizeParamsMin, false},
		{invalidEdgeSizeParamsMax, false},
		{invalidOutEdgeSizeParamsMin, false},
		{invalidOutEdgeSizeParamsMax, false},
		{invalidInEdgeSizeParamsMin, false},
		{invalidInEdgeSizeParamsMax, false},
		{p1, true},
		{p2, true},
		//{p3, true},
		{p4, false},
		{p5, false},
		{p6, false},
		{p7, false},
		{p8, false},
	}

	c := newNgtONNGChecker()
	for _, test := range cases {
		err := c.CheckTrain(test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_ngtONNGChecker_CheckValidDataType(t *testing.T) {

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
			dType:    schemapb.DataType_FloatVector,
			errIsNil: true,
		},
		{
			dType:    schemapb.DataType_BinaryVector,
			errIsNil: false,
		},
	}

	c := newNgtONNGChecker()
	for _, test := range cases {
		err := c.CheckValidDataType(test.dType)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
