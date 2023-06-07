package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func Test_ngtPANNGChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:                       strconv.Itoa(128),
		EdgeSize:                  strconv.Itoa(10),
		ForcedlyPrunedEdgeSize:    strconv.Itoa(60),
		SelectivelyPrunedEdgeSize: strconv.Itoa(30),
		Metric:                    L2,
	}

	invalidEdgeSizeParamsMin := copyParams(validParams)
	invalidEdgeSizeParamsMin[EdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidEdgeSizeParamsMax := copyParams(validParams)
	invalidEdgeSizeParamsMax[EdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	invalidFPEdgeSizeParamsMin := copyParams(validParams)
	invalidFPEdgeSizeParamsMin[ForcedlyPrunedEdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidFPEdgeSizeParamsMax := copyParams(validParams)
	invalidFPEdgeSizeParamsMax[ForcedlyPrunedEdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	invalidSPEdgeSizeParamsMin := copyParams(validParams)
	invalidSPEdgeSizeParamsMin[SelectivelyPrunedEdgeSize] = strconv.Itoa(NgtMinEdgeSize - 1)

	invalidSPEdgeSizeParamsMax := copyParams(validParams)
	invalidSPEdgeSizeParamsMax[SelectivelyPrunedEdgeSize] = strconv.Itoa(NgtMaxEdgeSize + 1)

	invalidSPFPParams := copyParams(validParams)
	invalidSPFPParams[SelectivelyPrunedEdgeSize] = strconv.Itoa(60)
	invalidSPFPParams[ForcedlyPrunedEdgeSize] = strconv.Itoa(30)

	p1 := map[string]string{
		DIM:                       strconv.Itoa(128),
		EdgeSize:                  strconv.Itoa(10),
		ForcedlyPrunedEdgeSize:    strconv.Itoa(60),
		SelectivelyPrunedEdgeSize: strconv.Itoa(30),
		Metric:                    L2,
	}
	p2 := map[string]string{
		DIM:                       strconv.Itoa(128),
		EdgeSize:                  strconv.Itoa(10),
		ForcedlyPrunedEdgeSize:    strconv.Itoa(60),
		SelectivelyPrunedEdgeSize: strconv.Itoa(30),
		Metric:                    IP,
	}

	//p3 := map[string]string{
	//	DIM:                       strconv.Itoa(128),
	//	EdgeSize:                  strconv.Itoa(10),
	//	ForcedlyPrunedEdgeSize:    strconv.Itoa(60),
	//	SelectivelyPrunedEdgeSize: strconv.Itoa(30),
	//	Metric:                    COSINE,
	//}

	p4 := map[string]string{
		DIM:                       strconv.Itoa(128),
		EdgeSize:                  strconv.Itoa(10),
		ForcedlyPrunedEdgeSize:    strconv.Itoa(60),
		SelectivelyPrunedEdgeSize: strconv.Itoa(30),
		Metric:                    HAMMING,
	}
	p5 := map[string]string{
		DIM:                       strconv.Itoa(128),
		EdgeSize:                  strconv.Itoa(10),
		ForcedlyPrunedEdgeSize:    strconv.Itoa(60),
		SelectivelyPrunedEdgeSize: strconv.Itoa(30),
		Metric:                    JACCARD,
	}
	p6 := map[string]string{
		DIM:                       strconv.Itoa(128),
		EdgeSize:                  strconv.Itoa(10),
		ForcedlyPrunedEdgeSize:    strconv.Itoa(60),
		SelectivelyPrunedEdgeSize: strconv.Itoa(30),
		Metric:                    TANIMOTO,
	}
	p7 := map[string]string{
		DIM:                       strconv.Itoa(128),
		EdgeSize:                  strconv.Itoa(10),
		ForcedlyPrunedEdgeSize:    strconv.Itoa(60),
		SelectivelyPrunedEdgeSize: strconv.Itoa(30),
		Metric:                    SUBSTRUCTURE,
	}
	p8 := map[string]string{
		DIM:                       strconv.Itoa(128),
		EdgeSize:                  strconv.Itoa(10),
		ForcedlyPrunedEdgeSize:    strconv.Itoa(60),
		SelectivelyPrunedEdgeSize: strconv.Itoa(30),
		Metric:                    SUPERSTRUCTURE,
	}

	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{validParams, true},
		{invalidEdgeSizeParamsMin, false},
		{invalidEdgeSizeParamsMax, false},
		{invalidFPEdgeSizeParamsMin, false},
		{invalidFPEdgeSizeParamsMax, false},
		{invalidSPEdgeSizeParamsMin, false},
		{invalidSPEdgeSizeParamsMax, false},
		{invalidSPFPParams, false},
		{p1, true},
		{p2, true},
		//{p3, true},
		{p4, false},
		{p5, false},
		{p6, false},
		{p7, false},
		{p8, false},
	}

	c := newNgtPANNGChecker()
	for _, test := range cases {
		err := c.CheckTrain(test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_ngtPANNGChecker_CheckValidDataType(t *testing.T) {

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

	c := newNgtPANNGChecker()
	for _, test := range cases {
		err := c.CheckValidDataType(test.dType)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
