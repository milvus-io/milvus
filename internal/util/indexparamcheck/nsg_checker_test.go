package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func Test_nsgChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:          strconv.Itoa(128),
		KNNG:         strconv.Itoa(20),
		SearchLength: strconv.Itoa(40),
		OutDegree:    strconv.Itoa(30),
		CANDIDATE:    strconv.Itoa(100),
		Metric:       L2,
	}

	invalidMatricParams := copyParams(validParams)
	invalidMatricParams[Metric] = JACCARD

	invalidKNNGParamsMin := copyParams(validParams)
	invalidKNNGParamsMin[KNNG] = strconv.Itoa(MinKNNG - 1)

	invalidKNNGParamsMax := copyParams(validParams)
	invalidKNNGParamsMax[KNNG] = strconv.Itoa(MaxKNNG + 1)

	invalidLengthParamsMin := copyParams(validParams)
	invalidLengthParamsMin[SearchLength] = strconv.Itoa(MinSearchLength - 1)

	invalidLengthParamsMax := copyParams(validParams)
	invalidLengthParamsMax[SearchLength] = strconv.Itoa(MaxSearchLength + 1)

	invalidDegreeParamsMin := copyParams(validParams)
	invalidDegreeParamsMin[OutDegree] = strconv.Itoa(MinOutDegree - 1)

	invalidDegreeParamsMax := copyParams(validParams)
	invalidDegreeParamsMax[OutDegree] = strconv.Itoa(MaxOutDegree + 1)

	invalidPoolParamsMin := copyParams(validParams)
	invalidPoolParamsMin[CANDIDATE] = strconv.Itoa(MinCandidatePoolSize - 1)

	invalidPoolParamsMax := copyParams(validParams)
	invalidPoolParamsMax[CANDIDATE] = strconv.Itoa(MaxCandidatePoolSize + 1)

	p1 := map[string]string{
		DIM:          strconv.Itoa(128),
		KNNG:         strconv.Itoa(20),
		SearchLength: strconv.Itoa(40),
		OutDegree:    strconv.Itoa(30),
		CANDIDATE:    strconv.Itoa(100),
		Metric:       L2,
	}
	p2 := map[string]string{
		DIM:          strconv.Itoa(128),
		KNNG:         strconv.Itoa(20),
		SearchLength: strconv.Itoa(40),
		OutDegree:    strconv.Itoa(30),
		CANDIDATE:    strconv.Itoa(100),
		Metric:       IP,
	}

	//p3 := map[string]string{
	//	DIM:          strconv.Itoa(128),
	//	KNNG:         strconv.Itoa(20),
	//	SearchLength: strconv.Itoa(40),
	//	OutDegree:    strconv.Itoa(30),
	//	CANDIDATE:    strconv.Itoa(100),
	//	Metric:       COSINE,
	//}

	p4 := map[string]string{
		DIM:          strconv.Itoa(128),
		KNNG:         strconv.Itoa(20),
		SearchLength: strconv.Itoa(40),
		OutDegree:    strconv.Itoa(30),
		CANDIDATE:    strconv.Itoa(100),
		Metric:       HAMMING,
	}
	p5 := map[string]string{
		DIM:          strconv.Itoa(128),
		KNNG:         strconv.Itoa(20),
		SearchLength: strconv.Itoa(40),
		OutDegree:    strconv.Itoa(30),
		CANDIDATE:    strconv.Itoa(100),
		Metric:       JACCARD,
	}
	p6 := map[string]string{
		DIM:          strconv.Itoa(128),
		KNNG:         strconv.Itoa(20),
		SearchLength: strconv.Itoa(40),
		OutDegree:    strconv.Itoa(30),
		CANDIDATE:    strconv.Itoa(100),
		Metric:       TANIMOTO,
	}
	p7 := map[string]string{
		DIM:          strconv.Itoa(128),
		KNNG:         strconv.Itoa(20),
		SearchLength: strconv.Itoa(40),
		OutDegree:    strconv.Itoa(30),
		CANDIDATE:    strconv.Itoa(100),
		Metric:       SUBSTRUCTURE,
	}
	p8 := map[string]string{
		DIM:          strconv.Itoa(128),
		KNNG:         strconv.Itoa(20),
		SearchLength: strconv.Itoa(40),
		OutDegree:    strconv.Itoa(30),
		CANDIDATE:    strconv.Itoa(100),
		Metric:       SUPERSTRUCTURE,
	}

	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{validParams, true},
		{invalidMatricParams, false},
		{invalidKNNGParamsMin, false},
		{invalidKNNGParamsMax, false},
		{invalidLengthParamsMin, false},
		{invalidLengthParamsMax, false},
		{invalidDegreeParamsMin, false},
		{invalidDegreeParamsMax, false},
		{invalidPoolParamsMin, false},
		{invalidPoolParamsMax, false},
		{p1, true},
		{p2, true},
		//{p3, true},
		{p4, false},
		{p5, false},
		{p6, false},
		{p7, false},
		{p8, false},
	}

	c := newNsgChecker()
	for _, test := range cases {
		err := c.CheckTrain(test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_nsgChecker_CheckValidDataType(t *testing.T) {

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

	c := newNsgChecker()
	for _, test := range cases {
		err := c.CheckValidDataType(test.dType)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
