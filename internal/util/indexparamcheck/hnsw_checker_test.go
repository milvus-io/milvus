package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

func Test_hnswChecker_CheckTrain(t *testing.T) {
	validParams := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         metric.L2,
	}

	invalidEfParamsMin := copyParams(validParams)
	invalidEfParamsMin[EFConstruction] = strconv.Itoa(HNSWMinEfConstruction - 1)

	invalidEfParamsMax := copyParams(validParams)
	invalidEfParamsMax[EFConstruction] = strconv.Itoa(HNSWMaxEfConstruction + 1)

	invalidMParamsMin := copyParams(validParams)
	invalidMParamsMin[HNSWM] = strconv.Itoa(HNSWMinM - 1)

	invalidMParamsMax := copyParams(validParams)
	invalidMParamsMax[HNSWM] = strconv.Itoa(HNSWMaxM + 1)

	p1 := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         metric.L2,
	}
	p2 := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         metric.IP,
	}
	p3 := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         metric.COSINE,
	}

	p4 := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         metric.HAMMING,
	}
	p5 := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         metric.JACCARD,
	}
	p6 := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         metric.SUBSTRUCTURE,
	}
	p7 := map[string]string{
		DIM:            strconv.Itoa(128),
		HNSWM:          strconv.Itoa(16),
		EFConstruction: strconv.Itoa(200),
		Metric:         metric.SUPERSTRUCTURE,
	}

	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{validParams, true},
		{invalidEfParamsMin, false},
		{invalidEfParamsMax, false},
		{invalidMParamsMin, false},
		{invalidMParamsMax, false},
		{p1, true},
		{p2, true},
		{p3, true},
		{p4, true},
		{p5, true},
		{p6, true},
		{p7, true},
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("HNSW")
	for _, test := range cases {
		test.params[common.IndexTypeKey] = "HNSW"
		var err error
		if CheckStrByValues(test.params, common.MetricTypeKey, BinaryVectorMetrics) {
			err = c.CheckTrain(schemapb.DataType_BinaryVector, test.params)
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

func Test_hnswChecker_CheckValidDataType(t *testing.T) {
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

	c, _ := GetIndexCheckerMgrInstance().GetChecker("HNSW")
	for _, test := range cases {
		err := c.CheckValidDataType("HNSW", &schemapb.FieldSchema{DataType: test.dType})
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_hnswChecker_SetDefaultMetricType(t *testing.T) {
	cases := []struct {
		dType      schemapb.DataType
		metricType string
	}{
		{
			dType:      schemapb.DataType_FloatVector,
			metricType: metric.COSINE,
		},
		{
			dType:      schemapb.DataType_Float16Vector,
			metricType: metric.COSINE,
		},
		{
			dType:      schemapb.DataType_BFloat16Vector,
			metricType: metric.COSINE,
		},
		{
			dType:      schemapb.DataType_SparseFloatVector,
			metricType: metric.IP,
		},
		{
			dType:      schemapb.DataType_BinaryVector,
			metricType: metric.HAMMING,
		},
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("HNSW")
	for _, test := range cases {
		p := map[string]string{
			DIM:            strconv.Itoa(128),
			HNSWM:          strconv.Itoa(16),
			EFConstruction: strconv.Itoa(200),
		}
		c.SetDefaultMetricTypeIfNotExist(test.dType, p)
		assert.Equal(t, p[Metric], test.metricType)
	}
}
