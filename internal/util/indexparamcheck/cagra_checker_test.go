package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

func Test_cagraChecker_CheckTrain(t *testing.T) {
	p1 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.L2,
	}
	p2 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.IP,
	}
	p3 := map[string]string{
		DIM:              strconv.Itoa(128),
		Metric:           metric.L2,
		CagraInterDegree: strconv.Itoa(20),
	}

	p4 := map[string]string{
		DIM:              strconv.Itoa(128),
		Metric:           metric.L2,
		CagraGraphDegree: strconv.Itoa(20),
	}
	p5 := map[string]string{
		DIM:              strconv.Itoa(128),
		Metric:           metric.L2,
		CagraInterDegree: strconv.Itoa(60),
		CagraGraphDegree: strconv.Itoa(20),
	}
	p6 := map[string]string{
		DIM:              strconv.Itoa(128),
		Metric:           metric.L2,
		CagraInterDegree: strconv.Itoa(20),
		CagraGraphDegree: strconv.Itoa(60),
	}
	p7 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.SUPERSTRUCTURE,
	}
	p8 := map[string]string{
		DIM:              strconv.Itoa(128),
		Metric:           metric.L2,
		CagraInterDegree: "error",
		CagraGraphDegree: strconv.Itoa(20),
	}
	p9 := map[string]string{
		DIM:              strconv.Itoa(128),
		Metric:           metric.L2,
		CagraInterDegree: strconv.Itoa(20),
		CagraGraphDegree: "error",
	}
	p10 := map[string]string{
		DIM:    strconv.Itoa(0),
		Metric: metric.L2,
	}
	p11 := map[string]string{
		DIM:            strconv.Itoa(128),
		Metric:         metric.L2,
		CagraBuildAlgo: "IVF_PQ",
	}
	p12 := map[string]string{
		DIM:            strconv.Itoa(128),
		Metric:         metric.L2,
		CagraBuildAlgo: "HNSW",
	}
	p13 := map[string]string{
		DIM:                      strconv.Itoa(128),
		Metric:                   metric.L2,
		RaftCacheDatasetOnDevice: "false",
	}
	p14 := map[string]string{
		DIM:                      strconv.Itoa(128),
		Metric:                   metric.L2,
		RaftCacheDatasetOnDevice: "False",
	}
	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{p1, true},
		{p2, true},
		{p3, true},
		{p4, true},
		{p5, true},
		{p6, false},
		{p7, false},
		{p8, false},
		{p9, false},
		{p10, false},
		{p11, true},
		{p12, false},
		{p13, true},
		{p14, false},
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("GPU_CAGRA")
	if c == nil {
		log.Error("can not get index checker instance, please enable GPU and rerun it")
		return
	}
	for _, test := range cases {
		err := c.CheckTrain(schemapb.DataType_FloatVector, test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
