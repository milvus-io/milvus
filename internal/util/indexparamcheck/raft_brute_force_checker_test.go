package indexparamcheck

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/util/metric"
)

func Test_raftbfChecker_CheckTrain(t *testing.T) {
	p1 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.L2,
	}
	p2 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.IP,
	}
	p3 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.COSINE,
	}

	p4 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.HAMMING,
	}
	p5 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.JACCARD,
	}
	p6 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.SUBSTRUCTURE,
	}
	p7 := map[string]string{
		DIM:    strconv.Itoa(128),
		Metric: metric.SUPERSTRUCTURE,
	}
	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{p1, true},
		{p2, true},
		{p3, false},
		{p4, false},
		{p5, false},
		{p6, false},
		{p7, false},
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("GPU_BRUTE_FORCE")
	for _, test := range cases {
		err := c.CheckTrain(schemapb.DataType_FloatVector, test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
