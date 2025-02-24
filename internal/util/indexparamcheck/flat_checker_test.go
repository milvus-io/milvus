package indexparamcheck

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

func Test_flatChecker_CheckTrain(t *testing.T) {
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
		{p3, true},
		{p4, false},
		{p5, false},
		{p6, false},
		{p7, false},
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("FLAT")
	for _, test := range cases {
		test.params[common.IndexTypeKey] = "FLAT"
		err := c.CheckTrain(schemapb.DataType_FloatVector, test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}

func Test_flatChecker_StaticCheck(t *testing.T) {
	cases := []struct {
		params   map[string]string
		errIsNil bool
	}{
		{
			// metrics not found.
			params:   map[string]string{},
			errIsNil: false,
		},
		{
			// invalid metric.
			params: map[string]string{
				Metric: metric.HAMMING,
			},
			errIsNil: false,
		},
		{
			// normal case.
			params: map[string]string{
				Metric: metric.L2,
			},
			errIsNil: true,
		},
	}

	c, _ := GetIndexCheckerMgrInstance().GetChecker("FLAT")
	for _, test := range cases {
		test.params[common.IndexTypeKey] = "FLAT"
		err := c.StaticCheck(schemapb.DataType_FloatVector, test.params)
		if test.errIsNil {
			assert.NoError(t, err)
		} else {
			assert.Error(t, err)
		}
	}
}
