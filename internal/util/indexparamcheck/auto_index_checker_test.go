package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

func Test_AUTOINDEXChecker_CheckTrain_BinaryMetrics(t *testing.T) {
	c, err := GetIndexCheckerMgrInstance().GetChecker(string(AutoIndex))
	assert.NoError(t, err)

	cases := []struct {
		name     string
		metric   string
		wantErr  bool
	}{
		{name: "hamming", metric: metric.HAMMING, wantErr: false},
		{name: "jaccard", metric: metric.JACCARD, wantErr: false},
		{name: "tanimoto", metric: "TANIMOTO", wantErr: true},
		{name: "superstructure", metric: metric.SUPERSTRUCTURE, wantErr: true},
		{name: "substructure", metric: metric.SUBSTRUCTURE, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]string{
				common.IndexTypeKey: string(AutoIndex),
				Metric:              tc.metric,
			}
			err := c.CheckTrain(schemapb.DataType_BinaryVector, schemapb.DataType_None, params)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
