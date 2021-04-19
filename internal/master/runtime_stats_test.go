package master

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

func TestRuntimeStats_UpdateFieldStats(t *testing.T) {
	runtimeStats := NewRuntimeStats()
	cases := []*struct {
		collID  UniqueID
		fieldID UniqueID
		peerID  int64
		nums    int64
	}{
		{1, 1, 2, 10},
		{1, 2, 2, 20},
		{2, 2, 2, 30},
		{2, 2, 3, 40},
		{1, 1, 2, 100},
	}
	for _, testcase := range cases {
		err := runtimeStats.UpdateFieldStat(testcase.collID, testcase.fieldID, &FieldRuntimeStats{
			peerID:               testcase.peerID,
			indexParams:          []*commonpb.KeyValuePair{},
			numOfRelatedSegments: testcase.nums,
		})
		assert.Nil(t, err)
		statsArray := runtimeStats.collStats[testcase.collID].fieldStats[testcase.fieldID]
		assert.NotEmpty(t, statsArray)

		found := 0
		for _, s := range statsArray {
			if s.peerID == testcase.peerID {
				found++
				assert.EqualValues(t, s.numOfRelatedSegments, testcase.nums)
			}
		}
		assert.EqualValues(t, 1, found)
	}
}
