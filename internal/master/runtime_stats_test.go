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
		err := runtimeStats.UpdateFieldStat(testcase.collID, testcase.fieldID, &FieldIndexRuntimeStats{
			peerID:               testcase.peerID,
			indexParams:          []*commonpb.KeyValuePair{},
			numOfRelatedSegments: testcase.nums,
		})
		assert.Nil(t, err)
		statsArray := runtimeStats.collStats[testcase.collID].fieldIndexStats[testcase.fieldID]
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

func TestRuntimeStats_GetTotalNumOfRelatedSegments(t *testing.T) {
	runtimeStats := NewRuntimeStats()
	runtimeStats.collStats = make(map[UniqueID]*CollRuntimeStats)

	runtimeStats.collStats[1] = &CollRuntimeStats{
		fieldIndexStats: map[UniqueID][]*FieldIndexRuntimeStats{
			100: {
				{1, []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}, 10},
				{3, []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}, 20},
				{2, []*commonpb.KeyValuePair{{Key: "k2", Value: "v2"}}, 20},
			},
			200: {
				{1, []*commonpb.KeyValuePair{}, 20},
			},
		},
	}

	runtimeStats.collStats[2] = &CollRuntimeStats{
		fieldIndexStats: map[UniqueID][]*FieldIndexRuntimeStats{
			100: {
				{1, []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}, 10},
			},
		},
	}
	assert.EqualValues(t, 30, runtimeStats.GetTotalNumOfRelatedSegments(1, 100, []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}))
	assert.EqualValues(t, 20, runtimeStats.GetTotalNumOfRelatedSegments(1, 100, []*commonpb.KeyValuePair{{Key: "k2", Value: "v2"}}))
	assert.EqualValues(t, 20, runtimeStats.GetTotalNumOfRelatedSegments(1, 200, []*commonpb.KeyValuePair{}))
	assert.EqualValues(t, 10, runtimeStats.GetTotalNumOfRelatedSegments(2, 100, []*commonpb.KeyValuePair{{Key: "k1", Value: "v1"}}))
}
