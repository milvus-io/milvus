package master

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type RuntimeStats struct {
	collStats map[UniqueID]*CollRuntimeStats // collection id to array of field statistic
}

func (rs *RuntimeStats) UpdateFieldStat(collID UniqueID, fieldID UniqueID, stats *FieldRuntimeStats) error {
	peerID := stats.peerID
	_, ok := rs.collStats[collID]
	if !ok {
		rs.collStats[collID] = &CollRuntimeStats{
			fieldStats: make(map[UniqueID][]*FieldRuntimeStats),
		}
	}

	collRuntimeStats := rs.collStats[collID]
	fieldStats := collRuntimeStats.fieldStats[fieldID]
	for i, v := range fieldStats {
		if v.peerID == peerID { // todo compare index params
			fieldStats[i] = stats
			return nil
		}
	}

	collRuntimeStats.fieldStats[fieldID] = append(collRuntimeStats.fieldStats[fieldID], stats)
	return nil
}

type CollRuntimeStats struct {
	fieldStats map[UniqueID][]*FieldRuntimeStats
}

type FieldRuntimeStats struct {
	peerID               int64
	indexParams          []*commonpb.KeyValuePair
	numOfRelatedSegments int64
}

func NewRuntimeStats() *RuntimeStats {
	return &RuntimeStats{
		collStats: make(map[UniqueID]*CollRuntimeStats),
	}
}
