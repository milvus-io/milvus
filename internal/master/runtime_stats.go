package master

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type RuntimeStats struct {
	collStats map[UniqueID]*CollRuntimeStats // collection id to array of field statistic
	mu        sync.RWMutex
}

func (rs *RuntimeStats) UpdateFieldStat(collID UniqueID, fieldID UniqueID, stats *FieldIndexRuntimeStats) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	peerID := stats.peerID
	_, ok := rs.collStats[collID]
	if !ok {
		rs.collStats[collID] = &CollRuntimeStats{
			fieldIndexStats: make(map[UniqueID][]*FieldIndexRuntimeStats),
		}
	}

	collRuntimeStats := rs.collStats[collID]
	fieldStats := collRuntimeStats.fieldIndexStats[fieldID]
	for i, v := range fieldStats {
		if v.peerID == peerID && typeutil.CompareIndexParams(v.indexParams, stats.indexParams) {
			fieldStats[i] = stats
			return nil
		}
	}

	collRuntimeStats.fieldIndexStats[fieldID] = append(collRuntimeStats.fieldIndexStats[fieldID], stats)
	return nil
}

func (rs *RuntimeStats) GetTotalNumOfRelatedSegments(collID UniqueID, fieldID UniqueID, indexParams []*commonpb.KeyValuePair) int64 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	collStats, ok := rs.collStats[collID]
	if !ok {
		return 0
	}

	fieldStats, ok := collStats.fieldIndexStats[fieldID]
	if !ok {
		return 0
	}

	var total int64 = 0
	for _, stat := range fieldStats {
		if typeutil.CompareIndexParams(stat.indexParams, indexParams) {
			total += stat.numOfRelatedSegments
		}
	}

	return total
}

type CollRuntimeStats struct {
	fieldIndexStats map[UniqueID][]*FieldIndexRuntimeStats
}

type FieldIndexRuntimeStats struct {
	peerID               int64
	indexParams          []*commonpb.KeyValuePair
	numOfRelatedSegments int64
}

func NewRuntimeStats() *RuntimeStats {
	return &RuntimeStats{
		collStats: make(map[UniqueID]*CollRuntimeStats),
	}
}
