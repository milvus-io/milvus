package datacoord

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/metrics"
	"sync"
)

// A local cache of segment metric update. Must call commit() to take effect.
type SegMetricMutation struct {
	sync.RWMutex
	stateChange       map[string]map[string]int // segment state, seg level -> state change count (to increase or decrease).
	rowCountChange    int64                     // Change in # of rows.
	rowCountAccChange int64                     // Total # of historical added rows, accumulated.
}

// addNewSeg update metrics update for a new segment.
func (s *SegMetricMutation) addNewSeg(state commonpb.SegmentState, level datapb.SegmentLevel, rowCount int64) {
	s.Lock()
	defer s.Lock()
	if _, ok := s.stateChange[level.String()]; !ok {
		s.stateChange[level.String()] = make(map[string]int)
	}
	s.stateChange[level.String()][state.String()] += 1

	s.rowCountChange += rowCount
	s.rowCountAccChange += rowCount
}

// commit persists all updates in current segMetricMutation, should and must be called AFTER segment state change
// has persisted in Etcd.
func (s *SegMetricMutation) commit() {
	s.Lock()
	defer s.Lock()
	for level, submap := range s.stateChange {
		for state, change := range submap {
			metrics.DataCoordNumSegments.WithLabelValues(state, level).Add(float64(change))
		}
	}
	metrics.DataCoordNumStoredRowsCounter.WithLabelValues().Add(float64(s.rowCountAccChange))
}

// append updates current segMetricMutation when segment state change happens.
func (s *SegMetricMutation) append(oldState, newState commonpb.SegmentState, level datapb.SegmentLevel, rowCountUpdate int64) {
	s.Lock()
	defer s.Lock()
	if oldState != newState {
		if _, ok := s.stateChange[level.String()]; !ok {
			s.stateChange[level.String()] = make(map[string]int)
		}
		s.stateChange[level.String()][oldState.String()] -= 1
		s.stateChange[level.String()][newState.String()] += 1
	}
	// Update # of rows on new flush operations and drop operations.
	if isFlushState(newState) && !isFlushState(oldState) {
		// If new flush.
		s.rowCountChange += rowCountUpdate
		s.rowCountAccChange += rowCountUpdate
	} else if newState == commonpb.SegmentState_Dropped && oldState != newState {
		// If new drop.
		s.rowCountChange -= rowCountUpdate
	}
}

func isFlushState(state commonpb.SegmentState) bool {
	return state == commonpb.SegmentState_Flushing || state == commonpb.SegmentState_Flushed
}
