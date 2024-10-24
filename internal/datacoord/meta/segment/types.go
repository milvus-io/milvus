// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segment

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// SegmenManager is the interface for segment manager API.
type SegmentManager interface {
	AddSegment(context.Context, *SegmentInfo) error
	GetSegmentByID(typeutil.UniqueID, ...SegmentFilter) *SegmentInfo
	GetSegments(...SegmentFilter) []*SegmentInfo
	UpdateSegment(ctx context.Context, segmentID int64, operators ...SegmentOperator) error
	UpdateSegmentsIf(ctx context.Context, condition Condition, actions ...SegmentOperator) error
	UpdateSegmentsInfo(ctx context.Context, operators ...UpdateOperator) error
	DropSegment(ctx context.Context, segmentID typeutil.UniqueID) error
}

func getFieldBinlogs(id typeutil.UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
	for _, binlog := range binlogs {
		if id == binlog.GetFieldID() {
			return binlog
		}
	}
	return nil
}

func mergeFieldBinlogs(currentBinlogs []*datapb.FieldBinlog, newBinlogs []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	for _, newBinlog := range newBinlogs {
		fieldBinlogs := getFieldBinlogs(newBinlog.GetFieldID(), currentBinlogs)
		if fieldBinlogs == nil {
			currentBinlogs = append(currentBinlogs, newBinlog)
		} else {
			fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, newBinlog.Binlogs...)
		}
	}
	return currentBinlogs
}

// A local cache of segment metric update. Must call commit() to take effect.
type segMetricMutation struct {
	stateChange       map[string]map[string]int // segment state, seg level -> state change count (to increase or decrease).
	rowCountChange    int64                     // Change in # of rows.
	rowCountAccChange int64                     // Total # of historical added rows, accumulated.
}

// updateSegStateAndPrepareMetrics updates a segment's in-memory state and prepare for the corresponding metric update.
func updateSegStateAndPrepareMetrics(segToUpdate *SegmentInfo, targetState commonpb.SegmentState, metricMutation *segMetricMutation) {
	log.Debug("updating segment state and updating metrics",
		zap.Int64("segmentID", segToUpdate.GetID()),
		zap.String("old state", segToUpdate.GetState().String()),
		zap.String("new state", targetState.String()),
		zap.Int64("# of rows", segToUpdate.GetNumOfRows()))
	metricMutation.append(segToUpdate.GetState(), targetState, segToUpdate.GetLevel(), segToUpdate.GetNumOfRows())
	segToUpdate.State = targetState
	if targetState == commonpb.SegmentState_Dropped {
		segToUpdate.DroppedAt = uint64(time.Now().UnixNano())
	}
}

// addNewSeg update metrics update for a new segment.
func (s *segMetricMutation) addNewSeg(state commonpb.SegmentState, level datapb.SegmentLevel, rowCount int64) {
	if _, ok := s.stateChange[level.String()]; !ok {
		s.stateChange[level.String()] = make(map[string]int)
	}
	s.stateChange[level.String()][state.String()] += 1

	s.rowCountChange += rowCount
	s.rowCountAccChange += rowCount
}

// commit persists all updates in current segMetricMutation, should and must be called AFTER segment state change
// has persisted in Etcd.
func (s *segMetricMutation) commit() {
	for level, submap := range s.stateChange {
		for state, change := range submap {
			metrics.DataCoordNumSegments.WithLabelValues(state, level).Add(float64(change))
		}
	}
}

// append updates current segMetricMutation when segment state change happens.
func (s *segMetricMutation) append(oldState, newState commonpb.SegmentState, level datapb.SegmentLevel, rowCountUpdate int64) {
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
	return state == commonpb.SegmentState_Flushed || state == commonpb.SegmentState_Flushing
}

type CompactionGroup struct {
	CollectionID int64
	PartitionID  int64
	ChannelName  string
	Segments     []*SegmentInfo
}
