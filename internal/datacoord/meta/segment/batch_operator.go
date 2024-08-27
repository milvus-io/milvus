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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/log"
)

type updateSegmentPack struct {
	meta     *segmentManager
	segments map[int64]*SegmentInfo
	// for update etcd binlog paths
	increments map[int64]metastore.BinlogsIncrement
	// for update segment metric after alter segments
	metricMutation *segMetricMutation
}

func (p *updateSegmentPack) Get(segmentID int64) *SegmentInfo {
	if segment, ok := p.segments[segmentID]; ok {
		return segment
	}

	segment := p.meta.GetSegmentByID(segmentID)
	if segment == nil || !WithHealthyState().Match(segment) {
		log.Warn("meta update: get segment failed - segment not found",
			zap.Int64("segmentID", segmentID),
			zap.Bool("segment nil", segment == nil),
			zap.Bool("segment unhealthy", !WithHealthyState().Match(segment)))
		return nil
	}

	p.segments[segmentID] = segment.Clone()
	return p.segments[segmentID]
}

type UpdateOperator func(*updateSegmentPack) bool

func CreateL0Operator(collectionID, partitionID, segmentID int64, channel string) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.meta.GetSegmentByID(segmentID)
		if segment == nil {
			log.Info("meta update: add new l0 segment",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID))

			modPack.segments[segmentID] = &SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					ID:            segmentID,
					CollectionID:  collectionID,
					PartitionID:   partitionID,
					InsertChannel: channel,
					NumOfRows:     0,
					State:         commonpb.SegmentState_Flushed,
					Level:         datapb.SegmentLevel_L0,
				},
			}
			modPack.metricMutation.addNewSeg(commonpb.SegmentState_Flushed, datapb.SegmentLevel_L0, 0)
		}
		return true
	}
}

func UpdateStorageVersionOperator(segmentID int64, version int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Info("meta update: update storage version - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}

		segment.StorageVersion = version
		return true
	}
}

// Set status of segment
// and record dropped time when change segment status to dropped
func UpdateStatusOperator(segmentID int64, status commonpb.SegmentState) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update status failed - segment not found",
				zap.Int64("segmentID", segmentID),
				zap.String("status", status.String()))
			return false
		}

		updateSegStateAndPrepareMetrics(segment, status, modPack.metricMutation)
		if status == commonpb.SegmentState_Dropped {
			segment.DroppedAt = uint64(time.Now().UnixNano())
		}
		return true
	}
}

func UpdateCompactedOperator(segmentID int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update binlog failed - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}
		segment.Compacted = true
		return true
	}
}

func UpdateSegmentLevelOperator(segmentID int64, level datapb.SegmentLevel) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update level fail - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}
		if segment.LastLevel == segment.Level && segment.Level == level {
			log.Debug("segment already is this level", zap.Int64("segID", segmentID), zap.String("level", level.String()))
			return true
		}
		segment.LastLevel = segment.Level
		segment.Level = level
		return true
	}
}

func UpdateSegmentPartitionStatsVersionOperator(segmentID int64, version int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update partition stats version fail - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}
		segment.LastPartitionStatsVersion = segment.PartitionStatsVersion
		segment.PartitionStatsVersion = version
		log.Debug("update segment version", zap.Int64("segmentID", segmentID), zap.Int64("PartitionStatsVersion", version), zap.Int64("LastPartitionStatsVersion", segment.LastPartitionStatsVersion))
		return true
	}
}

func RevertSegmentLevelOperator(segmentID int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: revert level fail - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}
		segment.Level = segment.LastLevel
		log.Debug("revert segment level", zap.Int64("segmentID", segmentID), zap.String("LastLevel", segment.LastLevel.String()))
		return true
	}
}

func RevertSegmentPartitionStatsVersionOperator(segmentID int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: revert level fail - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}
		segment.PartitionStatsVersion = segment.LastPartitionStatsVersion
		log.Debug("revert segment partition stats version", zap.Int64("segmentID", segmentID), zap.Int64("LastPartitionStatsVersion", segment.LastPartitionStatsVersion))
		return true
	}
}

// Add binlogs in segmentInfo
func AddBinlogsOperator(segmentID int64, binlogs, statslogs, deltalogs []*datapb.FieldBinlog) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: add binlog failed - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}

		segment.Binlogs = mergeFieldBinlogs(segment.GetBinlogs(), binlogs)
		segment.Statslogs = mergeFieldBinlogs(segment.GetStatslogs(), statslogs)
		segment.Deltalogs = mergeFieldBinlogs(segment.GetDeltalogs(), deltalogs)
		modPack.increments[segmentID] = metastore.BinlogsIncrement{
			Segment: segment.SegmentInfo,
		}
		return true
	}
}

func UpdateBinlogsOperator(segmentID int64, binlogs, statslogs, deltalogs []*datapb.FieldBinlog) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update binlog failed - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}

		segment.Binlogs = binlogs
		segment.Statslogs = statslogs
		segment.Deltalogs = deltalogs
		modPack.increments[segmentID] = metastore.BinlogsIncrement{
			Segment: segment.SegmentInfo,
		}
		return true
	}
}

// update startPosition
func UpdateStartPosition(startPositions []*datapb.SegmentStartPosition) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		for _, pos := range startPositions {
			if len(pos.GetStartPosition().GetMsgID()) == 0 {
				continue
			}
			s := modPack.Get(pos.GetSegmentID())
			if s == nil {
				continue
			}

			s.StartPosition = pos.GetStartPosition()
		}
		return true
	}
}

func UpdateDmlPosition(segmentID int64, dmlPosition *msgpb.MsgPosition) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		if len(dmlPosition.GetMsgID()) == 0 {
			log.Warn("meta update: update dml position failed - nil position msg id",
				zap.Int64("segmentID", segmentID))
			return false
		}

		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update dml position failed - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}

		segment.DmlPosition = dmlPosition
		return true
	}
}

// UpdateCheckPointOperator updates segment checkpoint and num rows
func UpdateCheckPointOperator(segmentID int64, checkpoints []*datapb.CheckPoint) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update checkpoint failed - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}

		for _, cp := range checkpoints {
			if cp.SegmentID != segmentID {
				// Don't think this is gonna to happen, ignore for now.
				log.Warn("checkpoint in segment is not same as flush segment to update, igreo", zap.Int64("current", segmentID), zap.Int64("checkpoint segment", cp.SegmentID))
				continue
			}

			if segment.DmlPosition != nil && segment.DmlPosition.Timestamp >= cp.Position.Timestamp {
				log.Warn("checkpoint in segment is larger than reported", zap.Any("current", segment.GetDmlPosition()), zap.Any("reported", cp.GetPosition()))
				// segment position in etcd is larger than checkpoint, then dont change it
				continue
			}

			segment.NumOfRows = cp.NumOfRows
			segment.DmlPosition = cp.GetPosition()
		}

		count := segmentutil.CalcRowCountFromBinLog(segment.SegmentInfo)
		if count != segment.currRows && count > 0 {
			log.Info("check point reported inconsistent with bin log row count",
				zap.Int64("current rows (wrong)", segment.currRows),
				zap.Int64("segment bin log row count (correct)", count))
			segment.NumOfRows = count
		}
		return true
	}
}

func UpdateImportedRows(segmentID int64, rows int64) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update NumOfRows failed - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}
		segment.currRows = rows
		segment.NumOfRows = rows
		segment.MaxRowNum = rows
		return true
	}
}

func UpdateIsImporting(segmentID int64, isImporting bool) UpdateOperator {
	return func(modPack *updateSegmentPack) bool {
		segment := modPack.Get(segmentID)
		if segment == nil {
			log.Warn("meta update: update isImporting failed - segment not found",
				zap.Int64("segmentID", segmentID))
			return false
		}
		segment.IsImporting = isImporting
		return true
	}
}

// SetRowCount sets rowCount info for SegmentInfo with provided segmentID
// if SegmentInfo not found, do nothing
// func SetRowCount(rowCount int64) SegmentOperator {
// 	return func(segment *SegmentInfo) bool {
// 		if segment.
// 	}
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.Clone(SetRowCount(rowCount))
// 	}
// }

// GetSegments iterates internal map and returns all SegmentInfo in a slice
// no deep copy applied
// the logPath in meta is empty
// func (s *SegmentsInfo) GetSegments() []*SegmentInfo {
// 	return lo.Values(s.segments)
// }

// // GetCompactionTo returns the segment that the provided segment is compacted to.
// // Return (nil, false) if given segmentID can not found in the meta.
// // Return (nil, true) if given segmentID can be found not no compaction to.
// // Return (notnil, true) if given segmentID can be found and has compaction to.
// func (s *SegmentsInfo) GetCompactionTo(fromSegmentID int64) (*SegmentInfo, bool) {
// 	if _, ok := s.segments[fromSegmentID]; !ok {
// 		return nil, false
// 	}
// 	if toID, ok := s.compactionTo[fromSegmentID]; ok {
// 		if to, ok := s.segments[toID]; ok {
// 			return to, true
// 		}
// 		log.Warn("unreachable code: compactionTo relation is broken", zap.Int64("from", fromSegmentID), zap.Int64("to", toID))
// 	}
// 	return nil, true
// }

// // SetRowCount sets rowCount info for SegmentInfo with provided segmentID
// // if SegmentInfo not found, do nothing
// func (s *SegmentsInfo) SetRowCount(segmentID UniqueID, rowCount int64) {
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.Clone(SetRowCount(rowCount))
// 	}
// }

// // SetState sets Segment State info for SegmentInfo with provided segmentID
// // if SegmentInfo not found, do nothing
// func (s *SegmentsInfo) SetState(segmentID UniqueID, state commonpb.SegmentState) {
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.Clone(SetState(state))
// 	}
// }

// // SetDmlPosition sets DmlPosition info (checkpoint for recovery) for SegmentInfo with provided segmentID
// // if SegmentInfo not found, do nothing
// func (s *SegmentsInfo) SetDmlPosition(segmentID UniqueID, pos *msgpb.MsgPosition) {
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.Clone(SetDmlPosition(pos))
// 	}
// }

// // SetStartPosition sets StartPosition info (recovery info when no checkout point found) for SegmentInfo with provided segmentID
// // if SegmentInfo not found, do nothing
// func (s *SegmentsInfo) SetStartPosition(segmentID UniqueID, pos *msgpb.MsgPosition) {
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.Clone(SetStartPosition(pos))
// 	}
// }

// // SetAllocations sets allocations for segment with specified id
// // if the segment id is not found, do nothing
// // uses `ShadowClone` since internal SegmentInfo is not changed
// func (s *SegmentsInfo) SetAllocations(segmentID UniqueID, allocations []*Allocation) {
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.ShadowClone(SetAllocations(allocations))
// 	}
// }

// // AddAllocation adds a new allocation to specified segment
// // if the segment is not found, do nothing
// // uses `Clone` since internal SegmentInfo's LastExpireTime is changed
// func (s *SegmentsInfo) AddAllocation(segmentID UniqueID, allocation *Allocation) {
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.Clone(AddAllocation(allocation))
// 	}
// }

// // SetCurrentRows sets rows count for segment
// // if the segment is not found, do nothing
// // uses `ShadowClone` since internal SegmentInfo is not changed
// func (s *SegmentsInfo) SetCurrentRows(segmentID UniqueID, rows int64) {
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.ShadowClone(SetCurrentRows(rows))
// 	}
// }

// // SetFlushTime sets flush time for segment
// // if the segment is not found, do nothing
// // uses `ShadowClone` since internal SegmentInfo is not changed
// func (s *SegmentsInfo) SetFlushTime(segmentID UniqueID, t time.Time) {
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.ShadowClone(SetFlushTime(t))
// 	}
// }

// // SetIsCompacting sets compaction status for segment
// func (s *SegmentsInfo) SetIsCompacting(segmentID UniqueID, isCompacting bool) {
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.ShadowClone(SetIsCompacting(isCompacting))
// 	}
// }

// // SetLevel sets level for segment
// func (s *SegmentsInfo) SetLevel(segmentID UniqueID, level datapb.SegmentLevel) {
// 	if segment, ok := s.segments[segmentID]; ok {
// 		s.segments[segmentID] = segment.ShadowClone(SetLevel(level))
// 	}
// }
