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

package task

import (
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// PendingSegmentLoads is an optional planning inspector. Keep it outside the
// Scheduler interface so older schedulers can retain their aggregate-delta path.
// It includes waiting tasks and excludes Grow actions already passed by a Move.
func (scheduler *taskScheduler) PendingSegmentLoads() map[int64][]*querypb.LoadingSegmentInfo {
	loads := make(map[int64][]*querypb.LoadingSegmentInfo)
	scheduler.segmentTasks.Range(func(_ replicaSegmentIndex, t Task) bool {
		segment, ok := t.(*SegmentTask)
		if !ok || segment.Status() == TaskStatusSucceeded || segment.Status() == TaskStatusFailed || segment.Status() == TaskStatusCanceled {
			return true
		}
		step := segment.Step()
		for i, action := range segment.Actions() {
			if i < step || action.Type() != ActionTypeGrow {
				continue
			}
			info := &querypb.LoadingSegmentInfo{SegmentId: segment.SegmentID(), CollectionId: segment.CollectionID(), NumRows: int64(action.WorkLoadEffect())}
			if scheduler.targetMgr != nil {
				if target := scheduler.targetMgr.GetSealedSegment(scheduler.ctx, segment.CollectionID(), segment.SegmentID(), meta.CurrentTargetFirst); target != nil {
					info.NumRows = target.GetNumOfRows()
					info.EstimatedMemoryBytes, info.EstimatedDiskBytes = pendingSegmentBytes(target)
				}
			}
			loads[action.Node()] = append(loads[action.Node()], info)
		}
		return true
	})
	return loads
}

// Binlog sizes are the available coordinator estimate, not a promise about
// native peak memory. QueryNode's loader retains the authoritative admission.
func pendingSegmentBytes(segment *datapb.SegmentInfo) (uint64, uint64) {
	var memory, disk uint64
	for _, fields := range [][]*datapb.FieldBinlog{segment.GetBinlogs(), segment.GetStatslogs(), segment.GetDeltalogs()} {
		for _, field := range fields {
			for _, binlog := range field.GetBinlogs() {
				if binlog.GetLogSize() > 0 {
					disk += uint64(binlog.GetLogSize())
				}
				size := binlog.GetMemorySize()
				if size <= 0 {
					size = binlog.GetLogSize()
				}
				if size > 0 {
					memory += uint64(size)
				}
			}
		}
	}
	return memory, disk
}
