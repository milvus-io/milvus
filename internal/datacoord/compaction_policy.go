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

package datacoord

import (
	"sort"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type singleCompactionPolicy interface {
	// generatePlan generates a compaction plan for single compaction, return nil if no plan can be generated.
	generatePlan(segment *SegmentInfo, timeTravel *timetravel) *datapb.CompactionPlan
}

type mergeCompactionPolicy interface {
	// generatePlan generates a compaction plan for merge compaction, return nil if no plan can be generated.
	generatePlan(segments []*SegmentInfo, timeTravel *timetravel) []*datapb.CompactionPlan
}

type singleCompactionFunc func(segment *SegmentInfo, timeTravel *timetravel) *datapb.CompactionPlan

func (f singleCompactionFunc) generatePlan(segment *SegmentInfo, timeTravel *timetravel) *datapb.CompactionPlan {
	return f(segment, timeTravel)
}

func chooseAllBinlogs(segment *SegmentInfo, timeTravel *timetravel) *datapb.CompactionPlan {
	var deltaLogs []*datapb.FieldBinlog
	for _, fieldBinlog := range segment.GetDeltalogs() {
		fbl := &datapb.FieldBinlog{
			FieldID: fieldBinlog.GetFieldID(),
			Binlogs: make([]*datapb.Binlog, 0, len(fieldBinlog.GetBinlogs())),
		}
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if binlog.TimestampTo < timeTravel.time {
				fbl.Binlogs = append(fbl.Binlogs, binlog)
			}
		}
		if len(fbl.Binlogs) > 0 {
			deltaLogs = append(deltaLogs, fbl)
		}
	}

	if len(deltaLogs) == 0 {
		return nil
	}

	return &datapb.CompactionPlan{
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID:           segment.GetID(),
				FieldBinlogs:        segment.GetBinlogs(),
				Field2StatslogPaths: segment.GetStatslogs(),
				Deltalogs:           deltaLogs,
			},
		},
		Type:       datapb.CompactionType_InnerCompaction,
		Timetravel: timeTravel.time,
		Channel:    segment.GetInsertChannel(),
	}
}

type mergeCompactionFunc func(segments []*SegmentInfo, timeTravel *timetravel) []*datapb.CompactionPlan

func (f mergeCompactionFunc) generatePlan(segments []*SegmentInfo, timeTravel *timetravel) []*datapb.CompactionPlan {
	return f(segments, timeTravel)
}

func greedyMergeCompaction(segments []*SegmentInfo, timeTravel *timetravel) []*datapb.CompactionPlan {
	if len(segments) == 0 {
		return nil
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].NumOfRows < segments[j].NumOfRows
	})

	return greedyGeneratePlans(segments, timeTravel)
}

func greedyGeneratePlans(sortedSegments []*SegmentInfo, timeTravel *timetravel) []*datapb.CompactionPlan {
	maxRowNumPerSegment := sortedSegments[0].MaxRowNum

	plans := make([]*datapb.CompactionPlan, 0)
	free := maxRowNumPerSegment
	plan := &datapb.CompactionPlan{
		Timetravel: timeTravel.time,
		Type:       datapb.CompactionType_MergeCompaction,
		Channel:    sortedSegments[0].GetInsertChannel(),
	}

	for _, s := range sortedSegments {
		segmentBinlogs := &datapb.CompactionSegmentBinlogs{
			SegmentID:           s.GetID(),
			FieldBinlogs:        s.GetBinlogs(),
			Field2StatslogPaths: s.GetStatslogs(),
			Deltalogs:           s.GetDeltalogs(),
		}

		if s.NumOfRows > free {
			// if the plan size is less than or equal to 1, it means that every unchecked segment is larger than half of max segment size
			// so there's no need to merge them
			if len(plan.SegmentBinlogs) <= 1 {
				break
			}
			plans = append(plans, plan)
			plan = &datapb.CompactionPlan{
				Timetravel: timeTravel.time,
				Type:       datapb.CompactionType_MergeCompaction,
				Channel:    sortedSegments[0].GetInsertChannel(),
			}
			free = maxRowNumPerSegment
		}
		plan.SegmentBinlogs = append(plan.SegmentBinlogs, segmentBinlogs)
		free -= s.GetNumOfRows()
	}

	// if plan contains zero or one segment, dont need to merge
	if len(plan.SegmentBinlogs) > 1 {
		plans = append(plans, plan)
	}

	return plans
}
