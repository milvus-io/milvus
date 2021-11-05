package datacoord

import (
	"sort"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type singleCompactionPolicy interface {
	// shouldSingleCompaction generates a compaction plan for single comapction, return nil if no plan can be generated.
	generatePlan(segment *SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan
}

type mergeCompactionPolicy interface {
	// shouldMergeCompaction generates a compaction plan for merge compaction, return nil if no plan can be generated.
	generatePlan(segments []*SegmentInfo, timetravel *timetravel) []*datapb.CompactionPlan
}

type singleCompactionFunc func(segment *SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan

func (f singleCompactionFunc) generatePlan(segment *SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan {
	return f(segment, timetravel)
}

func chooseAllBinlogs(segment *SegmentInfo, timetravel *timetravel) *datapb.CompactionPlan {
	deltaLogs := make([]*datapb.DeltaLogInfo, 0)
	for _, l := range segment.GetDeltalogs() {
		if l.TimestampTo < timetravel.time {
			deltaLogs = append(deltaLogs, l)
		}
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
		Timetravel: timetravel.time,
		Channel:    segment.GetInsertChannel(),
	}
}

type mergeCompactionFunc func(segments []*SegmentInfo, timetravel *timetravel) []*datapb.CompactionPlan

func (f mergeCompactionFunc) generatePlan(segments []*SegmentInfo, timetravel *timetravel) []*datapb.CompactionPlan {
	return f(segments, timetravel)
}

func greedyMergeCompaction(segments []*SegmentInfo, timetravel *timetravel) []*datapb.CompactionPlan {
	if len(segments) == 0 {
		return nil
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].NumOfRows < segments[j].NumOfRows
	})

	return greedyGeneratePlans(segments, timetravel)
}

func greedyGeneratePlans(sortedSegments []*SegmentInfo, timetravel *timetravel) []*datapb.CompactionPlan {
	maxRowNumPerSegment := sortedSegments[0].MaxRowNum

	plans := make([]*datapb.CompactionPlan, 0)
	free := maxRowNumPerSegment
	plan := &datapb.CompactionPlan{
		Timetravel: timetravel.time,
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
				Timetravel: timetravel.time,
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
