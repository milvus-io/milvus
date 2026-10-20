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
	"context"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func isCollectionAutoCompactionEnabled(coll *collectionInfo) bool {
	if coll == nil {
		return false
	}
	if coll.IsExternal() {
		mlog.Debug(context.TODO(), "collection auto compaction disabled for external collection", mlog.FieldCollectionID(coll.ID))
		return false
	}
	enabled, err := getCollectionAutoCompactionEnabled(coll.Properties)
	if err != nil {
		mlog.Warn(context.TODO(), "collection properties auto compaction not valid, returning false", mlog.Err(err))
		return false
	}
	return enabled
}

func hasTooManyDeletions(segment *SegmentInfo) bool {
	stats := segment.EnsureStats()
	deltaLogCount := int(stats.GetDeltaBinlogCount())
	totalDeletedRows := int(stats.GetDeleteNumRows())
	totalDeleteLogSize := stats.GetDeltaBinlogSize()

	// Too many deltalog files, accumulates IO count.
	if deltaLogCount > Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsInt() {
		mlog.Info(context.TODO(), "delta logs file count exceeds threshold",
			mlog.FieldSegmentID(segment.ID),
			mlog.Int("delta log count", deltaLogCount),
			mlog.Int("file number threshold", Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsInt()),
		)
		return true
	}

	// The proportion of deleted rows is too large, int64 PK tends to accumulates deleted row counts.
	if float64(totalDeletedRows)/float64(segment.GetNumOfRows()) >= Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat() {
		mlog.Info(context.TODO(), "deleted entities rows proportion exceeds threshold",
			mlog.FieldSegmentID(segment.ID),
			mlog.Int64("number of rows", segment.GetNumOfRows()),
			mlog.Int("deleted rows", totalDeletedRows),
			mlog.Float64("proportion threshold", Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat()),
		)
		return true
	}

	// Delete size is too large, varchar PK tends to accumulates deltalog size.
	if totalDeleteLogSize > Params.DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64() {
		mlog.Info(context.TODO(), "total delete entries size exceeds threshold",
			mlog.FieldSegmentID(segment.ID),
			mlog.Int64("numRows", segment.GetNumOfRows()),
			mlog.Int64("delete entries size", totalDeleteLogSize),
			mlog.Int64("size threshold", Params.DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64()),
		)
		return true
	}

	return false
}

func canTriggerSortCompaction(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed &&
		segment.GetLevel() != datapb.SegmentLevel_L0 &&
		(!segment.GetIsSorted() && !segment.GetIsSortedByNamespace()) &&
		!segment.GetIsImporting() &&
		!segment.isCompacting
}

func getExpectedSegmentSize(indexMeta *indexMeta, collectionID int64, schema *schemapb.CollectionSchema) int64 {
	allDiskIndex := indexMeta.AllDenseWithDiskIndex(collectionID, schema)
	if allDiskIndex {
		// Only if all dense vector fields index type are DiskANN, recalc segment max size here.
		return Params.DataCoordCfg.DiskSegmentMaxSize.GetAsInt64() * 1024 * 1024
	}
	// If some dense vector fields index type are not DiskANN, recalc segment max size using default policy.
	return Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
}

func isNormalManualCompactionCandidate(meta *meta, segment *SegmentInfo) bool {
	return isNormalManualCompactionSegment(segment) &&
		isSharedCompactionSelectable(meta, segment) &&
		isMixCompactionSelectable(segment)
}

func summaryCompactionState(triggerID int64, tasks []*datapb.CompactionTask) *compactionInfo {
	ret := &compactionInfo{}
	var executingCnt, pipeliningCnt, completedCnt, failedCnt, timeoutCnt, analyzingCnt, indexingCnt, cleanedCnt, metaSavedCnt, stats int
	mergeInfos := make(map[int64]*milvuspb.CompactionMergeInfo)

	for _, task := range tasks {
		if task == nil {
			continue
		}
		switch task.GetState() {
		case datapb.CompactionTaskState_executing:
			executingCnt++
		case datapb.CompactionTaskState_pipelining:
			pipeliningCnt++
		case datapb.CompactionTaskState_completed:
			completedCnt++
		case datapb.CompactionTaskState_failed:
			failedCnt++
		case datapb.CompactionTaskState_timeout:
			timeoutCnt++
		case datapb.CompactionTaskState_analyzing:
			analyzingCnt++
		case datapb.CompactionTaskState_indexing:
			indexingCnt++
		case datapb.CompactionTaskState_cleaned:
			cleanedCnt++
		case datapb.CompactionTaskState_meta_saved:
			metaSavedCnt++
		case datapb.CompactionTaskState_statistic:
			stats++
		default:
		}
		mergeInfos[task.GetPlanID()] = getCompactionMergeInfo(task)
	}

	ret.executingCnt = executingCnt + pipeliningCnt + analyzingCnt + indexingCnt + metaSavedCnt + stats
	ret.completedCnt = completedCnt
	ret.timeoutCnt = timeoutCnt
	ret.failedCnt = failedCnt
	ret.mergeInfos = mergeInfos

	if ret.executingCnt != 0 {
		ret.state = commonpb.CompactionState_Executing
	} else {
		ret.state = commonpb.CompactionState_Completed
	}

	mlog.Info(context.TODO(), "compaction states",
		mlog.Int64("triggerID", triggerID),
		mlog.String("state", ret.state.String()),
		mlog.Int("executingCnt", executingCnt),
		mlog.Int("pipeliningCnt", pipeliningCnt),
		mlog.Int("completedCnt", completedCnt),
		mlog.Int("failedCnt", failedCnt),
		mlog.Int("timeoutCnt", timeoutCnt),
		mlog.Int("analyzingCnt", analyzingCnt),
		mlog.Int("indexingCnt", indexingCnt),
		mlog.Int("cleanedCnt", cleanedCnt),
		mlog.Int("metaSavedCnt", metaSavedCnt))
	return ret
}

type compactionInfo struct {
	state        commonpb.CompactionState
	executingCnt int
	completedCnt int
	failedCnt    int
	timeoutCnt   int
	mergeInfos   map[int64]*milvuspb.CompactionMergeInfo
}

// getCollectionAutoCompactionEnabled returns whether auto compaction for collection is enabled.
// if not set, returns global auto compaction config.
func getCollectionAutoCompactionEnabled(properties map[string]string) (bool, error) {
	// when collection is on truncating, disable auto compaction.
	if _, ok := properties[common.CollectionOnTruncatingKey]; ok {
		return false, nil
	}
	v, ok := properties[common.CollectionAutoCompactionKey]
	if ok {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return false, err
		}
		return enabled, nil
	}
	return Params.DataCoordCfg.EnableAutoCompaction.GetAsBool(), nil
}

func getCompactionMergeInfo(task *datapb.CompactionTask) *milvuspb.CompactionMergeInfo {
	/*
		segments := task.GetPlan().GetSegmentBinlogs()
		var sources []int64
		for _, s := range segments {
			sources = append(sources, s.GetSegmentID())
		}
	*/
	var target int64 = -1
	if len(task.GetResultSegments()) > 0 {
		target = task.GetResultSegments()[0]
	}
	// Public and internal compaction enums share the same wire values.
	return &milvuspb.CompactionMergeInfo{
		Sources:       task.GetInputSegments(),
		Target:        target,
		PlanId:        task.GetPlanID(),
		TriggerId:     task.GetTriggerID(),
		CollectionId:  task.GetCollectionID(),
		PartitionId:   task.GetPartitionID(),
		Channel:       task.GetChannel(),
		Type:          commonpb.CompactionType(task.GetType()),
		State:         commonpb.CompactionTaskState(task.GetState()),
		FailureReason: task.GetFailReason(),
		Targets:       task.GetResultSegments(),
	}
}

func isNormalManualCompactionSegment(segment *SegmentInfo) bool {
	return isSegmentHealthy(segment) &&
		isFlushed(segment) &&
		!segment.GetIsImporting() &&
		segment.GetLevel() != datapb.SegmentLevel_L0 &&
		segment.GetLevel() != datapb.SegmentLevel_L2
}

func isSharedCompactionSelectable(meta *meta, segment *SegmentInfo) bool {
	return !segment.isCompacting && !meta.isSegmentCompactionProtected(segment.GetID())
}

func isMixCompactionSelectable(segment *SegmentInfo) bool {
	return !segment.GetIsInvisible() &&
		(segment.GetIsSorted() || segment.GetIsSortedByNamespace())
}
