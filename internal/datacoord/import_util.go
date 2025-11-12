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
	"fmt"
	"math"
	"path"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/taskcommon"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func WrapTaskLog(task ImportTask, fields ...zap.Field) []zap.Field {
	res := []zap.Field{
		zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("jobID", task.GetJobID()),
		zap.Int64("collectionID", task.GetCollectionID()),
		zap.String("type", task.GetType().String()),
		zap.String("state", task.GetTaskState().String()),
		zap.Int64("nodeID", task.GetNodeID()),
	}
	res = append(res, fields...)
	return res
}

func NewPreImportTasks(fileGroups [][]*internalpb.ImportFile,
	job ImportJob, alloc allocator.Allocator, importMeta ImportMeta,
) ([]ImportTask, error) {
	idStart, _, err := alloc.AllocN(int64(len(fileGroups)))
	if err != nil {
		return nil, err
	}
	tasks := make([]ImportTask, 0, len(fileGroups))
	for i, files := range fileGroups {
		fileStats := lo.Map(files, func(f *internalpb.ImportFile, _ int) *datapb.ImportFileStats {
			return &datapb.ImportFileStats{
				ImportFile: f,
			}
		})
		taskProto := &datapb.PreImportTask{
			JobID:        job.GetJobID(),
			TaskID:       idStart + int64(i),
			CollectionID: job.GetCollectionID(),
			State:        datapb.ImportTaskStateV2_Pending,
			FileStats:    fileStats,
			CreatedTime:  time.Now().Format("2006-01-02T15:04:05Z07:00"),
		}
		task := &preImportTask{
			importMeta: importMeta,
			tr:         timerecord.NewTimeRecorder("preimport task"),
			times:      taskcommon.NewTimes(),
		}
		task.task.Store(taskProto)
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func NewImportTasks(fileGroups [][]*datapb.ImportFileStats,
	job ImportJob, alloc allocator.Allocator, meta *meta, importMeta ImportMeta, segmentMaxSize int,
) ([]ImportTask, error) {
	idBegin, _, err := alloc.AllocN(int64(len(fileGroups)))
	if err != nil {
		return nil, err
	}
	tasks := make([]ImportTask, 0, len(fileGroups))
	for i, group := range fileGroups {
		taskProto := &datapb.ImportTaskV2{
			JobID:        job.GetJobID(),
			TaskID:       idBegin + int64(i),
			CollectionID: job.GetCollectionID(),
			NodeID:       NullNodeID,
			State:        datapb.ImportTaskStateV2_Pending,
			FileStats:    group,
			CreatedTime:  time.Now().Format("2006-01-02T15:04:05Z07:00"),
		}
		task := &importTask{
			alloc:      alloc,
			meta:       meta,
			importMeta: importMeta,
			tr:         timerecord.NewTimeRecorder("import task"),
			times:      taskcommon.NewTimes(),
		}
		task.task.Store(taskProto)
		segments, err := AssignSegments(job, task, alloc, meta, int64(segmentMaxSize))
		if err != nil {
			return nil, err
		}
		taskProto.SegmentIDs = segments
		if enableSortCompaction() {
			sortedSegIDBegin, _, err := alloc.AllocN(int64(len(segments)))
			if err != nil {
				return nil, err
			}
			taskProto.SortedSegmentIDs = lo.RangeFrom(sortedSegIDBegin, len(segments))
			log.Info("preallocate sorted segment ids", WrapTaskLog(task, zap.Int64s("segmentIDs", taskProto.SortedSegmentIDs))...)
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func GetSegmentMaxSize(job ImportJob, meta *meta) int {
	if importutilv2.IsL0Import(job.GetOptions()) {
		return paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.GetAsInt()
	}

	return int(getExpectedSegmentSize(meta, job.GetCollectionID(), job.GetSchema()))
}

func AssignSegments(job ImportJob, task ImportTask, alloc allocator.Allocator, meta *meta, segmentMaxSize int64) ([]int64, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(job.GetSchema())
	if err != nil {
		return nil, err
	}

	// merge hashed sizes
	hashedDataSize := make(map[string]map[int64]int64) // vchannel->(partitionID->size)
	for _, fileStats := range task.GetFileStats() {
		for vchannel, partStats := range fileStats.GetHashedStats() {
			if hashedDataSize[vchannel] == nil {
				hashedDataSize[vchannel] = make(map[int64]int64)
			}
			for partitionID, size := range partStats.GetPartitionDataSize() {
				hashedDataSize[vchannel][partitionID] += size
			}
		}
	}

	isL0Import := importutilv2.IsL0Import(job.GetOptions())
	segmentLevel := datapb.SegmentLevel_L1
	if isL0Import {
		segmentLevel = datapb.SegmentLevel_L0
	}

	storageVersion := storage.StorageV1
	if Params.CommonCfg.EnableStorageV2.GetAsBool() {
		storageVersion = storage.StorageV2
	}

	// alloc new segments
	segments := make([]int64, 0)
	addSegment := func(vchannel string, partitionID int64, size int64) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for size > 0 {
			segmentInfo, err := AllocImportSegment(ctx, alloc, meta,
				task.GetJobID(), task.GetTaskID(), task.GetCollectionID(),
				partitionID, vchannel, job.GetDataTs(), segmentLevel, storageVersion)
			if err != nil {
				return err
			}
			segments = append(segments, segmentInfo.GetID())
			size -= segmentMaxSize
		}
		return nil
	}

	for vchannel, partitionSizes := range hashedDataSize {
		for partitionID, size := range partitionSizes {
			if pkField.GetAutoID() && size == 0 {
				// When autoID is enabled, the preimport task estimates row distribution by
				// evenly dividing the total row count (numRows) across all vchannels:
				// `estimatedCount = numRows / vchannelNum`.
				//
				// However, the actual import task hashes real auto-generated IDs to determine
				// the target vchannel. This mismatch can lead to inaccurate row distribution estimation
				// in such corner cases:
				//
				// - Importing 1 row into 2 vchannels:
				//     • Preimport: 1 / 2 = 0 → both v0 and v1 are estimated to have 0 rows
				//     • Import: real autoID (e.g., 457975852966809057) hashes to v1
				//       → actual result: v0 = 0, v1 = 1
				//
				// To avoid such inconsistencies, we ensure that at least one segment is
				// allocated for each vchannel when autoID is enabled.
				size = 1
			}
			err := addSegment(vchannel, partitionID, size)
			if err != nil {
				return nil, err
			}
		}
	}
	return segments, nil
}

func AllocImportSegment(ctx context.Context,
	alloc allocator.Allocator,
	meta *meta,
	jobID int64, taskID int64,
	collectionID UniqueID, partitionID UniqueID,
	channelName string,
	dataTimestamp uint64,
	level datapb.SegmentLevel,
	storageVersion int64,
) (*SegmentInfo, error) {
	log := log.Ctx(ctx)
	id, err := alloc.AllocID(ctx)
	if err != nil {
		log.Error("failed to alloc id for import segment", zap.Error(err))
		return nil, err
	}
	ts := dataTimestamp
	if ts == 0 {
		ts, err = alloc.AllocTimestamp(ctx)
		if err != nil {
			return nil, err
		}
	}
	position := &msgpb.MsgPosition{
		ChannelName: channelName,
		MsgID:       nil,
		Timestamp:   ts,
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:             id,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		InsertChannel:  channelName,
		NumOfRows:      0,
		State:          commonpb.SegmentState_Importing,
		MaxRowNum:      0,
		Level:          level,
		LastExpireTime: math.MaxUint64,
		StartPosition:  position,
		DmlPosition:    position,
		StorageVersion: storageVersion,
	}
	segmentInfo.IsImporting = true
	segment := NewSegmentInfo(segmentInfo)
	if err = meta.AddSegment(ctx, segment); err != nil {
		log.Error("failed to add import segment", zap.Error(err))
		return nil, err
	}
	log.Info("add import segment done",
		zap.Int64("jobID", jobID),
		zap.Int64("taskID", taskID),
		zap.Int64("collectionID", segmentInfo.CollectionID),
		zap.Int64("segmentID", segmentInfo.ID),
		zap.String("channel", segmentInfo.InsertChannel),
		zap.String("level", level.String()))

	return segment, nil
}

func AssemblePreImportRequest(task ImportTask, job ImportJob) *datapb.PreImportRequest {
	importFiles := lo.Map(task.(*preImportTask).GetFileStats(),
		func(fileStats *datapb.ImportFileStats, _ int) *internalpb.ImportFile {
			return fileStats.GetImportFile()
		})

	req := &datapb.PreImportRequest{
		JobID:         task.GetJobID(),
		TaskID:        task.GetTaskID(),
		CollectionID:  task.GetCollectionID(),
		PartitionIDs:  job.GetPartitionIDs(),
		Vchannels:     job.GetVchannels(),
		Schema:        job.GetSchema(),
		ImportFiles:   importFiles,
		Options:       job.GetOptions(),
		TaskSlot:      task.GetTaskSlot(),
		StorageConfig: createStorageConfig(),
	}
	WrapPluginContext(task.GetCollectionID(), job.GetSchema().GetProperties(), req)
	return req
}

func AssembleImportRequest(task ImportTask, job ImportJob, meta *meta, alloc allocator.Allocator) (*datapb.ImportRequest, error) {
	requestSegments := make([]*datapb.ImportRequestSegment, 0)
	for _, segmentID := range task.(*importTask).GetSegmentIDs() {
		segment := meta.GetSegment(context.TODO(), segmentID)
		if segment == nil {
			return nil, merr.WrapErrSegmentNotFound(segmentID, "assemble import request failed")
		}
		requestSegments = append(requestSegments, &datapb.ImportRequestSegment{
			SegmentID:   segment.GetID(),
			PartitionID: segment.GetPartitionID(),
			Vchannel:    segment.GetInsertChannel(),
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ts := job.GetDataTs()
	var err error
	if ts == 0 {
		ts, err = alloc.AllocTimestamp(ctx)
		if err != nil {
			return nil, err
		}
	}

	totalRows := lo.SumBy(task.GetFileStats(), func(stat *datapb.ImportFileStats) int64 {
		return stat.GetTotalRows()
	})

	// Pre-allocate IDs for autoIDs and logIDs.
	fieldsNum := len(job.GetSchema().GetFields()) + 2 // userFields + tsField + rowIDField
	binlogNum := fieldsNum + 2                        // binlogs + statslog + BM25Statslog
	expansionFactor := paramtable.Get().DataCoordCfg.ImportPreAllocIDExpansionFactor.GetAsInt64()
	preAllocIDNum := (totalRows + 1) * int64(binlogNum) * expansionFactor

	idBegin, idEnd, err := common.AllocAutoID(func(n uint32) (int64, int64, error) {
		ids, ide, e := alloc.AllocN(int64(n))
		return int64(ids), int64(ide), e
	}, uint32(preAllocIDNum), Params.CommonCfg.ClusterID.GetAsUint64())
	if err != nil {
		return nil, err
	}

	log.Info("pre-allocate ids and ts for import task", WrapTaskLog(task,
		zap.Int64("totalRows", totalRows),
		zap.Int("fieldsNum", fieldsNum),
		zap.Int64("idBegin", idBegin),
		zap.Int64("idEnd", idEnd),
		zap.Uint64("ts", ts))...,
	)

	importFiles := lo.Map(task.GetFileStats(), func(fileStat *datapb.ImportFileStats, _ int) *internalpb.ImportFile {
		return fileStat.GetImportFile()
	})

	storageVersion := storage.StorageV1
	if Params.CommonCfg.EnableStorageV2.GetAsBool() {
		storageVersion = storage.StorageV2
	}
	req := &datapb.ImportRequest{
		ClusterID:       Params.CommonCfg.ClusterPrefix.GetValue(),
		JobID:           task.GetJobID(),
		TaskID:          task.GetTaskID(),
		CollectionID:    task.GetCollectionID(),
		PartitionIDs:    job.GetPartitionIDs(),
		Vchannels:       job.GetVchannels(),
		Schema:          job.GetSchema(),
		Files:           importFiles,
		Options:         job.GetOptions(),
		Ts:              ts,
		IDRange:         &datapb.IDRange{Begin: idBegin, End: idEnd},
		RequestSegments: requestSegments,
		StorageConfig:   createStorageConfig(),
		TaskSlot:        task.GetTaskSlot(),
		StorageVersion:  storageVersion,
	}
	WrapPluginContext(task.GetCollectionID(), job.GetSchema().GetProperties(), req)
	return req, nil
}

func RegroupImportFiles(job ImportJob, files []*datapb.ImportFileStats, segmentMaxSize int) [][]*datapb.ImportFileStats {
	if len(files) == 0 {
		return nil
	}

	threshold := paramtable.Get().DataCoordCfg.MaxSizeInMBPerImportTask.GetAsInt() * 1024 * 1024
	maxSizePerFileGroup := segmentMaxSize * len(job.GetPartitionIDs()) * len(job.GetVchannels())
	if maxSizePerFileGroup > threshold {
		maxSizePerFileGroup = threshold
	}

	fileGroups := make([][]*datapb.ImportFileStats, 0)
	currentGroup := make([]*datapb.ImportFileStats, 0)
	currentSum := 0
	sort.Slice(files, func(i, j int) bool {
		return files[i].GetTotalMemorySize() < files[j].GetTotalMemorySize()
	})
	for _, file := range files {
		size := int(file.GetTotalMemorySize())
		if size > maxSizePerFileGroup {
			fileGroups = append(fileGroups, []*datapb.ImportFileStats{file})
		} else if currentSum+size <= maxSizePerFileGroup {
			currentGroup = append(currentGroup, file)
			currentSum += size
		} else {
			fileGroups = append(fileGroups, currentGroup)
			currentGroup = []*datapb.ImportFileStats{file}
			currentSum = size
		}
	}
	if len(currentGroup) > 0 {
		fileGroups = append(fileGroups, currentGroup)
	}
	return fileGroups
}

func CheckDiskQuota(ctx context.Context, job ImportJob, meta *meta, importMeta ImportMeta) (int64, error) {
	if !Params.QuotaConfig.DiskProtectionEnabled.GetAsBool() {
		return 0, nil
	}
	if importutilv2.SkipDiskQuotaCheck(job.GetOptions()) {
		log.Info("skip disk quota check for import", zap.Int64("jobID", job.GetJobID()))
		return 0, nil
	}

	var (
		requestedTotal       int64
		requestedCollections = make(map[int64]int64)
	)
	for _, j := range importMeta.GetJobBy(ctx) {
		requested := j.GetRequestedDiskSize()
		requestedTotal += requested
		requestedCollections[j.GetCollectionID()] += requested
	}

	err := merr.WrapErrServiceQuotaExceeded("disk quota exceeded, please allocate more resources")
	quotaInfo := meta.GetQuotaInfo()
	totalUsage, collectionsUsage := quotaInfo.TotalBinlogSize, quotaInfo.CollectionBinlogSize

	tasks := importMeta.GetTaskBy(ctx, WithJob(job.GetJobID()), WithType(PreImportTaskType))
	files := make([]*datapb.ImportFileStats, 0)
	for _, task := range tasks {
		files = append(files, task.GetFileStats()...)
	}
	requestSize := lo.SumBy(files, func(file *datapb.ImportFileStats) int64 {
		return file.GetTotalMemorySize()
	})

	totalDiskQuota := Params.QuotaConfig.DiskQuota.GetAsFloat()
	if float64(totalUsage+requestedTotal+requestSize) > totalDiskQuota {
		log.Warn("global disk quota exceeded", zap.Int64("jobID", job.GetJobID()),
			zap.Bool("enabled", Params.QuotaConfig.DiskProtectionEnabled.GetAsBool()),
			zap.Int64("totalUsage", totalUsage),
			zap.Int64("requestedTotal", requestedTotal),
			zap.Int64("requestSize", requestSize),
			zap.Float64("totalDiskQuota", totalDiskQuota))
		return 0, err
	}
	collectionDiskQuota := Params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat()
	colID := job.GetCollectionID()
	if float64(collectionsUsage[colID]+requestedCollections[colID]+requestSize) > collectionDiskQuota {
		log.Warn("collection disk quota exceeded", zap.Int64("jobID", job.GetJobID()),
			zap.Bool("enabled", Params.QuotaConfig.DiskProtectionEnabled.GetAsBool()),
			zap.Int64("collectionsUsage", collectionsUsage[colID]),
			zap.Int64("requestedCollection", requestedCollections[colID]),
			zap.Int64("requestSize", requestSize),
			zap.Float64("collectionDiskQuota", collectionDiskQuota))
		return 0, err
	}
	return requestSize, nil
}

func getPendingProgress(ctx context.Context, jobID int64, importMeta ImportMeta) float32 {
	tasks := importMeta.GetTaskBy(context.TODO(), WithJob(jobID), WithType(PreImportTaskType))
	preImportingFiles := lo.SumBy(tasks, func(task ImportTask) int {
		return len(task.GetFileStats())
	})
	totalFiles := len(importMeta.GetJob(ctx, jobID).GetFiles())
	if totalFiles == 0 {
		return 1
	}
	return float32(preImportingFiles) / float32(totalFiles)
}

func getPreImportingProgress(ctx context.Context, jobID int64, importMeta ImportMeta) float32 {
	tasks := importMeta.GetTaskBy(ctx, WithJob(jobID), WithType(PreImportTaskType))
	completedTasks := lo.Filter(tasks, func(task ImportTask, _ int) bool {
		return task.GetState() == datapb.ImportTaskStateV2_Completed
	})
	if len(tasks) == 0 {
		return 1
	}
	return float32(len(completedTasks)) / float32(len(tasks))
}

func getImportRowsInfo(ctx context.Context, jobID int64, importMeta ImportMeta, meta *meta) (importedRows, totalRows int64) {
	tasks := importMeta.GetTaskBy(ctx, WithJob(jobID), WithType(ImportTaskType))
	segmentIDs := make([]int64, 0)
	for _, task := range tasks {
		totalRows += lo.SumBy(task.GetFileStats(), func(file *datapb.ImportFileStats) int64 {
			return file.GetTotalRows()
		})
		segmentIDs = append(segmentIDs, task.(*importTask).GetSegmentIDs()...)
	}
	importedRows = meta.GetSegmentsTotalNumRows(segmentIDs)
	return
}

func getImportingProgress(ctx context.Context, jobID int64, importMeta ImportMeta, meta *meta) (float32, int64, int64) {
	importedRows, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
	if totalRows == 0 {
		return 1, importedRows, totalRows
	}
	return float32(importedRows) / float32(totalRows), importedRows, totalRows
}

func getStatsProgress(ctx context.Context, jobID int64, importMeta ImportMeta, meta *meta) float32 {
	if !enableSortCompaction() {
		return 1
	}
	tasks := importMeta.GetTaskBy(ctx, WithJob(jobID), WithType(ImportTaskType))
	targetSegmentIDs := lo.FlatMap(tasks, func(t ImportTask, _ int) []int64 {
		return t.(*importTask).GetSortedSegmentIDs()
	})
	if len(targetSegmentIDs) == 0 {
		return 1
	}
	doneCnt := 0
	for _, segID := range targetSegmentIDs {
		seg := meta.GetHealthySegment(ctx, segID)
		if seg != nil {
			doneCnt++
		}
	}
	return float32(doneCnt) / float32(len(targetSegmentIDs))
}

func getIndexBuildingProgress(ctx context.Context, jobID int64, importMeta ImportMeta, meta *meta) float32 {
	job := importMeta.GetJob(ctx, jobID)
	if !Params.DataCoordCfg.WaitForIndex.GetAsBool() {
		return 1
	}
	tasks := importMeta.GetTaskBy(ctx, WithJob(jobID), WithType(ImportTaskType))
	originSegmentIDs := lo.FlatMap(tasks, func(t ImportTask, _ int) []int64 {
		return t.(*importTask).GetSegmentIDs()
	})
	targetSegmentIDs := lo.FlatMap(tasks, func(t ImportTask, _ int) []int64 {
		return t.(*importTask).GetSortedSegmentIDs()
	})
	if len(originSegmentIDs) == 0 {
		return 1
	}
	if !enableSortCompaction() {
		targetSegmentIDs = originSegmentIDs
	}
	unindexed := meta.indexMeta.GetUnindexedSegments(job.GetCollectionID(), targetSegmentIDs)
	return float32(len(targetSegmentIDs)-len(unindexed)) / float32(len(targetSegmentIDs))
}

// GetJobProgress calculates the importing job progress.
// The weight of each status is as follows:
// 10%: Pending
// 30%: PreImporting
// 30%: Importing
// 10%: Stats
// 10%: IndexBuilding
// 10%: Completed
// TODO: Wrap a function to map status to user status.
// TODO: Save these progress to job instead of recalculating.
func GetJobProgress(ctx context.Context, jobID int64,
	importMeta ImportMeta, meta *meta,
) (int64, internalpb.ImportJobState, int64, int64, string) {
	job := importMeta.GetJob(ctx, jobID)
	if job == nil {
		return 0, internalpb.ImportJobState_Failed, 0, 0, fmt.Sprintf("import job does not exist, jobID=%d", jobID)
	}
	switch job.GetState() {
	case internalpb.ImportJobState_Pending:
		progress := getPendingProgress(ctx, jobID, importMeta)
		return int64(progress * 10), internalpb.ImportJobState_Pending, 0, 0, ""

	case internalpb.ImportJobState_PreImporting:
		progress := getPreImportingProgress(ctx, jobID, importMeta)
		return 10 + int64(progress*30), internalpb.ImportJobState_Importing, 0, 0, ""

	case internalpb.ImportJobState_Importing:
		progress, importedRows, totalRows := getImportingProgress(ctx, jobID, importMeta, meta)
		return 10 + 30 + int64(progress*30), internalpb.ImportJobState_Importing, importedRows, totalRows, ""

	case internalpb.ImportJobState_Sorting:
		progress := getStatsProgress(ctx, jobID, importMeta, meta)
		_, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
		return 10 + 30 + 30 + int64(progress*10), internalpb.ImportJobState_Importing, totalRows, totalRows, ""

	case internalpb.ImportJobState_IndexBuilding:
		progress := getIndexBuildingProgress(ctx, jobID, importMeta, meta)
		_, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
		return 10 + 30 + 30 + 10 + int64(progress*10), internalpb.ImportJobState_Importing, totalRows, totalRows, ""

	case internalpb.ImportJobState_Completed:
		_, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
		return 100, internalpb.ImportJobState_Completed, totalRows, totalRows, ""

	case internalpb.ImportJobState_Failed:
		return 0, internalpb.ImportJobState_Failed, 0, 0, job.GetReason()
	}
	return 0, internalpb.ImportJobState_None, 0, 0, "unknown import job state"
}

func GetTaskProgresses(ctx context.Context, jobID int64, importMeta ImportMeta, meta *meta) []*internalpb.ImportTaskProgress {
	progresses := make([]*internalpb.ImportTaskProgress, 0)
	tasks := importMeta.GetTaskBy(ctx, WithJob(jobID), WithType(ImportTaskType))
	for _, task := range tasks {
		totalRows := lo.SumBy(task.GetFileStats(), func(file *datapb.ImportFileStats) int64 {
			return file.GetTotalRows()
		})
		importedRows := meta.GetSegmentsTotalNumRows(task.(*importTask).GetSegmentIDs())
		progress := int64(100)
		if totalRows != 0 {
			progress = int64(float32(importedRows) / float32(totalRows) * 100)
		}
		for _, fileStat := range task.GetFileStats() {
			progresses = append(progresses, &internalpb.ImportTaskProgress{
				FileName:     fmt.Sprintf("%v", fileStat.GetImportFile().GetPaths()),
				FileSize:     fileStat.GetFileSize(),
				Reason:       task.GetReason(),
				Progress:     progress,
				CompleteTime: task.(*importTask).GetCompleteTime(),
				State:        task.GetState().String(),
				ImportedRows: progress * fileStat.GetTotalRows() / 100,
				TotalRows:    fileStat.GetTotalRows(),
			})
		}
	}
	return progresses
}

func DropImportTask(task ImportTask, cluster session.Cluster, tm ImportMeta) error {
	if task.GetNodeID() == NullNodeID {
		return nil
	}
	err := cluster.DropImport(task.GetNodeID(), task.GetTaskID())
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		return err
	}
	log.Info("drop import in datanode done", WrapTaskLog(task)...)
	return tm.UpdateTask(context.TODO(), task.GetTaskID(), UpdateNodeID(NullNodeID))
}

func ListBinlogsAndGroupBySegment(ctx context.Context,
	cm storage.ChunkManager, importFile *internalpb.ImportFile,
) ([]*internalpb.ImportFile, error) {
	if len(importFile.GetPaths()) == 0 {
		return nil, merr.WrapErrImportFailed("no insert binlogs to import")
	}
	if len(importFile.GetPaths()) > 2 {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("too many input paths for binlog import. "+
			"Valid paths length should be one or two, but got paths:%s", importFile.GetPaths()))
	}

	insertPrefix := importFile.GetPaths()[0]
	segmentInsertPaths, _, err := storage.ListAllChunkWithPrefix(ctx, cm, insertPrefix, false)
	if err != nil {
		return nil, err
	}
	segmentImportFiles := lo.Map(segmentInsertPaths, func(segmentPath string, _ int) *internalpb.ImportFile {
		return &internalpb.ImportFile{Paths: []string{segmentPath}}
	})

	if len(importFile.GetPaths()) < 2 {
		return segmentImportFiles, nil
	}
	deltaPrefix := importFile.GetPaths()[1]
	segmentDeltaPaths, _, err := storage.ListAllChunkWithPrefix(ctx, cm, deltaPrefix, false)
	if err != nil {
		return nil, err
	}
	if len(segmentDeltaPaths) == 0 {
		return segmentImportFiles, nil
	}
	deltaSegmentIDs := lo.KeyBy(segmentDeltaPaths, func(deltaPrefix string) string {
		return path.Base(deltaPrefix)
	})

	for i := range segmentImportFiles {
		segmentID := path.Base(segmentImportFiles[i].GetPaths()[0])
		if deltaPrefix, ok := deltaSegmentIDs[segmentID]; ok {
			segmentImportFiles[i].Paths = append(segmentImportFiles[i].Paths, deltaPrefix)
		}
	}
	return segmentImportFiles, nil
}

func LogResultSegmentsInfo(jobID int64, meta *meta, segmentIDs []int64) {
	type (
		segments    = []*SegmentInfo
		segmentInfo struct {
			ID   int64
			Rows int64
			Size int64
		}
	)
	segmentsByChannelAndPartition := make(map[string]map[int64]segments) // channel => [partition => segments]
	for _, segmentInfo := range meta.GetSegmentInfos(segmentIDs) {
		channel := segmentInfo.GetInsertChannel()
		partition := segmentInfo.GetPartitionID()
		if _, ok := segmentsByChannelAndPartition[channel]; !ok {
			segmentsByChannelAndPartition[channel] = make(map[int64]segments)
		}
		segmentsByChannelAndPartition[channel][partition] = append(segmentsByChannelAndPartition[channel][partition], segmentInfo)
	}
	var (
		totalRows int64
		totalSize int64
	)
	for channel, partitionSegments := range segmentsByChannelAndPartition {
		for partitionID, segments := range partitionSegments {
			infos := lo.Map(segments, func(segment *SegmentInfo, _ int) *segmentInfo {
				rows := segment.GetNumOfRows()
				size := segment.getSegmentSize()
				totalRows += rows
				totalSize += size
				return &segmentInfo{
					ID:   segment.GetID(),
					Rows: rows,
					Size: size,
				}
			})
			log.Info("import segments info", zap.Int64("jobID", jobID),
				zap.String("channel", channel), zap.Int64("partitionID", partitionID),
				zap.Int("segmentsNum", len(segments)), zap.Any("segmentsInfo", infos),
			)
		}
	}
	log.Info("import result info", zap.Int64("jobID", jobID),
		zap.Int64("totalRows", totalRows), zap.Int64("totalSize", totalSize))
}

// ValidateBinlogImportRequest validates the binlog import request.
func ValidateBinlogImportRequest(ctx context.Context, cm storage.ChunkManager,
	reqFiles []*msgpb.ImportFile, options []*commonpb.KeyValuePair,
) error {
	files := lo.Map(reqFiles, func(file *msgpb.ImportFile, _ int) *internalpb.ImportFile {
		return &internalpb.ImportFile{Id: file.GetId(), Paths: file.GetPaths()}
	})
	_, err := ListBinlogImportRequestFiles(ctx, cm, files, options)
	return err
}

// ListBinlogImportRequestFiles lists the binlog files from the request.
// TODO: dyh, remove listing binlog after backup-restore derectly passed the segments paths.
func ListBinlogImportRequestFiles(ctx context.Context, cm storage.ChunkManager,
	reqFiles []*internalpb.ImportFile, options []*commonpb.KeyValuePair,
) ([]*internalpb.ImportFile, error) {
	isBackup := importutilv2.IsBackup(options)
	if !isBackup {
		return reqFiles, nil
	}
	resFiles := make([]*internalpb.ImportFile, 0)
	pool := conc.NewPool[struct{}](hardware.GetCPUNum() * 2)
	defer pool.Release()
	futures := make([]*conc.Future[struct{}], 0, len(reqFiles))
	mu := &sync.Mutex{}
	for _, importFile := range reqFiles {
		importFile := importFile
		futures = append(futures, pool.Submit(func() (struct{}, error) {
			segmentPrefixes, err := ListBinlogsAndGroupBySegment(ctx, cm, importFile)
			if err != nil {
				return struct{}{}, err
			}
			mu.Lock()
			defer mu.Unlock()
			resFiles = append(resFiles, segmentPrefixes...)
			return struct{}{}, nil
		}))
	}
	err := conc.AwaitAll(futures...)
	if err != nil {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("list binlogs failed, err=%s", err))
	}

	resFiles = lo.Filter(resFiles, func(file *internalpb.ImportFile, _ int) bool {
		return len(file.GetPaths()) > 0
	})
	if len(resFiles) == 0 {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("no binlog to import, input=%s", reqFiles))
	}
	if len(resFiles) > paramtable.Get().DataCoordCfg.MaxFilesPerImportReq.GetAsInt() {
		return nil, merr.WrapErrImportFailed(fmt.Sprintf("The max number of import files should not exceed %d, but got %d",
			paramtable.Get().DataCoordCfg.MaxFilesPerImportReq.GetAsInt(), len(resFiles)))
	}
	log.Info("list binlogs prefixes for import done", zap.Int("num", len(resFiles)), zap.Any("binlog_prefixes", resFiles))
	return resFiles, nil
}

// ValidateMaxImportJobExceed checks if the number of import jobs exceeds the limit.
func ValidateMaxImportJobExceed(ctx context.Context, importMeta ImportMeta) error {
	maxNum := paramtable.Get().DataCoordCfg.MaxImportJobNum.GetAsInt()
	executingNum := importMeta.CountJobBy(ctx, WithoutJobStates(internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed))
	if executingNum >= maxNum {
		return merr.WrapErrImportFailed(
			fmt.Sprintf("The number of jobs has reached the limit, please try again later. " +
				"If your request is set to only import a single file, " +
				"please consider importing multiple files in one request for better efficiency."))
	}
	return nil
}

// CalculateTaskSlot calculates the required resource slots for an import task based on CPU and memory constraints
// The function uses a dual-constraint approach:
// 1. CPU constraint: Based on the number of files to process in parallel
// 2. Memory constraint: Based on the total buffer size required for all virtual channels and partitions
// Returns the maximum of the two constraints to ensure sufficient resources
func CalculateTaskSlot(task ImportTask, importMeta ImportMeta) int {
	job := importMeta.GetJob(context.TODO(), task.GetJobID())

	// Calculate CPU-based slots
	fileNumPerSlot := paramtable.Get().DataCoordCfg.ImportFileNumPerSlot.GetAsInt()
	cpuBasedSlots := len(task.GetFileStats()) / fileNumPerSlot
	if cpuBasedSlots < 1 {
		cpuBasedSlots = 1
	}

	// Calculate memory-based slots
	var taskBufferSize int
	baseBufferSize := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt()
	if task.GetType() == ImportTaskType {
		// ImportTask use dynamic buffer size calculated by vchannels and partitions
		taskBufferSize = int(baseBufferSize) * len(job.GetVchannels()) * len(job.GetPartitionIDs())
	} else {
		// PreImportTask use fixed buffer size
		taskBufferSize = baseBufferSize
	}
	isL0Import := importutilv2.IsL0Import(job.GetOptions())
	if isL0Import {
		// L0 import use fixed buffer size
		taskBufferSize = paramtable.Get().DataNodeCfg.ImportDeleteBufferSize.GetAsInt()
	}
	memoryLimitPerSlot := paramtable.Get().DataCoordCfg.ImportMemoryLimitPerSlot.GetAsInt()
	memoryBasedSlots := taskBufferSize / memoryLimitPerSlot

	// Return the larger value to ensure both CPU and memory constraints are satisfied
	if cpuBasedSlots > memoryBasedSlots {
		return cpuBasedSlots
	}
	return memoryBasedSlots
}

func createSortCompactionTask(ctx context.Context,
	originSegment *SegmentInfo,
	targetSegmentID int64,
	meta *meta,
	handler Handler,
	alloc allocator.Allocator,
) (*datapb.CompactionTask, error) {
	if originSegment.GetNumOfRows() == 0 {
		operator := UpdateStatusOperator(originSegment.GetID(), commonpb.SegmentState_Dropped)
		err := meta.UpdateSegmentsInfo(ctx, operator)
		if err != nil {
			log.Ctx(ctx).Warn("import zero num row segment, but mark it dropped failed", zap.Error(err))
			return nil, err
		}
		return nil, nil
	}
	collection, err := handler.GetCollection(ctx, originSegment.GetCollectionID())
	if err != nil {
		log.Warn("Failed to create sort compaction task because get collection fail", zap.Error(err))
		return nil, err
	}

	collectionTTL, err := getCollectionTTL(collection.Properties)
	if err != nil {
		log.Warn("failed to apply triggerSegmentSortCompaction, get collection ttl failed")
		return nil, err
	}

	startID, _, err := alloc.AllocN(2)
	if err != nil {
		log.Warn("fFailed to submit compaction view to scheduler because allocate id fail", zap.Error(err))
		return nil, err
	}

	expectedSize := getExpectedSegmentSize(meta, collection.ID, collection.Schema)
	task := &datapb.CompactionTask{
		PlanID:             startID + 1,
		TriggerID:          startID,
		State:              datapb.CompactionTaskState_pipelining,
		StartTime:          time.Now().Unix(),
		CollectionTtl:      collectionTTL.Nanoseconds(),
		Type:               datapb.CompactionType_SortCompaction,
		CollectionID:       originSegment.GetCollectionID(),
		PartitionID:        originSegment.GetPartitionID(),
		Channel:            originSegment.GetInsertChannel(),
		Schema:             collection.Schema,
		InputSegments:      []int64{originSegment.GetID()},
		ResultSegments:     []int64{},
		TotalRows:          originSegment.GetNumOfRows(),
		LastStateStartTime: time.Now().Unix(),
		MaxSize:            getExpandedSize(expectedSize),
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: targetSegmentID,
			End:   targetSegmentID + 1,
		},
	}

	log.Ctx(ctx).Info("create sort compaction task success", zap.Int64("segmentID", originSegment.GetID()),
		zap.Int64("targetSegmentID", targetSegmentID), zap.Int64("num rows", originSegment.GetNumOfRows()))
	return task, nil
}
