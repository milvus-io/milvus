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
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/importid"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func WrapTaskLog(task ImportTask, fields ...mlog.Field) []mlog.Field {
	res := []mlog.Field{
		mlog.FieldTaskID(task.GetTaskID()),
		mlog.FieldJobID(task.GetJobID()),
		mlog.FieldCollectionID(task.GetCollectionID()),
		mlog.String("type", task.GetType().String()),
		mlog.String("state", task.GetTaskState().String()),
		mlog.FieldNodeID(task.GetNodeID()),
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
			mlog.Info(context.TODO(), "preallocate sorted segment ids", WrapTaskLog(task, mlog.Int64s("segmentIDs", taskProto.SortedSegmentIDs))...)
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

func importStorageVersion(isL0Import bool) int64 {
	if isL0Import {
		return storage.StorageV2
	}
	if paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool() {
		return storage.StorageV3
	}
	return storage.StorageV2
}

func importUseLoonFFI(isL0Import bool) bool {
	return !isL0Import && paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool()
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

	storageVersion := importStorageVersion(isL0Import)

	// alloc new segments
	segments := make([]int64, 0)
	addSegment := func(vchannel string, partitionID int64, size int64) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for size > 0 {
			segmentInfo, err := AllocImportSegment(ctx, alloc, meta,
				task.GetJobID(), task.GetTaskID(), task.GetCollectionID(),
				partitionID, vchannel, job.GetDataTs(), segmentLevel, storageVersion, 0)
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
	schemaVersion int32,
) (*SegmentInfo, error) {
	id, err := alloc.AllocID(ctx)
	if err != nil {
		mlog.Error(ctx, "failed to alloc id for import segment", mlog.Err(err))
		return nil, err
	}
	if dataTimestamp == 0 {
		_, err = alloc.AllocTimestamp(ctx)
		if err != nil {
			return nil, err
		}
	}
	return addImportSegment(ctx, meta, id, jobID, taskID, collectionID, partitionID, channelName, level, storageVersion, schemaVersion)
}

func addImportSegment(
	ctx context.Context,
	meta *meta,
	id, jobID, taskID, collectionID, partitionID int64,
	channelName string,
	level datapb.SegmentLevel,
	storageVersion int64,
	schemaVersion int32,
) (*SegmentInfo, error) {
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
		StorageVersion: storageVersion,
		SchemaVersion:  schemaVersion,
	}
	segmentInfo.IsImporting = true
	segment := NewSegmentInfo(segmentInfo)
	// A preallocated import segment has not written any binlog yet, so it must
	// not carry a zero-value Statistics object; NewSegmentInfo fills one in
	// when the caller leaves Stats nil. Reset it so import v3 result
	// validation can tell a clean preallocated segment apart from one that
	// already received output.
	segmentInfo.Stats = nil
	if err := meta.AddSegment(ctx, segment); err != nil {
		mlog.Error(ctx, "failed to add import segment", mlog.Err(err))
		return nil, err
	}
	mlog.Info(ctx, "add import segment done",
		mlog.FieldJobID(jobID),
		mlog.FieldTaskID(taskID),
		mlog.FieldCollectionID(segmentInfo.CollectionID),
		mlog.FieldSegmentID(segmentInfo.ID),
		mlog.String("channel", segmentInfo.InsertChannel),
		mlog.String("level", level.String()))

	return segment, nil
}

func AssemblePreImportRequest(task ImportTask, job ImportJob) *datapb.PreImportRequest {
	importFiles := lo.Map(task.GetFileStats(),
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
		PluginContext: GetReadPluginContext(job.GetOptions()),
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

	// Reserve the task-level log id range. AutoID jobs whose files carry per-file row id
	// ranges consume it only for binlog logIDs; backup/L0 and legacy no-range jobs may also
	// consume it per row for PK/RowID. ReserveLogIDs also checks every file's ID range
	// against the exact preimport count and fails terminally with ErrIDRangeTooSmall when
	// a range cannot hold its rows.
	idRange, err := importid.ReserveLogIDs(job.GetSchema(), task.GetFileStats(),
		alloc.AllocN, Params.CommonCfg.ClusterID.GetAsUint64())
	if err != nil {
		return nil, err
	}

	mlog.Info(context.TODO(), "pre-allocate ids and ts for import task", WrapTaskLog(task,
		mlog.Int64("totalRows", totalRows),
		mlog.Int64("idBegin", idRange.GetBegin()),
		mlog.Int64("idEnd", idRange.GetEnd()),
		mlog.Uint64("ts", ts))...,
	)

	importFiles := lo.Map(task.GetFileStats(), func(fileStat *datapb.ImportFileStats, _ int) *internalpb.ImportFile {
		return fileStat.GetImportFile()
	})
	isL0Import := importutilv2.IsL0Import(job.GetOptions())
	storageVersion := importStorageVersion(isL0Import)
	useLoonFFI := importUseLoonFFI(isL0Import)

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
		IDRange:         idRange,
		RequestSegments: requestSegments,
		StorageConfig:   createStorageConfig(),
		TaskSlot:        task.GetTaskSlot(),
		StorageVersion:  storageVersion,
		PluginContext:   GetReadPluginContext(job.GetOptions()),
		UseLoonFfi:      useLoonFFI,
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
		mlog.Info(ctx, "skip disk quota check for import", mlog.FieldJobID(job.GetJobID()))
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

	tasks := importMeta.GetTaskByJob(ctx, job.GetJobID(), WithType(PreImportTaskType))
	files := make([]*datapb.ImportFileStats, 0)
	for _, task := range tasks {
		files = append(files, task.GetFileStats()...)
	}
	requestSize := lo.SumBy(files, func(file *datapb.ImportFileStats) int64 {
		return file.GetTotalMemorySize()
	})

	totalDiskQuota := Params.QuotaConfig.DiskQuota.GetAsFloat()
	if float64(totalUsage+requestedTotal+requestSize) > totalDiskQuota {
		mlog.Warn(ctx, "global disk quota exceeded", mlog.FieldJobID(job.GetJobID()),
			mlog.Bool("enabled", Params.QuotaConfig.DiskProtectionEnabled.GetAsBool()),
			mlog.Int64("totalUsage", totalUsage),
			mlog.Int64("requestedTotal", requestedTotal),
			mlog.Int64("requestSize", requestSize),
			mlog.Float64("totalDiskQuota", totalDiskQuota))
		return 0, err
	}
	collectionDiskQuota := Params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat()
	colID := job.GetCollectionID()
	if float64(collectionsUsage[colID]+requestedCollections[colID]+requestSize) > collectionDiskQuota {
		mlog.Warn(ctx, "collection disk quota exceeded", mlog.FieldJobID(job.GetJobID()),
			mlog.Bool("enabled", Params.QuotaConfig.DiskProtectionEnabled.GetAsBool()),
			mlog.Int64("collectionsUsage", collectionsUsage[colID]),
			mlog.Int64("requestedCollection", requestedCollections[colID]),
			mlog.Int64("requestSize", requestSize),
			mlog.Float64("collectionDiskQuota", collectionDiskQuota))
		return 0, err
	}
	return requestSize, nil
}

// CheckImportV3DiskQuota is the V3 counterpart of CheckDiskQuota. V3 has no
// PreImportTask file stats, so the planner passes the actual Reshard fragment
// bytes. The hierarchical merge writes its intermediates to the DataNode's
// local disk, so the object store only ever holds one copy of the normalized
// data: the reshard fragments, which the final segments supersede and the job
// GC then removes. The request therefore reserves the normalized data volume
// itself -- the same decoded-volume metric CheckDiskQuota reserves -- which is
// a conservative upper bound on the compressed physical bytes.
func CheckImportV3DiskQuota(ctx context.Context, job ImportJob, meta *meta, importMeta ImportMeta, fragmentBytes int64) (int64, error) {
	if !Params.QuotaConfig.DiskProtectionEnabled.GetAsBool() {
		return 0, nil
	}
	if importutilv2.SkipDiskQuotaCheck(job.GetOptions()) {
		mlog.Info(ctx, "skip disk quota check for import", mlog.FieldJobID(job.GetJobID()))
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
	requestSize := fragmentBytes

	totalDiskQuota := Params.QuotaConfig.DiskQuota.GetAsFloat()
	if float64(totalUsage+requestedTotal+requestSize) > totalDiskQuota {
		mlog.Warn(ctx, "global disk quota exceeded", mlog.FieldJobID(job.GetJobID()),
			mlog.Bool("enabled", Params.QuotaConfig.DiskProtectionEnabled.GetAsBool()),
			mlog.Int64("totalUsage", totalUsage),
			mlog.Int64("requestedTotal", requestedTotal),
			mlog.Int64("requestSize", requestSize),
			mlog.Float64("totalDiskQuota", totalDiskQuota))
		return 0, err
	}
	collectionDiskQuota := Params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat()
	colID := job.GetCollectionID()
	if float64(collectionsUsage[colID]+requestedCollections[colID]+requestSize) > collectionDiskQuota {
		mlog.Warn(ctx, "collection disk quota exceeded", mlog.FieldJobID(job.GetJobID()),
			mlog.Bool("enabled", Params.QuotaConfig.DiskProtectionEnabled.GetAsBool()),
			mlog.Int64("collectionsUsage", collectionsUsage[colID]),
			mlog.Int64("requestedCollection", requestedCollections[colID]),
			mlog.Int64("requestSize", requestSize),
			mlog.Float64("collectionDiskQuota", collectionDiskQuota))
		return 0, err
	}
	return requestSize, nil
}

func getPendingProgress(ctx context.Context, jobID int64, importMeta ImportMeta) float32 {
	tasks := importMeta.GetTaskByJob(ctx, jobID, WithType(PreImportTaskType))
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
	tasks := importMeta.GetTaskByJob(ctx, jobID, WithType(PreImportTaskType))
	if job := importMeta.GetJob(ctx, jobID); job != nil && job.GetVersion() == datapb.ImportJobVersion_ImportJobVersionV3 {
		tasks = importMeta.GetTaskByJob(ctx, jobID, WithType(PreImportV2TaskType))
	}
	completedTasks := lo.Filter(tasks, func(task ImportTask, _ int) bool {
		return task.GetState() == datapb.ImportTaskStateV2_Completed
	})
	if len(tasks) == 0 {
		return 1
	}
	return float32(len(completedTasks)) / float32(len(tasks))
}

func getReshardProgress(ctx context.Context, jobID int64, importMeta ImportMeta) float32 {
	tasks := importMeta.GetTaskByJob(ctx, jobID, WithType(ReshardTaskType))
	completedTasks := lo.Filter(tasks, func(task ImportTask, _ int) bool {
		return task.GetState() == datapb.ImportTaskStateV2_Completed
	})
	if len(tasks) == 0 {
		return 1
	}
	return float32(len(completedTasks)) / float32(len(tasks))
}

func getImportRowsInfo(ctx context.Context, jobID int64, importMeta ImportMeta, meta *meta) (importedRows, totalRows int64) {
	job := importMeta.GetJob(ctx, jobID)
	if job != nil && job.GetVersion() == datapb.ImportJobVersion_ImportJobVersionV3 {
		tasks := importMeta.GetTaskByJob(ctx, jobID, WithType(ImportTaskV3Type))
		segmentIDs := make([]int64, 0)
		for _, generic := range tasks {
			task := generic.(*importTaskV3)
			persisted := task.task.Load()
			if segmentID := persisted.GetSegmentId(); segmentID != 0 {
				segmentIDs = append(segmentIDs, segmentID)
			}
			totalRows += persisted.GetRows()
		}
		importedRows = meta.GetSegmentsTotalNumRows(segmentIDs)
		return importedRows, totalRows
	}
	tasks := importMeta.GetTaskByJob(ctx, jobID, WithType(ImportTaskType))
	segmentIDs := make([]int64, 0)
	for _, task := range tasks {
		totalRows += lo.SumBy(task.GetFileStats(), func(file *datapb.ImportFileStats) int64 {
			return file.GetTotalRows()
		})
		segmentIDs = append(segmentIDs, task.(*importTask).GetSegmentIDs()...)
	}
	importedRows = meta.GetSegmentsTotalNumRows(segmentIDs)
	return importedRows, totalRows
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
	tasks := importMeta.GetTaskByJob(ctx, jobID, WithType(ImportTaskType))
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
	if job.GetVersion() == datapb.ImportJobVersion_ImportJobVersionV3 {
		tasks := importMeta.GetTaskByJob(ctx, jobID, WithType(ImportTaskV3Type))
		targetSegmentIDs := make([]int64, 0)
		for _, task := range tasks {
			if segmentID := task.(*importTaskV3).task.Load().GetSegmentId(); segmentID != 0 {
				segment := meta.GetHealthySegment(ctx, segmentID)
				if segment != nil && segment.GetNumOfRows() > 0 {
					targetSegmentIDs = append(targetSegmentIDs, segmentID)
				}
			}
		}
		if len(targetSegmentIDs) == 0 {
			return 1
		}
		unindexed := meta.indexMeta.GetUnindexedSegments(job.GetCollectionID(), targetSegmentIDs)
		return float32(len(targetSegmentIDs)-len(unindexed)) / float32(len(targetSegmentIDs))
	}
	tasks := importMeta.GetTaskByJob(ctx, jobID, WithType(ImportTaskType))
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
// ImportJobVersionV1:
// 10%: Pending
// 30%: PreImporting/AssigningIDRange
// 30%: Importing
// 10%: Stats
// 10%: IndexBuilding
// 10%: Completed
// ImportJobVersionV3:
// 10%: Pending
// 5%: PreImporting/AssigningIDRange
// 30%: Resharding
// 5%: Planning
// 10%: Importing
// 30%: IndexBuilding
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
	if isV3Job(job) {
		return getV3JobProgress(ctx, job, importMeta, meta)
	}
	return getV1JobProgress(ctx, job, importMeta, meta)
}

// getV1JobProgress maps the legacy import job states to progress.
func getV1JobProgress(ctx context.Context, job ImportJob, importMeta ImportMeta, meta *meta) (int64, internalpb.ImportJobState, int64, int64, string) {
	jobID := job.GetJobID()
	switch job.GetState() {
	case internalpb.ImportJobState_Pending:
		progress := getPendingProgress(ctx, jobID, importMeta)
		return int64(progress * 10), internalpb.ImportJobState_Pending, 0, 0, ""

	case internalpb.ImportJobState_PreImporting, internalpb.ImportJobState_AssigningIDRange:
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

	case internalpb.ImportJobState_Uncommitted:
		_, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
		if job.GetAutoCommit() {
			return 99, internalpb.ImportJobState_Importing, totalRows, totalRows, ""
		}
		return 99, internalpb.ImportJobState_Uncommitted, totalRows, totalRows, ""

	case internalpb.ImportJobState_Committing:
		_, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
		if job.GetAutoCommit() {
			return 99, internalpb.ImportJobState_Importing, totalRows, totalRows, ""
		}
		return 99, internalpb.ImportJobState_Committing, totalRows, totalRows, ""

	case internalpb.ImportJobState_Completed:
		_, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
		return 100, internalpb.ImportJobState_Completed, totalRows, totalRows, ""

	case internalpb.ImportJobState_Failed:
		return 0, internalpb.ImportJobState_Failed, 0, 0, job.GetReason()
	}
	return 0, internalpb.ImportJobState_None, 0, 0, "unknown import job state"
}

// getV3JobProgress maps the Import V3 job states to progress.
func getV3JobProgress(ctx context.Context, job ImportJob, importMeta ImportMeta, meta *meta) (int64, internalpb.ImportJobState, int64, int64, string) {
	jobID := job.GetJobID()
	switch job.GetState() {
	case internalpb.ImportJobState_Pending:
		progress := getPendingProgress(ctx, jobID, importMeta)
		return int64(progress * 10), internalpb.ImportJobState_Pending, 0, 0, ""

	case internalpb.ImportJobState_PreImporting, internalpb.ImportJobState_AssigningIDRange:
		progress := getPreImportingProgress(ctx, jobID, importMeta)
		return 10 + int64(progress*5), internalpb.ImportJobState_Importing, 0, 0, ""

	case internalpb.ImportJobState_Resharding:
		progress := getReshardProgress(ctx, jobID, importMeta)
		return 10 + 5 + int64(progress*30), internalpb.ImportJobState_Importing, 0, 0, ""

	case internalpb.ImportJobState_Planning:
		return 10 + 5 + 30, internalpb.ImportJobState_Importing, 0, 0, ""

	case internalpb.ImportJobState_Importing:
		progress, importedRows, totalRows := getImportingProgress(ctx, jobID, importMeta, meta)
		return 10 + 5 + 30 + 5 + int64(progress*10), internalpb.ImportJobState_Importing, importedRows, totalRows, ""

	case internalpb.ImportJobState_IndexBuilding:
		progress := getIndexBuildingProgress(ctx, jobID, importMeta, meta)
		_, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
		return 10 + 5 + 30 + 5 + 10 + int64(progress*30), internalpb.ImportJobState_Importing, totalRows, totalRows, ""

	case internalpb.ImportJobState_Uncommitted:
		_, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
		if job.GetAutoCommit() {
			return 99, internalpb.ImportJobState_Importing, totalRows, totalRows, ""
		}
		return 99, internalpb.ImportJobState_Uncommitted, totalRows, totalRows, ""

	case internalpb.ImportJobState_Committing:
		_, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
		if job.GetAutoCommit() {
			return 99, internalpb.ImportJobState_Importing, totalRows, totalRows, ""
		}
		return 99, internalpb.ImportJobState_Committing, totalRows, totalRows, ""

	case internalpb.ImportJobState_Completed:
		_, totalRows := getImportRowsInfo(ctx, jobID, importMeta, meta)
		return 100, internalpb.ImportJobState_Completed, totalRows, totalRows, ""

	case internalpb.ImportJobState_Failed:
		return 0, internalpb.ImportJobState_Failed, 0, 0, job.GetReason()
	}
	return 0, internalpb.ImportJobState_None, 0, 0, "unknown import job state"
}

func GetTaskProgresses(ctx context.Context, jobID int64, importMeta ImportMeta, meta *meta) []*internalpb.ImportTaskProgress {
	if job := importMeta.GetJob(ctx, jobID); job != nil && isV3Job(job) {
		return getV3TaskProgresses(ctx, job, importMeta)
	}
	progresses := make([]*internalpb.ImportTaskProgress, 0)
	tasks := importMeta.GetTaskByJob(ctx, jobID, WithType(ImportTaskType))
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

// getV3TaskProgresses projects the V3 per-file progress. V3 has no per-file
// import task: a reshard task owns a set of source files and hashes them into
// fragments, so one entry is emitted per source file of every reshard task.
// The numerator is the rows the reshard worker has hashed for that file (hash
// time, not the later merge flush); the denominator is the count-only preimport
// row count when the job ran it. A job that skipped preimport (backup, or a
// collection without a resolvable primary key) has no denominator: its file
// entries report total_rows 0 and only reach 100 once the owning reshard run
// completed.
func getV3TaskProgresses(ctx context.Context, job ImportJob, importMeta ImportMeta) []*internalpb.ImportTaskProgress {
	pathsByFile := make(map[int64]string)
	for _, file := range job.GetFiles() {
		pathsByFile[file.GetId()] = fmt.Sprintf("%v", file.GetPaths())
	}
	rowsByFile, sizeByFile := make(map[int64]int64), make(map[int64]int64)
	for _, task := range importMeta.GetTaskByJob(ctx, job.GetJobID(), WithType(PreImportV2TaskType)) {
		for _, stat := range task.GetFileStats() {
			fileID := stat.GetImportFile().GetId()
			rowsByFile[fileID] += stat.GetTotalRows()
			sizeByFile[fileID] += stat.GetFileSize()
		}
	}

	progresses := make([]*internalpb.ImportTaskProgress, 0)
	for _, generic := range importMeta.GetTaskByJob(ctx, job.GetJobID(), WithType(ReshardTaskType)) {
		task, ok := generic.(*reshardTask)
		if !ok {
			continue
		}
		p := task.task.Load()
		hashed := make(map[int64]int64)
		for _, progress := range task.getSourceProgress() {
			hashed[progress.GetFileId()] = progress.GetHashedRows()
		}
		for _, fileID := range p.GetSourceIds() {
			totalRows := rowsByFile[fileID]
			importedRows := hashed[fileID]
			if totalRows > 0 && importedRows > totalRows {
				importedRows = totalRows
			}
			progress := int64(0)
			if totalRows > 0 {
				progress = importedRows * 100 / totalRows
			} else if p.GetState() == datapb.ImportTaskStateV2_Completed {
				progress = 100
			}
			progresses = append(progresses, &internalpb.ImportTaskProgress{
				FileName:     pathsByFile[fileID],
				FileSize:     sizeByFile[fileID],
				Reason:       p.GetReason(),
				Progress:     progress,
				CompleteTime: "",
				State:        p.GetState().String(),
				ImportedRows: importedRows,
				TotalRows:    totalRows,
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
	mlog.Info(context.TODO(), "drop import in datanode done", WrapTaskLog(task)...)
	return tm.UpdateTask(context.TODO(), task.GetTaskID(), UpdateNodeID(NullNodeID))
}

func validateBinlogImportPaths(paths []string) error {
	if len(paths) == 0 {
		return merr.WrapErrImportFailed("no insert binlogs to import")
	}
	if len(paths) > 2 {
		return merr.WrapErrImportFailedMsg("too many input paths for binlog import. "+
			"Valid paths length should be one or two, but got paths:%s", paths)
	}
	return nil
}

func ListBinlogsAndGroupBySegment(ctx context.Context,
	cm storage.ChunkManager, importFile *internalpb.ImportFile,
) ([]*internalpb.ImportFile, error) {
	if err := validateBinlogImportPaths(importFile.GetPaths()); err != nil {
		return nil, err
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
	deltaSegmentIDs := lo.KeyBy(segmentDeltaPaths, path.Base)

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
			mlog.Info(context.TODO(), "import segments info", mlog.FieldJobID(jobID),
				mlog.String("channel", channel), mlog.FieldPartitionID(partitionID),
				mlog.Int("segmentsNum", len(segments)), mlog.Any("segmentsInfo", infos),
			)
		}
	}
	mlog.Info(context.TODO(), "import result info", mlog.FieldJobID(jobID),
		mlog.Int64("totalRows", totalRows), mlog.Int64("totalSize", totalSize))
}

// normalizeStorageKey folds a storage key into a single namespace so that a
// candidate path and a deny-list entry are always comparable.
//
// Rooting at "/" before cleaning does three things at once:
//   - an absolute storage root (localStorage.path, e.g. /var/lib/milvus/data)
//     and a relative one (minio.rootPath, e.g. files) end up in the same
//     namespace, so the deny list applies to both;
//   - any number of leading slashes collapses to one, so "//files/insert_log"
//     cannot dodge an entry that "/files/insert_log" matches;
//   - a leading ".." is resolved away rather than preserved, which path.Clean
//     cannot do for a relative path. This matters because LocalChunkManager
//     opens the caller's path with os.Open, where "../files/insert_log/x"
//     resolves against the process working directory.
//
// Applying POSIX cleaning to REMOTE object keys is deliberate, not an oversight.
// An S3/MinIO key is an opaque string and RemoteChunkManager passes it through
// verbatim, so literal prefix matching would describe what a single backend does
// more precisely -- and would let "files/../files/insert_log/x" and every other
// syntactic variant of an internal prefix through. RemoteChunkManager fronts
// MinIO, S3, GCS, Azure Blob, OSS and COS, whose key normalization is not uniform
// and has historically included key-to-filesystem-path mappings; on any backend
// that does normalize, those variants read real internal data. The deny list
// therefore compares the cleaned form on purpose, accepting that a caller key
// which cleans onto an internal prefix (say "files//insert_log/x", a distinct
// object on a literal backend) is over-rejected. Over-rejecting is recoverable;
// under-rejecting is not, and no syntax distinguishes an accidental doubled
// slash from a deliberate one.
func normalizeStorageKey(key string) string {
	return path.Clean("/" + key)
}

// comparableStorageKey returns the form of key that the deny list compares.
// Under local storage the read is os.Open, which follows symlinks and /proc
// magic links such as /proc/self/root, so the key is resolved first. A key
// that cannot be resolved is an error, which rejects the import.
//
// This resolve and the later open are not atomic, and the datanode opens the
// caller's original string rather than what was resolved here. A caller who can
// write to the staging directory can swap a symlink in between. Closing that
// needs O_NOFOLLOW or a resolve-and-recheck at the read.
func comparableStorageKey(key string, localStorage bool) (string, error) {
	if localStorage {
		resolved, err := filepath.EvalSymlinks(key)
		if err != nil {
			// Classified by the caller: a candidate path is caller-supplied,
			// while the storage root is the server's own configuration.
			return "", err
		}
		key = resolved
	}
	return normalizeStorageKey(key), nil
}

// appendDenied adds one internal directory to the deny list.
//
// Under local storage it adds the resolved form as well. rootPath is already
// resolved, but a directory below it can itself be a symlink -- an operator
// moving the cache subtree onto another disk is the ordinary case. Candidate
// paths are compared after full resolution, so an entry that exists only in
// its unresolved spelling would never match one. Both forms are kept: the
// unresolved one still matches a caller who spells the alias, and a segment
// that does not exist yet cannot be resolved and cannot be read either.
func appendDenied(denied []string, dir string, localStorage bool) []string {
	lexical := normalizeStorageKey(dir)
	denied = append(denied, lexical)
	if !localStorage {
		return denied
	}
	resolved, err := filepath.EvalSymlinks(lexical)
	if err != nil {
		return denied
	}
	if key := normalizeStorageKey(resolved); key != lexical {
		denied = append(denied, key)
	}
	return denied
}

// ValidateImportFilePaths rejects ordinary imports whose caller-supplied paths
// point into Milvus's own internal storage layout under the storage root path.
//
// RBAC authorizes an import against the target collection name only; the file
// paths never participate in that decision. Refusing Milvus's own data
// directories keeps an ordinary import inside caller-supplied staging data.
//
// Binlog import (backup=true) and L0 import are exempt: reading insert_log and
// delta_log is exactly what they do. They are gated instead by the cluster-level
// ImportBinlog privilege, checked in the proxy.
func ValidateImportFilePaths(cm storage.ChunkManager, files []*msgpb.ImportFile, options []*commonpb.KeyValuePair) error {
	if importutilv2.IsBackup(options) || importutilv2.IsL0Import(options) {
		return nil
	}

	// Segments rooted at localStorage.path share the ChunkManager root only when
	// the storage type is local. Denying them on a MinIO-backed cluster would
	// reject caller paths Milvus never writes -- <minio.rootPath>/tmp/... being
	// the one people actually stage imports under.
	localStorage := paramtable.Get().CommonCfg.StorageType.GetValue() == "local"

	segments := make([]string, 0,
		len(common.InternalStorageRootSegments)+len(common.LocalOnlyStorageRootSegments))
	segments = append(segments, common.InternalStorageRootSegments...)
	if localStorage {
		segments = append(segments, common.LocalOnlyStorageRootSegments...)
	}

	rootPath, err := comparableStorageKey(cm.RootPath(), localStorage)
	if err != nil {
		// localStorage.path is operator-owned, so an unresolvable root is a
		// server-side fault: a dropped mount or a missing directory. Reporting
		// it as an InputError would bucket it as a bad caller path and stop
		// retry.Do from retrying a recoverable condition.
		return merr.WrapErrImportSysFailedMsg(
			"cannot resolve storage root %s: %v", cm.RootPath(), err)
	}
	denied := make([]string, 0, 2*len(segments)+2)
	for _, segment := range segments {
		denied = appendDenied(denied, path.Join(rootPath, segment), localStorage)
	}
	if localStorage {
		// Legacy StorageV3 segments stay at <localStorage.path>/<minio.rootPath>/insert_log
		// across an upgrade and are read in place: migration protects exactly that
		// directory (storage/localmigrate/migrate.go legacyNamespace) and an update to
		// a legacy manifest keeps its base. An empty or "." prefix collapses onto the
		// <root>/insert_log entry above.
		legacyInsertLog := path.Join(rootPath,
			paramtable.Get().MinioCfg.RootPath.GetValue(), common.SegmentInsertLogPath)
		denied = appendDenied(denied, legacyInsertLog, localStorage)
	} else {
		// Explore planning manifests live at the bucket root on remote storage,
		// outside minio.rootPath (external_collection_refresh_manager.go
		// exploreDirForChunkManager), so no root-anchored entry can reach them.
		// They are milvus-table-explore.json, which the import extension
		// whitelist accepts, so nothing else bounds them either.
		denied = appendDenied(denied, common.ExploreTempRootPath, localStorage)
	}

	for _, file := range files {
		for _, filePath := range file.GetPaths() {
			// The deny entries are anchored at the storage root, but under local
			// storage the read is os.Open, which resolves a relative key against
			// the datanode's working directory instead. The two namespaces never
			// meet, so a relative key can never match a deny entry no matter how
			// it is normalized -- with WORKDIR /milvus and
			// localStorage.path=/milvus/data, "data/snapshots/..." reads the
			// snapshot directory while comparing as "/data/snapshots/...".
			// Every legitimate local staging path is absolute, so refuse the rest.
			if localStorage && !path.IsAbs(filePath) {
				return merr.WrapErrImportFailedMsg(
					"import path %s must be absolute under common.storageType=local", filePath)
			}

			cleaned, err := comparableStorageKey(filePath, localStorage)
			if err != nil {
				return merr.WrapErrImportFailedMsg(
					"cannot resolve import path %s: %v", filePath, err)
			}
			// Compare both forms of the candidate. The resolved one catches an
			// alias of the path as a whole -- /proc/self/root, a staging symlink
			// into the root. The lexical one catches a symlink at any depth
			// BELOW a registered segment: with <root>/cache/1 -> /nvme/cache-1
			// the resolved form leaves the root's namespace entirely, so no
			// root-anchored entry can match it, while the lexical form still
			// reads <root>/cache/... and hits the entry. A caller who spells the
			// relocated directory directly is out of reach of either form; that
			// needs canonicalization at the read, see Known limitations.
			forms := []string{cleaned}
			if lexical := normalizeStorageKey(filePath); lexical != cleaned {
				forms = append(forms, lexical)
			}
			for _, deniedPath := range denied {
				for _, form := range forms {
					// Boundary match, not a raw prefix match: a raw prefix would also
					// reject a caller's own "files/insert_logs_2026/a.json".
					if form == deniedPath || strings.HasPrefix(form, deniedPath+"/") {
						return merr.WrapErrImportFailedMsg(
							"import path %s is not allowed: %s is a Milvus internal storage directory",
							filePath, deniedPath)
					}
				}
			}
		}
	}
	return nil
}

// ValidateBinlogImportRequest validates the binlog import request.
func ValidateBinlogImportRequest(ctx context.Context, cm storage.ChunkManager,
	reqFiles []*msgpb.ImportFile, options []*commonpb.KeyValuePair,
) error {
	files := lo.Map(reqFiles, func(file *msgpb.ImportFile, _ int) *internalpb.ImportFile {
		return &internalpb.ImportFile{
			Id:    file.GetId(),
			Paths: file.GetPaths(),
		}
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
	// Validate the whole request before listing so storage failures cannot
	// mask invalid path counts in later files.
	for _, importFile := range reqFiles {
		if err := validateBinlogImportPaths(importFile.GetPaths()); err != nil {
			return nil, err
		}
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
		if !errors.Is(err, merr.ErrImportFailed) {
			err = merr.WrapErrServiceUnavailableErr(err, "list binlogs failed")
		}
		return nil, err
	}

	resFiles = lo.Filter(resFiles, func(file *internalpb.ImportFile, _ int) bool {
		return len(file.GetPaths()) > 0
	})
	if len(resFiles) == 0 {
		return nil, merr.WrapErrImportFailedMsg("no binlog to import, input=%s", reqFiles)
	}
	if len(resFiles) > paramtable.Get().DataCoordCfg.MaxFilesPerImportReq.GetAsInt() {
		return nil, merr.WrapErrImportFailedMsg("The max number of import files should not exceed %d, but got %d",
			paramtable.Get().DataCoordCfg.MaxFilesPerImportReq.GetAsInt(), len(resFiles))
	}
	mlog.Info(ctx, "list binlogs prefixes for import done", mlog.Int("num", len(resFiles)), mlog.Any("binlog_prefixes", resFiles))
	return resFiles, nil
}

// ValidateMaxImportJobExceed checks if the number of import jobs exceeds the limit.
func ValidateMaxImportJobExceed(ctx context.Context, importMeta ImportMeta) error {
	maxNum := paramtable.Get().DataCoordCfg.MaxImportJobNum.GetAsInt()
	executingNum := importMeta.CountJobBy(ctx, WithoutJobStates(internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed))
	if executingNum >= maxNum {
		return merr.WrapErrImportSysFailed(
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
		taskBufferSize = baseBufferSize * len(job.GetVchannels()) * len(job.GetPartitionIDs())
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
	t ImportTask,
	originSegment *SegmentInfo,
	targetSegmentID int64,
	meta *meta,
	handler Handler,
	alloc allocator.Allocator,
) (*datapb.CompactionTask, error) {
	log := mlog.With(WrapTaskLog(t)...)
	if originSegment.GetNumOfRows() == 0 {
		operator := UpdateStatusOperator(originSegment.GetID(), commonpb.SegmentState_Dropped)
		err := meta.UpdateSegmentsInfo(ctx, operator)
		if err != nil {
			log.Warn(ctx, "import zero num row segment, but mark it dropped failed", mlog.Err(err))
			return nil, err
		}
		return nil, nil
	}
	collection, err := handler.GetCollection(ctx, originSegment.GetCollectionID())
	if err != nil {
		log.Warn(ctx, "Failed to create sort compaction task because get collection fail", mlog.Err(err))
		return nil, err
	}

	collectionTTL, err := common.GetCollectionTTLFromMap(collection.Properties)
	if err != nil {
		log.Warn(ctx, "Failed to create sort compaction task because get collection ttl failed")
		return nil, err
	}

	startID, _, err := alloc.AllocN(2)
	if err != nil {
		log.Warn(ctx, "Failed to create sort compaction task because allocate id fail", mlog.Err(err))
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
		MaxSize:            expectedSize,
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: targetSegmentID,
			End:   targetSegmentID + 1,
		},
	}

	log.Info(ctx, "create sort compaction task success", mlog.FieldSegmentID(originSegment.GetID()),
		mlog.Int64("targetSegmentID", targetSegmentID), mlog.Int64("num rows", originSegment.GetNumOfRows()))
	return task, nil
}
