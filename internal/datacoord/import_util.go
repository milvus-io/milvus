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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
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
		zap.Int64("nodeID", task.GetNodeID()),
	}
	res = append(res, fields...)
	return res
}

func NewPreImportTasks(fileGroups [][]*internalpb.ImportFile,
	job ImportJob,
	alloc allocator.Allocator,
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
		task := &preImportTask{
			PreImportTask: &datapb.PreImportTask{
				JobID:        job.GetJobID(),
				TaskID:       idStart + int64(i),
				CollectionID: job.GetCollectionID(),
				State:        datapb.ImportTaskStateV2_Pending,
				FileStats:    fileStats,
				CreatedTime:  time.Now().Format("2006-01-02T15:04:05Z07:00"),
			},
			tr: timerecord.NewTimeRecorder("preimport task"),
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func NewImportTasks(fileGroups [][]*datapb.ImportFileStats,
	job ImportJob, alloc allocator.Allocator, meta *meta,
) ([]ImportTask, error) {
	idBegin, _, err := alloc.AllocN(int64(len(fileGroups)))
	if err != nil {
		return nil, err
	}
	tasks := make([]ImportTask, 0, len(fileGroups))
	for i, group := range fileGroups {
		task := &importTask{
			ImportTaskV2: &datapb.ImportTaskV2{
				JobID:        job.GetJobID(),
				TaskID:       idBegin + int64(i),
				CollectionID: job.GetCollectionID(),
				NodeID:       NullNodeID,
				State:        datapb.ImportTaskStateV2_Pending,
				FileStats:    group,
				CreatedTime:  time.Now().Format("2006-01-02T15:04:05Z07:00"),
			},
			tr: timerecord.NewTimeRecorder("import task"),
		}
		segments, err := AssignSegments(job, task, alloc, meta)
		if err != nil {
			return nil, err
		}
		task.SegmentIDs = segments
		if paramtable.Get().DataCoordCfg.EnableStatsTask.GetAsBool() {
			statsSegIDBegin, _, err := alloc.AllocN(int64(len(segments)))
			if err != nil {
				return nil, err
			}
			task.StatsSegmentIDs = lo.RangeFrom(statsSegIDBegin, len(segments))
			log.Info("preallocate stats segment ids", WrapTaskLog(task, zap.Int64s("segmentIDs", task.StatsSegmentIDs))...)
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func AssignSegments(job ImportJob, task ImportTask, alloc allocator.Allocator, meta *meta) ([]int64, error) {
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

	segmentMaxSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
	if isL0Import {
		segmentMaxSize = paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.GetAsInt64()
	}
	segmentLevel := datapb.SegmentLevel_L1
	if isL0Import {
		segmentLevel = datapb.SegmentLevel_L0
	}

	// alloc new segments
	segments := make([]int64, 0)
	addSegment := func(vchannel string, partitionID int64, size int64) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for size > 0 {
			segmentInfo, err := AllocImportSegment(ctx, alloc, meta,
				task.GetJobID(), task.GetTaskID(), task.GetCollectionID(), partitionID, vchannel, segmentLevel)
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
				// When autoID is enabled, the preimport and import phases use different primary keys,
				// which can lead to inaccurate row distribution estimation across vchannels.
				//
				// Specifically:
				// - In the *preimport* phase, the system simulates row distribution using dummy primary keys,
				//   typically ranging from 0 to numRows.
				// - In the *import* phase, real auto-generated primary keys (e.g., 457975852966809057) are used.
				//
				// This mismatch can result in slight estimation errors. For example:
				//  Suppose we're importing a single row into two vchannels:
				//    - Preimport uses pk = 0 → hashes to vchannel-0 → estimated count: vchannel-0 = 1, vchannel-1 = 0
				//    - Import uses pk = 457975852966809057 → hashes to vchannel-1 → actual count: vchannel-0 = 0, vchannel-1 = 1
				//
				// To resolve such corner case, here we ensure that at least one segment is allocated
				// for each vchannel during autoID-enabled imports.
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
	level datapb.SegmentLevel,
) (*SegmentInfo, error) {
	log := log.Ctx(ctx)
	id, err := alloc.AllocID(ctx)
	if err != nil {
		log.Error("failed to alloc id for import segment", zap.Error(err))
		return nil, err
	}
	ts, err := alloc.AllocTimestamp(ctx)
	if err != nil {
		return nil, err
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
	return &datapb.PreImportRequest{
		JobID:        task.GetJobID(),
		TaskID:       task.GetTaskID(),
		CollectionID: task.GetCollectionID(),
		PartitionIDs: job.GetPartitionIDs(),
		Vchannels:    job.GetVchannels(),
		Schema:       job.GetSchema(),
		ImportFiles:  importFiles,
		Options:      job.GetOptions(),
	}
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
	ts, err := alloc.AllocTimestamp(ctx)
	if err != nil {
		return nil, err
	}

	totalRows := lo.SumBy(task.GetFileStats(), func(stat *datapb.ImportFileStats) int64 {
		return stat.GetTotalRows()
	})

	// Allocated IDs are used for rowID and the BEGINNING of the logID.
	allocNum := totalRows + 1

	idBegin, idEnd, err := alloc.AllocN(allocNum)
	if err != nil {
		return nil, err
	}

	importFiles := lo.Map(task.GetFileStats(), func(fileStat *datapb.ImportFileStats, _ int) *internalpb.ImportFile {
		return fileStat.GetImportFile()
	})
	return &datapb.ImportRequest{
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
	}, nil
}

func RegroupImportFiles(job ImportJob, files []*datapb.ImportFileStats, allDiskIndex bool) [][]*datapb.ImportFileStats {
	if len(files) == 0 {
		return nil
	}

	var segmentMaxSize int
	if allDiskIndex {
		// Only if all vector fields index type are DiskANN, recalc segment max size here.
		segmentMaxSize = Params.DataCoordCfg.DiskSegmentMaxSize.GetAsInt() * 1024 * 1024
	} else {
		// If some vector fields index type are not DiskANN, recalc segment max size using default policy.
		segmentMaxSize = Params.DataCoordCfg.SegmentMaxSize.GetAsInt() * 1024 * 1024
	}
	isL0Import := importutilv2.IsL0Import(job.GetOptions())
	if isL0Import {
		segmentMaxSize = paramtable.Get().DataNodeCfg.FlushDeleteBufferBytes.GetAsInt()
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

func CheckDiskQuota(job ImportJob, meta *meta, imeta ImportMeta) (int64, error) {
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
	for _, j := range imeta.GetJobBy(context.TODO()) {
		requested := j.GetRequestedDiskSize()
		requestedTotal += requested
		requestedCollections[j.GetCollectionID()] += requested
	}

	err := merr.WrapErrServiceQuotaExceeded("disk quota exceeded, please allocate more resources")
	quotaInfo := meta.GetQuotaInfo()
	totalUsage, collectionsUsage := quotaInfo.TotalBinlogSize, quotaInfo.CollectionBinlogSize

	tasks := imeta.GetTaskBy(context.TODO(), WithJob(job.GetJobID()), WithType(PreImportTaskType))
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

func getPendingProgress(jobID int64, imeta ImportMeta) float32 {
	tasks := imeta.GetTaskBy(context.TODO(), WithJob(jobID), WithType(PreImportTaskType))
	preImportingFiles := lo.SumBy(tasks, func(task ImportTask) int {
		return len(task.GetFileStats())
	})
	totalFiles := len(imeta.GetJob(context.TODO(), jobID).GetFiles())
	if totalFiles == 0 {
		return 1
	}
	return float32(preImportingFiles) / float32(totalFiles)
}

func getPreImportingProgress(jobID int64, imeta ImportMeta) float32 {
	tasks := imeta.GetTaskBy(context.TODO(), WithJob(jobID), WithType(PreImportTaskType))
	completedTasks := lo.Filter(tasks, func(task ImportTask, _ int) bool {
		return task.GetState() == datapb.ImportTaskStateV2_Completed
	})
	if len(tasks) == 0 {
		return 1
	}
	return float32(len(completedTasks)) / float32(len(tasks))
}

func getImportRowsInfo(jobID int64, imeta ImportMeta, meta *meta) (importedRows, totalRows int64) {
	tasks := imeta.GetTaskBy(context.TODO(), WithJob(jobID), WithType(ImportTaskType))
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

func getImportingProgress(jobID int64, imeta ImportMeta, meta *meta) (float32, int64, int64) {
	importedRows, totalRows := getImportRowsInfo(jobID, imeta, meta)
	if totalRows == 0 {
		return 1, importedRows, totalRows
	}
	return float32(importedRows) / float32(totalRows), importedRows, totalRows
}

func getStatsProgress(jobID int64, imeta ImportMeta, sjm StatsJobManager) float32 {
	if !Params.DataCoordCfg.EnableStatsTask.GetAsBool() {
		return 1
	}
	tasks := imeta.GetTaskBy(context.TODO(), WithJob(jobID), WithType(ImportTaskType))
	originSegmentIDs := lo.FlatMap(tasks, func(t ImportTask, _ int) []int64 {
		return t.(*importTask).GetSegmentIDs()
	})
	if len(originSegmentIDs) == 0 {
		return 1
	}
	doneCnt := 0
	for _, originSegmentID := range originSegmentIDs {
		t := sjm.GetStatsTask(originSegmentID, indexpb.StatsSubJob_Sort)
		if t.GetState() == indexpb.JobState_JobStateFinished {
			doneCnt++
		}
	}
	return float32(doneCnt) / float32(len(originSegmentIDs))
}

func getIndexBuildingProgress(jobID int64, imeta ImportMeta, meta *meta) float32 {
	job := imeta.GetJob(context.TODO(), jobID)
	if !Params.DataCoordCfg.WaitForIndex.GetAsBool() {
		return 1
	}
	tasks := imeta.GetTaskBy(context.TODO(), WithJob(jobID), WithType(ImportTaskType))
	originSegmentIDs := lo.FlatMap(tasks, func(t ImportTask, _ int) []int64 {
		return t.(*importTask).GetSegmentIDs()
	})
	targetSegmentIDs := lo.FlatMap(tasks, func(t ImportTask, _ int) []int64 {
		return t.(*importTask).GetStatsSegmentIDs()
	})
	if len(originSegmentIDs) == 0 {
		return 1
	}
	if !Params.DataCoordCfg.EnableStatsTask.GetAsBool() {
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
func GetJobProgress(jobID int64, imeta ImportMeta, meta *meta, sjm StatsJobManager) (int64, internalpb.ImportJobState, int64, int64, string) {
	job := imeta.GetJob(context.TODO(), jobID)
	if job == nil {
		return 0, internalpb.ImportJobState_Failed, 0, 0, fmt.Sprintf("import job does not exist, jobID=%d", jobID)
	}
	switch job.GetState() {
	case internalpb.ImportJobState_Pending:
		progress := getPendingProgress(jobID, imeta)
		return int64(progress * 10), internalpb.ImportJobState_Pending, 0, 0, ""

	case internalpb.ImportJobState_PreImporting:
		progress := getPreImportingProgress(jobID, imeta)
		return 10 + int64(progress*30), internalpb.ImportJobState_Importing, 0, 0, ""

	case internalpb.ImportJobState_Importing:
		progress, importedRows, totalRows := getImportingProgress(jobID, imeta, meta)
		return 10 + 30 + int64(progress*30), internalpb.ImportJobState_Importing, importedRows, totalRows, ""

	case internalpb.ImportJobState_Stats:
		progress := getStatsProgress(jobID, imeta, sjm)
		_, totalRows := getImportRowsInfo(jobID, imeta, meta)
		return 10 + 30 + 30 + int64(progress*10), internalpb.ImportJobState_Importing, totalRows, totalRows, ""

	case internalpb.ImportJobState_IndexBuilding:
		progress := getIndexBuildingProgress(jobID, imeta, meta)
		_, totalRows := getImportRowsInfo(jobID, imeta, meta)
		return 10 + 30 + 30 + 10 + int64(progress*10), internalpb.ImportJobState_Importing, totalRows, totalRows, ""

	case internalpb.ImportJobState_Completed:
		_, totalRows := getImportRowsInfo(jobID, imeta, meta)
		return 100, internalpb.ImportJobState_Completed, totalRows, totalRows, ""

	case internalpb.ImportJobState_Failed:
		return 0, internalpb.ImportJobState_Failed, 0, 0, job.GetReason()
	}
	return 0, internalpb.ImportJobState_None, 0, 0, "unknown import job state"
}

func GetTaskProgresses(jobID int64, imeta ImportMeta, meta *meta) []*internalpb.ImportTaskProgress {
	progresses := make([]*internalpb.ImportTaskProgress, 0)
	tasks := imeta.GetTaskBy(context.TODO(), WithJob(jobID), WithType(ImportTaskType))
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

func DropImportTask(task ImportTask, cluster Cluster, tm ImportMeta) error {
	if task.GetNodeID() == NullNodeID {
		return nil
	}
	req := &datapb.DropImportRequest{
		JobID:  task.GetJobID(),
		TaskID: task.GetTaskID(),
	}
	err := cluster.DropImport(task.GetNodeID(), req)
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		return err
	}
	log.Info("drop import in datanode done", WrapTaskLog(task)...)
	return tm.UpdateTask(context.TODO(), task.GetTaskID(), UpdateNodeID(NullNodeID))
}

func ListBinlogsAndGroupBySegment(ctx context.Context, cm storage.ChunkManager, importFile *internalpb.ImportFile) ([]*internalpb.ImportFile, error) {
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
