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
	"path"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func WrapTaskLog(task ImportTask, fields ...zap.Field) []zap.Field {
	res := []zap.Field{
		zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("jobID", task.GetJobID()),
		zap.Int64("collectionID", task.GetCollectionID()),
		zap.String("type", task.GetType().String()),
	}
	res = append(res, fields...)
	return res
}

func NewPreImportTasks(fileGroups [][]*internalpb.ImportFile,
	job ImportJob,
	alloc allocator,
) ([]ImportTask, error) {
	idStart, _, err := alloc.allocN(int64(len(fileGroups)))
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
			},
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func NewImportTasks(fileGroups [][]*datapb.ImportFileStats,
	job ImportJob,
	manager Manager,
	alloc allocator,
) ([]ImportTask, error) {
	idBegin, _, err := alloc.allocN(int64(len(fileGroups)))
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
			},
		}
		segments, err := AssignSegments(task, manager)
		if err != nil {
			return nil, err
		}
		task.SegmentIDs = segments
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func AssignSegments(task ImportTask, manager Manager) ([]int64, error) {
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

	segmentMaxSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024

	// alloc new segments
	segments := make([]int64, 0)
	addSegment := func(vchannel string, partitionID int64, size int64) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for size > 0 {
			segmentInfo, err := manager.AllocImportSegment(ctx, task.GetTaskID(), task.GetCollectionID(), partitionID, vchannel)
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
			err := addSegment(vchannel, partitionID, size)
			if err != nil {
				return nil, err
			}
		}
	}
	return segments, nil
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

func AssembleImportRequest(task ImportTask, job ImportJob, meta *meta, alloc allocator) (*datapb.ImportRequest, error) {
	requestSegments := make([]*datapb.ImportRequestSegment, 0)
	for _, segmentID := range task.(*importTask).GetSegmentIDs() {
		segment := meta.GetSegment(segmentID)
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
	ts, err := alloc.allocTimestamp(ctx)
	if err != nil {
		return nil, err
	}
	totalRows := lo.SumBy(task.GetFileStats(), func(stat *datapb.ImportFileStats) int64 {
		return stat.GetTotalRows()
	})
	idBegin, idEnd, err := alloc.allocN(totalRows)
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
		AutoIDRange:     &datapb.AutoIDRange{Begin: idBegin, End: idEnd},
		RequestSegments: requestSegments,
	}, nil
}

func RegroupImportFiles(job ImportJob, files []*datapb.ImportFileStats) [][]*datapb.ImportFileStats {
	if len(files) == 0 {
		return nil
	}

	segmentMaxSize := paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt() * 1024 * 1024
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

func AddImportSegment(cluster Cluster, meta *meta, segmentID int64) error {
	segment := meta.GetSegment(segmentID)
	req := &datapb.AddImportSegmentRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithSourceID(paramtable.GetNodeID()),
		),
		SegmentId:    segment.GetID(),
		ChannelName:  segment.GetInsertChannel(),
		CollectionId: segment.GetCollectionID(),
		PartitionId:  segment.GetPartitionID(),
		RowNum:       segment.GetNumOfRows(),
		StatsLog:     segment.GetStatslogs(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := cluster.AddImportSegment(ctx, req)
	return err
}

func getPendingProgress(jobID int64, imeta ImportMeta) float32 {
	tasks := imeta.GetTaskBy(WithJob(jobID), WithType(PreImportTaskType))
	preImportingFiles := lo.SumBy(tasks, func(task ImportTask) int {
		return len(task.GetFileStats())
	})
	totalFiles := len(imeta.GetJob(jobID).GetFiles())
	return float32(preImportingFiles) / float32(totalFiles)
}

func getPreImportingProgress(jobID int64, imeta ImportMeta) float32 {
	tasks := imeta.GetTaskBy(WithJob(jobID), WithType(PreImportTaskType))
	completedTasks := lo.Filter(tasks, func(task ImportTask, _ int) bool {
		return task.GetState() == datapb.ImportTaskStateV2_Completed
	})
	return float32(len(completedTasks)) / float32(len(tasks))
}

func getImportingProgress(jobID int64, imeta ImportMeta, meta *meta) float32 {
	var (
		importedRows int64
		totalRows    int64
	)
	tasks := imeta.GetTaskBy(WithJob(jobID), WithType(ImportTaskType))
	for _, task := range tasks {
		totalRows += lo.SumBy(task.GetFileStats(), func(file *datapb.ImportFileStats) int64 {
			return file.GetTotalRows()
		})
		segmentIDs := task.(*importTask).GetSegmentIDs()
		for _, segmentID := range segmentIDs {
			segment := meta.GetSegment(segmentID)
			if segment == nil {
				log.Warn("cannot find segment, may be compacted", WrapTaskLog(task, zap.Int64("segmentID", segmentID))...)
				continue
			}
			importedRows += segment.currRows
		}
	}
	importingProgress := float32(importedRows) / float32(totalRows)

	var (
		unsetIsImportingSegment int64
		totalSegment            int64
	)
	for _, task := range tasks {
		segmentIDs := task.(*importTask).GetSegmentIDs()
		for _, segmentID := range segmentIDs {
			segment := meta.GetSegment(segmentID)
			if segment == nil {
				log.Warn("cannot find segment, may be compacted", WrapTaskLog(task, zap.Int64("segmentID", segmentID))...)
				continue
			}
			totalSegment++
			if !segment.GetIsImporting() {
				unsetIsImportingSegment++
			}
		}
	}
	completedProgress := float32(unsetIsImportingSegment) / float32(totalSegment)
	return importingProgress*0.8 + completedProgress*0.2
}

func GetImportProgress(jobID int64, imeta ImportMeta, meta *meta) (int64, internalpb.ImportJobState, string) {
	job := imeta.GetJob(jobID)
	switch job.GetState() {
	case internalpb.ImportJobState_Pending:
		progress := getPendingProgress(jobID, imeta)
		return int64(progress * 10), internalpb.ImportJobState_Pending, ""

	case internalpb.ImportJobState_PreImporting:
		progress := getPreImportingProgress(jobID, imeta)
		return 10 + int64(progress*40), internalpb.ImportJobState_Importing, ""

	case internalpb.ImportJobState_Importing:
		progress := getImportingProgress(jobID, imeta, meta)
		return 10 + 40 + int64(progress*50), internalpb.ImportJobState_Importing, ""

	case internalpb.ImportJobState_Completed:
		return 100, internalpb.ImportJobState_Completed, ""

	case internalpb.ImportJobState_Failed:
		return 0, internalpb.ImportJobState_Failed, job.GetReason()
	}
	return 0, internalpb.ImportJobState_Failed, "unknown import job state"
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
	return tm.UpdateTask(task.GetTaskID(), UpdateNodeID(NullNodeID))
}

func ListBinlogsAndGroupBySegment(ctx context.Context, cm storage.ChunkManager, importFile *internalpb.ImportFile) ([]*internalpb.ImportFile, error) {
	if len(importFile.GetPaths()) < 1 {
		return nil, merr.WrapErrImportFailed("no insert binlogs to import")
	}

	segmentInsertPaths, _, err := cm.ListWithPrefix(ctx, importFile.GetPaths()[0], false)
	if err != nil {
		return nil, err
	}
	segmentImportFiles := lo.Map(segmentInsertPaths, func(segmentPath string, _ int) *internalpb.ImportFile {
		return &internalpb.ImportFile{Paths: []string{segmentPath}}
	})

	if len(importFile.GetPaths()) < 2 {
		return segmentImportFiles, nil
	}
	segmentDeltaPaths, _, err := cm.ListWithPrefix(context.Background(), importFile.GetPaths()[1], false)
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
