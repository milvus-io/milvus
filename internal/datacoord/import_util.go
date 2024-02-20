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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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
				State:        internalpb.ImportState_Pending,
				FileStats:    fileStats,
			},
			lastActiveTime: time.Now(),
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
				State:        internalpb.ImportState_Pending,
				FileStats:    group,
			},
			lastActiveTime: time.Now(),
		}
		segments, err := AssignSegments(task, manager, job.GetSchema())
		if err != nil {
			return nil, err
		}
		task.SegmentIDs = segments
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func AssignSegments(task ImportTask, manager Manager, schema *schemapb.CollectionSchema) ([]int64, error) {
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
			segmentInfo, err := manager.AllocImportSegment(ctx, task.GetTaskID(), task.GetCollectionID(),
				partitionID, vchannel, schema)
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

func RegroupImportFiles(job ImportJob, files []*datapb.ImportFileStats) ([][]*datapb.ImportFileStats, error) {
	if len(files) == 0 {
		return nil, nil
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
	return fileGroups, nil
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

func GetImportProgress(jobID int64, tm ImportMeta, meta *meta) (int64, internalpb.ImportState, string) {
	tasks := tm.GetTaskBy(WithJob(jobID), WithType(PreImportTaskType))
	var (
		preparingProgress float32 = 100
		preImportProgress float32
		importProgress    float32
		segStateProgress  float32
	)
	totalTaskNum := len(tm.GetTaskBy(WithJob(jobID)))
	for _, task := range tasks {
		switch task.GetState() {
		case internalpb.ImportState_Failed:
			return 0, internalpb.ImportState_Failed, task.GetReason()
		case internalpb.ImportState_Pending:
			preparingProgress -= 100 / float32(totalTaskNum)
		case internalpb.ImportState_Completed:
			preImportProgress += 100 / float32(len(tasks))
		}
	}
	tasks = tm.GetTaskBy(WithJob(jobID), WithType(ImportTaskType))
	var (
		unsetImportStateSegments = 0
		totalSegments            = 0
	)
	for _, task := range tasks {
		switch task.GetState() {
		case internalpb.ImportState_Failed:
			return 0, internalpb.ImportState_Failed, task.GetReason()
		case internalpb.ImportState_Pending:
			preparingProgress -= 100 / float32(totalTaskNum)
		case internalpb.ImportState_InProgress:
			segmentIDs := task.(*importTask).GetSegmentIDs()
			var (
				importedRows int64
				totalRows    int64
			)
			for _, segmentID := range segmentIDs {
				segment := meta.GetSegment(segmentID)
				if segment == nil {
					return 0, internalpb.ImportState_Failed, merr.WrapErrSegmentNotFound(segmentID).Error()
				}
				importedRows += segment.currRows
				totalRows += segment.GetMaxRowNum()
				totalSegments++
			}
			importProgress += (float32(importedRows) / float32(totalRows)) * 100 / float32(len(tasks))
		case internalpb.ImportState_Completed:
			segmentIDs := task.(*importTask).GetSegmentIDs()
			for _, segmentID := range segmentIDs {
				segment := meta.GetSegment(segmentID)
				if segment == nil {
					return 0, internalpb.ImportState_Failed, merr.WrapErrSegmentNotFound(segmentID).Error()
				}
				totalSegments++
				if !segment.GetIsImporting() {
					unsetImportStateSegments++
				}
			}
			importProgress += 100 / float32(len(tasks))
		}
	}
	if totalSegments == 0 {
		// If there is no data in the import files, the totalSegments will be 0.
		// And, if all tasks are completed, we should set segStateProgress to 100.
		completedTasks := tm.GetTaskBy(WithJob(jobID), WithType(ImportTaskType), WithStates(internalpb.ImportState_Completed))
		if len(completedTasks) == len(tasks) {
			segStateProgress = 100
		}
	} else {
		segStateProgress = 100 * float32(unsetImportStateSegments) / float32(totalSegments)
	}
	progress := preparingProgress*0.1 + preImportProgress*0.4 + importProgress*0.4 + segStateProgress*0.1
	if progress == 100 {
		return 100, internalpb.ImportState_Completed, ""
	}
	return int64(progress), internalpb.ImportState_InProgress, ""
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

	insertLogs, _, err := cm.ListWithPrefix(ctx, importFile.GetPaths()[0], true)
	if err != nil {
		return nil, err
	}
	segmentInsertPaths := lo.Uniq(lo.Map(insertLogs, func(fullPath string, _ int) string {
		fieldPath := path.Dir(fullPath)
		segmentPath := path.Dir(fieldPath)
		return segmentPath
	}))

	segmentPrefixes := lo.Map(segmentInsertPaths, func(segmentPath string, _ int) *internalpb.ImportFile {
		return &internalpb.ImportFile{Paths: []string{segmentPath}}
	})

	if len(importFile.GetPaths()) < 2 {
		return segmentPrefixes, nil
	}
	deltaLogs, _, err := cm.ListWithPrefix(context.Background(), importFile.GetPaths()[1], true)
	if err != nil {
		return nil, err
	}
	if len(deltaLogs) == 0 {
		return segmentPrefixes, nil
	}

	segmentDeltaPaths := lo.Uniq(lo.Map(deltaLogs, func(fullPath string, _ int) string {
		segmentPath := path.Dir(fullPath)
		return segmentPath
	}))
	segmentDeltaMap := lo.KeyBy(segmentDeltaPaths, func(deltaPrefix string) string {
		return path.Base(deltaPrefix)
	})

	for i := range segmentPrefixes {
		segmentID := path.Base(segmentPrefixes[i].GetPaths()[0])
		if deltaPrefix, ok := segmentDeltaMap[segmentID]; ok {
			segmentPrefixes[i].Paths = append(segmentPrefixes[i].Paths, deltaPrefix)
		}
	}
	return segmentPrefixes, nil
}
