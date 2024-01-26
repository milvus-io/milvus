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
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
	"sort"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func WrapLogFields(task ImportTask, fields ...zap.Field) []zap.Field {
	res := []zap.Field{
		zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("jobID", task.GetJobID()),
		zap.Int64("collectionID", task.GetCollectionID()),
		zap.Int64("nodeID", task.GetNodeID()),
		zap.String("state", task.GetState().String()),
		zap.String("type", task.GetType().String()),
	}
	res = append(res, fields...)
	return res
}

func AssemblePreImportRequest(task ImportTask) *datapb.PreImportRequest {
	importFiles := lo.Map(task.(*preImportTask).GetFileStats(),
		func(fileStats *datapb.ImportFileStats, _ int) *internalpb.ImportFile {
			return fileStats.GetImportFile()
		})
	pt := task.(*preImportTask)
	return &datapb.PreImportRequest{
		JobID:        task.GetJobID(),
		TaskID:       task.GetTaskID(),
		CollectionID: task.GetCollectionID(),
		PartitionIDs: pt.GetPartitionIDs(),
		Vchannels:    pt.GetVchannels(),
		Schema:       task.GetSchema(),
		ImportFiles:  importFiles,
		Options:      pt.GetOptions(),
	}
}

func AssignSegments(task ImportTask, manager Manager) ([]int64, error) {
	// merge hashed rows
	hashedRows := make(map[string]map[int64]int64) // vchannel->(partitionID->rows)
	for _, file := range task.GetFileStats() {
		for vchannel, partRows := range file.GetHashedRows() {
			if hashedRows[vchannel] == nil {
				hashedRows[vchannel] = make(map[int64]int64)
			}
			for partitionID, rows := range partRows.GetPartitionRows() {
				hashedRows[vchannel][partitionID] += rows
			}
		}
	}

	maxRowsPerSegment, err := calBySchemaPolicy(task.GetSchema())
	if err != nil {
		return nil, err
	}

	// alloc new segments
	segments := make([]int64, 0)
	addSegment := func(vchannel string, partitionID int64, rows int64) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for rows > 0 {
			segmentInfo, err := manager.AddImportSegment(ctx, task.GetCollectionID(),
				partitionID, vchannel, maxRowsPerSegment)
			if err != nil {
				return err
			}
			segments = append(segments, segmentInfo.GetID())
			rows -= segmentInfo.GetMaxRowNum()
		}
		return nil
	}

	for vchannel, partitionRows := range hashedRows {
		for partitionID, rows := range partitionRows {
			err = addSegment(vchannel, partitionID, rows)
			if err != nil {
				return nil, err
			}
		}
	}
	return segments, nil
}

func AssembleImportRequest(task ImportTask, meta *meta, alloc allocator) (*datapb.ImportRequest, error) {
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
			MaxRows:     segment.GetMaxRowNum(),
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
		Schema:          task.GetSchema(),
		Files:           importFiles,
		Options:         task.(*importTask).GetOptions(),
		Ts:              ts,
		AutoIDRange:     &datapb.AutoIDRange{Begin: idBegin, End: idEnd},
		RequestSegments: requestSegments,
	}, nil
}

func RegroupImportFiles(tasks []ImportTask) ([][]*datapb.ImportFileStats, error) {
	if len(tasks) == 0 {
		return nil, nil
	}
	pt := tasks[0].(*preImportTask)
	files := lo.FlatMap(tasks, func(t ImportTask, _ int) []*datapb.ImportFileStats {
		return t.(*preImportTask).GetFileStats()
	})
	maxRowsPerSegment, err := calBySchemaPolicy(pt.GetSchema())
	if err != nil {
		return nil, err
	}
	maxRowsPerFileGroup := maxRowsPerSegment * len(pt.GetPartitionIDs()) * len(pt.GetVchannels())

	fileGroups := make([][]*datapb.ImportFileStats, 0)
	currentGroup := make([]*datapb.ImportFileStats, 0)
	currentSum := 0
	sort.Slice(files, func(i, j int) bool {
		return files[i].GetTotalRows() < files[j].GetTotalRows()
	})
	for _, file := range files {
		rows := int(file.GetTotalRows())
		if rows > maxRowsPerFileGroup {
			fileGroups = append(fileGroups, []*datapb.ImportFileStats{file})
		} else if currentSum+rows <= maxRowsPerFileGroup {
			currentGroup = append(currentGroup, file)
			currentSum += rows
		} else {
			fileGroups = append(fileGroups, currentGroup)
			currentGroup = []*datapb.ImportFileStats{file}
			currentSum = rows
		}
	}
	if len(currentGroup) > 0 {
		fileGroups = append(fileGroups, currentGroup)
	}
	return fileGroups, nil
}

func NewImportTasks(fileGroups [][]*datapb.ImportFileStats,
	pt *preImportTask,
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
				JobID:        pt.GetJobID(),
				TaskID:       idBegin + int64(i),
				CollectionID: pt.GetCollectionID(),
				NodeID:       NullNodeID,
				State:        internalpb.ImportState_Pending,
				TimeoutTs:    pt.GetTimeoutTs(),
				FileStats:    group,
				Options:      pt.GetOptions(),
			},
			schema:         pt.GetSchema(),
			lastActiveTime: time.Now(),
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

func AreAllTasksFinished(tasks []ImportTask, meta *meta, imeta ImportMeta) bool {
	var finished = true
	for _, task := range tasks {
		if task.GetState() != internalpb.ImportState_Completed {
			return false
		}
		segmentIDs := task.(*importTask).GetSegmentIDs()
		validSegments := make([]int64, 0)
		for _, segmentID := range segmentIDs {
			segment := meta.GetSegment(segmentID)
			if segment.GetState() == commonpb.SegmentState_Dropped {
				continue // this segment has been compacted
			}
			validSegments = append(validSegments, segmentID)
			if segment.GetIsImporting() {
				finished = false
			}
		}
		if len(validSegments) < len(segmentIDs) {
			err := imeta.Update(task.GetTaskID(), UpdateSegmentIDs(validSegments))
			if err != nil {
				log.Warn("update segmentIDs failed", WrapLogFields(task, zap.Error(err))...)
			}
		}
	}
	return finished
}

func GetImportProgress(jobID int64, imeta ImportMeta, meta *meta) (int64, internalpb.ImportState, string) {
	tasks := imeta.GetBy(WithJob(jobID), WithType(PreImportTaskType))
	var (
		preparingProgress float32 = 100
		preImportProgress float32 = 0
		importProgress    float32 = 0
		segStateProgress  float32 = 0
	)
	totalTaskNum := len(imeta.GetBy(WithJob(jobID)))
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
	tasks = imeta.GetBy(WithJob(jobID), WithType(ImportTaskType))
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
			preparingProgress += 100 / float32(len(tasks))
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
				if !segment.GetIsImporting() {
					unsetImportStateSegments++
				}
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
		segStateProgress = 100
	} else {
		segStateProgress = 100 * float32(unsetImportStateSegments) / float32(totalSegments)
	}
	progress := preparingProgress*0.1 + preImportProgress*0.4 + importProgress*0.4 + segStateProgress*0.1
	if progress == 100 {
		return 100, internalpb.ImportState_Completed, ""
	}
	return int64(progress), internalpb.ImportState_InProgress, ""
}

func DropImportTask(task ImportTask, cluster Cluster, imeta ImportMeta) error {
	if task.GetNodeID() == NullNodeID {
		return nil
	}
	req := &datapb.DropImportRequest{
		JobID:  task.GetJobID(),
		TaskID: task.GetTaskID(),
	}
	err := cluster.DropImport(task.GetNodeID(), req)
	if err != nil {
		return err
	}
	return imeta.Update(task.GetTaskID(), UpdateNodeID(NullNodeID))
}
