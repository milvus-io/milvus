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
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"sort"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func WrapLogFields(task ImportTask, err error) []zap.Field {
	fields := []zap.Field{
		zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("requestID", task.GetRequestID()),
		zap.Int64("collectionID", task.GetCollectionID()),
		zap.Int64("nodeID", task.GetNodeID()),
		zap.String("state", task.GetState().String()),
		zap.String("type", task.GetType().String()),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	return fields
}

func AssemblePreImportRequest(task ImportTask) *datapb.PreImportRequest {
	importFiles := lo.Map(task.(*preImportTask).GetFileStats(),
		func(fileStats *datapb.ImportFileStats, _ int) *internalpb.ImportFile {
			return fileStats.GetImportFile()
		})
	pt := task.(*preImportTask)
	return &datapb.PreImportRequest{
		RequestID:    task.GetRequestID(),
		TaskID:       task.GetTaskID(),
		CollectionID: task.GetCollectionID(),
		PartitionIDs: pt.GetPartitionIDs(),
		Vchannels:    pt.GetVchannels(),
		Schema:       task.GetSchema(),
		ImportFiles:  importFiles,
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
	for vchannel, partitionRows := range hashedRows {
		for partitionID, rows := range partitionRows {
			for rows > 0 {
				segmentInfo, err := manager.AddImportSegment(context.TODO(), task.GetCollectionID(), // TODO: dyh, fix context
					partitionID, vchannel, maxRowsPerSegment)
				if err != nil {
					return nil, err
				}
				segments = append(segments, segmentInfo.GetID())
				rows -= segmentInfo.GetMaxRowNum()
			}
		}
	}
	return segments, nil
}

func AssembleImportRequest(task ImportTask, meta *meta, alloc allocator) (*datapb.ImportRequest, error) {
	requestSegments := make(map[int64]*datapb.ImportRequestSegment, 0)
	for _, segmentID := range task.(*importTask).GetSegmentIDs() {
		segment := meta.GetSegment(segmentID)
		if segment == nil {
			return nil, merr.WrapErrSegmentNotFound(segmentID, "assemble import request failed")
		}
		//idBegin, idEnd, err := alloc.allocN(segment.GetMaxRowNum())
		//if err != nil {
		//	return nil, err
		//}
		//ts, err := alloc.allocTimestamp(context.Background()) // TODO: dyh, resolve context
		requestSegments[segmentID] = &datapb.ImportRequestSegment{
			SegmentID:   segment.GetID(),
			PartitionID: segment.GetPartitionID(),
			Vchannel:    segment.GetInsertChannel(),
			//Ts:           ts,
			//AutoIDRanges: &datapb.AutoIDRange{Begin: idBegin, End: idEnd},
		}
	}
	ts, err := alloc.allocTimestamp(context.Background()) // TODO: dyh, resolve context
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
		RequestID:       task.GetRequestID(),
		TaskID:          task.GetTaskID(),
		CollectionID:    task.GetCollectionID(),
		Schema:          task.GetSchema(),
		Files:           importFiles,
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
	requestID int64,
	collectionID int64,
	schema *schemapb.CollectionSchema,
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
				RequestID:    requestID,
				TaskID:       idBegin + int64(i),
				CollectionID: collectionID,
				NodeID:       NullNodeID,
				State:        internalpb.ImportState_Pending,
				FileStats:    group,
			},
			schema: schema,
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // TODO: config
	defer cancel()
	_, err := cluster.AddImportSegment(ctx, req) // TODO: handle resp
	return err
}

func AreAllTasksFinished(tasks []ImportTask, meta *meta) bool {
	for _, task := range tasks {
		if task.GetState() != internalpb.ImportState_Completed {
			return false
		}
		segmentIDs := task.(*importTask).GetSegmentIDs()
		for _, segmentID := range segmentIDs {
			segment := meta.GetSegment(segmentID)
			if segment.GetIsImporting() {
				return false
			}
		}
	}
	return true
}

func GetImportProgress(requestID int64, imeta ImportMeta, meta *meta) (int64, internalpb.ImportState, string) {
	tasks := imeta.GetBy(WithReq(requestID), WithType(PreImportTaskType))
	var (
		preparingProgress float32 = 100
		preImportProgress float32 = 0
		importProgress    float32 = 0
	)
	totalTaskNum := len(imeta.GetBy(WithReq(requestID)))
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
	tasks = imeta.GetBy(WithReq(requestID), WithType(ImportTaskType))
	var (
		unsetImportStateSegments int = 0
		totalSegments            int = 0
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
	unsetImportStateProgress := 100 * float32(unsetImportStateSegments) / float32(totalSegments)
	progress := preparingProgress*0.1 + preImportProgress*0.4 + importProgress*0.4 + unsetImportStateProgress*0.1
	if progress == 100 {
		return 100, internalpb.ImportState_Completed, ""
	}
	return int64(progress), internalpb.ImportState_InProgress, ""
}
