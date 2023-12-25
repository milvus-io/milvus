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
	"sort"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
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
		func(fileStats *datapb.ImportFileStats, _ int) *datapb.ImportFile {
			return fileStats.GetImportFile()
		})
	pt := task.(*preImportTask)
	return &datapb.PreImportRequest{
		RequestID:    task.GetRequestID(),
		TaskID:       task.GetTaskID(),
		CollectionID: task.GetCollectionID(),
		PartitionIDs: pt.GetPartitionIDs(),
		Vchannels:    pt.GetVchannels(),
		Schema:       pt.GetSchema(),
		ImportFiles:  importFiles,
	}
}

func AssembleImportRequest(task ImportTask, manager *SegmentManager, alloc allocator, imeta ImportMeta) (*datapb.ImportRequest, error) {
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

	// alloc new segments
	segmentsInfo := make([]*datapb.ImportSegmentRequestInfo, 0)
	for vchannel, partitionRows := range hashedRows {
		for partitionID, rows := range partitionRows {
			for rows > 0 {
				segmentInfo, err := manager.openNewSegment(context.TODO(), task.GetCollectionID(), // TODO: dyh, fix context
					partitionID, vchannel, commonpb.SegmentState_Importing, datapb.SegmentLevel_L1)
				if err != nil {
					return nil, err
				}
				idBegin, idEnd, err := alloc.allocN(segmentInfo.GetMaxRowNum())
				if err != nil {
					return nil, err
				}
				segmentsInfo = append(segmentsInfo, &datapb.ImportSegmentRequestInfo{
					SegmentID:    segmentInfo.GetID(),
					PartitionID:  partitionID,
					Vchannel:     vchannel,
					AutoIDRanges: &datapb.AutoIDRange{Begin: idBegin, End: idEnd},
				})
				rows -= segmentInfo.GetMaxRowNum()
			}
		}
	}
	err := imeta.Update(task.GetTaskID(), UpdateSegmentIDs(lo.Map(segmentsInfo,
		func(info *datapb.ImportSegmentRequestInfo, _ int) int64 {
			return info.GetSegmentID()
		})))
	if err != nil {
		return nil, err
	}
	importFiles := lo.Map(task.GetFileStats(), func(fileStat *datapb.ImportFileStats, _ int) *datapb.ImportFile {
		return fileStat.GetImportFile()
	})
	return &datapb.ImportRequest{
		RequestID:    task.GetRequestID(),
		TaskID:       task.GetTaskID(),
		CollectionID: task.GetCollectionID(),
		Schema:       task.GetSchema(),
		Files:        importFiles,
		SegmentsInfo: segmentsInfo,
	}, nil
}

func AssembleImportTasks(preimportTasks []ImportTask, alloc allocator) ([]ImportTask, error) {
	if len(preimportTasks) == 0 {
		return nil, nil
	}
	pt := preimportTasks[0].(*preImportTask)
	files := lo.FlatMap(preimportTasks, func(t ImportTask, _ int) []*datapb.ImportFileStats {
		return t.(*preImportTask).GetFileStats()
	})
	maxRowsPerSegment, err := calBySchemaPolicy(pt.GetSchema())
	if err != nil {
		return nil, err
	}
	chunkMaxRows := maxRowsPerSegment * len(pt.GetPartitionIDs()) * len(pt.GetVchannels())

	chunks := make([][]*datapb.ImportFileStats, 0)
	currentChunk := make([]*datapb.ImportFileStats, 0)
	currentSum := 0
	sort.Slice(files, func(i, j int) bool {
		return files[i].GetTotalRows() < files[j].GetTotalRows()
	})
	for _, file := range files {
		rows := int(file.GetTotalRows())
		if rows > chunkMaxRows {
			chunks = append(chunks, []*datapb.ImportFileStats{file})
		} else if currentSum+rows <= chunkMaxRows {
			currentChunk = append(currentChunk, file)
			currentSum += rows
		} else {
			chunks = append(chunks, currentChunk)
			currentChunk = []*datapb.ImportFileStats{file}
			currentSum = rows
		}
	}
	if len(currentChunk) > 0 {
		chunks = append(chunks, currentChunk)
	}

	idBegin, _, err := alloc.allocN(int64(len(chunks)))
	if err != nil {
		return nil, err
	}
	tasks := make([]ImportTask, 0, len(chunks))
	for i, chunk := range chunks {
		task := &importTask{
			ImportTaskV2: &datapb.ImportTaskV2{
				RequestID:    pt.GetRequestID(),
				TaskID:       idBegin + int64(i),
				CollectionID: pt.GetCollectionID(),
				SegmentIDs:   nil,
				NodeID:       NullNodeID,
				State:        datapb.ImportState_Pending,
				FileStats:    chunk,
			},
			schema: pt.GetSchema(),
		}
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
		if task.GetState() != datapb.ImportState_Completed {
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
