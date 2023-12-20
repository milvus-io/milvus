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
	alloc "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"time"
)

func WrapLogFields(task ImportTask, err error) []zap.Field {
	fields := []zap.Field{
		zap.Int64("taskID", task.GetTaskID()),
		zap.Int64("requestID", task.GetRequestID()),
		zap.Int64("collectionID", task.GetCollectionID()),
		zap.Int64("partitionID", task.GetPartitionID()),
		zap.Int64("nodeID", task.GetNodeID()),
		zap.String("state", task.GetState().String()),
		zap.String("type", task.GetType().String()),
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	return fields
}

func AssemblePreImportRequest(task ImportTask, meta *meta) *datapb.PreImportRequest {
	collection := meta.GetCollection(task.GetCollectionID())
	importFiles := lo.Map(task.(*preImportTask).GetFileStats(),
		func(fileStats *datapb.ImportFileStats, _ int) *datapb.ImportFile {
			return fileStats.GetImportFile()
		})
	return &datapb.PreImportRequest{
		RequestID:    task.GetRequestID(),
		TaskID:       task.GetTaskID(),
		CollectionID: task.GetCollectionID(),
		PartitionID:  task.GetPartitionID(),
		Schema:       collection.Schema,
		ImportFiles:  importFiles,
	}
}

func AssembleImportRequest(task ImportTask, manager *SegmentManager, meta *meta, idAlloc *alloc.IDAllocator) (*datapb.ImportRequest, error) {
	collection := meta.GetCollection(task.GetCollectionID())
	segmentAutoIDRanges := make(map[int64]*datapb.AutoIDRange)
	segmentChannels := make(map[int64]string)
	for _, file := range task.GetFileStats() {
		for vchannel, rows := range file.GetChannelRows() {
			for rows > 0 {
				segmentInfo, err := manager.openNewSegment(context.TODO(), task.GetCollectionID(),
					task.GetPartitionID(), vchannel, commonpb.SegmentState_Importing, datapb.SegmentLevel_L1)
				if err != nil {
					return nil, err
				}
				idBegin, idEnd, err := idAlloc.Alloc(uint32(segmentInfo.GetMaxRowNum()))
				if err != nil {
					return nil, err
				}
				segmentAutoIDRanges[segmentInfo.ID] = &datapb.AutoIDRange{Begin: idBegin, End: idEnd}
				segmentChannels[segmentInfo.ID] = segmentInfo.InsertChannel
				rows -= segmentInfo.GetMaxRowNum()
			}
		}
	}
	importFiles := lo.Map(task.GetFileStats(), func(fileStat *datapb.ImportFileStats, _ int) *datapb.ImportFile {
		return fileStat.GetImportFile()
	})
	return &datapb.ImportRequest{
		RequestID:       task.GetRequestID(),
		TaskID:          task.GetTaskID(),
		CollectionID:    task.GetCollectionID(),
		PartitionID:     task.GetPartitionID(),
		Schema:          collection.Schema,
		ImportFiles:     importFiles,
		AutoIDRanges:    segmentAutoIDRanges,
		SegmentChannels: segmentChannels,
	}, nil
}

func AssembleImportTask(task ImportTask, allocator *alloc.IDAllocator) (*importTask, error) {
	taskID, err := allocator.AllocOne()
	if err != nil {
		return nil, err
	}
	return &importTask{
		&datapb.ImportTaskV2{
			RequestID:    task.GetRequestID(),
			TaskID:       taskID,
			CollectionID: task.GetCollectionID(),
			PartitionID:  task.GetPartitionID(),
			State:        datapb.ImportState_Pending,
			FileStats:    task.GetFileStats(),
		},
	}, nil
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
