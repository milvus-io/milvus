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
	"github.com/samber/lo"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	alloc "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

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
	segmentAutoIDRanges := make(map[int64]*datapb.AutoIDRange) // TODO: dyh, check if enable
	segmentChannels := make(map[int64]string)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // TODO: dyh, move to config
	defer cancel()
	for _, file := range task.GetFileStats() {
		for vchannel, rows := range file.GetChannelRows() {
			for rows > 0 {
				segmentInfo, err := manager.openNewSegment(ctx, task.GetCollectionID(),
					task.GetPartitionID(), vchannel, commonpb.SegmentState_Importing, datapb.SegmentLevel_L1)
				if err != nil {
					return nil, err // TODO: dyh, txn?
				}
				rows -= segmentInfo.GetMaxRowNum()
				idBegin, idEnd, err := idAlloc.Alloc(uint32(segmentInfo.GetMaxRowNum()))
				if err != nil {
					return nil, err
				}
				segmentAutoIDRanges[segmentInfo.ID] = &datapb.AutoIDRange{Begin: idBegin, End: idEnd}
				segmentChannels[segmentInfo.ID] = segmentInfo.InsertChannel
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
	_, err := cluster.AddImportSegment(context.TODO(), req) // TODO: handle resp
	return err
}
