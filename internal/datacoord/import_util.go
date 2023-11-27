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
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	alloc "github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func IsPreImportDone(task ImportTask) bool {
	for _, info := range task.FileInfos() {
		if len(info.GetFileSize()) != len(info.GetFileName()) {
			return false
		}
		if info.GetTotalRows() < 0 {
			return false
		}
	}
	return true
}

func IsImportDone(task ImportTask) bool {
	for _, info := range task.FileInfos() {
		if info.GetImportedRows() != info.GetTotalRows() {
			return false
		}
	}
	return true
}

func AssemblePreImportRequest(task ImportTask) *datapb.PreImportRequest {
	return &datapb.PreImportRequest{
		RequestID:    task.ReqID(),
		TaskID:       task.ID(),
		CollectionID: task.CollectionID(),
		PartitionID:  task.PartitionID(),
		Schema:       task.Schema(),
		FileInfos:    task.FileInfos(),
	}
}

func AssembleImportRequest(task ImportTask, manager *SegmentManager, idAlloc *alloc.IDAllocator) (*datapb.ImportRequest, error) {
	segmentInfos := make([]*datapb.SegmentInfo, 0)
	autoIDs := make([]int64, 0)                                              // TODO: check if enable
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // TODO: move to config
	defer cancel()
	for _, info := range task.FileInfos() {
		for vchannel, rows := range info.GetChannelRows() {
			for rows > 0 {
				segmentInfo, err := manager.openNewSegment(ctx, task.CollectionID(), task.PartitionID(), vchannel, commonpb.SegmentState_Importing, datapb.SegmentLevel_L1)
				if err != nil {
					return nil, err
				}
				rows -= segmentInfo.GetMaxRowNum()
				idBegin, idEnd, err := idAlloc.Alloc(uint32(segmentInfo.GetMaxRowNum()))
				if err != nil {
					return nil, err
				}
				segmentInfos = append(segmentInfos, segmentInfo.SegmentInfo)
				autoIDs = append(autoIDs, idBegin, idEnd)
			}
		}
	}
	return &datapb.ImportRequest{
		RequestID:    task.ReqID(),
		TaskID:       task.ID(),
		CollectionID: task.CollectionID(),
		PartitionID:  task.PartitionID(),
		Schema:       task.Schema(),
		SegmentInfos: segmentInfos,
		AutoIDs:      autoIDs,
		FileInfos:    task.FileInfos(),
	}, nil
}

func AddImportSegment(cluster *Cluster, meta *meta, segmentID int64) error {
	segment := meta.GetSegment(segmentID)
	ok, nodeID := cluster.channelManager.getNodeIDByChannelName(segment.GetInsertChannel())
	if !ok {
		return merr.WrapErrChannelNotFound(segment.GetInsertChannel(), "no DataNode watches this channel")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // TODO: move to config
	defer cancel()
	cli, err := cluster.sessionManager.getClient(ctx, nodeID)
	if err != nil {
		return err
	}
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
	_, err = cli.AddImportSegment(ctx, req) // TODO: handle resp
	return err
}
