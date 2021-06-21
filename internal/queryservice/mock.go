// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package queryservice

import (
	"context"
	"errors"
	"strconv"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

const (
	numSegment = 12
)

type RootCoordMock struct {
	types.RootCoord
	CollectionIDs     []UniqueID
	Col2partition     map[UniqueID][]UniqueID
	Partition2segment map[UniqueID][]UniqueID
}

func NewRootCoordMock() *RootCoordMock {
	collectionIDs := make([]UniqueID, 0)
	collectionIDs = append(collectionIDs, 1)

	col2partition := make(map[UniqueID][]UniqueID)
	partitionIDs := make([]UniqueID, 0)
	partitionIDs = append(partitionIDs, 1)
	col2partition[1] = partitionIDs

	partition2segment := make(map[UniqueID][]UniqueID)
	segmentIDs := make([]UniqueID, 0)
	for i := 0; i < numSegment; i++ {
		segmentIDs = append(segmentIDs, UniqueID(i))
	}
	partition2segment[1] = segmentIDs

	return &RootCoordMock{
		CollectionIDs:     collectionIDs,
		Col2partition:     col2partition,
		Partition2segment: partition2segment,
	}
}

func (rc *RootCoordMock) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
	collectionID := in.CollectionID
	partitionIDs := make([]UniqueID, 0)
	for _, id := range rc.CollectionIDs {
		if id == collectionID {
			partitions := rc.Col2partition[collectionID]
			partitionIDs = append(partitionIDs, partitions...)
		}
	}
	response := &milvuspb.ShowPartitionsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		PartitionIDs: partitionIDs,
	}

	return response, nil
}

func (rc *RootCoordMock) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentsRequest) (*milvuspb.ShowSegmentsResponse, error) {
	collectionID := in.CollectionID
	partitionID := in.PartitionID

	for _, id := range rc.CollectionIDs {
		if id == collectionID {
			partitions := rc.Col2partition[collectionID]
			for _, partition := range partitions {
				if partition == partitionID {
					return &milvuspb.ShowSegmentsResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
						},
						//SegmentIDs: rc.Partition2segment[partition],
					}, nil
				}
			}
		}
	}

	return nil, errors.New("collection id or partition id not found")
}

type DataMock struct {
	types.DataCoord
	SegmentIDs    []UniqueID
	SegmentStates map[UniqueID]*datapb.SegmentStateInfo
}

func NewDataMock() *DataMock {
	positions := make([]*internalpb.MsgPosition, 0)
	positions = append(positions, &internalpb.MsgPosition{ChannelName: "insert-" + strconv.FormatInt(0, 10)})
	positions = append(positions, &internalpb.MsgPosition{ChannelName: "insert-" + strconv.FormatInt(1, 10)})
	positions = append(positions, &internalpb.MsgPosition{ChannelName: "insert-" + strconv.FormatInt(2, 10)})
	positions = append(positions, &internalpb.MsgPosition{ChannelName: "insert-" + strconv.FormatInt(3, 10)})

	fillStates := func(segmentID UniqueID, time uint64, position *internalpb.MsgPosition) *datapb.SegmentStateInfo {
		return &datapb.SegmentStateInfo{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
			SegmentID:     segmentID,
			State:         commonpb.SegmentState_Flushed,
			StartPosition: position,
		}
	}
	segmentStates := make(map[UniqueID]*datapb.SegmentStateInfo)
	segmentIDs := make([]UniqueID, 0)
	for i := 0; i < numSegment; i++ {
		segmentIDs = append(segmentIDs, UniqueID(i))
		pick := i % 4
		segmentStates[UniqueID(i)] = fillStates(UniqueID(i), uint64(i), positions[pick])
	}

	return &DataMock{
		SegmentIDs:    segmentIDs,
		SegmentStates: segmentStates,
	}
}

func (data *DataMock) GetSegmentStates(ctx context.Context, req *datapb.GetSegmentStatesRequest) (*datapb.GetSegmentStatesResponse, error) {
	ret := &datapb.GetSegmentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
	}
	for _, segID := range req.SegmentIDs {
		for _, segmentID := range data.SegmentIDs {
			if segmentID == segID {
				ret.States = append(ret.States, data.SegmentStates[segmentID])
			}
		}
	}

	if ret.States == nil {
		return ret, nil
	}

	return ret, nil
}
