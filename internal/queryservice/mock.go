package queryservice

import (
	"context"
	"errors"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
)

const (
	numSegment = 12
)

type MasterMock struct {
	CollectionIDs     []UniqueID
	Col2partition     map[UniqueID][]UniqueID
	Partition2segment map[UniqueID][]UniqueID
}

func NewMasterMock() *MasterMock {
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

	return &MasterMock{
		CollectionIDs:     collectionIDs,
		Col2partition:     col2partition,
		Partition2segment: partition2segment,
	}
}

func (master *MasterMock) ShowPartitions(ctx context.Context, in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	collectionID := in.CollectionID
	partitionIDs := make([]UniqueID, 0)
	for _, id := range master.CollectionIDs {
		if id == collectionID {
			partitions := master.Col2partition[collectionID]
			partitionIDs = append(partitionIDs, partitions...)
		}
	}
	response := &milvuspb.ShowPartitionResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		PartitionIDs: partitionIDs,
	}

	return response, nil
}

func (master *MasterMock) ShowSegments(ctx context.Context, in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	collectionID := in.CollectionID
	partitionID := in.PartitionID

	for _, id := range master.CollectionIDs {
		if id == collectionID {
			partitions := master.Col2partition[collectionID]
			for _, partition := range partitions {
				if partition == partitionID {
					return &milvuspb.ShowSegmentResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_SUCCESS,
						},
						//SegmentIDs: master.Partition2segment[partition],
					}, nil
				}
			}
		}
	}

	return nil, errors.New("collection id or partition id not found")
}

type DataMock struct {
	SegmentIDs    []UniqueID
	SegmentStates map[UniqueID]*datapb.SegmentStateInfo
}

func NewDataMock() *DataMock {
	positions := make([]*internalpb2.MsgPosition, 0)
	positions = append(positions, &internalpb2.MsgPosition{ChannelName: "insert-" + strconv.FormatInt(0, 10)})
	positions = append(positions, &internalpb2.MsgPosition{ChannelName: "insert-" + strconv.FormatInt(1, 10)})
	positions = append(positions, &internalpb2.MsgPosition{ChannelName: "insert-" + strconv.FormatInt(2, 10)})
	positions = append(positions, &internalpb2.MsgPosition{ChannelName: "insert-" + strconv.FormatInt(3, 10)})

	fillStates := func(segmentID UniqueID, time uint64, position *internalpb2.MsgPosition) *datapb.SegmentStateInfo {
		return &datapb.SegmentStateInfo{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_SUCCESS,
			},
			SegmentID:     segmentID,
			State:         commonpb.SegmentState_SegmentFlushed,
			CreateTime:    time,
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

func (data *DataMock) GetSegmentStates(ctx context.Context, req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	ret := &datapb.SegmentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
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
func (data *DataMock) GetInsertChannels(ctx context.Context, req *datapb.InsertChannelRequest) (*internalpb2.StringList, error) {
	return &internalpb2.StringList{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
		Values: []string{"insert-0", "insert-1", "insert-2", "insert-3"},
	}, nil
}
