package queryservice

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
)

type masterMock struct {
	collectionIDs     []UniqueID
	col2partition     map[UniqueID][]UniqueID
	partition2segment map[UniqueID][]UniqueID
}

func newMasterMock() *masterMock {
	collectionIDs := make([]UniqueID, 0)
	collectionIDs = append(collectionIDs, 1)

	col2partition := make(map[UniqueID][]UniqueID)
	partitionIDs := make([]UniqueID, 0)
	partitionIDs = append(partitionIDs, 1)
	col2partition[1] = partitionIDs

	partition2segment := make(map[UniqueID][]UniqueID)
	segmentIDs := make([]UniqueID, 0)
	segmentIDs = append(segmentIDs, 1)
	segmentIDs = append(segmentIDs, 2)
	segmentIDs = append(segmentIDs, 3)
	segmentIDs = append(segmentIDs, 4)
	segmentIDs = append(segmentIDs, 5)
	segmentIDs = append(segmentIDs, 6)
	partition2segment[1] = segmentIDs

	return &masterMock{
		collectionIDs:     collectionIDs,
		col2partition:     col2partition,
		partition2segment: partition2segment,
	}
}

func (master *masterMock) ShowPartitions(in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error) {
	collectionID := in.CollectionID
	partitionIDs := make([]UniqueID, 0)
	for _, id := range master.collectionIDs {
		if id == collectionID {
			partitions := master.col2partition[collectionID]
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

func (master *masterMock) ShowSegments(in *milvuspb.ShowSegmentRequest) (*milvuspb.ShowSegmentResponse, error) {
	collectionID := in.CollectionID
	partitionID := in.PartitionID

	for _, id := range master.collectionIDs {
		if id == collectionID {
			partitions := master.col2partition[collectionID]
			for _, partition := range partitions {
				if partition == partitionID {
					return &milvuspb.ShowSegmentResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_SUCCESS,
						},
						SegmentIDs: master.partition2segment[partition],
					}, nil
				}
			}
		}
	}

	return nil, errors.New("collection id or partition id not found")
}

type dataMock struct {
	segmentIDs    []UniqueID
	segmentStates map[UniqueID]*datapb.SegmentStateInfo
}

func newDataMock() *dataMock {
	positions1 := make([]*internalpb2.MsgPosition, 0)
	positions2 := make([]*internalpb2.MsgPosition, 0)
	positions1 = append(positions1, &internalpb2.MsgPosition{ChannelName: "insertChannel-" + strconv.FormatInt(1, 10)})
	positions1 = append(positions1, &internalpb2.MsgPosition{ChannelName: "insertChannel-" + strconv.FormatInt(2, 10)})
	positions2 = append(positions2, &internalpb2.MsgPosition{ChannelName: "insertChannel-" + strconv.FormatInt(3, 10)})
	positions2 = append(positions2, &internalpb2.MsgPosition{ChannelName: "insertChannel-" + strconv.FormatInt(4, 10)})

	segmentIDs := make([]UniqueID, 0)
	segmentIDs = append(segmentIDs, 1)
	segmentIDs = append(segmentIDs, 2)
	segmentIDs = append(segmentIDs, 3)
	segmentIDs = append(segmentIDs, 4)
	segmentIDs = append(segmentIDs, 5)
	segmentIDs = append(segmentIDs, 6)

	fillStates := func(segmentID UniqueID, time uint64, position []*internalpb2.MsgPosition, state datapb.SegmentState) *datapb.SegmentStateInfo {
		return &datapb.SegmentStateInfo{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_SUCCESS,
			},
			SegmentID:      segmentID,
			State:          state,
			CreateTime:     time,
			StartPositions: position,
		}
	}
	segmentStates := make(map[UniqueID]*datapb.SegmentStateInfo)
	segmentStates[1] = fillStates(1, 1, positions1, datapb.SegmentState_SegmentFlushed)
	segmentStates[2] = fillStates(2, 2, positions2, datapb.SegmentState_SegmentFlushed)
	segmentStates[3] = fillStates(3, 3, positions1, datapb.SegmentState_SegmentFlushed)
	segmentStates[4] = fillStates(4, 4, positions2, datapb.SegmentState_SegmentFlushed)
	segmentStates[5] = fillStates(5, 5, positions1, datapb.SegmentState_SegmentGrowing)
	segmentStates[6] = fillStates(6, 6, positions2, datapb.SegmentState_SegmentGrowing)

	return &dataMock{
		segmentIDs:    segmentIDs,
		segmentStates: segmentStates,
	}
}

func (data *dataMock) GetSegmentStates(req *datapb.SegmentStatesRequest) (*datapb.SegmentStatesResponse, error) {
	ret := &datapb.SegmentStatesResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
		},
	}
	for _, segID := range req.SegmentIDs {
		for _, segmentID := range data.segmentIDs {
			if segmentID == segID {
				ret.States = append(ret.States, data.segmentStates[segmentID])
			}
		}
	}

	if ret.States == nil {
		return nil, errors.New("segment id not found")
	}

	return ret, nil
}

func TestQueryService_Init(t *testing.T) {
	service, err := NewQueryService(context.Background())
	assert.Nil(t, err)
	service.Init()
	service.Start()

	t.Run("Test create channel", func(t *testing.T) {
		response, err := service.CreateQueryChannel()
		assert.Nil(t, err)
		assert.Equal(t, response.RequestChannel, "query-0")
		assert.Equal(t, response.ResultChannel, "queryResult-0")
	})

	t.Run("Test Get statistics channel", func(t *testing.T) {
		response, err := service.GetStatisticsChannel()
		assert.Nil(t, err)
		assert.Equal(t, response, "query-node-stats")
	})

	t.Run("Test Get timeTick channel", func(t *testing.T) {
		response, err := service.GetTimeTickChannel()
		assert.Nil(t, err)
		assert.Equal(t, response, "queryTimeTick")
	})

	service.Stop()
}

func TestQueryService_load(t *testing.T) {
	service, err := NewQueryService(context.Background())
	assert.Nil(t, err)
	service.Init()
	service.Start()
	service.SetMasterService(newMasterMock())
	service.SetDataService(newDataMock())
	registerNodeRequest := &querypb.RegisterNodeRequest{
		Address: &commonpb.Address{},
	}
	service.RegisterNode(registerNodeRequest)

	t.Run("Test LoadSegment", func(t *testing.T) {
		loadCollectionRequest := &querypb.LoadCollectionRequest{
			CollectionID: 1,
		}
		response, err := service.LoadCollection(loadCollectionRequest)
		assert.Nil(t, err)
		assert.Equal(t, response.ErrorCode, commonpb.ErrorCode_SUCCESS)
	})

	t.Run("Test LoadPartition", func(t *testing.T) {
		loadPartitionRequest := &querypb.LoadPartitionRequest{
			CollectionID: 1,
			PartitionIDs: []UniqueID{1},
		}
		response, err := service.LoadPartitions(loadPartitionRequest)
		assert.Nil(t, err)
		assert.Equal(t, response.ErrorCode, commonpb.ErrorCode_SUCCESS)
	})
}
