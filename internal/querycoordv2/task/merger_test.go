package task

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/stretchr/testify/suite"
)

type MergerSuite struct {
	suite.Suite
	// Data
	collectionID int64
	replicaID    int64
	nodeID       int64
	requests     map[int64]*querypb.LoadSegmentsRequest

	merger *Merger[segmentIndex, *querypb.LoadSegmentsRequest]
}

func (suite *MergerSuite) SetupSuite() {
	suite.collectionID = 1000
	suite.replicaID = 100
	suite.nodeID = 1
	suite.requests = map[int64]*querypb.LoadSegmentsRequest{
		1: {
			DstNodeID:    suite.nodeID,
			CollectionID: suite.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     1,
					InsertChannel: "dmc0",
				},
			},
			DeltaPositions: []*internalpb.MsgPosition{
				{
					ChannelName: "dmc0",
					Timestamp:   2,
				},
				{
					ChannelName: "dmc1",
					Timestamp:   3,
				},
			},
		},
		2: {
			DstNodeID:    suite.nodeID,
			CollectionID: suite.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     2,
					InsertChannel: "dmc0",
				},
			},
			DeltaPositions: []*internalpb.MsgPosition{
				{
					ChannelName: "dmc0",
					Timestamp:   3,
				},
				{
					ChannelName: "dmc1",
					Timestamp:   2,
				},
			},
		},
		3: {
			DstNodeID:    suite.nodeID,
			CollectionID: suite.collectionID,
			Infos: []*querypb.SegmentLoadInfo{
				{
					SegmentID:     3,
					InsertChannel: "dmc0",
				},
			},
			DeltaPositions: []*internalpb.MsgPosition{
				{
					ChannelName: "dmc0",
					Timestamp:   1,
				},
				{
					ChannelName: "dmc1",
					Timestamp:   1,
				},
			},
		},
	}
}

func (suite *MergerSuite) SetupTest() {
	suite.merger = NewMerger[segmentIndex, *querypb.LoadSegmentsRequest]()
}

func (suite *MergerSuite) TestMerge() {
	const (
		requestNum = 5
		timeout    = 5 * time.Second
	)
	ctx := context.Background()

	for segmentID := int64(1); segmentID <= 3; segmentID++ {
		task := NewSegmentTask(ctx, timeout, 0, suite.collectionID, suite.replicaID,
			NewSegmentAction(suite.nodeID, ActionTypeGrow, segmentID))

		suite.merger.Add(NewLoadSegmentsTask(task, 0, suite.requests[segmentID]))
	}

	suite.merger.Start(ctx)
	defer suite.merger.Stop()
	taskI := <-suite.merger.Chan()
	task := taskI.(*LoadSegmentsTask)
	suite.Len(task.tasks, 3)
	suite.Len(task.steps, 3)
	suite.EqualValues(1, task.Result().DeltaPositions[0].Timestamp)
	suite.EqualValues(1, task.Result().DeltaPositions[1].Timestamp)
}

func TestMerger(t *testing.T) {
	suite.Run(t, new(MergerSuite))
}
