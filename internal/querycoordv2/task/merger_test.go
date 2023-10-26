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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	paramtable.Init()
	paramtable.Get().Save(Params.QueryCoordCfg.TaskMergeCap.Key, "3")
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
			DeltaPositions: []*msgpb.MsgPosition{
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
			DeltaPositions: []*msgpb.MsgPosition{
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
			DeltaPositions: []*msgpb.MsgPosition{
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
		task, err := NewSegmentTask(ctx, timeout, WrapIDSource(0), suite.collectionID, suite.replicaID,
			NewSegmentAction(suite.nodeID, ActionTypeGrow, "", segmentID))
		suite.NoError(err)
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
	suite.merger.Stop()
	_, ok := <-suite.merger.Chan()
	suite.Equal(ok, false)
}

func TestMerger(t *testing.T) {
	suite.Run(t, new(MergerSuite))
}
