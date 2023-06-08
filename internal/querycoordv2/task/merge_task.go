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
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
)

type MergeableTask[K comparable, R any] interface {
	ID() K
	Merge(other MergeableTask[K, R])
}

var _ MergeableTask[segmentIndex, *querypb.LoadSegmentsRequest] = (*LoadSegmentsTask)(nil)

type segmentIndex struct {
	NodeID       int64
	CollectionID int64
	Shard        string
}

type LoadSegmentsTask struct {
	tasks []*SegmentTask
	steps []int
	req   *querypb.LoadSegmentsRequest
}

func NewLoadSegmentsTask(task *SegmentTask, step int, req *querypb.LoadSegmentsRequest) *LoadSegmentsTask {
	return &LoadSegmentsTask{
		tasks: []*SegmentTask{task},
		steps: []int{step},
		req:   req,
	}
}

func (task *LoadSegmentsTask) ID() segmentIndex {
	return segmentIndex{
		NodeID:       task.req.GetDstNodeID(),
		CollectionID: task.req.GetCollectionID(),
		Shard:        task.req.GetInfos()[0].GetInsertChannel(),
	}
}

func (task *LoadSegmentsTask) Merge(other MergeableTask[segmentIndex, *querypb.LoadSegmentsRequest]) {
	otherTask := other.(*LoadSegmentsTask)
	task.tasks = append(task.tasks, otherTask.tasks...)
	task.steps = append(task.steps, otherTask.steps...)
	task.req.Infos = append(task.req.Infos, otherTask.req.GetInfos()...)
	positions := make(map[string]*msgpb.MsgPosition)
	for _, position := range task.req.DeltaPositions {
		positions[position.GetChannelName()] = position
	}
	for _, position := range otherTask.req.GetDeltaPositions() {
		merged, ok := positions[position.GetChannelName()]
		if !ok || merged.GetTimestamp() > position.GetTimestamp() {
			merged = position
		}
		positions[position.GetChannelName()] = merged
	}
	task.req.DeltaPositions = make([]*msgpb.MsgPosition, 0, len(positions))
	for _, position := range positions {
		task.req.DeltaPositions = append(task.req.DeltaPositions, position)
	}
}

func (task *LoadSegmentsTask) Result() *querypb.LoadSegmentsRequest {
	return task.req
}
