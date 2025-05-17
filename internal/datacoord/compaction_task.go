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
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type CompactionTask interface {
	task.Task
	// Process performs the task's state machine
	//
	// Returns:
	//   - <bool>:  whether the task state machine ends.
	//
	// Notes:
	//
	//	`end` doesn't mean the task completed, its state may be completed or failed or timeout.
	Process() bool
	// Clean performs clean logic for a fail/timeout task
	Clean() bool
	BuildCompactionRequest() (*datapb.CompactionPlan, error)
	GetSlotUsage() int64
	GetLabel() string

	SetTask(*datapb.CompactionTask)
	GetTaskProto() *datapb.CompactionTask
	ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask

	SetNodeID(UniqueID) error
	NeedReAssignNodeID() bool
	SaveTaskMeta() error

	PreparePlan() bool
	CheckCompactionContainsSegment(segmentID int64) bool
}

type compactionTaskOpt func(task *datapb.CompactionTask)

func setNodeID(nodeID int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.NodeID = nodeID
	}
}

func setFailReason(reason string) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.FailReason = reason
	}
}

func setEndTime(endTime int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.EndTime = endTime
	}
}

func setTimeoutInSeconds(dur int32) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.TimeoutInSeconds = dur
	}
}

func setResultSegments(segments []int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.ResultSegments = segments
	}
}

func setTmpSegments(segments []int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.TmpSegments = segments
	}
}

func setState(state datapb.CompactionTaskState) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.State = state
	}
}

func setStartTime(startTime int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.StartTime = startTime
	}
}

func setRetryTimes(retryTimes int32) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.RetryTimes = retryTimes
	}
}

func setLastStateStartTime(lastStateStartTime int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.LastStateStartTime = lastStateStartTime
	}
}

func setAnalyzeTaskID(id int64) compactionTaskOpt {
	return func(task *datapb.CompactionTask) {
		task.AnalyzeTaskID = id
	}
}
