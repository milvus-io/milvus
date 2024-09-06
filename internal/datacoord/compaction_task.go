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
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type CompactionTask interface {
	Process() bool
	BuildCompactionRequest() (*datapb.CompactionPlan, error)

	GetTriggerID() UniqueID
	GetPlanID() UniqueID
	GetState() datapb.CompactionTaskState
	GetChannel() string
	GetLabel() string

	GetType() datapb.CompactionType
	GetCollectionID() int64
	GetPartitionID() int64
	GetInputSegments() []int64
	GetStartTime() int64
	GetTimeoutInSeconds() int32
	GetPos() *msgpb.MsgPosition

	GetPlan() *datapb.CompactionPlan
	GetResult() *datapb.CompactionPlanResult

	GetNodeID() UniqueID
	GetSpan() trace.Span
	ShadowClone(opts ...compactionTaskOpt) *datapb.CompactionTask
	SetNodeID(UniqueID) error
	SetTask(*datapb.CompactionTask)
	SetSpan(trace.Span)
	SetResult(*datapb.CompactionPlanResult)
	EndSpan()
	CleanLogPath()
	NeedReAssignNodeID() bool
	SaveTaskMeta() error
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
