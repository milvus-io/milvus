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
	ProcessTask(*compactionPlanHandler) error
	BuildCompactionRequest(*compactionPlanHandler) (*datapb.CompactionPlan, error)

	GetTriggerID() int64
	GetPlanID() int64
	GetState() datapb.CompactionTaskState
	GetChannel() string
	GetType() datapb.CompactionType
	GetCollectionID() int64
	GetPartitionID() int64
	GetInputSegments() []int64
	GetStartTime() uint64
	GetTimeoutInSeconds() int32
	GetPos() *msgpb.MsgPosition

	GetPlan() *datapb.CompactionPlan
	GetResult() *datapb.CompactionPlanResult
	GetNodeID() int64
	GetSpan() trace.Span

	ShadowClone(opts ...compactionTaskOpt) CompactionTask
	SetNodeID(int64)
	SetState(datapb.CompactionTaskState)
	SetTask(*datapb.CompactionTask)
	SetSpan(trace.Span)
	SetPlan(*datapb.CompactionPlan)
	SetStartTime(startTime uint64)
	SetResult(*datapb.CompactionPlanResult)
	EndSpan()
	CleanLogPath()
}

type compactionTaskOpt func(task CompactionTask)

func setNodeID(nodeID int64) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetNodeID(nodeID)
	}
}

func setPlan(plan *datapb.CompactionPlan) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetPlan(plan)
	}
}

func setState(state datapb.CompactionTaskState) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetState(state)
	}
}

func setTask(ctask *datapb.CompactionTask) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetTask(ctask)
	}
}

func endSpan() compactionTaskOpt {
	return func(task CompactionTask) {
		task.EndSpan()
	}
}

func setStartTime(startTime uint64) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetStartTime(startTime)
	}
}

func setResult(result *datapb.CompactionPlanResult) compactionTaskOpt {
	return func(task CompactionTask) {
		task.SetResult(result)
	}
}

// cleanLogPath clean the log info in the defaultCompactionTask object for avoiding the memory leak
func cleanLogPath() compactionTaskOpt {
	return func(task CompactionTask) {
		task.CleanLogPath()
	}
}
