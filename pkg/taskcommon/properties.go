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

package taskcommon

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// properties keys
const (
	// request
	ClusterIDKey   = "cluster_id"
	TaskIDKey      = "task_id"
	TypeKey        = "task_type"
	SubTypeKey     = "task_sub_type" // optional, only for Stats
	SlotKey        = "task_slot"
	NumRowsKey     = "num_row"      // optional, only for Index, Stats
	TaskVersionKey = "task_version" // optional, only for Index, Stats and Analyze

	// result
	StateKey  = "task_state"
	ReasonKey = "task_reason"
)

type Properties map[string]string

func NewProperties(properties map[string]string) Properties {
	if properties == nil {
		properties = map[string]string{}
	}
	return properties
}

func WrapErrTaskPropertyLack(lackProperty string, taskID any) error {
	return fmt.Errorf("cannot find property '%s' for task '%v'", lackProperty, taskID)
}

func (p Properties) AppendClusterID(clusterID string) {
	p[ClusterIDKey] = clusterID
}

func (p Properties) AppendTaskID(taskID int64) {
	p[TaskIDKey] = fmt.Sprintf("%d", taskID)
}

func (p Properties) AppendType(t Type) {
	switch t {
	case QuerySlot, PreImport, Import, Compaction, Index, Stats, Analyze:
		p[TypeKey] = t
	default:
		p[TypeKey] = TypeNone
	}
}

func (p Properties) AppendSubType(subType string) {
	p[SubTypeKey] = subType
}

func (p Properties) AppendTaskSlot(slot int64) {
	p[SlotKey] = fmt.Sprintf("%d", slot)
}

func (p Properties) AppendNumRows(rows int64) {
	p[NumRowsKey] = fmt.Sprintf("%d", rows)
}

func (p Properties) AppendTaskVersion(version int64) {
	p[TaskVersionKey] = fmt.Sprintf("%d", version)
}

func (p Properties) AppendReason(reason string) {
	p[ReasonKey] = reason
}

func (p Properties) AppendTaskState(state State) {
	p[StateKey] = state.String()
}

func (p Properties) GetTaskType() (Type, error) {
	if _, ok := p[TypeKey]; !ok {
		return "", WrapErrTaskPropertyLack(TypeKey, p[TaskIDKey])
	}
	switch p[TypeKey] {
	case QuerySlot, PreImport, Import, Compaction, Index, Stats, Analyze:
		return p[TypeKey], nil
	default:
		return p[TypeKey], fmt.Errorf("unrecognized task type '%s', taskID=%s", p[TypeKey], p[TaskIDKey])
	}
}

func (p Properties) GetJobType() (indexpb.JobType, error) {
	taskType, err := p.GetTaskType()
	if err != nil {
		return indexpb.JobType_JobTypeNone, err
	}
	switch taskType {
	case Index:
		return indexpb.JobType_JobTypeIndexJob, nil
	case Stats:
		return indexpb.JobType_JobTypeStatsJob, nil
	case Analyze:
		return indexpb.JobType_JobTypeAnalyzeJob, nil
	default:
		return indexpb.JobType_JobTypeNone, nil
	}
}

func (p Properties) GetSubTaskType() string {
	return p[SubTypeKey]
}

func (p Properties) GetClusterID() (string, error) {
	if _, ok := p[ClusterIDKey]; !ok {
		return "", WrapErrTaskPropertyLack(ClusterIDKey, p[TaskIDKey])
	}
	return p[ClusterIDKey], nil
}

func (p Properties) GetTaskID() (int64, error) {
	if _, ok := p[TaskIDKey]; !ok {
		return 0, WrapErrTaskPropertyLack(TaskIDKey, 0)
	}
	return strconv.ParseInt(p[TaskIDKey], 10, 64)
}

func (p Properties) GetTaskState() (State, error) {
	if _, ok := p[StateKey]; !ok {
		return 0, WrapErrTaskPropertyLack(StateKey, p[TaskIDKey])
	}
	stateStr := p[StateKey]
	if _, ok := indexpb.JobState_value[stateStr]; !ok {
		return None, fmt.Errorf("invalid task state '%v', taskID=%s", stateStr, p[TaskIDKey])
	}
	return State(indexpb.JobState_value[stateStr]), nil
}

func (p Properties) GetTaskReason() string {
	return p[ReasonKey]
}

func (p Properties) GetTaskSlot() (int64, error) {
	if _, ok := p[SlotKey]; !ok {
		return 0, WrapErrTaskPropertyLack(SlotKey, p[TaskIDKey])
	}
	return strconv.ParseInt(p[SlotKey], 10, 64)
}

func (p Properties) GetNumRows() int64 {
	if _, ok := p[NumRowsKey]; !ok {
		return 0
	}
	rows, err := strconv.ParseInt(p[NumRowsKey], 10, 64)
	if err != nil {
		return 0
	}
	return rows
}

func (p Properties) GetTaskVersion() int64 {
	if _, ok := p[TaskVersionKey]; !ok {
		return 0
	}
	version, err := strconv.ParseInt(p[TaskVersionKey], 10, 64)
	if err != nil {
		return 0
	}
	return version
}
