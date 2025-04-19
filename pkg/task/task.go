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

import "github.com/milvus-io/milvus/pkg/v2/proto/indexpb"

const TypeKey = "task_type"

type Type string

const (
	None       Type = "None"
	QuerySlot  Type = "QuerySlot"
	PreImport  Type = "PreImport"
	Import     Type = "Import"
	Compaction Type = "Compaction"
	Index      Type = "Index"
	Stats      Type = "Stats"
	Analyze    Type = "Analyze"
)

type State int32

const (
	Invalid State = iota
	Pending
	InProgress
	Finished
	Failed
	Retry
)

var stateName = map[State]string{
	0: "Invalid",
	1: "Pending",
	2: "InProgress",
	3: "Finished",
	4: "Failed",
	5: "Retry",
}

func (s State) String() string {
	return stateName[s]
}

func GetTaskTypeFromProperties(properties map[string]string) Type {
	if properties == nil {
		return None
	}

	taskType, ok := properties[TypeKey]
	if !ok {
		return None
	}

	switch Type(taskType) {
	case QuerySlot, PreImport, Import, Compaction, Index, Stats, Analyze:
		return Type(taskType)
	default:
		return None
	}
}

func GetJobTypeFromProperties(properties map[string]string) indexpb.JobType {
	taskType := GetTaskTypeFromProperties(properties)
	switch taskType {
	case Index:
		return indexpb.JobType_JobTypeIndexJob
	case Stats:
		return indexpb.JobType_JobTypeStatsJob
	case Analyze:
		return indexpb.JobType_JobTypeAnalyzeJob
	default:
		return indexpb.JobType_JobTypeNone
	}
}

type Properties map[string]string

func NewProperties() Properties {
	return make(map[string]string)
}

func (p Properties) AppendType(t Type) {
	switch t {
	case QuerySlot, PreImport, Import, Compaction, Index, Stats, Analyze:
		p[TypeKey] = string(t)
	default:
		p[TypeKey] = string(None)
	}
}
