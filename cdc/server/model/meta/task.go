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

package meta

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/cdc/server/model"
)

type TaskState int

const (
	TaskStateInitial TaskState = iota
	TaskStateRunning
	TaskStatePaused

	TaskStateTerminate TaskState = 100
)

const (
	// MinTaskState Must ATTENTION the of `add` and `reduce` method of server.TaskNumMetric
	// if you add new task state !!!!
	MinTaskState = TaskStateInitial
	MaxTaskState = TaskStatePaused
)

func (t TaskState) IsValidTaskState() bool {
	return t >= MinTaskState && t <= MaxTaskState
}

func (t TaskState) String() string {
	switch t {
	case TaskStateInitial:
		return "Initial"
	case TaskStateRunning:
		return "Running"
	case TaskStatePaused:
		return "Paused"
	case TaskStateTerminate:
		return "Terminate"
	default:
		return fmt.Sprintf("Unknown value[%d]", t)
	}
}

type TaskInfo struct {
	TaskID             string
	MilvusConnectParam model.MilvusConnectParam
	WriterCacheConfig  model.BufferConfig
	CollectionInfos    []model.CollectionInfo
	State              TaskState
	FailedReason       string
}

func (t *TaskInfo) CollectionNames() []string {
	names := make([]string, len(t.CollectionInfos))
	for i, info := range t.CollectionInfos {
		names[i] = info.Name
	}
	return names
}

type TaskCollectionPosition struct {
	TaskID         string
	CollectionName string
	// Positions key -> channel name, value -> check point
	Positions map[string]*commonpb.KeyDataPair
}
