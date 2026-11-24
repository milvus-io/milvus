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

package metastore

import (
	"reflect"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// AnalyzeTaskType is the model type of analyze tasks, values of
// *indexpb.AnalyzeTask at datacoord-meta/analyze-task/{taskID}, unchanged
// from the typed catalog methods.
var AnalyzeTaskType = RegisterModelType(ModelType{
	Root:   "datacoord-meta/analyze-task",
	Depth:  1,
	GoType: reflect.TypeOf((*indexpb.AnalyzeTask)(nil)),
	Coordinate: func(v any) []string {
		return []string{strconv.FormatInt(v.(*indexpb.AnalyzeTask).GetTaskID(), 10)}
	},
	Marshal: func(v any) ([]byte, error) {
		return proto.Marshal(v.(*indexpb.AnalyzeTask))
	},
	Unmarshal: func(value []byte) (any, error) {
		task := &indexpb.AnalyzeTask{}
		if err := proto.Unmarshal(value, task); err != nil {
			return nil, err
		}
		return task, nil
	},
})

// SaveAnalyzeTask returns an UpdateAction that persists task, creating or
// replacing its record.
func SaveAnalyzeTask(task *indexpb.AnalyzeTask) UpdateAction {
	return UpdateAction{Type: ActionUpdate, Entry: task}
}

// DropAnalyzeTask returns an UpdateAction that removes an analyze task.
func DropAnalyzeTask(taskID int64) UpdateAction {
	return UpdateAction{Type: ActionDelete, Entry: &indexpb.AnalyzeTask{TaskID: taskID}}
}
