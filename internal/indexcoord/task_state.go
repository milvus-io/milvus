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

package indexcoord

type indexTaskState int32

const (
	// when we receive a index task
	indexTaskInit indexTaskState = iota
	// we've sent index task to scheduler, and wait for building index.
	indexTaskInProgress
	// task done, wait to be cleaned
	indexTaskDone
	// index task need to retry.
	indexTaskRetry
	// task has been deleted.
	indexTaskDeleted
	// task needs to recycle meta info on IndexNode
	indexTaskRecycle
	// reserved
	_
	// task needs to be prepared
	indexTaskPreparing
)

var TaskStateNames = map[indexTaskState]string{
	0: "Init",
	1: "InProgress",
	2: "Done",
	3: "Retry",
	4: "Deleted",
	5: "Recycle",
	6: "Wait",
	7: "Preparing",
}

func (x indexTaskState) String() string {
	ret, ok := TaskStateNames[x]
	if !ok {
		return "None"
	}
	return ret
}
