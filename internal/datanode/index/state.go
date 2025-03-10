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

package index

type TaskState int32

const (
	TaskStateNormal  TaskState = 0
	TaskStateAbandon TaskState = 1
	TaskStateRetry   TaskState = 2
	TaskStateFailed  TaskState = 3
)

var TaskStateNames = map[TaskState]string{
	0: "Normal",
	1: "Abandon",
	2: "Retry",
	3: "Failed",
}

func (x TaskState) String() string {
	ret, ok := TaskStateNames[x]
	if !ok {
		return "None"
	}
	return ret
}
