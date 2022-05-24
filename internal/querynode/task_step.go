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

package querynode

type TaskStep int32

const (
	TaskStepUnissued    TaskStep = 0
	TaskStepEnqueue     TaskStep = 1
	TaskStepPreExecute  TaskStep = 2
	TaskStepExecute     TaskStep = 3
	TaskStepPostExecute TaskStep = 4
)

var TaskStepNames = map[TaskStep]string{
	0: "Unissued",
	1: "Enqueue",
	2: "PreExecute",
	3: "Execute",
	4: "PostExecute",
}

func (x TaskStep) String() string {
	ret, ok := TaskStepNames[x]
	if !ok {
		return "None"
	}
	return ret
}
