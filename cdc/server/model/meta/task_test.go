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
	"testing"

	"github.com/milvus-io/milvus/cdc/server/model"
	"github.com/stretchr/testify/assert"
)

func TestTaskState(t *testing.T) {
	state := TaskState(1000)
	assert.False(t, state.IsValidTaskState())
	assert.Contains(t, state.String(), "Unknown value")

	state = TaskStateInitial
	assert.True(t, state.IsValidTaskState())
	assert.Equal(t, "Initial", state.String())

	state = TaskStatePaused
	assert.True(t, state.IsValidTaskState())
	assert.Equal(t, "Paused", state.String())

	state = TaskStateRunning
	assert.True(t, state.IsValidTaskState())
	assert.Equal(t, "Running", state.String())

	state = TaskStateTerminate
	assert.False(t, state.IsValidTaskState())
	assert.Equal(t, "Terminate", state.String())
}

func TestTaskInfo(t *testing.T) {
	info := &TaskInfo{
		CollectionInfos: nil,
	}
	assert.Len(t, info.CollectionNames(), 0)

	info.CollectionInfos = []model.CollectionInfo{
		{Name: "foo"},
		{Name: "too"},
	}
	assert.Len(t, info.CollectionInfos, 2)
	assert.Equal(t, []string{"foo", "too"}, info.CollectionNames())
}
