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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestTerminalFailureStateAlwaysHasReason(t *testing.T) {
	tests := []struct {
		name  string
		state datapb.CompactionTaskState
	}{
		{name: "failed", state: datapb.CompactionTaskState_failed},
		{name: "timeout", state: datapb.CompactionTaskState_timeout},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := &datapb.CompactionTask{}
			setState(test.state)(task)
			assert.NotEmpty(t, task.GetFailReason())

			reason := task.GetFailReason()
			setState(datapb.CompactionTaskState_cleaned)(task)
			assert.Equal(t, reason, task.GetFailReason())
		})
	}
}

func TestSpecificFailureReasonOverridesDefault(t *testing.T) {
	task := &datapb.CompactionTask{}
	setState(datapb.CompactionTaskState_failed)(task)
	setFailReason("specific failure")(task)
	assert.Equal(t, "specific failure", task.GetFailReason())
}
