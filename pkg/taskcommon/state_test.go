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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func TestFromCompactionState(t *testing.T) {
	tests := []struct {
		name     string
		input    datapb.CompactionTaskState
		expected State
	}{
		{"pipelining maps to Init", datapb.CompactionTaskState_pipelining, Init},
		{"executing maps to InProgress", datapb.CompactionTaskState_executing, InProgress},
		{"analyzing maps to InProgress", datapb.CompactionTaskState_analyzing, InProgress},
		{"completed maps to Finished", datapb.CompactionTaskState_completed, Finished},
		{"meta_saved maps to Finished", datapb.CompactionTaskState_meta_saved, Finished},
		{"statistic maps to Finished", datapb.CompactionTaskState_statistic, Finished},
		{"indexing maps to Finished", datapb.CompactionTaskState_indexing, Finished},
		{"cleaned maps to Finished", datapb.CompactionTaskState_cleaned, Finished},
		{"failed maps to Failed", datapb.CompactionTaskState_failed, Failed},
		{"timeout maps to Retry", datapb.CompactionTaskState_timeout, Retry},
		{"unknown maps to None", datapb.CompactionTaskState_unknown, None},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, FromCompactionState(tt.input))
		})
	}
}
