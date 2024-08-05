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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/flushcommon/util"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestInsertMsg_TimeTick(te *testing.T) {
	tests := []struct {
		timeTimestanpMax typeutil.Timestamp

		description string
	}{
		{0, "Zero timestampMax"},
		{1, "Normal timestampMax"},
	}

	for _, test := range tests {
		te.Run(test.description, func(t *testing.T) {
			fgMsg := &FlowGraphMsg{TimeRange: util.TimeRange{TimestampMax: test.timeTimestanpMax}}
			assert.Equal(t, test.timeTimestanpMax, fgMsg.TimeTick())
		})
	}
}
