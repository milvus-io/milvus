// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
)

func TestFlowGraphMsg_insertMsg(t *testing.T) {
	msg, err := genSimpleInsertMsg()
	assert.NoError(t, err)
	timestampMax := Timestamp(1000)
	im := insertMsg{
		insertMessages: []*msgstream.InsertMsg{
			msg,
		},
		timeRange: TimeRange{
			timestampMin: 0,
			timestampMax: timestampMax,
		},
	}
	time := im.TimeTick()
	assert.Equal(t, timestampMax, time)
}

func TestFlowGraphMsg_serviceTimeMsg(t *testing.T) {
	timestampMax := Timestamp(1000)
	stm := serviceTimeMsg{
		timeRange: TimeRange{
			timestampMin: 0,
			timestampMax: timestampMax,
		},
	}
	time := stm.TimeTick()
	assert.Equal(t, timestampMax, time)
}
