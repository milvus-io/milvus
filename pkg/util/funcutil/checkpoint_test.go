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

package funcutil

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
)

func TestIsDroppedChannelCheckpoint(t *testing.T) {
	cases := []struct {
		name     string
		pos      *msgpb.MsgPosition
		expected bool
	}{
		{
			name:     "nil position is not sentinel",
			pos:      nil,
			expected: false,
		},
		{
			name:     "zero timestamp is not sentinel",
			pos:      &msgpb.MsgPosition{Timestamp: 0},
			expected: false,
		},
		{
			// A typical Milvus TSO-encoded timestamp (physical ms shifted left 18 bits).
			name:     "normal recent timestamp is not sentinel",
			pos:      &msgpb.MsgPosition{Timestamp: 450000000000000000},
			expected: false,
		},
		{
			name:     "exact sentinel timestamp is sentinel",
			pos:      &msgpb.MsgPosition{Timestamp: math.MaxUint64},
			expected: true,
		},
		{
			name:     "sentinel timestamp with MsgID set is still sentinel",
			pos:      &msgpb.MsgPosition{Timestamp: math.MaxUint64, MsgID: []byte{1, 2, 3}},
			expected: true,
		},
		{
			name:     "timestamp one below sentinel is not sentinel",
			pos:      &msgpb.MsgPosition{Timestamp: math.MaxUint64 - 1},
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsDroppedChannelCheckpoint(tc.pos))
		})
	}
}

func TestDroppedChannelCheckpointTimestamp(t *testing.T) {
	// Contract: the sentinel value is math.MaxUint64. Anything else would
	// break MarkChannelCheckpointDropped's intent that the sentinel can
	// never be overwritten by a normal checkpoint update.
	assert.Equal(t, uint64(math.MaxUint64), DroppedChannelCheckpointTimestamp)
}
