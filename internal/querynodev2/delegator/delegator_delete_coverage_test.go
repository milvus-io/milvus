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

package delegator

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

func TestSegmentDeleteReplayStartTs(t *testing.T) {
	t.Run("prefers_delete_covered_position", func(t *testing.T) {
		info := &querypb.SegmentLoadInfo{
			StartPosition:         &msgpb.MsgPosition{Timestamp: 10},
			DeleteCoveredPosition: &msgpb.MsgPosition{Timestamp: 100},
		}
		assert.EqualValues(t, 100, segmentDeleteReplayStartTs(info))
	})

	t.Run("falls_back_to_start_position_when_nil", func(t *testing.T) {
		info := &querypb.SegmentLoadInfo{
			StartPosition: &msgpb.MsgPosition{Timestamp: 10},
		}
		assert.EqualValues(t, 10, segmentDeleteReplayStartTs(info))
	})

	t.Run("falls_back_when_covered_ts_is_zero", func(t *testing.T) {
		info := &querypb.SegmentLoadInfo{
			StartPosition:         &msgpb.MsgPosition{Timestamp: 10},
			DeleteCoveredPosition: &msgpb.MsgPosition{Timestamp: 0},
		}
		assert.EqualValues(t, 10, segmentDeleteReplayStartTs(info))
	})
}
