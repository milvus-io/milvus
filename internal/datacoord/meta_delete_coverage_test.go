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

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

// segWithCoverage builds a *SegmentInfo carrying an optional
// delete_covered_position (covered>0) and optional deltalog TimestampTo values.
func segWithCoverage(covered uint64, deltaTos ...uint64) *SegmentInfo {
	info := &datapb.SegmentInfo{}
	if covered > 0 {
		info.DeleteCoveredPosition = &msgpb.MsgPosition{Timestamp: covered}
	}
	if len(deltaTos) > 0 {
		binlogs := make([]*datapb.Binlog, 0, len(deltaTos))
		for _, to := range deltaTos {
			binlogs = append(binlogs, &datapb.Binlog{TimestampTo: to})
		}
		info.Deltalogs = []*datapb.FieldBinlog{{Binlogs: binlogs}}
	}
	return NewSegmentInfo(info)
}

func TestInputDeleteCoverageTs(t *testing.T) {
	t.Run("covered_position_only", func(t *testing.T) {
		ts, known := inputDeleteCoverageTs(segWithCoverage(100))
		assert.True(t, known)
		assert.EqualValues(t, 100, ts)
	})

	t.Run("deltalogs_only_takes_max_timestamp_to", func(t *testing.T) {
		ts, known := inputDeleteCoverageTs(segWithCoverage(0, 50, 120, 80))
		assert.True(t, known)
		assert.EqualValues(t, 120, ts)
	})

	t.Run("both_present_newest_wins", func(t *testing.T) {
		// deltalog newer than covered position
		ts, known := inputDeleteCoverageTs(segWithCoverage(100, 50, 130))
		assert.True(t, known)
		assert.EqualValues(t, 130, ts)
		// covered position newer than deltalogs
		ts, known = inputDeleteCoverageTs(segWithCoverage(200, 50, 130))
		assert.True(t, known)
		assert.EqualValues(t, 200, ts)
	})

	t.Run("neither_present_is_unknown", func(t *testing.T) {
		_, known := inputDeleteCoverageTs(segWithCoverage(0))
		assert.False(t, known)
	})
}

func TestComputeDeleteCoveredPosition(t *testing.T) {
	const channel = "by-dev-rootcoord-dml_0_v0"

	t.Run("min_coverage_across_inputs", func(t *testing.T) {
		p := computeDeleteCoveredPosition(channel, []*SegmentInfo{
			segWithCoverage(0, 300),
			segWithCoverage(150),
			segWithCoverage(0, 200),
		})
		if assert.NotNil(t, p) {
			// the least-covered input (150) bounds the output
			assert.EqualValues(t, 150, p.GetTimestamp())
			assert.Equal(t, channel, p.GetChannelName())
		}
	})

	t.Run("any_unknown_input_yields_nil", func(t *testing.T) {
		// a nil result makes the delegator fall back to start_position (minTs),
		// which is always safe (never loses a delete).
		p := computeDeleteCoveredPosition(channel, []*SegmentInfo{
			segWithCoverage(0, 300),
			segWithCoverage(0), // coverage unknown
		})
		assert.Nil(t, p)
	})

	t.Run("empty_inputs_yields_nil", func(t *testing.T) {
		assert.Nil(t, computeDeleteCoveredPosition(channel, nil))
	})

	t.Run("single_input", func(t *testing.T) {
		p := computeDeleteCoveredPosition(channel, []*SegmentInfo{segWithCoverage(0, 77)})
		if assert.NotNil(t, p) {
			assert.EqualValues(t, 77, p.GetTimestamp())
		}
	})
}
