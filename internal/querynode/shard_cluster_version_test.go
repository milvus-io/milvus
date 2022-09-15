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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func channelClose(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestShardClusterVersion(t *testing.T) {

	t.Run("new version", func(t *testing.T) {
		v := NewShardClusterVersion(1, SegmentsStatus{}, nil)

		assert.True(t, v.IsCurrent())
		assert.Equal(t, int64(1), v.versionID)
		assert.Equal(t, int64(0), v.inUse.Load())
	})

	t.Run("version expired", func(t *testing.T) {
		v := NewShardClusterVersion(1, SegmentsStatus{}, nil)
		assert.True(t, v.IsCurrent())
		ch := v.Expire()

		assert.False(t, v.IsCurrent())
		assert.Eventually(t, func() bool {
			return channelClose(ch)
		}, time.Second, time.Millisecond*10)
	})

	t.Run("In use check", func(t *testing.T) {
		v := NewShardClusterVersion(1, SegmentsStatus{
			1: shardSegmentInfo{segmentID: 1, partitionID: 0, nodeID: 1},
			2: shardSegmentInfo{segmentID: 2, partitionID: 1, nodeID: 2},
		}, nil)
		allocs := v.GetAllocation([]int64{1})
		assert.EqualValues(t, map[int64][]int64{2: {2}}, allocs)

		assert.Equal(t, int64(1), v.inUse.Load())

		ch := v.Expire()
		assert.False(t, channelClose(ch))

		v.FinishUsage()

		assert.Eventually(t, func() bool {
			return channelClose(ch)
		}, time.Second, time.Millisecond*10)

	})

	t.Run("wait last version", func(t *testing.T) {
		lastVersion := NewShardClusterVersion(1, SegmentsStatus{}, nil)
		lastVersion.GetAllocation(nil)
		currentVersion := NewShardClusterVersion(2, SegmentsStatus{}, lastVersion)

		ch := currentVersion.Expire()

		select {
		case <-ch:
			t.FailNow()
		default:
		}

		lastVersion.FinishUsage()

		assert.Eventually(t, func() bool {
			return channelClose(ch)
		}, time.Second, time.Millisecond*10)
	})
}
