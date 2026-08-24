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

package tsoutil

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestParseHybridTs(t *testing.T) {
	var ts uint64 = 426152581543231492
	physical, logical := ParseHybridTs(ts)
	physicalTime := time.Unix(physical/1000, physical%1000*time.Millisecond.Nanoseconds())
	mlog.Debug(context.TODO(), "TestParseHybridTs",
		mlog.Int64("physical", physical),
		mlog.Int64("logical", logical),
		mlog.Any("physical time", physicalTime))
}

func Test_Tso(t *testing.T) {
	t.Run("test ComposeTSByTime", func(t *testing.T) {
		physical := time.Unix(1700000000, 123456789)
		timestamp := ComposeTSByTime(physical)
		pRes, lRes := ParseHybridTs(timestamp)
		assert.Equal(t, physical.UnixMilli(), pRes)
		assert.Equal(t, int64(0), lRes)
	})

	t.Run("test ComposeTSByTimeWithLogical", func(t *testing.T) {
		physical := time.Unix(1700000000, 123456789)
		logical := int64(1000)
		timestamp := ComposeTSByTimeWithLogical(physical, logical)
		pRes, lRes := ParseHybridTs(timestamp)
		assert.Equal(t, physical.UnixMilli(), pRes)
		assert.Equal(t, logical, lRes)
	})
}

func TestCalculateDuration(t *testing.T) {
	now := time.Now()
	ts1 := ComposeTSByTime(now)
	durationInMilliSecs := int64(20 * 1000)
	ts2 := ComposeTSByTime(now.Add(time.Duration(durationInMilliSecs) * time.Millisecond))
	diff := CalculateDuration(ts2, ts1)
	assert.Equal(t, durationInMilliSecs, diff)
}

func TestAddPhysicalDurationOnTs(t *testing.T) {
	now := time.Now()
	ts1 := ComposeTSByTime(now)
	duration := time.Millisecond * (20 * 1000)
	ts2 := AddPhysicalDurationOnTs(ts1, duration)
	ts3 := ComposeTSByTime(now.Add(duration))
	// diff := CalculateDuration(ts2, ts1)
	assert.Equal(t, ts3, ts2)

	ts2 = AddPhysicalDurationOnTs(ts1, -duration)
	ts3 = ComposeTSByTime(now.Add(-duration))
	// diff := CalculateDuration(ts2, ts1)
	assert.Equal(t, ts3, ts2)
}

func TestIsValidUnixMilli(t *testing.T) {
	physicalTs := uint64(time.Now().UnixMilli())
	assert.True(t, IsValidPhysicalTs(physicalTs))
	assert.False(t, IsValidHybridTs(physicalTs))

	hybridTs := ComposeTSByTime(time.Now())
	assert.False(t, IsValidPhysicalTs(hybridTs))
	assert.True(t, IsValidHybridTs(hybridTs))
}
