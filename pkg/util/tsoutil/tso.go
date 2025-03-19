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
	"time"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	logicalBits     = 18
	logicalBitsMask = (1 << logicalBits) - 1
)

// ComposeTS returns a timestamp composed of physical part and logical part
func ComposeTS(physical, logical int64) uint64 {
	return uint64((physical << logicalBits) + logical)
}

// ComposeTSByTime returns a timestamp composed of physical time.Time and logical time
func ComposeTSByTime(physical time.Time, logical int64) uint64 {
	return ComposeTS(physical.UnixNano()/int64(time.Millisecond), logical)
}

// GetCurrentTime returns the current timestamp
func GetCurrentTime() typeutil.Timestamp {
	return ComposeTSByTime(time.Now(), 0)
}

// ParseTS parses the ts to (physical,logical).
func ParseTS(ts uint64) (time.Time, uint64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}

func PhysicalTime(ts uint64) time.Time {
	physicalTime, _ := ParseTS(ts)
	return physicalTime
}

// PhysicalTimeSeconds returns the physical time in seconds
func PhysicalTimeSeconds(ts uint64) float64 {
	return float64(ts>>logicalBits) / 1000
}

// ParseHybridTs parses the ts to (physical, logical), physical part is of utc-timestamp format.
func ParseHybridTs(ts uint64) (int64, int64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	return int64(physical), int64(logical)
}

// CalculateDuration returns the number of milliseconds obtained by subtracting ts2 from ts1.
func CalculateDuration(ts1, ts2 typeutil.Timestamp) int64 {
	p1, _ := ParseHybridTs(ts1)
	p2, _ := ParseHybridTs(ts2)
	return p1 - p2
}

// Mod24H parses the ts to millisecond in one day
func Mod24H(ts uint64) uint64 {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	physical = physical % (uint64(24 * 60 * 60 * 1000))
	return (physical << logicalBits) | logical
}

// AddPhysicalDurationOnTs adds physical interval on ts
func AddPhysicalDurationOnTs(ts uint64, duration time.Duration) uint64 {
	msecs := duration.Milliseconds()
	physical, logical := ParseHybridTs(ts)
	return ComposeTS(physical+msecs, logical)
}

// SubByNow ts is a hybrid
func SubByNow(ts uint64) int64 {
	utcT, _ := ParseHybridTs(ts)
	now := time.Now().UnixMilli()
	return now - utcT
}

func PhysicalTimeFormat(ts uint64) string {
	return PhysicalTime(ts).Format(time.DateTime)
}

const (
	minUnixMillis = 1546300800000   // 2019-01-01 00:00:00 UTC
	maxUnixMillis = 253402300799000 // 9999-12-31 23:59:59 UTC
)

func IsValidPhysicalTs(t uint64) bool {
	return t >= minUnixMillis && t <= maxUnixMillis
}

func IsValidHybridTs(t uint64) bool {
	return IsValidPhysicalTs(t >> logicalBits)
}
