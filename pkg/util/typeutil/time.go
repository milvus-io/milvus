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

package typeutil

import (
	"math"
	"time"
)

// MaxTimestamp is the max timestamp.
const MaxTimestamp = Timestamp(math.MaxUint64)

// ZeroTime is a zero time.
var ZeroTime = time.Time{}

// ZeroTimestamp is a zero timestamp
var ZeroTimestamp = Timestamp(0)

// ParseTimestamp returns a timestamp for a given byte slice.
func ParseTimestamp(data []byte) (time.Time, error) {
	// we use big endian here for compatibility issues
	nano, err := BigEndianBytesToUint64(data)
	if err != nil {
		return ZeroTime, err
	}

	return time.Unix(0, int64(nano)), nil
}

// SubTimeByWallClock returns the duration between two different timestamps.
func SubTimeByWallClock(after, before time.Time) time.Duration {
	return time.Duration(after.UnixNano() - before.UnixNano())
}

func TimestampToString(ts uint64) string {
	if ts <= 0 {
		return ""
	}
	ut := time.UnixMilli(int64(ts))
	return ut.Format(time.DateTime)
}
