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

package util

import (
	"encoding/binary"
	"time"
)

// Endian is type alias of binary.LittleEndian.
// Milvus uses little endian by default.
var Endian = binary.LittleEndian

const (
	logicalBits     = 18
	logicalBitsMask = (1 << logicalBits) - 1
)

// ParseHybridTs parses the ts to (physical, logical), physical part is of utc-timestamp format.
func ParseHybridTs(ts uint64) (int64, int64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	return int64(physical), int64(logical)
}

// SubByNow ts is a hybrid
func SubByNow(ts uint64) int64 {
	utcT, _ := ParseHybridTs(ts)
	now := time.Now().UnixMilli()
	return now - utcT
}
