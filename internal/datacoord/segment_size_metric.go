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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// isMainIndexSizeMetric reports whether dataCoord.segment.sizeMetric enables
// main-index-column semantics.
func isMainIndexSizeMetric() bool {
	return typeutil.IsMainIndexSizeMetric(Params.DataCoordCfg.SizeMetric.GetValue())
}

// maxFullSegmentSizeBytes returns the configured hard whole-row ceiling in
// bytes (0 = disabled).
func maxFullSegmentSizeBytes() int64 {
	value := Params.DataCoordCfg.MaxFullSegmentSize.GetAsInt64()
	if value <= 0 {
		return 0
	}
	return value * 1024 * 1024
}

// capByCeiling caps a whole-row size at the hard ceiling. The ceiling is only
// consulted under the mainIndex metric; under wholeRow it is a no-op so current
// behavior is preserved. Compaction planning and import splitting measure
// whole-row bytes (matching the datanode executor), so capping in whole-row
// bytes keeps output segments bounded by the ceiling (I4).
func capByCeiling(size int64) int64 {
	if !isMainIndexSizeMetric() {
		return size
	}
	if ceilingBytes := maxFullSegmentSizeBytes(); ceilingBytes > 0 && size > ceilingBytes {
		return ceilingBytes
	}
	return size
}
