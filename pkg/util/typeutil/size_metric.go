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

// Segment size metric constants. The active metric is configured by
// dataCoord.segment.sizeMetric and interpreted at segment creation time
// (invariant I1 of the better-segmentation design).
const (
	// SizeMetricWholeRow constrains a segment's whole-row binary size — the
	// default and historically the only behavior.
	SizeMetricWholeRow = "wholeRow"
	// SizeMetricMainIndex constrains the main index column (the vector field
	// with the largest index memory footprint) instead of the whole row.
	SizeMetricMainIndex = "mainIndex"
)

// IsMainIndexSizeMetric reports whether the given size metric string enables
// main-index-column semantics.
func IsMainIndexSizeMetric(metric string) bool {
	return metric == SizeMetricMainIndex
}
