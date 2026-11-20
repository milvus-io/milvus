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

package externalrefresh

// externalRefreshBaseBoundaries refines the existing balanced file ranges by
// source rows. Inputs are validated by the ownership planner: filesPerTask > 0,
// rowsPerTask >= 0, and all counts nonnegative when the row budget is enabled.
// Files stay indivisible. A file larger than the budget owns a range by itself.
// Empty snapshots retain [0,0] so Paimon can retire baseline segments.
func externalRefreshBaseBoundaries(rows []int64, filesPerTask, rowsPerTask int64) []int {
	n := int64(len(rows))
	chunks := max(int64(1), (n-1)/filesPerTask+1)
	chunkSize := (n-1)/chunks + 1
	boundaries := []int{0}
	for begin := int64(0); begin < n; {
		end := min(begin+chunkSize, n)
		var count int64
		start := begin
		for i := begin; rowsPerTask > 0 && i < end; i++ {
			// Subtraction avoids overflowing when counts approach MaxInt64.
			if i > start && (rows[start] > rowsPerTask || rows[i] > rowsPerTask-count) {
				boundaries = append(boundaries, int(i))
				start, count = i, 0
			}
			count += min(rows[i], rowsPerTask)
		}
		boundaries = append(boundaries, int(end))
		begin = end
	}
	if n == 0 {
		boundaries = append(boundaries, 0)
	}
	return boundaries
}
