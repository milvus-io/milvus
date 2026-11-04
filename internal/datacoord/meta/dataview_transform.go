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

package meta

// SegmentTransformStart resolves legacy ordinary and committed-import metadata
// conservatively. A compacted base needs its own cursor: its first surviving
// row is not evidence of the Transform prefix covered by its parent revisions.
func SegmentTransformStart(segment *SegmentInfo) uint64 {
	if start := segment.GetTransformStartAfterTimetick(); start != 0 {
		return start
	}
	if commit := segment.GetCommitTimestamp(); commit != 0 {
		return commit
	}
	if segment.GetIsImporting() || segment.GetCreatedByCompaction() || len(segment.GetCompactionFrom()) != 0 {
		return 0
	}
	return segment.GetStartPosition().GetTimestamp()
}

func MinSegmentTransformStart(segments []*SegmentInfo) uint64 {
	var start uint64
	for i, segment := range segments {
		cursor := SegmentTransformStart(segment)
		if i == 0 || cursor < start {
			start = cursor
		}
	}
	return start
}
