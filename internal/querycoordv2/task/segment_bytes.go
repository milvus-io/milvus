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

package task

import "github.com/milvus-io/milvus/internal/querycoordv2/meta"

// defaultBytesPerRow stands in for a segment with no binlog sizes yet.
const defaultBytesPerRow = 512

// SegmentBytes estimates the memory a QueryNode holds for a segment: indexed
// fields count their index size, other fields their in-memory binlog size.
// It is QueryCoord's own estimate from the segment info DataCoord reports;
// the QueryNode's final figure is not known until the segment is loaded.
func SegmentBytes(s *meta.Segment) float64 {
	if s == nil {
		return 0
	}
	var indexed map[int64]int64
	if len(s.IndexInfo) > 0 {
		indexed = make(map[int64]int64, len(s.IndexInfo))
	}
	for _, info := range s.IndexInfo {
		if info.GetIndexSize() > 0 {
			indexed[info.GetFieldID()] += info.GetIndexSize()
		}
	}
	var b int64
	for _, fb := range s.GetBinlogs() {
		if _, ok := indexed[fb.GetFieldID()]; ok {
			continue
		}
		for _, l := range fb.GetBinlogs() {
			if m := l.GetMemorySize(); m > 0 {
				b += m
			} else {
				b += l.GetLogSize()
			}
		}
	}
	for _, sz := range indexed {
		b += sz
	}
	if b == 0 {
		b = s.GetNumOfRows() * defaultBytesPerRow
	}
	return float64(b)
}
