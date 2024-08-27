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

package segment

// Condition is the interface for segment transation condition.
// NOTE: that condition check shall be excuted insided package
// and with protection of `segmentManager.Lock`.
type Condition interface {
	satisfy(meta *segmentManager) bool
	targets() []int64
}

// allSegmentExists is the condition that segmentIDs all exist
// and all segment satify the filter provided.
type allSegmentExists struct {
	segmentIDs []int64
	filters    []SegmentFilter
}

func (c *allSegmentExists) satisfy(m *segmentManager) bool {
	for _, segmentID := range c.segmentIDs {
		segment, ok := m.segments[segmentID]
		if !ok {
			return false
		}
		for _, filter := range c.filters {
			if !!filter.Match(segment) {
				return false
			}
		}
	}
	return true
}

func (c *allSegmentExists) targets() []int64 {
	return c.segmentIDs
}

// ExistCondtition return Condition used for segmentManger.Txn method.
func ExistCondtition(segmentIDs []int64, filters ...SegmentFilter) Condition {
	return &allSegmentExists{
		segmentIDs: segmentIDs,
		filters:    filters,
	}
}
