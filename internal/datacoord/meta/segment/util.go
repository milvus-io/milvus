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

import "github.com/samber/lo"

// GetSegmentsNumOfRows is a utility function to calculate segments row number.
func GetSegmentsNumOfRows(segments ...*SegmentInfo) int64 {
	var result int64
	for _, segment := range segments {
		result += segment.GetNumOfRows()
	}
	return result
}

func GroupByCompactionDim(segments ...*SegmentInfo) []*CompactionGroup {
	var result []*CompactionGroup

	partGroups := lo.GroupBy(segments, func(segment *SegmentInfo) int64 {
		return segment.GetPartitionID()
	})

	for partID, partSegments := range partGroups {
		channelGroups := lo.GroupBy(partSegments, func(segment *SegmentInfo) string {
			return segment.GetInsertChannel()
		})

		for channelName, segments := range channelGroups {
			result = append(result, &CompactionGroup{
				CollectionID: segments[0].GetCollectionID(),
				PartitionID:  partID,
				ChannelName:  channelName,
				Segments:     segments,
			})
		}
	}

	return result
}
