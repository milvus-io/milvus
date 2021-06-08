// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package dataservice

import (
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

// flushMonitor check segments / channels meet the provided flush policy
type flushMonitor struct {
	meta          *meta
	segmentPolicy SegmentFlushPolicy
	channelPolicy ChannelFlushPolicy
}

// SegmentFlushPolicy checks segment size and returns whether segment needs to be flushed
type SegmentFlushPolicy func(*datapb.SegmentInfo) bool

// ChannelFlushPolicy checks segments inside single Vchannel count and returns segment ids needs to be flushed
type ChannelFlushPolicy func(string, []*datapb.SegmentInfo, *internalpb.MsgPosition) []UniqueID

// emptyFlushMonitor returns empty flush montior
func emptyFlushMonitor(meta *meta) flushMonitor {
	return flushMonitor{
		meta: meta,
	}
}

// defaultFlushMonitor generates auto flusher with default policies
func defaultFlushMonitor(meta *meta) flushMonitor {
	return flushMonitor{
		meta: meta,
		// segmentPolicy: estSegmentSizePolicy(1024, 1024*1024*1536), // row 1024 byte, limit 1.5GiB
		channelPolicy: channelSizeEpochPolicy(1024, uint64(time.Hour)),
	}
}

// CheckSegments check segments meet flush policy, returns segment id needs to flush
func (f flushMonitor) CheckSegments(segments []*datapb.SegmentInfo) []UniqueID {
	if f.segmentPolicy == nil {
		return []UniqueID{}
	}
	result := make([]UniqueID, 0, len(segments))
	for _, segment := range segments {
		if f.segmentPolicy(segment) {
			result = append(result, segment.ID)
		}
	}
	return result
}

// CheckChannels check channels changed, apply `ChannelPolicy`
func (f flushMonitor) CheckChannels(channels []string, latest *internalpb.MsgPosition) []UniqueID {
	segHits := make(map[UniqueID]struct{})
	for _, channel := range channels {
		segments := f.meta.GetSegmentsByChannel(channel)

		growingSegments := make([]*datapb.SegmentInfo, 0, len(segments))
		for _, segment := range segments {
			if segment.State != commonpb.SegmentState_Growing {
				continue
			}
			growingSegments = append(growingSegments, segment)
			if f.segmentPolicy != nil && f.segmentPolicy(segment) {
				segHits[segment.ID] = struct{}{}
			}
		}
		if f.channelPolicy != nil {
			hits := f.channelPolicy(channel, growingSegments, latest)
			for _, hit := range hits {
				segHits[hit] = struct{}{}
			}
		}
	}

	result := make([]UniqueID, 0, len(segHits))
	for segID := range segHits {
		result = append(result, segID)
	}

	return result
}

// deprecated
func estSegmentSizePolicy(rowSize, limit int64) SegmentFlushPolicy {
	return func(seg *datapb.SegmentInfo) bool {
		if seg == nil {
			return false
		}
		if seg.NumOfRows*rowSize > limit {
			return true
		}
		return false
	}
}

// channelSizeEpochPolicy policy check channel sizes and segment life time
// segmentMax is the max number of segment allowed in the channel
// epochDuration is the max live time segment has
func channelSizeEpochPolicy(segmentMax int, epochDuration uint64) ChannelFlushPolicy {
	return func(channel string, segments []*datapb.SegmentInfo, latest *internalpb.MsgPosition) []UniqueID {
		if len(segments) < segmentMax && latest == nil {
			return []UniqueID{}
		}
		sortSegmentsByDmlPos(segments)
		result := []UniqueID{}
		overflow := len(segments) - segmentMax
		for idx, segment := range segments {
			if idx < overflow {
				result = append(result, segment.ID)
				continue
			}
			if latest != nil {
				if segment.DmlPosition == nil || latest.Timestamp-segment.DmlPosition.Timestamp > epochDuration {
					result = append(result, segment.ID)
					continue
				}
			}
			break
		}
		return result
	}
}

// sortSegmentsByDmlPos sorts input segments in ascending order by `DmlPosition.Timestamp`, nil value is less than 0
func sortSegmentsByDmlPos(segments []*datapb.SegmentInfo) {
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].DmlPosition == nil {
			return true
		}
		if segments[j].DmlPosition == nil {
			return false
		}
		return segments[i].DmlPosition.Timestamp < segments[j].DmlPosition.Timestamp
	})
}
