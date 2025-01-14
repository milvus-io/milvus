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

import "github.com/milvus-io/milvus/pkg/proto/datapb"

// SegmentOperator is function type to update segment info.
type SegmentOperator func(segment *SegmentInfo) bool

func SetMaxRowCount(maxRow int64) SegmentOperator {
	return func(segment *SegmentInfo) bool {
		if segment.MaxRowNum == maxRow {
			return false
		}
		segment.MaxRowNum = maxRow
		return true
	}
}

func SetTextIndexLogs(textIndexLogs map[int64]*datapb.TextIndexStats) SegmentOperator {
	return func(segment *SegmentInfo) bool {
		if segment.TextStatsLogs == nil {
			segment.TextStatsLogs = make(map[int64]*datapb.TextIndexStats)
		}
		for field, logs := range textIndexLogs {
			segment.TextStatsLogs[field] = logs
		}
		return true
	}
}

type segmentCriterion struct {
	collectionID int64
	channel      string
	partitionID  int64
	others       []SegmentFilter
}

func (sc *segmentCriterion) Match(segment *SegmentInfo) bool {
	for _, filter := range sc.others {
		if !filter.Match(segment) {
			return false
		}
	}
	return true
}

type SegmentFilter interface {
	Match(segment *SegmentInfo) bool
	AddFilter(*segmentCriterion)
}

type CollectionFilter int64

func (f CollectionFilter) Match(segment *SegmentInfo) bool {
	return segment.GetCollectionID() == int64(f)
}

func (f CollectionFilter) AddFilter(criterion *segmentCriterion) {
	criterion.collectionID = int64(f)
}

func WithCollection(collectionID int64) SegmentFilter {
	return CollectionFilter(collectionID)
}

type ChannelFilter string

func (f ChannelFilter) Match(segment *SegmentInfo) bool {
	return segment.GetInsertChannel() == string(f)
}

func (f ChannelFilter) AddFilter(criterion *segmentCriterion) {
	criterion.channel = string(f)
}

// WithChannel WithCollection has a higher priority if both WithCollection and WithChannel are in condition together.
func WithChannel(channel string) SegmentFilter {
	return ChannelFilter(channel)
}

type SegmentFilterFunc func(*SegmentInfo) bool

func (f SegmentFilterFunc) Match(segment *SegmentInfo) bool {
	return f(segment)
}

func (f SegmentFilterFunc) AddFilter(criterion *segmentCriterion) {
	criterion.others = append(criterion.others, f)
}
