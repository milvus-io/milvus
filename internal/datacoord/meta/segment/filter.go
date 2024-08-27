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

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type segmentCriterion struct {
	segmentIDs   typeutil.Set[int64]
	collectionID int64
	channel      string
	partitionID  int64
	// compact from second index
	compactFrom int64
	others      []SegmentFilter
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

type SegmentIDsFilter struct {
	segmentIDs typeutil.Set[int64]
}

func (f SegmentIDsFilter) Match(segment *SegmentInfo) bool {
	return f.segmentIDs.Contain(segment.GetID())
}

func (f SegmentIDsFilter) AddFilter(criterion *segmentCriterion) {
	criterion.segmentIDs = f.segmentIDs.Clone()
}

func WithSegmentIDs(segmentIDs ...int64) SegmentFilter {
	set := typeutil.NewSet(segmentIDs...)
	return SegmentIDsFilter{
		segmentIDs: set,
	}
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

type SegmentFilterFunc func(*SegmentInfo) bool

func (f SegmentFilterFunc) Match(segment *SegmentInfo) bool {
	return f(segment)
}

func (f SegmentFilterFunc) AddFilter(criterion *segmentCriterion) {
	criterion.others = append(criterion.others, f)
}

func WithSegmentFilterFunc(f func(*SegmentInfo) bool) SegmentFilter {
	return SegmentFilterFunc(f)
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

type StateFilter struct {
	states typeutil.Set[commonpb.SegmentState]
}

func (f StateFilter) Match(segment *SegmentInfo) bool {
	return f.states.Contain(segment.GetState())
}

func (f StateFilter) AddFilter(criterion *segmentCriterion) {
	criterion.others = append(criterion.others, f)
}

// WithSegmentState return segment filter with segment state.
func WithSegmentState(states ...commonpb.SegmentState) SegmentFilter {
	return StateFilter{
		states: typeutil.NewSet(states...),
	}
}

type CompactFromFilter int64

func (f CompactFromFilter) Match(segment *SegmentInfo) bool {
	for _, from := range segment.GetCompactionFrom() {
		if from == int64(f) {
			return true
		}
	}
	return false
}

func (f CompactFromFilter) AddFilter(criterion *segmentCriterion) {
	criterion.compactFrom = int64(f)
}

func WithCompactFrom(compactFrom int64) SegmentFilter {
	return CompactFromFilter(compactFrom)
}

// WithFlushState is suger function to get StateFilter with flush state.
func WithFlushState() SegmentFilter {
	return WithSegmentState(commonpb.SegmentState_Flushed, commonpb.SegmentState_Flushing)
}

// WithHealthyState is suger function to get StateFilter with healthy state.
func WithHealthyState() SegmentFilter {
	return WithSegmentState(commonpb.SegmentState_Growing, commonpb.SegmentState_Importing, commonpb.SegmentState_Flushed, commonpb.SegmentState_Flushing, commonpb.SegmentState_Sealed)
}
