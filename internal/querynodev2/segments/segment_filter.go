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

package segments

import (
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// segmentCriterion is the segment filter criterion obj.
type segmentCriterion struct {
	segmentIDs  typeutil.Set[int64]
	channel     metautil.Channel
	segmentType SegmentType
	others      []SegmentFilter
}

func (c *segmentCriterion) Match(segment Segment) bool {
	for _, filter := range c.others {
		if !filter.Match(segment) {
			return false
		}
	}
	return true
}

// SegmentFilter is the interface for segment selection criteria.
type SegmentFilter interface {
	Match(segment Segment) bool
	AddFilter(*segmentCriterion)
}

// SegmentFilterFunc is a type wrapper for `func(Segment) bool` to SegmentFilter.
type SegmentFilterFunc func(segment Segment) bool

func (f SegmentFilterFunc) Match(segment Segment) bool {
	return f(segment)
}

func (f SegmentFilterFunc) AddFilter(c *segmentCriterion) {
	c.others = append(c.others, f)
}

// SegmentIDFilter is the specific segment filter for SegmentID only.
type SegmentIDFilter int64

func (f SegmentIDFilter) Match(segment Segment) bool {
	return segment.ID() == int64(f)
}

func (f SegmentIDFilter) AddFilter(c *segmentCriterion) {
	if c.segmentIDs == nil {
		c.segmentIDs = typeutil.NewSet(int64(f))
		return
	}
	c.segmentIDs = c.segmentIDs.Intersection(typeutil.NewSet(int64(f)))
}

type SegmentIDsFilter struct {
	segmentIDs typeutil.Set[int64]
}

func (f SegmentIDsFilter) Match(segment Segment) bool {
	return f.segmentIDs.Contain(segment.ID())
}

func (f SegmentIDsFilter) AddFilter(c *segmentCriterion) {
	if c.segmentIDs == nil {
		c.segmentIDs = f.segmentIDs
		return
	}
	c.segmentIDs = c.segmentIDs.Intersection(f.segmentIDs)
}

type SegmentTypeFilter SegmentType

func (f SegmentTypeFilter) Match(segment Segment) bool {
	return segment.Type() == SegmentType(f)
}

func (f SegmentTypeFilter) AddFilter(c *segmentCriterion) {
	c.segmentType = SegmentType(f)
}

func WithSkipEmpty() SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.InsertCount() > 0
	})
}

func WithPartition(partitionID typeutil.UniqueID) SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.Partition() == partitionID
	})
}

func WithChannel(channel string) SegmentFilter {
	ac, err := metautil.ParseChannel(channel, channelMapper)
	if err != nil {
		return SegmentFilterFunc(func(segment Segment) bool {
			return false
		})
	}
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.Shard().Equal(ac)
	})
}

func WithType(typ SegmentType) SegmentFilter {
	return SegmentTypeFilter(typ)
}

func WithID(id int64) SegmentFilter {
	return SegmentIDFilter(id)
}

func WithIDs(ids ...int64) SegmentFilter {
	return SegmentIDsFilter{
		segmentIDs: typeutil.NewSet(ids...),
	}
}

func WithLevel(level datapb.SegmentLevel) SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.Level() == level
	})
}
