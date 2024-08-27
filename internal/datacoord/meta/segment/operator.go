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
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// SegmentOperator is function type to update segment info.
type SegmentOperator interface {
	IsMetaUpdate() bool
	UpdateSegment(*SegmentInfo) (updated bool)
}

type SegmentOperatorShallowFunc func(segment *SegmentInfo) bool

func (f SegmentOperatorShallowFunc) IsMetaUpdate() bool {
	return false
}

func (f SegmentOperatorShallowFunc) UpdateSegment(segment *SegmentInfo) bool {
	return f(segment)
}

type SegmentOperatorFunc func(segment *SegmentInfo) bool

func (f SegmentOperatorFunc) IsMetaUpdate() bool {
	return true
}

func (f SegmentOperatorFunc) UpdateSegment(segment *SegmentInfo) bool {
	return f(segment)
}

func SetMaxRowCount(maxRow int64) SegmentOperator {
	return SegmentOperatorFunc(func(segment *SegmentInfo) bool {
		if segment.MaxRowNum == maxRow {
			return false
		}
		segment.MaxRowNum = maxRow
		return true
	})
}

// SetRowCount is the option to set row count for segment info
func SetRowCount(rowCount int64) SegmentOperator {
	return SegmentOperatorFunc(func(segment *SegmentInfo) bool {
		if segment.NumOfRows == rowCount {
			return false
		}
		segment.NumOfRows = rowCount
		return true
	})
}

// SetExpireTime is the option to set expire time for segment info
func SetExpireTime(expireTs typeutil.Timestamp) SegmentOperator {
	return SegmentOperatorFunc(func(segment *SegmentInfo) bool {
		if segment.LastExpireTime == expireTs {
			return false
		}
		segment.LastExpireTime = expireTs
		return true
	})
}

// SetState is the option to set state for segment info
func SetState(state commonpb.SegmentState) SegmentOperator {
	return SegmentOperatorFunc(func(segment *SegmentInfo) bool {
		if segment.State == state {
			return false
		}
		segment.State = state
		return true
	})
}

// SetDmlPosition is the option to set dml position for segment info
func SetDmlPosition(pos *msgpb.MsgPosition) SegmentOperator {
	return SegmentOperatorFunc(func(segment *SegmentInfo) bool {
		if proto.Equal(segment.GetDmlPosition(), pos) {
			return false
		}
		segment.DmlPosition = pos
		return true
	})
}

// SetStartPosition is the option to set start position for segment info
func SetStartPosition(pos *msgpb.MsgPosition) SegmentOperator {
	return SegmentOperatorFunc(func(segment *SegmentInfo) bool {
		if proto.Equal(segment.GetStartPosition(), pos) {
			return false
		}
		segment.StartPosition = pos
		return true
	})
}

// SetAllocations is the option to set allocations for segment info
func SetAllocations(allocations []*Allocation) SegmentOperator {
	return SegmentOperatorShallowFunc(func(segment *SegmentInfo) bool {
		segment.allocations = allocations
		return true
	})
}

// AddAllocation is the option to add allocation info for segment info
func AddAllocation(allocation *Allocation) SegmentOperator {
	return SegmentOperatorShallowFunc(func(segment *SegmentInfo) bool {
		segment.allocations = append(segment.allocations, allocation)
		segment.LastExpireTime = allocation.ExpireTime
		return true
	})
}

// SetCurrentRows is the option to set current row count for segment info
func SetCurrentRows(rows int64) SegmentOperator {
	return SegmentOperatorShallowFunc(func(segment *SegmentInfo) bool {
		segment.currRows = rows
		segment.lastWrittenTime = time.Now()
		return true
	})
}

func SetLastWrittenTime(lastWritten time.Time) SegmentOperator {
	return SegmentOperatorShallowFunc(func(segment *SegmentInfo) bool {
		segment.lastWrittenTime = lastWritten
		return true
	})
}

// SetFlushTime is the option to set flush time for segment info
func SetFlushTime(t time.Time) SegmentOperator {
	return SegmentOperatorShallowFunc(func(segment *SegmentInfo) bool {
		segment.lastFlushTime = t
		return true
	})
}

// SetIsCompacting is the option to set compaction state for segment info
func SetIsCompacting(isCompacting bool) SegmentOperator {
	return SegmentOperatorShallowFunc(func(segment *SegmentInfo) bool {
		segment.isCompacting = isCompacting
		return true
	})
}

// SetLevel is the option to set level for segment info
func SetLevel(level datapb.SegmentLevel) SegmentOperator {
	return SegmentOperatorFunc(func(segment *SegmentInfo) bool {
		segment.Level = level
		return true
	})
}
