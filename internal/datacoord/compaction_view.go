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

import (
	"fmt"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
)

type CompactionView interface {
	GetGroupLabel() *CompactionGroupLabel
	GetSegmentsView() []*SegmentView
	Append(segments ...*SegmentView)
	String() string
	Trigger() (CompactionView, string)
	ForceTrigger() (CompactionView, string)
}

type CompactionGroupLabel struct {
	CollectionID UniqueID
	PartitionID  UniqueID
	Channel      string
}

func (label *CompactionGroupLabel) Key() string {
	return fmt.Sprintf("%d-%s", label.PartitionID, label.Channel)
}

func (label *CompactionGroupLabel) Equal(other *CompactionGroupLabel) bool {
	return other != nil &&
		other.CollectionID == label.CollectionID &&
		other.PartitionID == label.PartitionID &&
		other.Channel == label.Channel
}

func (label *CompactionGroupLabel) String() string {
	return fmt.Sprintf("coll=%d, part=%d, channel=%s", label.CollectionID, label.PartitionID, label.Channel)
}

type SegmentView struct {
	ID UniqueID

	label *CompactionGroupLabel

	State commonpb.SegmentState
	Level datapb.SegmentLevel

	// positions
	dmlPos *msgpb.MsgPosition

	// size
	Size       float64
	ExpireSize float64
	DeltaSize  float64

	NumOfRows int64
	MaxRowNum int64

	// file numbers
	BinlogCount   int
	StatslogCount int
	DeltalogCount int

	// row count
	DeltaRowCount int
}

func (s *SegmentView) Clone() *SegmentView {
	return &SegmentView{
		ID:            s.ID,
		label:         s.label,
		State:         s.State,
		Level:         s.Level,
		dmlPos:        s.dmlPos,
		Size:          s.Size,
		ExpireSize:    s.ExpireSize,
		DeltaSize:     s.DeltaSize,
		BinlogCount:   s.BinlogCount,
		StatslogCount: s.StatslogCount,
		DeltalogCount: s.DeltalogCount,
		DeltaRowCount: s.DeltaRowCount,
		NumOfRows:     s.NumOfRows,
		MaxRowNum:     s.MaxRowNum,
	}
}

var canonizer = common.NewCanonizer[*SegmentInfo, *SegmentView](8196, 10*time.Minute, func(segment *SegmentInfo) *SegmentView {
	return &SegmentView{
		ID: segment.ID,
		label: &CompactionGroupLabel{
			CollectionID: segment.CollectionID,
			PartitionID:  segment.PartitionID,
			Channel:      segment.GetInsertChannel(),
		},

		State: segment.GetState(),
		Level: segment.GetLevel(),

		// positions
		dmlPos: segment.GetDmlPosition(),

		DeltaSize:     GetBinlogSizeAsBytes(segment.GetDeltalogs()),
		DeltalogCount: GetBinlogCount(segment.GetDeltalogs()),
		DeltaRowCount: GetBinlogEntriesNum(segment.GetDeltalogs()),

		Size:          GetBinlogSizeAsBytes(segment.GetBinlogs()),
		BinlogCount:   GetBinlogCount(segment.GetBinlogs()),
		StatslogCount: GetBinlogCount(segment.GetStatslogs()),

		NumOfRows: segment.NumOfRows,
		MaxRowNum: segment.MaxRowNum,
		// TODO: set the following
		// ExpireSize float64
	}
})

func GetViewsByInfo(segments ...*SegmentInfo) []*SegmentView {
	return lo.Map(segments, func(segment *SegmentInfo, _ int) *SegmentView {
		o, _ := canonizer.Canonize(segment)
		return o
	})
}

func (v *SegmentView) Equal(other *SegmentView) bool {
	return v.Size == other.Size &&
		v.ExpireSize == other.ExpireSize &&
		v.DeltaSize == other.DeltaSize &&
		v.BinlogCount == other.BinlogCount &&
		v.StatslogCount == other.StatslogCount &&
		v.DeltalogCount == other.DeltalogCount &&
		v.NumOfRows == other.NumOfRows &&
		v.DeltaRowCount == other.DeltaRowCount
}

func (v *SegmentView) String() string {
	return fmt.Sprintf("ID=%d, label=<%s>, state=%s, level=%s, binlogSize=%.2f, binlogCount=%d, deltaSize=%.2f, deltalogCount=%d, deltaRowCount=%d, expireSize=%.2f",
		v.ID, v.label, v.State.String(), v.Level.String(), v.Size, v.BinlogCount, v.DeltaSize, v.DeltalogCount, v.DeltaRowCount, v.ExpireSize)
}

func (v *SegmentView) LevelZeroString() string {
	return fmt.Sprintf("<ID=%d, level=%s, deltaSize=%.2f, deltaLogCount=%d, deltaRowCount=%d>",
		v.ID, v.Level.String(), v.DeltaSize, v.DeltalogCount, v.DeltaRowCount)
}

func GetBinlogCount(fieldBinlogs []*datapb.FieldBinlog) int {
	var num int
	for _, binlog := range fieldBinlogs {
		num += len(binlog.GetBinlogs())
	}
	return num
}

func GetBinlogEntriesNum(fieldBinlogs []*datapb.FieldBinlog) int {
	var num int
	for _, fbinlog := range fieldBinlogs {
		for _, binlog := range fbinlog.GetBinlogs() {
			num += int(binlog.GetEntriesNum())
		}
	}
	return num
}

func GetBinlogSizeAsBytes(fieldBinlogs []*datapb.FieldBinlog) float64 {
	var deltaSize float64
	for _, deltaLogs := range fieldBinlogs {
		for _, l := range deltaLogs.GetBinlogs() {
			deltaSize += float64(l.GetMemorySize())
		}
	}
	return deltaSize
}
