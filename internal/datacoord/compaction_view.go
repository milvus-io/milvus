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
)

type CompactionView interface {
	GetGroupLabel() *CompactionGroupLabel
	GetSegmentsView() []*SegmentView
	String() string
}

type FullViews struct {
	collections map[int64][]*SegmentView // collectionID
}

type SegmentViewSelector func(view *SegmentView) bool

func (v *FullViews) GetSegmentViewBy(collectionID UniqueID, selector SegmentViewSelector) []*SegmentView {
	views, ok := v.collections[collectionID]
	if !ok {
		return nil
	}

	var ret []*SegmentView

	for _, view := range views {
		if selector == nil || selector(view) {
			ret = append(ret, view.Clone())
		}
	}

	return ret
}

type CompactionGroupLabel struct {
	CollectionID UniqueID
	PartitionID  UniqueID
	Channel      string
}

func (label *CompactionGroupLabel) Key() string {
	return fmt.Sprintf("%d-%s", label.PartitionID, label.Channel)
}

func (label *CompactionGroupLabel) IsMinGroup() bool {
	return len(label.Channel) != 0 && label.PartitionID != 0 && label.CollectionID != 0
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
	startPos *msgpb.MsgPosition
	dmlPos   *msgpb.MsgPosition

	// size
	Size       float64
	ExpireSize float64
	DeltaSize  float64

	// row count
	NumOfRows     int64
	DeltaRowCount int64
	MaxRowNum     int64

	// file numbers
	BinlogCount   int64
	StatslogCount int64
	DeltalogCount int64
}

func (s *SegmentView) Clone() *SegmentView {
	return &SegmentView{
		ID:            s.ID,
		label:         s.label,
		State:         s.State,
		Level:         s.Level,
		startPos:      s.startPos,
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

func GetViewsByInfo(segments ...*SegmentInfo) []*SegmentView {
	return lo.Map(segments, func(segment *SegmentInfo, _ int) *SegmentView {
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
			startPos: segment.GetStartPosition(),
			dmlPos:   segment.GetDmlPosition(),

			Size:      GetBinlogSizeAsBytes(segment.GetBinlogs()),
			DeltaSize: GetBinlogSizeAsBytes(segment.GetDeltalogs()),
			// TODO: set the following
			// ExpireSize float64

			DeltalogCount: GetBinlogCount(segment.GetDeltalogs()),
			BinlogCount:   GetBinlogCount(segment.GetBinlogs()),
			StatslogCount: GetBinlogCount(segment.GetStatslogs()),

			NumOfRows:     segment.NumOfRows,
			MaxRowNum:     segment.MaxRowNum,
			DeltaRowCount: GetBinlogEntriesNum(segment.GetDeltalogs()),
		}
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

func GetBinlogCount(fieldBinlogs []*datapb.FieldBinlog) int64 {
	var num int64
	for _, binlog := range fieldBinlogs {
		num += int64(len(binlog.GetBinlogs()))
	}
	return num
}

func GetBinlogEntriesNum(fieldBinlogs []*datapb.FieldBinlog) int64 {
	var num int64
	for _, fbinlog := range fieldBinlogs {
		for _, binlog := range fbinlog.GetBinlogs() {
			num += binlog.GetEntriesNum()
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

var _ CompactionView = (*MixSegmentView)(nil)

type MixSegmentView struct {
	label         *CompactionGroupLabel
	segments      []*SegmentView
	collectionTTL time.Duration
	triggerID     int64
}

func (v *MixSegmentView) GetGroupLabel() *CompactionGroupLabel {
	if v == nil {
		return &CompactionGroupLabel{}
	}
	return v.label
}

func (v *MixSegmentView) GetSegmentsView() []*SegmentView {
	if v == nil {
		return nil
	}

	return v.segments
}

func (v *MixSegmentView) Append(segments ...*SegmentView) {
	if v.segments == nil {
		v.segments = segments
		return
	}

	v.segments = append(v.segments, segments...)
}

func (v *MixSegmentView) String() string {
	strs := lo.Map(v.segments, func(segView *SegmentView, _ int) string {
		return segView.String()
	})
	return fmt.Sprintf("label=<%s>,  segments=%v", v.label.String(), strs)
}

func (v *MixSegmentView) Trigger() (CompactionView, string) {
	return v, ""
}

func (v *MixSegmentView) ForceTrigger() (CompactionView, string) {
	panic("implement me")
}
