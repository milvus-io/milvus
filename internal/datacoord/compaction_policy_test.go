package datacoord

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

func Test_greedyMergeCompaction(t *testing.T) {
	type args struct {
		segments   []*SegmentInfo
		timetravel *timetravel
	}
	tests := []struct {
		name string
		args args
		want []*datapb.CompactionPlan
	}{
		{
			"test normal merge",
			args{
				[]*SegmentInfo{
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 1, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log3"}}}}},
				},
				&timetravel{1000},
			},
			[]*datapb.CompactionPlan{
				{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}},
						{SegmentID: 2, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log3"}}}},
					},

					Type:       datapb.CompactionType_MergeCompaction,
					Timetravel: 1000,
				},
			},
		},
		{
			"test unmergable segments",
			args{
				[]*SegmentInfo{
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 99, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log2"}}}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 99, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log3"}}}}},
				},
				&timetravel{1000},
			},
			[]*datapb.CompactionPlan{
				{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}},
						{SegmentID: 2, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log2"}}}},
					},
					Type:       datapb.CompactionType_MergeCompaction,
					Timetravel: 1000,
				},
			},
		},
		{
			"test multi plans",
			args{
				[]*SegmentInfo{
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 50, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 50, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log2"}}}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 50, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log3"}}}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 50, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log4"}}}}},
				},
				&timetravel{1000},
			},
			[]*datapb.CompactionPlan{
				{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}},
						{SegmentID: 2, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log2"}}}},
					},
					Type:       datapb.CompactionType_MergeCompaction,
					Timetravel: 1000,
				},
				{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 3, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log3"}}}},
						{SegmentID: 4, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log4"}}}},
					},
					Type:       datapb.CompactionType_MergeCompaction,
					Timetravel: 1000,
				},
			},
		},
		{
			"test empty plan",
			args{
				[]*SegmentInfo{
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 50, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 51, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log3"}}}}},
				},
				&timetravel{1000},
			},
			[]*datapb.CompactionPlan{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := greedyMergeCompaction(tt.args.segments, tt.args.timetravel)
			assert.EqualValues(t, tt.want, got)
		})
	}
}
