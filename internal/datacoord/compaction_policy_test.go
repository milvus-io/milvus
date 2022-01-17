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
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

func getFieldBinlogPaths(id int64, paths ...string) *datapb.FieldBinlog {
	l := &datapb.FieldBinlog{
		FieldID: id,
		Binlogs: make([]*datapb.Binlog, 0, len(paths)),
	}
	for _, path := range paths {
		l.Binlogs = append(l.Binlogs, &datapb.Binlog{LogPath: path})
	}
	return l
}

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

					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 1, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3")}}},
				},
				&timetravel{1000},
			},
			[]*datapb.CompactionPlan{
				{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}},
						{SegmentID: 2, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3")}},
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
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: "log1"}}}}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 99, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: "log2"}}}}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 99, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []*datapb.Binlog{{LogPath: "log3"}}}}}},
				},
				&timetravel{1000},
			},
			[]*datapb.CompactionPlan{
				{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}},
						{SegmentID: 2, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log2")}},
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
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 50, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 50, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log2")}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 50, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3")}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 4, NumOfRows: 50, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log4")}}},
				},
				&timetravel{1000},
			},
			[]*datapb.CompactionPlan{
				{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}},
						{SegmentID: 2, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log2")}},
					},
					Type:       datapb.CompactionType_MergeCompaction,
					Timetravel: 1000,
				},
				{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 3, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3")}},
						{SegmentID: 4, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log4")}},
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
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 50, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 51, MaxRowNum: 100, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3")}}},
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

func Test_chooseAllBinlogs(t *testing.T) {
	type args struct {
		segment    *SegmentInfo
		timetravel *timetravel
	}
	tests := []struct {
		name string
		args args
		want *datapb.CompactionPlan
	}{
		{
			"test no delta logs",
			args{
				segment: &SegmentInfo{
					SegmentInfo: &datapb.SegmentInfo{
						ID:        1,
						Deltalogs: nil,
					},
				},
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chooseAllBinlogs(tt.args.segment, tt.args.timetravel)
			assert.EqualValues(t, tt.want, got)
		})
	}
}
