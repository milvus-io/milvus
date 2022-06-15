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
	"sync"
	"testing"
	"time"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/assert"
)

// TODO not completed

func Test_compactionPlanHandler_execCompactionPlan(t *testing.T) {
	ch := make(chan interface{}, 1)
	type fields struct {
		plans     map[int64]*compactionTask
		sessions  *SessionManager
		chManager *ChannelManager
	}
	type args struct {
		signal *compactionSignal
		plan   *datapb.CompactionPlan
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		err     error
	}{
		{
			"test exec compaction",
			fields{
				plans: map[int64]*compactionTask{},
				sessions: &SessionManager{
					sessions: struct {
						sync.RWMutex
						data map[int64]*Session
					}{
						data: map[int64]*Session{
							1: {client: &mockDataNodeClient{ch: ch}},
						},
					},
				},
				chManager: &ChannelManager{
					store: &ChannelStore{
						channelsInfo: map[int64]*NodeChannelInfo{
							1: {NodeID: 1, Channels: []*channel{{Name: "ch1"}}},
						},
					},
				},
			},
			args{
				signal: &compactionSignal{id: 100},
				plan:   &datapb.CompactionPlan{PlanID: 1, Channel: "ch1", Type: datapb.CompactionType_MergeCompaction},
			},
			false,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compactionPlanHandler{
				plans:     tt.fields.plans,
				sessions:  tt.fields.sessions,
				chManager: tt.fields.chManager,
			}
			err := c.execCompactionPlan(tt.args.signal, tt.args.plan)
			assert.Equal(t, tt.err, err)
			if err == nil {
				<-ch
				task := c.getCompaction(tt.args.plan.PlanID)
				assert.Equal(t, tt.args.plan, task.plan)
				assert.Equal(t, tt.args.signal, task.triggerInfo)
			}
		})
	}
}

func Test_compactionPlanHandler_completeCompaction(t *testing.T) {
	type fields struct {
		plans    map[int64]*compactionTask
		sessions *SessionManager
		meta     *meta
		flushCh  chan UniqueID
	}
	type args struct {
		result *datapb.CompactionResult
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		want    *compactionTask
	}{
		{
			"test complete non existed compaction task",
			fields{
				plans: map[int64]*compactionTask{1: {}},
			},
			args{
				result: &datapb.CompactionResult{PlanID: 2},
			},
			true,
			nil,
		},
		{
			"test complete completed task",
			fields{
				plans: map[int64]*compactionTask{1: {state: completed}},
			},
			args{
				result: &datapb.CompactionResult{PlanID: 1},
			},
			true,
			nil,
		},
		{
			"test complete inner compaction",
			fields{
				map[int64]*compactionTask{
					1: {
						triggerInfo: &compactionSignal{id: 1},
						state:       executing,
						plan: &datapb.CompactionPlan{
							PlanID: 1,
							SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
								{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}},
							},
							Type: datapb.CompactionType_InnerCompaction,
						},
					},
				},
				nil,
				&meta{
					client: memkv.NewMemoryKV(),
					segments: &SegmentsInfo{
						map[int64]*SegmentInfo{
							1: {SegmentInfo: &datapb.SegmentInfo{ID: 1, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}}},
						},
					},
				},
				make(chan UniqueID, 1),
			},
			args{
				result: &datapb.CompactionResult{
					PlanID:     1,
					SegmentID:  1,
					InsertLogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log2")},
				},
			},
			false,
			&compactionTask{
				triggerInfo: &compactionSignal{id: 1},
				state:       completed,
				plan: &datapb.CompactionPlan{
					PlanID: 1,
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}},
					},
					Type: datapb.CompactionType_InnerCompaction,
				},
			},
		},
		{
			"test complete merge compaction",
			fields{
				map[int64]*compactionTask{
					1: {
						triggerInfo: &compactionSignal{id: 1},
						state:       executing,
						plan: &datapb.CompactionPlan{
							PlanID: 1,
							SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{

								{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}},
								{SegmentID: 2, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log2")}},
							},
							Type: datapb.CompactionType_MergeCompaction,
						},
					},
				},
				nil,
				&meta{
					client: memkv.NewMemoryKV(),
					segments: &SegmentsInfo{
						map[int64]*SegmentInfo{

							1: {SegmentInfo: &datapb.SegmentInfo{ID: 1, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}}},
							2: {SegmentInfo: &datapb.SegmentInfo{ID: 2, Binlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log2")}}},
						},
					},
				},
				make(chan UniqueID, 1),
			},
			args{
				result: &datapb.CompactionResult{
					PlanID:     1,
					SegmentID:  3,
					InsertLogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log3")},
				},
			},
			false,
			&compactionTask{
				triggerInfo: &compactionSignal{id: 1},
				state:       completed,
				plan: &datapb.CompactionPlan{
					PlanID: 1,
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log1")}},
						{SegmentID: 2, FieldBinlogs: []*datapb.FieldBinlog{getFieldBinlogPaths(1, "log2")}},
					},
					Type: datapb.CompactionType_MergeCompaction,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compactionPlanHandler{
				plans:    tt.fields.plans,
				sessions: tt.fields.sessions,
				meta:     tt.fields.meta,
				flushCh:  tt.fields.flushCh,
			}
			err := c.completeCompaction(tt.args.result)
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func Test_compactionPlanHandler_getCompaction(t *testing.T) {
	type fields struct {
		plans    map[int64]*compactionTask
		sessions *SessionManager
	}
	type args struct {
		planID int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *compactionTask
	}{
		{
			"test get non existed task",
			fields{plans: map[int64]*compactionTask{}},
			args{planID: 1},
			nil,
		},
		{
			"test get existed task",
			fields{
				plans: map[int64]*compactionTask{1: {
					state: executing,
				}},
			},
			args{planID: 1},
			&compactionTask{
				state: executing,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compactionPlanHandler{
				plans:    tt.fields.plans,
				sessions: tt.fields.sessions,
			}
			got := c.getCompaction(tt.args.planID)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func Test_compactionPlanHandler_expireCompaction(t *testing.T) {
	type fields struct {
		plans    map[int64]*compactionTask
		sessions *SessionManager
		meta     *meta
	}
	type args struct {
		ts Timestamp
	}

	ts := time.Now()
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		expired   []int64
		unexpired []int64
	}{
		{
			"test expire compaction task",
			fields{
				plans: map[int64]*compactionTask{
					1: {
						state: executing,
						plan: &datapb.CompactionPlan{
							PlanID:           1,
							StartTime:        tsoutil.ComposeTS(ts.UnixNano()/int64(time.Millisecond), 0),
							TimeoutInSeconds: 10,
							SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
								{SegmentID: 1},
							},
						},
					},
					2: {
						state: executing,
						plan: &datapb.CompactionPlan{
							PlanID:           2,
							StartTime:        tsoutil.ComposeTS(ts.UnixNano()/int64(time.Millisecond), 0),
							TimeoutInSeconds: 1,
						},
					},
				},
				meta: &meta{
					segments: &SegmentsInfo{
						map[int64]*SegmentInfo{
							1: {SegmentInfo: &datapb.SegmentInfo{ID: 1}},
						},
					},
				},
			},
			args{ts: tsoutil.ComposeTS(ts.Add(5*time.Second).UnixNano()/int64(time.Millisecond), 0)},
			false,
			[]int64{2},
			[]int64{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compactionPlanHandler{
				plans:    tt.fields.plans,
				sessions: tt.fields.sessions,
				meta:     tt.fields.meta,
			}

			err := c.expireCompaction(tt.args.ts)
			assert.Equal(t, tt.wantErr, err != nil)

			for _, id := range tt.expired {
				task := c.getCompaction(id)
				assert.Equal(t, timeout, task.state)
			}

			for _, id := range tt.unexpired {
				task := c.getCompaction(id)
				assert.NotEqual(t, timeout, task.state)
			}
		})
	}
}

func Test_newCompactionPlanHandler(t *testing.T) {
	type args struct {
		sessions  *SessionManager
		cm        *ChannelManager
		meta      *meta
		allocator allocator
		flush     chan UniqueID
	}
	tests := []struct {
		name string
		args args
		want *compactionPlanHandler
	}{
		{
			"test new handler",
			args{
				&SessionManager{},
				&ChannelManager{},
				&meta{},
				newMockAllocator(),
				nil,
			},
			&compactionPlanHandler{
				plans:     map[int64]*compactionTask{},
				sessions:  &SessionManager{},
				chManager: &ChannelManager{},
				meta:      &meta{},
				allocator: newMockAllocator(),
				flushCh:   nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newCompactionPlanHandler(tt.args.sessions, tt.args.cm, tt.args.meta, tt.args.allocator, tt.args.flush)
			assert.EqualValues(t, tt.want, got)
		})
	}
}

func Test_getCompactionTasksBySignalID(t *testing.T) {
	type fields struct {
		plans map[int64]*compactionTask
	}
	type args struct {
		signalID int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []*compactionTask
	}{
		{
			"test get compaction tasks",
			fields{
				plans: map[int64]*compactionTask{
					1: {
						triggerInfo: &compactionSignal{id: 1},
						state:       executing,
					},
					2: {
						triggerInfo: &compactionSignal{id: 1},
						state:       completed,
					},
					3: {
						triggerInfo: &compactionSignal{id: 1},
						state:       timeout,
					},
				},
			},
			args{1},
			[]*compactionTask{
				{
					triggerInfo: &compactionSignal{id: 1},
					state:       executing,
				},
				{
					triggerInfo: &compactionSignal{id: 1},
					state:       completed,
				},
				{
					triggerInfo: &compactionSignal{id: 1},
					state:       timeout,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &compactionPlanHandler{
				plans: tt.fields.plans,
			}
			got := h.getCompactionTasksBySignalID(tt.args.signalID)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

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
