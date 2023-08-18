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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"

	mockkv "github.com/milvus-io/milvus/internal/kv/mocks"
)

func Test_compactionPlanHandler_execCompactionPlan(t *testing.T) {
	type fields struct {
		plans            map[int64]*compactionTask
		sessions         *SessionManager
		chManager        *ChannelManager
		allocatorFactory func() allocator
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
							1: {client: &mockDataNodeClient{ch: make(chan interface{}, 1)}},
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
				allocatorFactory: func() allocator { return newMockAllocator() },
			},
			args{
				signal: &compactionSignal{id: 100},
				plan:   &datapb.CompactionPlan{PlanID: 1, Channel: "ch1", Type: datapb.CompactionType_MergeCompaction},
			},
			false,
			nil,
		},
		{
			"test exec compaction failed",
			fields{
				plans: map[int64]*compactionTask{},
				sessions: &SessionManager{
					sessions: struct {
						sync.RWMutex
						data map[int64]*Session
					}{
						data: map[int64]*Session{
							1: {client: &mockDataNodeClient{ch: make(chan interface{}, 1), compactionResp: &commonpb.Status{ErrorCode: commonpb.ErrorCode_CacheFailed}}},
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
				allocatorFactory: func() allocator { return newMockAllocator() },
			},
			args{
				signal: &compactionSignal{id: 100},
				plan:   &datapb.CompactionPlan{PlanID: 1, Channel: "ch1", Type: datapb.CompactionType_MergeCompaction},
			},
			true,
			nil,
		},
		{
			"test_allocate_ts_failed",
			fields{
				plans: map[int64]*compactionTask{},
				sessions: &SessionManager{
					sessions: struct {
						sync.RWMutex
						data map[int64]*Session
					}{
						data: map[int64]*Session{
							1: {client: &mockDataNodeClient{ch: make(chan interface{}, 1), compactionResp: &commonpb.Status{ErrorCode: commonpb.ErrorCode_CacheFailed}}},
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
				allocatorFactory: func() allocator {
					a := &NMockAllocator{}
					start := time.Now()
					a.EXPECT().allocTimestamp(mock.Anything).Call.Return(func(_ context.Context) Timestamp {
						return tsoutil.ComposeTSByTime(time.Now(), 0)
					}, func(_ context.Context) error {
						if time.Since(start) > time.Second*2 {
							return nil
						}
						return errors.New("mocked")
					})
					return a
				},
			},
			args{
				signal: &compactionSignal{id: 100},
				plan:   &datapb.CompactionPlan{PlanID: 1, Channel: "ch1", Type: datapb.CompactionType_MergeCompaction},
			},
			true,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, err := newMemoryMeta()
			require.NoError(t, err)
			c := newCompactionPlanHandler(
				tt.fields.sessions,
				tt.fields.chManager,
				meta,
				tt.fields.allocatorFactory(),
				make(chan UniqueID, 10),
			)
			Params.Save(Params.DataCoordCfg.CompactionCheckIntervalInSeconds.Key, "1")
			c.start()
			err = c.execCompactionPlan(tt.args.signal, tt.args.plan)
			assert.Equal(t, tt.err, err)
			if err == nil {
				task := c.getCompaction(tt.args.plan.PlanID)
				if !tt.wantErr {
					assert.Equal(t, tt.args.plan, task.plan)
					assert.Equal(t, tt.args.signal, task.triggerInfo)
					assert.Equal(t, int64(1), c.executingTaskNum.Load())
				} else {

					assert.Eventually(t,
						func() bool {
							c.plansGuard.RLock()
							defer c.plansGuard.RUnlock()
							ch, ok := c.parallelCh.Get(1)
							assert.True(t, ok)
							return c.executingTaskNum.Load() == int64(0) && len(ch) == 0
						},
						5*time.Second, 100*time.Millisecond)
				}
			}
			c.stop()
		})
	}
}

func Test_compactionPlanHandler_execWithParallels(t *testing.T) {

	mockDataNode := &mocks.MockDataNode{}
	paramtable.Get().Save(Params.DataCoordCfg.CompactionCheckIntervalInSeconds.Key, "1")
	defer paramtable.Get().Reset(Params.DataCoordCfg.CompactionCheckIntervalInSeconds.Key)

	meta, err := newMemoryMeta()
	require.NoError(t, err)
	c := newCompactionPlanHandler(
		&SessionManager{
			sessions: struct {
				sync.RWMutex
				data map[int64]*Session
			}{
				data: map[int64]*Session{
					1: {client: mockDataNode},
				},
			},
		},
		&ChannelManager{
			store: &ChannelStore{
				channelsInfo: map[int64]*NodeChannelInfo{
					1: {NodeID: 1, Channels: []*channel{{Name: "ch1"}}},
				},
			},
		},
		meta,
		newMockAllocator(),
		make(chan UniqueID, 10),
	)

	signal := &compactionSignal{id: 100}
	plan1 := &datapb.CompactionPlan{PlanID: 1, Channel: "ch1", Type: datapb.CompactionType_MergeCompaction}
	plan2 := &datapb.CompactionPlan{PlanID: 2, Channel: "ch1", Type: datapb.CompactionType_MergeCompaction}
	plan3 := &datapb.CompactionPlan{PlanID: 3, Channel: "ch1", Type: datapb.CompactionType_MergeCompaction}

	c.parallelCh.Insert(1, make(chan struct{}, 2))

	var mut sync.RWMutex
	called := 0

	mockDataNode.EXPECT().Compaction(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *datapb.CompactionPlan) {
		mut.Lock()
		defer mut.Unlock()
		called++
	}).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil).Times(3)
	go func() {
		c.execCompactionPlan(signal, plan1)
		c.execCompactionPlan(signal, plan2)
		c.execCompactionPlan(signal, plan3)
	}()

	// wait for dispatch signal
	ch, ok := c.parallelCh.Get(1)
	assert.True(t, ok)
	for i := 0; i < 3; i++ {
		<-ch
	}

	// wait for compaction called
	assert.Eventually(t, func() bool {
		mut.RLock()
		defer mut.RUnlock()
		return called == 3
	}, time.Second, time.Millisecond*10)

	tasks := c.getCompactionTasksBySignalID(0)
	max, min := uint64(0), uint64(0)
	for _, v := range tasks {
		if max < v.plan.GetStartTime() {
			max = v.plan.GetStartTime()
		}
		if min > v.plan.GetStartTime() {
			min = v.plan.GetStartTime()
		}
	}

	log.Debug("start time", zap.Uint64("min", min), zap.Uint64("max", max))
	assert.Less(t, uint64(2), max-min)
}

func getInsertLogPath(rootPath string, segmentID typeutil.UniqueID) string {
	return metautil.BuildInsertLogPath(rootPath, 10, 100, segmentID, 1000, 10000)
}

func getStatsLogPath(rootPath string, segmentID typeutil.UniqueID) string {
	return metautil.BuildStatsLogPath(rootPath, 10, 100, segmentID, 1000, 10000)
}

func getDeltaLogPath(rootPath string, segmentID typeutil.UniqueID) string {
	return metautil.BuildDeltaLogPath(rootPath, 10, 100, segmentID, 10000)
}

func Test_compactionPlanHandler_handleMergeCompactionResult(t *testing.T) {
	mockDataNode := &mocks.MockDataNode{}
	call := mockDataNode.EXPECT().SyncSegments(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *datapb.SyncSegmentsRequest) {}).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)

	dataNodeID := UniqueID(111)

	seg1 := &datapb.SegmentInfo{
		ID:        1,
		Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(101, getInsertLogPath("log1", 1))},
		Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getStatsLogPath("log2", 1))},
		Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getDeltaLogPath("log3", 1))},
	}

	seg2 := &datapb.SegmentInfo{
		ID:        2,
		Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(101, getInsertLogPath("log4", 2))},
		Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getStatsLogPath("log5", 2))},
		Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getDeltaLogPath("log6", 2))},
	}

	plan := &datapb.CompactionPlan{
		PlanID: 1,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID:           seg1.ID,
				FieldBinlogs:        seg1.GetBinlogs(),
				Field2StatslogPaths: seg1.GetStatslogs(),
				Deltalogs:           seg1.GetDeltalogs(),
			},
			{
				SegmentID:           seg2.ID,
				FieldBinlogs:        seg2.GetBinlogs(),
				Field2StatslogPaths: seg2.GetStatslogs(),
				Deltalogs:           seg2.GetDeltalogs(),
			},
		},
		Type: datapb.CompactionType_MergeCompaction,
	}

	sessions := &SessionManager{
		sessions: struct {
			sync.RWMutex
			data map[int64]*Session
		}{
			data: map[int64]*Session{
				dataNodeID: {client: mockDataNode}},
		},
	}

	task := &compactionTask{
		triggerInfo: &compactionSignal{id: 1},
		state:       executing,
		plan:        plan,
		dataNodeID:  dataNodeID,
	}

	plans := map[int64]*compactionTask{1: task}

	metakv := mockkv.NewMetaKv(t)
	metakv.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("failed")).Maybe()
	metakv.EXPECT().MultiSave(mock.Anything).Return(errors.New("failed")).Maybe()
	metakv.EXPECT().HasPrefix(mock.Anything).Return(false, nil).Maybe()
	errMeta := &meta{
		catalog: &datacoord.Catalog{MetaKv: metakv},
		segments: &SegmentsInfo{
			map[int64]*SegmentInfo{
				seg1.ID: {SegmentInfo: seg1},
				seg2.ID: {SegmentInfo: seg2},
			},
		},
	}

	meta := &meta{
		catalog: &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
		segments: &SegmentsInfo{
			map[int64]*SegmentInfo{
				seg1.ID: {SegmentInfo: seg1},
				seg2.ID: {SegmentInfo: seg2},
			},
		},
	}
	c := newCompactionPlanHandler(
		sessions,
		&ChannelManager{
			store: &ChannelStore{
				channelsInfo: map[int64]*NodeChannelInfo{
					1: {NodeID: 1, Channels: []*channel{{Name: "ch1"}}},
				},
			},
		},
		meta,
		newMockAllocator(),
		make(chan UniqueID, 10),
	)

	c.plans = plans

	c2 := newCompactionPlanHandler(
		sessions,
		&ChannelManager{
			store: &ChannelStore{
				channelsInfo: map[int64]*NodeChannelInfo{
					1: {NodeID: 1, Channels: []*channel{{Name: "ch1"}}},
				},
			},
		},
		errMeta,
		newMockAllocator(),
		make(chan UniqueID, 10),
	)

	c2.plans = plans

	compactionResult := &datapb.CompactionResult{
		PlanID:              1,
		SegmentID:           3,
		NumOfRows:           15,
		InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogPaths(101, getInsertLogPath("log301", 3))},
		Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getStatsLogPath("log302", 3))},
		Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(101, getDeltaLogPath("log303", 3))},
	}

	compactionResult2 := &datapb.CompactionResult{
		PlanID:              1,
		SegmentID:           3,
		NumOfRows:           0,
		InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogPaths(101, getInsertLogPath("log301", 3))},
		Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getStatsLogPath("log302", 3))},
		Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(101, getDeltaLogPath("log303", 3))},
	}

	has, err := c.meta.HasSegments([]UniqueID{1, 2})
	require.NoError(t, err)
	require.True(t, has)

	has, err = c.meta.HasSegments([]UniqueID{3})
	require.Error(t, err)
	require.False(t, has)

	err = c.handleMergeCompactionResult(plan, compactionResult)
	assert.NoError(t, err)

	err = c.handleMergeCompactionResult(plan, compactionResult2)
	assert.NoError(t, err)

	err = c2.handleMergeCompactionResult(plan, compactionResult2)
	assert.Error(t, err)

	has, err = c.meta.HasSegments([]UniqueID{1, 2, 3})
	require.NoError(t, err)
	require.True(t, has)

	call.Unset()
	call = mockDataNode.EXPECT().SyncSegments(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *datapb.SyncSegmentsRequest) {}).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_UnexpectedError}, nil)
	err = c.handleMergeCompactionResult(plan, compactionResult2)
	assert.Error(t, err)
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

// TODO
func Test_compactionPlanHandler_updateCompaction(t *testing.T) {
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
		timeout   []int64
		failed    []int64
		unexpired []int64
	}{
		{
			"test update compaction task",
			fields{
				plans: map[int64]*compactionTask{
					1: {
						state:      executing,
						dataNodeID: 1,
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
						state:      executing,
						dataNodeID: 2,
						plan: &datapb.CompactionPlan{
							PlanID:           2,
							StartTime:        tsoutil.ComposeTS(ts.UnixNano()/int64(time.Millisecond), 0),
							TimeoutInSeconds: 1,
						},
					},
					3: {
						state:      executing,
						dataNodeID: 2,
						plan: &datapb.CompactionPlan{
							PlanID:           3,
							StartTime:        tsoutil.ComposeTS(ts.UnixNano()/int64(time.Millisecond), 0),
							TimeoutInSeconds: 1,
						},
					},
					4: {
						state:      executing,
						dataNodeID: 2,
						plan: &datapb.CompactionPlan{
							PlanID:           4,
							StartTime:        tsoutil.ComposeTS(ts.UnixNano()/int64(time.Millisecond), 0) - 200*1000,
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
				sessions: &SessionManager{
					sessions: struct {
						sync.RWMutex
						data map[int64]*Session
					}{
						data: map[int64]*Session{
							1: {client: &mockDataNodeClient{
								compactionStateResp: &datapb.CompactionStateResponse{
									Results: []*datapb.CompactionStateResult{
										{PlanID: 1, State: commonpb.CompactionState_Executing},
										{PlanID: 4, State: commonpb.CompactionState_Executing},
									},
								},
							}},
						},
					},
				},
			},
			args{ts: tsoutil.ComposeTS(ts.Add(5*time.Second).UnixNano()/int64(time.Millisecond), 0)},
			[]int64{4},
			[]int64{2, 3},
			[]int64{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &compactionPlanHandler{
				plans:            tt.fields.plans,
				sessions:         tt.fields.sessions,
				meta:             tt.fields.meta,
				executingTaskNum: atomic.NewInt64(4),
				parallelCh:       typeutil.NewConcurrentMap[int64, chan struct{}](),
			}

			c.updateCompaction(tt.args.ts)

			for _, id := range tt.timeout {
				task := c.getCompaction(id)
				assert.Equal(t, timeout, task.state)
			}

			for _, id := range tt.failed {
				task := c.getCompaction(id)
				assert.Equal(t, failed, task.state)
			}

			for _, id := range tt.unexpired {
				task := c.getCompaction(id)
				assert.NotEqual(t, failed, task.state)
			}

			assert.Equal(t, int64(1), c.executingTaskNum.Load())
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
						state:       failed,
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
					state:       failed,
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
func Test_compactionPlanHandler_completeCompaction(t *testing.T) {
	t.Run("test complete merge compaction task", func(t *testing.T) {
		mockDataNode := &mocks.MockDataNode{}
		mockDataNode.EXPECT().SyncSegments(mock.Anything, mock.Anything).Run(func(ctx context.Context, req *datapb.SyncSegmentsRequest) {}).Return(&commonpb.Status{ErrorCode: commonpb.ErrorCode_Success}, nil)

		dataNodeID := UniqueID(111)

		seg1 := &datapb.SegmentInfo{
			ID:        1,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(101, getInsertLogPath("log1", 1))},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getStatsLogPath("log2", 1))},
			Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getDeltaLogPath("log3", 1))},
		}

		seg2 := &datapb.SegmentInfo{
			ID:        2,
			Binlogs:   []*datapb.FieldBinlog{getFieldBinlogPaths(101, getInsertLogPath("log4", 2))},
			Statslogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getStatsLogPath("log5", 2))},
			Deltalogs: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getDeltaLogPath("log6", 2))},
		}

		plan := &datapb.CompactionPlan{
			PlanID: 1,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:           seg1.ID,
					FieldBinlogs:        seg1.GetBinlogs(),
					Field2StatslogPaths: seg1.GetStatslogs(),
					Deltalogs:           seg1.GetDeltalogs(),
				},
				{
					SegmentID:           seg2.ID,
					FieldBinlogs:        seg2.GetBinlogs(),
					Field2StatslogPaths: seg2.GetStatslogs(),
					Deltalogs:           seg2.GetDeltalogs(),
				},
			},
			StartTime:        tsoutil.GetCurrentTime(),
			TimeoutInSeconds: 5,
			Type:             datapb.CompactionType_MergeCompaction,
		}

		sessions := &SessionManager{
			sessions: struct {
				sync.RWMutex
				data map[int64]*Session
			}{
				data: map[int64]*Session{
					dataNodeID: {client: mockDataNode}},
			},
		}

		task := &compactionTask{
			triggerInfo: &compactionSignal{id: 1},
			state:       executing,
			plan:        plan,
			dataNodeID:  dataNodeID,
		}

		meta := &meta{
			catalog: &datacoord.Catalog{MetaKv: NewMetaMemoryKV()},
			segments: &SegmentsInfo{
				map[int64]*SegmentInfo{
					seg1.ID: {SegmentInfo: seg1},
					seg2.ID: {SegmentInfo: seg2},
				},
			},
		}
		compactionResult := datapb.CompactionResult{
			PlanID:              1,
			SegmentID:           3,
			InsertLogs:          []*datapb.FieldBinlog{getFieldBinlogPaths(101, getInsertLogPath("log301", 3))},
			Field2StatslogPaths: []*datapb.FieldBinlog{getFieldBinlogPaths(101, getStatsLogPath("log302", 3))},
			Deltalogs:           []*datapb.FieldBinlog{getFieldBinlogPaths(101, getDeltaLogPath("log303", 3))},
		}

		flushCh := make(chan UniqueID, 1)

		tests := []struct {
			description   string
			resultsState  commonpb.CompactionState
			resultNumRows int
		}{
			{"completed state", commonpb.CompactionState_Completed, 15},
			{"completed state with 0 numRow segment", commonpb.CompactionState_Completed, 0},
			{"executing state timeout", commonpb.CompactionState_Executing, 15},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				c := newCompactionPlanHandler(
					sessions,
					&ChannelManager{},
					meta,
					newMockAllocator(),
					flushCh,
				)

				c.plansGuard.Lock()
				c.addTask(1, task)
				c.plansGuard.Unlock()
				c.acquireQueue(dataNodeID)

				assert.Equal(t, int64(1), c.executingTaskNum.Load())
				ch, ok := c.parallelCh.Get(dataNodeID)
				assert.True(t, ok)
				assert.Equal(t, 1, len(ch))

				compactionResult.NumOfRows = int64(test.resultNumRows)
				stateResult := &datapb.CompactionStateResult{PlanID: 1, State: test.resultsState, Result: &compactionResult}
				err := c.completeCompaction(tsoutil.AddPhysicalDurationOnTs(tsoutil.GetCurrentTime(), 10*time.Second), task, stateResult)
				assert.NoError(t, err)

				if test.resultsState == commonpb.CompactionState_Completed {
					segID, ok := <-flushCh
					assert.True(t, ok)
					assert.Equal(t, compactionResult.GetSegmentID(), segID)
				}

				assert.Equal(t, int64(0), c.executingTaskNum.Load())

				ch, ok = c.parallelCh.Get(dataNodeID)
				assert.True(t, ok)
				assert.Equal(t, 0, len(ch))
			})
		}
	})
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

func getFieldBinlogPathsWithEntry(id int64, entry int64, paths ...string) *datapb.FieldBinlog {
	l := &datapb.FieldBinlog{
		FieldID: id,
		Binlogs: make([]*datapb.Binlog, 0, len(paths)),
	}
	for _, path := range paths {
		l.Binlogs = append(l.Binlogs, &datapb.Binlog{LogPath: path, EntriesNum: entry})
	}
	return l
}
