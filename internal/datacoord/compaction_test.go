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
		plan *datapb.CompactionPlan
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
				plan: &datapb.CompactionPlan{PlanID: 1, Channel: "ch1", Type: datapb.CompactionType_MergeCompaction},
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
			err := c.execCompactionPlan(tt.args.plan)
			assert.Equal(t, tt.err, err)
			if err == nil {
				<-ch
				task := c.getCompaction(tt.args.plan.PlanID)
				assert.Equal(t, tt.args.plan, task.plan)
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
		},
		{
			"test complete inner compaction",
			fields{
				map[int64]*compactionTask{
					1: {
						state: executing,
						plan: &datapb.CompactionPlan{
							PlanID: 1,
							SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
								{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}},
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
							1: {SegmentInfo: &datapb.SegmentInfo{ID: 1, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}}},
						},
					},
				},
				make(chan UniqueID, 1),
			},
			args{
				result: &datapb.CompactionResult{
					PlanID:     1,
					SegmentID:  1,
					InsertLogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log2"}}},
				},
			},
			false,
		},
		{
			"test complete merge compaction",
			fields{
				map[int64]*compactionTask{
					1: {
						state: executing,
						plan: &datapb.CompactionPlan{
							PlanID: 1,
							SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
								{SegmentID: 1, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}},
								{SegmentID: 2, FieldBinlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log2"}}}},
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
							1: {SegmentInfo: &datapb.SegmentInfo{ID: 1, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log1"}}}}},
							2: {SegmentInfo: &datapb.SegmentInfo{ID: 2, Binlogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log2"}}}}},
						},
					},
				},
				make(chan UniqueID, 1),
			},
			args{
				result: &datapb.CompactionResult{
					PlanID:     1,
					SegmentID:  3,
					InsertLogs: []*datapb.FieldBinlog{{FieldID: 1, Binlogs: []string{"log3"}}},
				},
			},
			false,
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
