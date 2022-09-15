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
	"sort"
	"testing"
	"time"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

type spyCompactionHandler struct {
	spyChan chan *datapb.CompactionPlan
}

// execCompactionPlan start to execute plan and return immediately
func (h *spyCompactionHandler) execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan) error {
	fmt.Println("plan ", plan.SegmentBinlogs)
	h.spyChan <- plan
	return nil
}

// completeCompaction record the result of a compaction
func (h *spyCompactionHandler) completeCompaction(result *datapb.CompactionResult) error {
	return nil
}

// getCompaction return compaction task. If planId does not exist, return nil.
func (h *spyCompactionHandler) getCompaction(planID int64) *compactionTask {
	panic("not implemented") // TODO: Implement
}

// expireCompaction set the compaction state to expired
func (h *spyCompactionHandler) updateCompaction(ts Timestamp) error {
	panic("not implemented") // TODO: Implement
}

// isFull return true if the task pool is full
func (h *spyCompactionHandler) isFull() bool {
	return false
}

// get compaction tasks by signal id
func (h *spyCompactionHandler) getCompactionTasksBySignalID(signalID int64) []*compactionTask {
	panic("not implemented") // TODO: Implement
}

func (h *spyCompactionHandler) start() {}

func (h *spyCompactionHandler) stop() {}

var _ compactionPlanContext = (*spyCompactionHandler)(nil)

func Test_compactionTrigger_force(t *testing.T) {
	type fields struct {
		meta              *meta
		allocator         allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
		segRefer          *SegmentReferenceManager
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	Params.Init()
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		wantPlans []*datapb.CompactionPlan
	}{
		{
			"test force compaction",
			fields{
				&meta{
					segments: &SegmentsInfo{
						map[int64]*SegmentInfo{
							1: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             1,
									CollectionID:   2,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      100,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
									Binlogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "log1"},
											},
										},
									},
									Deltalogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "deltalog1"},
											},
										},
									},
								},
							},
							2: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             2,
									CollectionID:   2,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      100,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
									Binlogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "log2"},
											},
										},
									},
									Deltalogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "deltalog2"},
											},
										},
									},
								},
							},
						},
					},
				},
				newMockAllocator(),
				nil,
				&spyCompactionHandler{spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
				&SegmentReferenceManager{segmentsLock: map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock{}},
			},
			args{
				2,
				&compactTime{travelTime: 200, expireTime: 0},
			},
			false,
			[]*datapb.CompactionPlan{
				{
					PlanID: 2,
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{
							SegmentID: 1,
							FieldBinlogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogPath: "log1"},
									},
								},
							},
							Field2StatslogPaths: nil,
							Deltalogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogPath: "deltalog1"},
									},
								},
							},
						},
						{
							SegmentID: 2,
							FieldBinlogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogPath: "log2"},
									},
								},
							},
							Field2StatslogPaths: nil,
							Deltalogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogPath: "deltalog2"},
									},
								},
							},
						},
					},
					StartTime:        0,
					TimeoutInSeconds: Params.DataCoordCfg.CompactionTimeoutInSeconds,
					Type:             datapb.CompactionType_MixCompaction,
					Timetravel:       200,
					Channel:          "ch1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				meta:              tt.fields.meta,
				allocator:         tt.fields.allocator,
				signals:           tt.fields.signals,
				compactionHandler: tt.fields.compactionHandler,
				globalTrigger:     tt.fields.globalTrigger,
				segRefer:          tt.fields.segRefer,
			}
			_, err := tr.forceTriggerCompaction(tt.args.collectionID, tt.args.compactTime)
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			plan := <-spy.spyChan
			sortPlanCompactionBinlogs(plan)
			assert.EqualValues(t, tt.wantPlans[0], plan)
		})
	}
}

// test force compaction with too many Segment
func Test_compactionTrigger_force_maxSegmentLimit(t *testing.T) {
	type fields struct {
		meta              *meta
		allocator         allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	Params.Init()

	segmentInfos := &SegmentsInfo{
		segments: make(map[UniqueID]*SegmentInfo),
	}
	for i := UniqueID(0); i < 50; i++ {
		info := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             i,
				CollectionID:   2,
				PartitionID:    1,
				LastExpireTime: 100,
				NumOfRows:      100,
				MaxRowNum:      300000,
				InsertChannel:  "ch1",
				State:          commonpb.SegmentState_Flushed,
				Binlogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{EntriesNum: 5, LogPath: "log1"},
						},
					},
				},
				Deltalogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{EntriesNum: 5, LogPath: "deltalog1"},
						},
					},
				},
			},
		}
		segmentInfos.segments[i] = info
	}

	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		wantPlans []*datapb.CompactionPlan
	}{
		{
			"test many segments",
			fields{
				&meta{
					segments: segmentInfos,
				},
				newMockAllocator(),
				nil,
				&spyCompactionHandler{spyChan: make(chan *datapb.CompactionPlan, 2)},
				nil,
			},
			args{
				2,
				&compactTime{travelTime: 200, expireTime: 0},
			},
			false,
			[]*datapb.CompactionPlan{
				{
					PlanID: 2,
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{
							SegmentID: 1,
							FieldBinlogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogPath: "log1"},
									},
								},
							},
							Field2StatslogPaths: nil,
							Deltalogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogPath: "deltalog1"},
									},
								},
							},
						},
						{
							SegmentID: 2,
							FieldBinlogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogPath: "log2"},
									},
								},
							},
							Field2StatslogPaths: nil,
							Deltalogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogPath: "deltalog2"},
									},
								},
							},
						},
					},
					StartTime:        3,
					TimeoutInSeconds: Params.DataCoordCfg.CompactionTimeoutInSeconds,
					Type:             datapb.CompactionType_MixCompaction,
					Timetravel:       200,
					Channel:          "ch1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				meta:              tt.fields.meta,
				allocator:         tt.fields.allocator,
				signals:           tt.fields.signals,
				compactionHandler: tt.fields.compactionHandler,
				globalTrigger:     tt.fields.globalTrigger,
				segRefer:          &SegmentReferenceManager{segmentsLock: map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock{}},
			}
			_, err := tr.forceTriggerCompaction(tt.args.collectionID, tt.args.compactTime)
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)

			// should be split into two plans
			plan := <-spy.spyChan
			assert.Equal(t, len(plan.SegmentBinlogs), 30)

			plan = <-spy.spyChan
			assert.Equal(t, len(plan.SegmentBinlogs), 20)
		})
	}
}

func sortPlanCompactionBinlogs(plan *datapb.CompactionPlan) {
	sort.Slice(plan.SegmentBinlogs, func(i, j int) bool {
		return plan.SegmentBinlogs[i].SegmentID < plan.SegmentBinlogs[j].SegmentID
	})
}

// Test no compaction selection
func Test_compactionTrigger_noplan(t *testing.T) {
	type fields struct {
		meta              *meta
		allocator         allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	Params.Init()
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		wantPlans []*datapb.CompactionPlan
	}{
		{
			"test no plan",
			fields{
				&meta{
					// 4 segment
					segments: &SegmentsInfo{
						map[int64]*SegmentInfo{
							1: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             1,
									CollectionID:   2,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      1,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
									Binlogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "log1", LogSize: 100},
											},
										},
									},
								},
							},
							2: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             2,
									CollectionID:   2,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      200,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
									Binlogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "log2", LogSize: int64(Params.DataCoordCfg.SegmentMaxSize)*1024*1024 - 1},
											},
										},
									},
									Deltalogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "deltalog2"},
											},
										},
									},
								},
							},
							3: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             3,
									CollectionID:   2,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      2,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
									Binlogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "log1", LogSize: 100},
											},
										},
									},
								},
							},
							4: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             4,
									CollectionID:   2,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      3,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
									Binlogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "log1", LogSize: 100},
											},
										},
									},
								},
							},
						},
					},
				},
				newMockAllocator(),
				make(chan *compactionSignal, 1),
				&spyCompactionHandler{spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
			},
			args{
				2,
				&compactTime{travelTime: 200, expireTime: 0},
			},
			false,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				meta:              tt.fields.meta,
				allocator:         tt.fields.allocator,
				signals:           tt.fields.signals,
				compactionHandler: tt.fields.compactionHandler,
				globalTrigger:     tt.fields.globalTrigger,
				segRefer:          &SegmentReferenceManager{segmentsLock: map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock{}},
			}
			tr.start()
			defer tr.stop()
			err := tr.triggerCompaction(tt.args.compactTime)
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			select {
			case val := <-spy.spyChan:
				assert.Fail(t, "we expect no compaction generated", val)
				return
			case <-time.After(3 * time.Second):
				return
			}
		})
	}
}

// Test compaction with small files
func Test_compactionTrigger_smallfiles(t *testing.T) {
	type fields struct {
		meta              *meta
		allocator         allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	Params.Init()
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		wantPlans []*datapb.CompactionPlan
	}{
		{
			"test small segment",
			fields{
				&meta{
					// 4 small segments
					segments: &SegmentsInfo{
						map[int64]*SegmentInfo{
							1: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             1,
									CollectionID:   2,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      50,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
									Binlogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "log1", LogSize: 100},
											},
										},
									},
									Deltalogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "deltalog1"},
											},
										},
									},
								},
							},
							2: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             2,
									CollectionID:   2,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      50,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
									Binlogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "log2", LogSize: 800},
											},
										},
									},
									Deltalogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "deltalog2"},
											},
										},
									},
								},
							},
							3: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             3,
									CollectionID:   2,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      50,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
									Binlogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "log1", LogSize: 100},
											},
										},
									},
									Deltalogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "deltalog1"},
											},
										},
									},
								},
							},
							4: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             4,
									CollectionID:   2,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      50,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
									Binlogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "log1", LogSize: 100},
											},
										},
									},
									Deltalogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogPath: "deltalog1"},
											},
										},
									},
								},
							},
						},
					},
				},
				newMockAllocator(),
				make(chan *compactionSignal, 1),
				&spyCompactionHandler{spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
			},
			args{
				2,
				&compactTime{travelTime: 200, expireTime: 0},
			},
			false,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				meta:              tt.fields.meta,
				allocator:         tt.fields.allocator,
				signals:           tt.fields.signals,
				compactionHandler: tt.fields.compactionHandler,
				globalTrigger:     tt.fields.globalTrigger,
				segRefer:          &SegmentReferenceManager{segmentsLock: map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock{}},
			}
			tr.start()
			defer tr.stop()
			err := tr.triggerCompaction(tt.args.compactTime)
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			select {
			case val := <-spy.spyChan:
				// 4 segments in the final pick list
				assert.Equal(t, len(val.SegmentBinlogs), 4)
				return
			case <-time.After(3 * time.Second):
				assert.Fail(t, "failed to get plan")
				return
			}
		})
	}
}

// Test segment compaction target size
func Test_compactionTrigger_noplan_random_size(t *testing.T) {
	type fields struct {
		meta              *meta
		allocator         allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	Params.Init()

	segmentInfos := &SegmentsInfo{
		segments: make(map[UniqueID]*SegmentInfo),
	}

	size := []int64{
		510, 500, 480, 300, 250, 200, 128, 128, 128, 127,
		40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
		20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
		10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
		10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
	}

	for i := UniqueID(0); i < 50; i++ {
		info := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             i,
				CollectionID:   2,
				PartitionID:    1,
				LastExpireTime: 100,
				NumOfRows:      size[i],
				MaxRowNum:      512,
				InsertChannel:  "ch1",
				State:          commonpb.SegmentState_Flushed,
				Binlogs: []*datapb.FieldBinlog{
					{
						Binlogs: []*datapb.Binlog{
							{EntriesNum: 5, LogPath: "log1", LogSize: size[i] * 1024 * 1024},
						},
					},
				},
			},
		}
		segmentInfos.segments[i] = info
	}

	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		wantPlans []*datapb.CompactionPlan
	}{
		{
			"test rand size segment",
			fields{
				&meta{
					segments: segmentInfos,
				},
				newMockAllocator(),
				make(chan *compactionSignal, 1),
				&spyCompactionHandler{spyChan: make(chan *datapb.CompactionPlan, 10)},
				nil,
			},
			args{
				2,
				&compactTime{travelTime: 200, expireTime: 0},
			},
			false,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				meta:              tt.fields.meta,
				allocator:         tt.fields.allocator,
				signals:           tt.fields.signals,
				compactionHandler: tt.fields.compactionHandler,
				globalTrigger:     tt.fields.globalTrigger,
				segRefer:          &SegmentReferenceManager{segmentsLock: map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock{}},
			}
			tr.start()
			defer tr.stop()
			err := tr.triggerCompaction(tt.args.compactTime)
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)

			// should be split into two plans
			var plans []*datapb.CompactionPlan
		WAIT:
			for {
				select {
				case val := <-spy.spyChan:
					plans = append(plans, val)
				case <-time.After(1 * time.Second):
					break WAIT
				}
			}

			for _, plan := range plans {

				size := int64(0)
				for _, log := range plan.SegmentBinlogs {
					size += log.FieldBinlogs[0].GetBinlogs()[0].LogSize
				}
				fmt.Println("target ", len(plan.SegmentBinlogs))
			}
			assert.Equal(t, len(plans), 3)
			// plan 1: 250 + 20 * 10 + 3 * 20
			// plan 2: 200 + 7 * 20 + 4 * 40
			// plan 3  128 + 6 * 40 + 127
			assert.Equal(t, len(plans[0].SegmentBinlogs), 24)
			assert.Equal(t, len(plans[1].SegmentBinlogs), 12)
			assert.Equal(t, len(plans[2].SegmentBinlogs), 8)
		})
	}
}

// Test shouldDoSingleCompaction
func Test_compactionTrigger_shouldDoSingleCompaction(t *testing.T) {
	Params.Init()

	trigger := newCompactionTrigger(&meta{}, &compactionPlanHandler{}, newMockAllocator(),
		&SegmentReferenceManager{segmentsLock: map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock{}})

	// Test too many files.
	var binlogs []*datapb.FieldBinlog
	for i := UniqueID(0); i < 5000; i++ {
		binlogs = append(binlogs, &datapb.FieldBinlog{
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 5, LogPath: "log1", LogSize: 100},
			},
		})
	}
	info := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   2,
			PartitionID:    1,
			LastExpireTime: 100,
			NumOfRows:      100,
			MaxRowNum:      300,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Flushed,
			Binlogs:        binlogs,
		},
	}

	couldDo := trigger.ShouldDoSingleCompaction(info, &compactTime{travelTime: 200, expireTime: 0})
	assert.True(t, couldDo)

	//Test expire triggered  compaction
	var binlogs2 []*datapb.FieldBinlog
	for i := UniqueID(0); i < 100; i++ {
		binlogs2 = append(binlogs2, &datapb.FieldBinlog{
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 5, LogPath: "log1", LogSize: 100000, TimestampFrom: 300, TimestampTo: 500},
			},
		})
	}

	for i := UniqueID(0); i < 100; i++ {
		binlogs2 = append(binlogs2, &datapb.FieldBinlog{
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 5, LogPath: "log1", LogSize: 1000000, TimestampFrom: 300, TimestampTo: 1000},
			},
		})
	}
	info2 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   2,
			PartitionID:    1,
			LastExpireTime: 600,
			NumOfRows:      10000,
			MaxRowNum:      300,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Flushed,
			Binlogs:        binlogs2,
		},
	}

	// expire time < Timestamp To
	couldDo = trigger.ShouldDoSingleCompaction(info2, &compactTime{travelTime: 200, expireTime: 300})
	assert.False(t, couldDo)

	// didn't reach single compaction size 10 * 1024 * 1024
	couldDo = trigger.ShouldDoSingleCompaction(info2, &compactTime{travelTime: 200, expireTime: 600})
	assert.False(t, couldDo)

	// expire time < Timestamp False
	couldDo = trigger.ShouldDoSingleCompaction(info2, &compactTime{travelTime: 200, expireTime: 1200})
	assert.True(t, couldDo)

	// Test Delete triggered compaction
	var binlogs3 []*datapb.FieldBinlog
	for i := UniqueID(0); i < 100; i++ {
		binlogs3 = append(binlogs2, &datapb.FieldBinlog{
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 5, LogPath: "log1", LogSize: 100000, TimestampFrom: 300, TimestampTo: 500},
			},
		})
	}

	info3 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   2,
			PartitionID:    1,
			LastExpireTime: 700,
			NumOfRows:      100,
			MaxRowNum:      300,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Flushed,
			Binlogs:        binlogs3,
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{EntriesNum: 200, LogPath: "deltalog1"},
					},
				},
			},
		},
	}

	// expire time < Timestamp To
	couldDo = trigger.ShouldDoSingleCompaction(info3, &compactTime{travelTime: 600, expireTime: 0})
	assert.False(t, couldDo)

	// deltalog is large enough, should do compaction
	couldDo = trigger.ShouldDoSingleCompaction(info3, &compactTime{travelTime: 800, expireTime: 0})
	assert.True(t, couldDo)
}

func Test_newCompactionTrigger(t *testing.T) {
	type args struct {
		meta              *meta
		compactionHandler compactionPlanContext
		allocator         allocator
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"test new trigger",
			args{
				&meta{},
				&compactionPlanHandler{},
				newMockAllocator(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newCompactionTrigger(tt.args.meta, tt.args.compactionHandler, tt.args.allocator,
				&SegmentReferenceManager{segmentsLock: map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock{}})
			assert.Equal(t, tt.args.meta, got.meta)
			assert.Equal(t, tt.args.compactionHandler, got.compactionHandler)
			assert.Equal(t, tt.args.allocator, got.allocator)
		})
	}
}

func Test_handleSignal(t *testing.T) {

	got := newCompactionTrigger(&meta{segments: NewSegmentsInfo()}, &compactionPlanHandler{}, newMockAllocator(),
		&SegmentReferenceManager{segmentsLock: map[UniqueID]map[UniqueID]*datapb.SegmentReferenceLock{}})
	signal := &compactionSignal{
		segmentID: 1,
	}
	assert.NotPanics(t, func() {
		got.handleSignal(signal)
	})
}
