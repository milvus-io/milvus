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
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type spyCompactionHandler struct {
	spyChan chan *datapb.CompactionPlan
}

// execCompactionPlan start to execute plan and return immediately
func (h *spyCompactionHandler) execCompactionPlan(signal *compactionSignal, plan *datapb.CompactionPlan) error {
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
	}

	paramtable.Init()
	paramtable.Get().Save(Params.CommonCfg.RetentionDuration.Key, "200")
	defer paramtable.Get().Reset(Params.CommonCfg.RetentionDuration.Key)

	pts, _ := tsoutil.ParseTS(0)
	ttRetention := pts.Add(-1 * Params.CommonCfg.RetentionDuration.GetAsDuration(time.Second))
	timeTravel := tsoutil.ComposeTS(ttRetention.UnixNano()/int64(time.Millisecond), 0)

	vecFieldID := int64(201)
	indexID := int64(1001)
	tests := []struct {
		name         string
		fields       fields
		collectionID UniqueID
		wantErr      bool
		wantPlans    []*datapb.CompactionPlan
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
								segmentIndexes: map[UniqueID]*model.SegmentIndex{
									indexID: {
										SegmentID:     1,
										CollectionID:  2,
										PartitionID:   1,
										NumRows:       100,
										IndexID:       indexID,
										BuildID:       1,
										NodeID:        0,
										IndexVersion:  1,
										IndexState:    commonpb.IndexState_Finished,
										FailReason:    "",
										IsDeleted:     false,
										CreateTime:    0,
										IndexFileKeys: nil,
										IndexSize:     0,
										WriteHandoff:  false,
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
								segmentIndexes: map[UniqueID]*model.SegmentIndex{
									indexID: {
										SegmentID:     2,
										CollectionID:  2,
										PartitionID:   1,
										NumRows:       100,
										IndexID:       indexID,
										BuildID:       2,
										NodeID:        0,
										IndexVersion:  1,
										IndexState:    commonpb.IndexState_Finished,
										FailReason:    "",
										IsDeleted:     false,
										CreateTime:    0,
										IndexFileKeys: nil,
										IndexSize:     0,
										WriteHandoff:  false,
									},
								},
							},
							3: {
								SegmentInfo: &datapb.SegmentInfo{
									ID:             3,
									CollectionID:   1111,
									PartitionID:    1,
									LastExpireTime: 100,
									NumOfRows:      100,
									MaxRowNum:      300,
									InsertChannel:  "ch1",
									State:          commonpb.SegmentState_Flushed,
								},
								segmentIndexes: map[UniqueID]*model.SegmentIndex{
									indexID: {
										SegmentID:     3,
										CollectionID:  1111,
										PartitionID:   1,
										NumRows:       100,
										IndexID:       indexID,
										BuildID:       3,
										NodeID:        0,
										IndexVersion:  1,
										IndexState:    commonpb.IndexState_InProgress,
										FailReason:    "",
										IsDeleted:     false,
										CreateTime:    0,
										IndexFileKeys: nil,
										IndexSize:     0,
										WriteHandoff:  false,
									},
								},
							},
						},
					},
					collections: map[int64]*collectionInfo{
						2: {
							ID: 2,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
										TypeParams: []*commonpb.KeyValuePair{
											{
												Key:   common.DimKey,
												Value: "128",
											},
										},
									},
								},
							},
							Properties: map[string]string{
								common.CollectionTTLConfigKey: "0",
							},
						},
						1111: {
							ID: 1111,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
										TypeParams: []*commonpb.KeyValuePair{
											{
												Key:   common.DimKey,
												Value: "128",
											},
										},
									},
								},
							},
							Properties: map[string]string{
								common.CollectionTTLConfigKey: "error",
							},
						},
						1000: {
							ID: 1000,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
										TypeParams: []*commonpb.KeyValuePair{
											{
												Key:   common.DimKey,
												Value: "128",
											},
										},
									},
								},
							},
						},
						// error (has no vector field)
						2000: {
							ID: 2000,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_Int16,
									},
								},
							},
						},
						// error (has no dim)
						3000: {
							ID: 3000,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
										TypeParams: []*commonpb.KeyValuePair{
											{},
										},
									},
								},
							},
						},
						// error (dim parse fail)
						4000: {
							ID: 4000,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
										TypeParams: []*commonpb.KeyValuePair{
											{
												Key:   common.DimKey,
												Value: "128error",
											},
										},
									},
								},
							},
						},
						10000: {
							ID: 10000,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
										TypeParams: []*commonpb.KeyValuePair{
											{
												Key:   common.DimKey,
												Value: "128",
											},
										},
									},
								},
							},
						},
					},
					indexes: map[UniqueID]map[UniqueID]*model.Index{
						2: {
							indexID: {
								TenantID:     "",
								CollectionID: 2,
								FieldID:      vecFieldID,
								IndexID:      indexID,
								IndexName:    "_default_idx",
								IsDeleted:    false,
								CreateTime:   0,
								TypeParams:   nil,
								IndexParams: []*commonpb.KeyValuePair{
									{
										Key:   common.IndexTypeKey,
										Value: "HNSW",
									},
								},
								IsAutoIndex:     false,
								UserIndexParams: nil,
							},
						},
						1000: {
							indexID: {
								TenantID:     "",
								CollectionID: 1000,
								FieldID:      vecFieldID,
								IndexID:      indexID,
								IndexName:    "_default_idx",
								IsDeleted:    false,
								CreateTime:   0,
								TypeParams:   nil,
								IndexParams: []*commonpb.KeyValuePair{
									{
										Key:   common.IndexTypeKey,
										Value: "DISKANN",
									},
								},
								IsAutoIndex:     false,
								UserIndexParams: nil,
							},
						},
					},
				},
				&MockAllocator0{},
				nil,
				&spyCompactionHandler{spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
			},
			2,
			false,
			[]*datapb.CompactionPlan{
				{
					PlanID: 0,
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
					TimeoutInSeconds: Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32(),
					Type:             datapb.CompactionType_MixCompaction,
					Timetravel:       timeTravel,
					Channel:          "ch1",
					TotalRows:        200,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				meta:                         tt.fields.meta,
				handler:                      newMockHandlerWithMeta(tt.fields.meta),
				allocator:                    tt.fields.allocator,
				signals:                      tt.fields.signals,
				compactionHandler:            tt.fields.compactionHandler,
				globalTrigger:                tt.fields.globalTrigger,
				estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
				estimateNonDiskSegmentPolicy: calBySchemaPolicy,
				testingOnly:                  true,
			}
			_, err := tr.forceTriggerCompaction(tt.collectionID)
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			plan := <-spy.spyChan
			sortPlanCompactionBinlogs(plan)
			assert.EqualValues(t, tt.wantPlans[0], plan)
		})

		t.Run(tt.name+" with DiskANN index", func(t *testing.T) {
			segmentIDs := make([]int64, 0)
			for _, segment := range tt.fields.meta.segments.GetSegments() {
				segmentIDs = append(segmentIDs, segment.GetID())
				// Collection 1000 means it has DiskANN index
				segment.CollectionID = 1000
			}
			tr := &compactionTrigger{
				meta:                         tt.fields.meta,
				handler:                      newMockHandlerWithMeta(tt.fields.meta),
				allocator:                    tt.fields.allocator,
				signals:                      tt.fields.signals,
				compactionHandler:            tt.fields.compactionHandler,
				globalTrigger:                tt.fields.globalTrigger,
				estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
				estimateNonDiskSegmentPolicy: calBySchemaPolicy,
				testingOnly:                  true,
			}
			tt.collectionID = 1000
			_, err := tr.forceTriggerCompaction(tt.collectionID)
			assert.Equal(t, tt.wantErr, err != nil)
			// expect max row num =  2048*1024*1024/(128*4) = 4194304
			assert.EqualValues(t, 4194304, tt.fields.meta.segments.GetSegments()[0].MaxRowNum)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			<-spy.spyChan
		})

		t.Run(tt.name+" with allocate ts error", func(t *testing.T) {
			//indexCood := newMockIndexCoord()
			tr := &compactionTrigger{
				meta:                         tt.fields.meta,
				handler:                      newMockHandlerWithMeta(tt.fields.meta),
				allocator:                    &FailsAllocator{allocIDSucceed: true},
				signals:                      tt.fields.signals,
				compactionHandler:            tt.fields.compactionHandler,
				globalTrigger:                tt.fields.globalTrigger,
				estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
				estimateNonDiskSegmentPolicy: calBySchemaPolicy,
				testingOnly:                  true,
			}

			{
				// test alloc ts fail for handle global signal
				signal := &compactionSignal{
					id:           0,
					isForce:      true,
					isGlobal:     true,
					collectionID: tt.collectionID,
				}
				tr.handleGlobalSignal(signal)

				spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
				hasPlan := true
				select {
				case <-spy.spyChan:
					hasPlan = true
				case <-time.After(2 * time.Second):
					hasPlan = false
				}
				assert.Equal(t, false, hasPlan)
			}

			{
				// test alloc ts fail for handle signal
				signal := &compactionSignal{
					id:           0,
					isForce:      true,
					collectionID: tt.collectionID,
					segmentID:    3,
				}
				tr.handleSignal(signal)

				spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
				hasPlan := true
				select {
				case <-spy.spyChan:
					hasPlan = true
				case <-time.After(2 * time.Second):
					hasPlan = false
				}
				assert.Equal(t, false, hasPlan)
			}
		})

		t.Run(tt.name+" with getCompact error", func(t *testing.T) {
			for _, segment := range tt.fields.meta.segments.GetSegments() {
				segment.CollectionID = 1111
			}
			tr := &compactionTrigger{
				meta:                         tt.fields.meta,
				handler:                      newMockHandlerWithMeta(tt.fields.meta),
				allocator:                    tt.fields.allocator,
				signals:                      tt.fields.signals,
				compactionHandler:            tt.fields.compactionHandler,
				globalTrigger:                tt.fields.globalTrigger,
				estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
				estimateNonDiskSegmentPolicy: calBySchemaPolicy,
				testingOnly:                  true,
			}

			{
				// test getCompactTime fail for handle global signal
				signal := &compactionSignal{
					id:           0,
					isForce:      true,
					isGlobal:     true,
					collectionID: 1111,
				}
				tr.handleGlobalSignal(signal)

				spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
				hasPlan := true
				select {
				case <-spy.spyChan:
					hasPlan = true
				case <-time.After(2 * time.Second):
					hasPlan = false
				}
				assert.Equal(t, false, hasPlan)
			}

			{
				// test getCompactTime fail for handle signal
				signal := &compactionSignal{
					id:           0,
					isForce:      true,
					collectionID: 1111,
					segmentID:    3,
				}
				tr.handleSignal(signal)

				spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
				hasPlan := true
				select {
				case <-spy.spyChan:
					hasPlan = true
				case <-time.After(2 * time.Second):
					hasPlan = false
				}
				assert.Equal(t, false, hasPlan)
			}
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
	vecFieldID := int64(201)
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
					collections: map[int64]*collectionInfo{
						2: {
							ID: 2,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
										TypeParams: []*commonpb.KeyValuePair{
											{
												Key:   common.DimKey,
												Value: "128",
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
					TimeoutInSeconds: Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32(),
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
				meta:                         tt.fields.meta,
				handler:                      newMockHandlerWithMeta(tt.fields.meta),
				allocator:                    tt.fields.allocator,
				signals:                      tt.fields.signals,
				compactionHandler:            tt.fields.compactionHandler,
				globalTrigger:                tt.fields.globalTrigger,
				estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
				estimateNonDiskSegmentPolicy: calBySchemaPolicy,
				testingOnly:                  true,
			}
			_, err := tr.forceTriggerCompaction(tt.args.collectionID)
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
	Params.DataCoordCfg.MinSegmentToMerge.DefaultValue = "4"
	vecFieldID := int64(201)
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
									NumOfRows:      200,
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
								lastFlushTime: time.Now(),
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
												{EntriesNum: 5, LogPath: "log2", LogSize: Params.DataCoordCfg.SegmentMaxSize.GetAsInt64()*1024*1024 - 1},
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
								lastFlushTime: time.Now(),
							},
						},
					},
					collections: map[int64]*collectionInfo{
						2: {
							ID: 2,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
										TypeParams: []*commonpb.KeyValuePair{
											{
												Key:   common.DimKey,
												Value: "128",
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
				meta:                         tt.fields.meta,
				handler:                      newMockHandlerWithMeta(tt.fields.meta),
				allocator:                    tt.fields.allocator,
				signals:                      tt.fields.signals,
				compactionHandler:            tt.fields.compactionHandler,
				globalTrigger:                tt.fields.globalTrigger,
				estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
				estimateNonDiskSegmentPolicy: calBySchemaPolicy,
				testingOnly:                  true,
			}
			tr.start()
			defer tr.stop()
			err := tr.triggerCompaction()
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

// Test compaction with prioritized candi
func Test_compactionTrigger_PrioritizedCandi(t *testing.T) {
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
	vecFieldID := int64(201)

	genSeg := func(segID, numRows int64) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:             segID,
			CollectionID:   2,
			PartitionID:    1,
			LastExpireTime: 100,
			NumOfRows:      numRows,
			MaxRowNum:      110,
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
		}
	}
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
								SegmentInfo:   genSeg(1, 20),
								lastFlushTime: time.Now().Add(-100 * time.Minute),
							},
							2: {
								SegmentInfo:   genSeg(2, 20),
								lastFlushTime: time.Now(),
							},
							3: {
								SegmentInfo:   genSeg(3, 20),
								lastFlushTime: time.Now(),
							},
							4: {
								SegmentInfo:   genSeg(4, 20),
								lastFlushTime: time.Now(),
							},
							5: {
								SegmentInfo:   genSeg(5, 20),
								lastFlushTime: time.Now(),
							},
							6: {
								SegmentInfo:   genSeg(6, 20),
								lastFlushTime: time.Now(),
							},
							7: {
								SegmentInfo:   genSeg(7, 20),
								lastFlushTime: time.Now(),
							},
							8: {
								SegmentInfo:   genSeg(8, 20),
								lastFlushTime: time.Now(),
							},
						},
					},
					collections: map[int64]*collectionInfo{
						2: {
							ID: 2,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
										TypeParams: []*commonpb.KeyValuePair{
											{
												Key:   common.DimKey,
												Value: "128",
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
				handler:           newMockHandlerWithMeta(tt.fields.meta),
				allocator:         tt.fields.allocator,
				signals:           tt.fields.signals,
				compactionHandler: tt.fields.compactionHandler,
				globalTrigger:     tt.fields.globalTrigger,
				testingOnly:       true,
			}
			tr.start()
			defer tr.stop()
			err := tr.triggerCompaction()
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			select {
			case val := <-spy.spyChan:
				// 5 segments in the final pick list
				assert.Equal(t, len(val.SegmentBinlogs), 5)
				return
			case <-time.After(3 * time.Second):
				assert.Fail(t, "failed to get plan")
				return
			}
		})
	}
}

// Test compaction with small candi
func Test_compactionTrigger_SmallCandi(t *testing.T) {
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
	vecFieldID := int64(201)

	genSeg := func(segID, numRows int64) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:             segID,
			CollectionID:   2,
			PartitionID:    1,
			LastExpireTime: 100,
			NumOfRows:      numRows,
			MaxRowNum:      110,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Flushed,
			Binlogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{EntriesNum: 5, LogPath: "log1", LogSize: 100},
					},
				},
			},
		}
	}
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
								SegmentInfo:   genSeg(1, 20),
								lastFlushTime: time.Now().Add(-100 * time.Minute),
							},
							2: {
								SegmentInfo:   genSeg(2, 20),
								lastFlushTime: time.Now(),
							},
							3: {
								SegmentInfo:   genSeg(3, 20),
								lastFlushTime: time.Now(),
							},
							4: {
								SegmentInfo:   genSeg(4, 20),
								lastFlushTime: time.Now(),
							},
							5: {
								SegmentInfo:   genSeg(5, 20),
								lastFlushTime: time.Now(),
							},
							6: {
								SegmentInfo:   genSeg(6, 20),
								lastFlushTime: time.Now(),
							},
							7: {
								SegmentInfo:   genSeg(7, 20),
								lastFlushTime: time.Now(),
							},
						},
					},
					collections: map[int64]*collectionInfo{
						2: {
							ID: 2,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
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
				meta:                         tt.fields.meta,
				handler:                      newMockHandlerWithMeta(tt.fields.meta),
				allocator:                    tt.fields.allocator,
				signals:                      tt.fields.signals,
				compactionHandler:            tt.fields.compactionHandler,
				globalTrigger:                tt.fields.globalTrigger,
				estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
				estimateNonDiskSegmentPolicy: calBySchemaPolicy,
				testingOnly:                  true,
			}
			tr.start()
			defer tr.stop()
			err := tr.triggerCompaction()
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			select {
			case val := <-spy.spyChan:
				// 6 segments in the final pick list.
				// 5 generated by the origin plan, 1 was added as additional segment.
				assert.Equal(t, len(val.SegmentBinlogs), 6)
				return
			case <-time.After(3 * time.Second):
				assert.Fail(t, "failed to get plan")
				return
			}
		})
	}
}

// Test compaction with small candi
func Test_compactionTrigger_SqueezeNonPlannedSegs(t *testing.T) {
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
	vecFieldID := int64(201)

	genSeg := func(segID, numRows int64) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:             segID,
			CollectionID:   2,
			PartitionID:    1,
			LastExpireTime: 100,
			NumOfRows:      numRows,
			MaxRowNum:      110,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Flushed,
			Binlogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{EntriesNum: 5, LogPath: "log1", LogSize: 100},
					},
				},
			},
		}
	}
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
								SegmentInfo:   genSeg(1, 60),
								lastFlushTime: time.Now().Add(-100 * time.Minute),
							},
							2: {
								SegmentInfo:   genSeg(2, 60),
								lastFlushTime: time.Now(),
							},
							3: {
								SegmentInfo:   genSeg(3, 60),
								lastFlushTime: time.Now(),
							},
							4: {
								SegmentInfo:   genSeg(4, 60),
								lastFlushTime: time.Now(),
							},
							5: {
								SegmentInfo:   genSeg(5, 26),
								lastFlushTime: time.Now(),
							},
							6: {
								SegmentInfo:   genSeg(6, 26),
								lastFlushTime: time.Now(),
							},
						},
					},
					collections: map[int64]*collectionInfo{
						2: {
							ID: 2,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
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
				meta:                         tt.fields.meta,
				handler:                      newMockHandlerWithMeta(tt.fields.meta),
				allocator:                    tt.fields.allocator,
				signals:                      tt.fields.signals,
				compactionHandler:            tt.fields.compactionHandler,
				globalTrigger:                tt.fields.globalTrigger,
				estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
				estimateNonDiskSegmentPolicy: calBySchemaPolicy,
				testingOnly:                  true,
			}
			tr.start()
			defer tr.stop()
			err := tr.triggerCompaction()
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			select {
			case val := <-spy.spyChan:
				// max # of rows == 110, expansion rate == 1.25.
				// segment 5 and 6 are squeezed into a non-planned segment. Total # of rows: 60 + 26 + 26 == 112,
				// which is greater than 110 but smaller than 110 * 1.25
				assert.Equal(t, len(val.SegmentBinlogs), 3)
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

	vecFieldID := int64(201)
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
			lastFlushTime: time.Now(),
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
					collections: map[int64]*collectionInfo{
						2: {
							ID: 2,
							Schema: &schemapb.CollectionSchema{
								Fields: []*schemapb.FieldSchema{
									{
										FieldID:  vecFieldID,
										DataType: schemapb.DataType_FloatVector,
										TypeParams: []*commonpb.KeyValuePair{
											{
												Key:   common.DimKey,
												Value: "128",
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
				handler:           newMockHandlerWithMeta(tt.fields.meta),
				allocator:         tt.fields.allocator,
				signals:           tt.fields.signals,
				compactionHandler: tt.fields.compactionHandler,
				globalTrigger:     tt.fields.globalTrigger,
				testingOnly:       true,
			}
			tr.start()
			defer tr.stop()
			err := tr.triggerCompaction()
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)

			// should be split into two plans
			var plans []*datapb.CompactionPlan
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
		WAIT:
			for {
				select {
				case val := <-spy.spyChan:
					plans = append(plans, val)
				case <-ticker.C:
					break WAIT
				}
			}

			for _, plan := range plans {

				size := int64(0)
				for _, log := range plan.SegmentBinlogs {
					size += log.FieldBinlogs[0].GetBinlogs()[0].LogSize
				}
			}
			assert.Equal(t, 4, len(plans))
			// plan 1: 250 + 20 * 10 + 3 * 20
			// plan 2: 200 + 7 * 20 + 4 * 40
			// plan 3: 128 + 6 * 40 + 127
			// plan 4: 300 + 128 + 128  ( < 512 * 1.25)
			assert.Equal(t, 24, len(plans[0].SegmentBinlogs))
			assert.Equal(t, 12, len(plans[1].SegmentBinlogs))
			assert.Equal(t, 8, len(plans[2].SegmentBinlogs))
			assert.Equal(t, 3, len(plans[3].SegmentBinlogs))
		})
	}
}

// Test shouldDoSingleCompaction
func Test_compactionTrigger_shouldDoSingleCompaction(t *testing.T) {
	Params.Init()

	trigger := newCompactionTrigger(&meta{}, &compactionPlanHandler{}, newMockAllocator(), newMockHandler())

	// Test too many deltalogs.
	var binlogs []*datapb.FieldBinlog
	for i := UniqueID(0); i < 1000; i++ {
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
			Deltalogs:      binlogs,
		},
	}

	couldDo := trigger.ShouldDoSingleCompaction(info, false, &compactTime{travelTime: 200, expireTime: 0})
	assert.True(t, couldDo)

	//Test too many stats log
	info = &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             1,
			CollectionID:   2,
			PartitionID:    1,
			LastExpireTime: 100,
			NumOfRows:      100,
			MaxRowNum:      300,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Flushed,
			Statslogs:      binlogs,
		},
	}

	couldDo = trigger.ShouldDoSingleCompaction(info, false, &compactTime{travelTime: 200, expireTime: 0})
	assert.True(t, couldDo)

	couldDo = trigger.ShouldDoSingleCompaction(info, true, &compactTime{travelTime: 200, expireTime: 0})
	assert.True(t, couldDo)

	// if only 10 bin logs, then disk index won't trigger compaction
	info.Statslogs = binlogs[0:20]
	couldDo = trigger.ShouldDoSingleCompaction(info, false, &compactTime{travelTime: 200, expireTime: 0})
	assert.True(t, couldDo)

	couldDo = trigger.ShouldDoSingleCompaction(info, true, &compactTime{travelTime: 200, expireTime: 0})
	assert.False(t, couldDo)
	//Test too many stats log but compacted
	info.CompactionFrom = []int64{0, 1}
	couldDo = trigger.ShouldDoSingleCompaction(info, false, &compactTime{travelTime: 200, expireTime: 0})
	assert.False(t, couldDo)

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
	couldDo = trigger.ShouldDoSingleCompaction(info2, false, &compactTime{travelTime: 200, expireTime: 300})
	assert.False(t, couldDo)

	// didn't reach single compaction size 10 * 1024 * 1024
	couldDo = trigger.ShouldDoSingleCompaction(info2, false, &compactTime{travelTime: 200, expireTime: 600})
	assert.False(t, couldDo)

	// expire time < Timestamp False
	couldDo = trigger.ShouldDoSingleCompaction(info2, false, &compactTime{travelTime: 200, expireTime: 1200})
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
	couldDo = trigger.ShouldDoSingleCompaction(info3, false, &compactTime{travelTime: 600, expireTime: 0})
	assert.False(t, couldDo)

	// deltalog is large enough, should do compaction
	couldDo = trigger.ShouldDoSingleCompaction(info3, false, &compactTime{travelTime: 800, expireTime: 0})
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
			got := newCompactionTrigger(tt.args.meta, tt.args.compactionHandler, tt.args.allocator, newMockHandler())
			assert.Equal(t, tt.args.meta, got.meta)
			assert.Equal(t, tt.args.compactionHandler, got.compactionHandler)
			assert.Equal(t, tt.args.allocator, got.allocator)
		})
	}
}

func Test_handleSignal(t *testing.T) {
	got := newCompactionTrigger(&meta{segments: NewSegmentsInfo()}, &compactionPlanHandler{}, newMockAllocator(), newMockHandler())
	signal := &compactionSignal{
		segmentID: 1,
	}
	assert.NotPanics(t, func() {
		got.handleSignal(signal)
	})
}

func Test_allocTs(t *testing.T) {
	got := newCompactionTrigger(&meta{segments: NewSegmentsInfo()}, &compactionPlanHandler{}, newMockAllocator(), newMockHandler())
	ts, err := got.allocTs()
	assert.NoError(t, err)
	assert.True(t, ts > 0)

	got = newCompactionTrigger(&meta{segments: NewSegmentsInfo()}, &compactionPlanHandler{}, &FailsAllocator{}, newMockHandler())
	ts, err = got.allocTs()
	assert.Error(t, err)
	assert.Equal(t, uint64(0), ts)
}

func Test_getCompactTime(t *testing.T) {
	collections := map[UniqueID]*collectionInfo{
		1: {
			ID:         1,
			Schema:     newTestSchema(),
			Partitions: []UniqueID{1},
			Properties: map[string]string{
				common.CollectionTTLConfigKey: "10",
			},
		},
		2: {
			ID:         2,
			Schema:     newTestSchema(),
			Partitions: []UniqueID{1},
			Properties: map[string]string{
				common.CollectionTTLConfigKey: "error",
			},
		},
	}

	m := &meta{segments: NewSegmentsInfo(), collections: collections}
	got := newCompactionTrigger(m, &compactionPlanHandler{}, newMockAllocator(),
		&ServerHandler{
			&Server{
				meta: m,
			},
		})
	coll := &collectionInfo{
		ID:         1,
		Schema:     newTestSchema(),
		Partitions: []UniqueID{1},
		Properties: map[string]string{
			common.CollectionTTLConfigKey: "10",
		},
	}
	now := tsoutil.GetCurrentTime()
	ct, err := got.getCompactTime(now, coll)
	assert.NoError(t, err)
	assert.NotNil(t, ct)
}

type CompactionTriggerSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	channel      string

	meta              *meta
	tr                *compactionTrigger
	allocator         *NMockAllocator
	handler           *NMockHandler
	compactionHandler *MockCompactionPlanContext
}

func (s *CompactionTriggerSuite) SetupSuite() {
}

func (s *CompactionTriggerSuite) genSeg(segID, numRows int64) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:             segID,
		CollectionID:   s.collectionID,
		PartitionID:    s.partitionID,
		LastExpireTime: 100,
		NumOfRows:      numRows,
		MaxRowNum:      110,
		InsertChannel:  s.channel,
		State:          commonpb.SegmentState_Flushed,
		Binlogs: []*datapb.FieldBinlog{
			{
				Binlogs: []*datapb.Binlog{
					{EntriesNum: 5, LogPath: "log1", LogSize: 100},
				},
			},
		},
	}
}

func (s *CompactionTriggerSuite) SetupTest() {
	s.collectionID = 100
	s.partitionID = 200
	s.channel = "dml_0_100v0"
	s.meta = &meta{segments: &SegmentsInfo{
		map[int64]*SegmentInfo{
			1: {
				SegmentInfo:   s.genSeg(1, 60),
				lastFlushTime: time.Now().Add(-100 * time.Minute),
			},
			2: {
				SegmentInfo:   s.genSeg(2, 60),
				lastFlushTime: time.Now(),
			},
			3: {
				SegmentInfo:   s.genSeg(3, 60),
				lastFlushTime: time.Now(),
			},
			4: {
				SegmentInfo:   s.genSeg(4, 60),
				lastFlushTime: time.Now(),
			},
			5: {
				SegmentInfo:   s.genSeg(5, 26),
				lastFlushTime: time.Now(),
			},
			6: {
				SegmentInfo:   s.genSeg(6, 26),
				lastFlushTime: time.Now(),
			},
		},
	},
	}
	s.allocator = NewNMockAllocator(s.T())
	s.compactionHandler = NewMockCompactionPlanContext(s.T())
	s.handler = NewNMockHandler(s.T())
	s.tr = newCompactionTrigger(
		s.meta,
		s.compactionHandler,
		s.allocator,
		s.handler,
	)
	s.tr.testingOnly = true
}

func (s *CompactionTriggerSuite) TestHandleSignal() {
	s.Run("getCompaction_failed", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		s.allocator.EXPECT().allocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(nil, errors.New("mocked"))
		tr.handleSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check compactionHandler.execCompactionPlan never called
	})

	s.Run("collectionAutoCompactionConfigError", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		s.allocator.EXPECT().allocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "bad_value",
			},
		}, nil)
		tr.handleSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check compactionHandler.execCompactionPlan never called
	})

	s.Run("collectionAutoCompactionDisabled", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		s.allocator.EXPECT().allocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "false",
			},
		}, nil)
		tr.handleSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check compactionHandler.execCompactionPlan never called
	})

	s.Run("collectionAutoCompactionDisabled_force", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		s.allocator.EXPECT().allocTimestamp(mock.Anything).Return(10000, nil)
		s.allocator.EXPECT().allocID(mock.Anything).Return(20000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "false",
			},
		}, nil)
		s.compactionHandler.EXPECT().execCompactionPlan(mock.Anything, mock.Anything).Return(nil)
		tr.handleSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      true,
		})
	})
}

func (s *CompactionTriggerSuite) TestHandleGlobalSignal() {
	s.Run("getCompaction_failed", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		s.allocator.EXPECT().allocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(nil, errors.New("mocked"))
		tr.handleGlobalSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check compactionHandler.execCompactionPlan never called
	})

	s.Run("collectionAutoCompactionConfigError", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		s.allocator.EXPECT().allocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "bad_value",
			},
		}, nil)
		tr.handleGlobalSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check compactionHandler.execCompactionPlan never called
	})

	s.Run("collectionAutoCompactionDisabled", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		s.allocator.EXPECT().allocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "false",
			},
		}, nil)
		tr.handleGlobalSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check compactionHandler.execCompactionPlan never called
	})

	s.Run("collectionAutoCompactionDisabled_force", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		s.allocator.EXPECT().allocTimestamp(mock.Anything).Return(10000, nil)
		s.allocator.EXPECT().allocID(mock.Anything).Return(20000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "false",
			},
		}, nil)
		s.compactionHandler.EXPECT().execCompactionPlan(mock.Anything, mock.Anything).Return(nil)
		tr.handleGlobalSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      true,
		})
	})
}

func TestCompactionTriggerSuite(t *testing.T) {
	suite.Run(t, new(CompactionTriggerSuite))
}
