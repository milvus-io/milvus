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
	"sort"
	satomic "sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type spyCompactionHandler struct {
	t       *testing.T
	spyChan chan *datapb.CompactionPlan
	meta    *meta
}

func (h *spyCompactionHandler) getCompactionTasksNumBySignalID(signalID int64) int {
	return 0
}

func (h *spyCompactionHandler) getCompactionInfo(signalID int64) *compactionInfo {
	return nil
}

var _ compactionPlanContext = (*spyCompactionHandler)(nil)

func (h *spyCompactionHandler) removeTasksByChannel(channel string) {}

// enqueueCompaction start to execute plan and return immediately
func (h *spyCompactionHandler) enqueueCompaction(task *datapb.CompactionTask) error {
	t := &mixCompactionTask{
		CompactionTask: task,
		meta:           h.meta,
	}
	alloc := newMock0Allocator(h.t)
	t.allocator = alloc
	t.ResultSegments = []int64{100, 200}
	plan, err := t.BuildCompactionRequest()
	h.spyChan <- plan
	return err
}

// completeCompaction record the result of a compaction
func (h *spyCompactionHandler) completeCompaction(result *datapb.CompactionPlanResult) error {
	return nil
}

// isFull return true if the task pool is full
func (h *spyCompactionHandler) isFull() bool {
	return false
}

func (h *spyCompactionHandler) start() {}

func (h *spyCompactionHandler) stop() {}

func newMockVersionManager() IndexEngineVersionManager {
	return &versionManagerImpl{}
}

var _ compactionPlanContext = (*spyCompactionHandler)(nil)

func Test_compactionTrigger_force(t *testing.T) {
	paramtable.Init()
	type fields struct {
		meta              *meta
		allocator         allocator.Allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()

	vecFieldID := int64(201)
	indexID := int64(1001)

	schema := &schemapb.CollectionSchema{
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
	}

	mock0Allocator := newMock0Allocator(t)

	tests := []struct {
		name         string
		fields       fields
		collectionID UniqueID
		wantErr      bool
		wantSegIDs   []int64
		wantPlans    []*datapb.CompactionPlan
	}{
		{
			"test force compaction",
			fields{
				&meta{
					catalog:    catalog,
					channelCPs: newChannelCps(),
					segments: &SegmentsInfo{
						segments: map[int64]*SegmentInfo{
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
												{EntriesNum: 5, LogID: 1},
											},
										},
									},
									Deltalogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogID: 1},
											},
										},
									},
									IsSorted: true,
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
												{EntriesNum: 5, LogID: 2},
											},
										},
									},
									Deltalogs: []*datapb.FieldBinlog{
										{
											Binlogs: []*datapb.Binlog{
												{EntriesNum: 5, LogID: 2},
											},
										},
									},
									IsSorted: true,
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
									IsSorted:       true,
								},
							},
						},
					},
					indexMeta: &indexMeta{
						segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
							1: {
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
							2: {
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
							3: {
								indexID: {
									SegmentID:     3,
									CollectionID:  1111,
									PartitionID:   1,
									NumRows:       100,
									IndexID:       indexID,
									BuildID:       3,
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
					collections: map[int64]*collectionInfo{
						2: {
							ID:     2,
							Schema: schema,
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
				},
				mock0Allocator,
				nil,
				&spyCompactionHandler{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
			},
			2,
			false,
			[]int64{
				1, 2,
			},
			[]*datapb.CompactionPlan{
				{
					PlanID: 0,
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{
							SegmentID: 1,
							FieldBinlogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogID: 1},
									},
								},
							},
							Field2StatslogPaths: nil,
							Deltalogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogID: 1},
									},
								},
							},
							InsertChannel: "ch1",
							CollectionID:  2,
							PartitionID:   1,
							IsSorted:      true,
						},
						{
							SegmentID: 2,
							FieldBinlogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogID: 2},
									},
								},
							},
							Field2StatslogPaths: nil,
							Deltalogs: []*datapb.FieldBinlog{
								{
									Binlogs: []*datapb.Binlog{
										{EntriesNum: 5, LogID: 2},
									},
								},
							},
							InsertChannel: "ch1",
							CollectionID:  2,
							PartitionID:   1,
							IsSorted:      true,
						},
					},
					// StartTime:        0,
					TimeoutInSeconds:       Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32(),
					Type:                   datapb.CompactionType_MixCompaction,
					Channel:                "ch1",
					TotalRows:              200,
					Schema:                 schema,
					PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 100, End: 200},
					SlotUsage:              8,
					MaxSize:                1342177280,
				},
			},
		},
	}
	for _, tt := range tests {
		tt.fields.compactionHandler.(*spyCompactionHandler).meta = tt.fields.meta
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
				closeCh:                      lifetime.NewSafeChan(),
				testingOnly:                  true,
			}
			_, err := tr.triggerManualCompaction(tt.collectionID)
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			plan := <-spy.spyChan
			plan.StartTime = 0
			sortPlanCompactionBinlogs(plan)
			assert.EqualValues(t, tt.wantPlans[0], plan)
		})

		t.Run(tt.name+" with DiskANN index", func(t *testing.T) {
			for _, segment := range tt.fields.meta.segments.GetSegments() {
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
				closeCh:                      lifetime.NewSafeChan(),
				testingOnly:                  true,
			}
			tt.collectionID = 1000
			_, err := tr.triggerManualCompaction(tt.collectionID)
			assert.Equal(t, tt.wantErr, err != nil)
			// expect max row num =  2048*1024*1024/(128*4) = 4194304
			// assert.EqualValues(t, 4194304, tt.fields.meta.segments.GetSegments()[0].MaxRowNum)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			<-spy.spyChan
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
				closeCh:                      lifetime.NewSafeChan(),
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
		allocator         allocator.Allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	vecFieldID := int64(201)
	segmentInfos := &SegmentsInfo{
		segments: make(map[UniqueID]*SegmentInfo),
	}

	indexMeta := newSegmentIndexMeta(nil)
	indexMeta.indexes = map[UniqueID]map[UniqueID]*model.Index{
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
				IsSorted: true,
			},
		}

		indexMeta.updateSegmentIndex(&model.SegmentIndex{
			SegmentID:    i,
			CollectionID: 2,
			PartitionID:  1,
			NumRows:      100,
			IndexID:      indexID,
			BuildID:      i,
			NodeID:       0,
			IndexVersion: 1,
			IndexState:   commonpb.IndexState_Finished,
		})

		segmentInfos.segments[i] = info
	}

	mock0Allocator := newMockAllocator(t)

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
					segments:   segmentInfos,
					channelCPs: newChannelCps(),
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
					indexMeta: indexMeta,
				},
				mock0Allocator,
				nil,
				&spyCompactionHandler{t: t, spyChan: make(chan *datapb.CompactionPlan, 2)},
				nil,
			},
			args{
				2,
				&compactTime{},
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
							IsSorted: true,
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
							IsSorted: true,
						},
					},
					StartTime:        3,
					TimeoutInSeconds: Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32(),
					Type:             datapb.CompactionType_MixCompaction,
					Channel:          "ch1",
					MaxSize:          1342177280,
				},
			},
		},
	}
	for _, tt := range tests {
		(tt.fields.compactionHandler).(*spyCompactionHandler).meta = tt.fields.meta
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
				closeCh:                      lifetime.NewSafeChan(),
				testingOnly:                  true,
			}
			_, err := tr.triggerManualCompaction(tt.args.collectionID)
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)

			// should be split into two plans
			plan := <-spy.spyChan
			assert.NotEmpty(t, plan)

			// TODO CZS
			// assert.Equal(t, len(plan.SegmentBinlogs), 30)
			plan = <-spy.spyChan
			assert.NotEmpty(t, plan)
			// TODO CZS
			// assert.Equal(t, len(plan.SegmentBinlogs), 20)
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
		allocator         allocator.Allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	Params.DataCoordCfg.MinSegmentToMerge.DefaultValue = "4"
	vecFieldID := int64(201)
	mock0Allocator := newMockAllocator(t)
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
					indexMeta: newSegmentIndexMeta(nil),
					// 4 segment
					channelCPs: newChannelCps(),

					segments: &SegmentsInfo{
						segments: map[int64]*SegmentInfo{
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
												{EntriesNum: 5, LogPath: "log1", LogSize: 100, MemorySize: 100},
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
												{EntriesNum: 5, LogPath: "log2", LogSize: Params.DataCoordCfg.SegmentMaxSize.GetAsInt64()*1024*1024 - 1, MemorySize: Params.DataCoordCfg.SegmentMaxSize.GetAsInt64()*1024*1024 - 1},
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
				mock0Allocator,
				make(chan *compactionSignal, 1),
				&spyCompactionHandler{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
			},
			args{
				2,
				&compactTime{},
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
				closeCh:                      lifetime.NewSafeChan(),
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
		allocator         allocator.Allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}
	vecFieldID := int64(201)

	genSeg := func(segID, numRows int64) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{
			ID:             segID,
			CollectionID:   2,
			PartitionID:    1,
			LastExpireTime: 100,
			NumOfRows:      numRows,
			MaxRowNum:      150,
			InsertChannel:  "ch1",
			State:          commonpb.SegmentState_Flushed,
			Binlogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{EntriesNum: numRows, LogPath: "log1", LogSize: 100, MemorySize: 100},
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
			IsSorted: true,
		}
	}
	mock0Allocator := newMockAllocator(t)

	genSegIndex := func(segID, indexID UniqueID, numRows int64) map[UniqueID]*model.SegmentIndex {
		return map[UniqueID]*model.SegmentIndex{
			indexID: {
				SegmentID:    segID,
				CollectionID: 2,
				PartitionID:  1,
				NumRows:      numRows,
				IndexID:      indexID,
				BuildID:      segID,
				NodeID:       0,
				IndexVersion: 1,
				IndexState:   commonpb.IndexState_Finished,
			},
		}
	}
	tests := []struct {
		name      string
		fields    fields
		wantErr   bool
		wantPlans []*datapb.CompactionPlan
	}{
		{
			"test small segment",
			fields{
				&meta{
					// 8 small segments
					channelCPs: newChannelCps(),

					segments: &SegmentsInfo{
						segments: map[int64]*SegmentInfo{
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
						},
					},
					indexMeta: &indexMeta{
						segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
							1: genSegIndex(1, indexID, 20),
							2: genSegIndex(2, indexID, 20),
							3: genSegIndex(3, indexID, 20),
							4: genSegIndex(4, indexID, 20),
							5: genSegIndex(5, indexID, 20),
							6: genSegIndex(6, indexID, 20),
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
				mock0Allocator,
				make(chan *compactionSignal, 1),
				&spyCompactionHandler{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
			},
			false,
			nil,
		},
	}
	for _, tt := range tests {
		(tt.fields.compactionHandler).(*spyCompactionHandler).meta = tt.fields.meta
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.meta.channelCPs.checkpoints["ch1"] = &msgpb.MsgPosition{
				Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
				MsgID:     []byte{1, 2, 3, 4},
			}
			tr := &compactionTrigger{
				meta:              tt.fields.meta,
				handler:           newMockHandlerWithMeta(tt.fields.meta),
				allocator:         tt.fields.allocator,
				signals:           tt.fields.signals,
				compactionHandler: tt.fields.compactionHandler,
				globalTrigger:     tt.fields.globalTrigger,
				closeCh:           lifetime.NewSafeChan(),
				testingOnly:       true,
			}
			tr.start()
			defer tr.stop()
			err := tr.triggerCompaction()
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			select {
			case val := <-spy.spyChan:
				// 6 segments in the final pick list
				assert.Equal(t, 6, len(val.SegmentBinlogs))
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
		allocator         allocator.Allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	vecFieldID := int64(201)
	mock0Allocator := newMockAllocator(t)

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
						{EntriesNum: 5, LogPath: "log1", LogSize: numRows * 1024 * 1024, MemorySize: numRows * 1024 * 1024},
					},
				},
			},
			IsSorted: true,
		}
	}

	genSegIndex := func(segID, indexID UniqueID, numRows int64) map[UniqueID]*model.SegmentIndex {
		return map[UniqueID]*model.SegmentIndex{
			indexID: {
				SegmentID:    segID,
				CollectionID: 2,
				PartitionID:  1,
				NumRows:      numRows,
				IndexID:      indexID,
				BuildID:      segID,
				NodeID:       0,
				IndexVersion: 1,
				IndexState:   commonpb.IndexState_Finished,
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
					channelCPs: newChannelCps(),

					segments: &SegmentsInfo{
						segments: map[int64]*SegmentInfo{
							1: {
								SegmentInfo:   genSeg(1, 200),
								lastFlushTime: time.Now().Add(-100 * time.Minute),
							},
							2: {
								SegmentInfo:   genSeg(2, 200),
								lastFlushTime: time.Now(),
							},
							3: {
								SegmentInfo:   genSeg(3, 200),
								lastFlushTime: time.Now(),
							},
							4: {
								SegmentInfo:   genSeg(4, 200),
								lastFlushTime: time.Now(),
							},
							5: {
								SegmentInfo:   genSeg(5, 200),
								lastFlushTime: time.Now(),
							},
							6: {
								SegmentInfo:   genSeg(6, 200),
								lastFlushTime: time.Now(),
							},
							7: {
								SegmentInfo:   genSeg(7, 200),
								lastFlushTime: time.Now(),
							},
						},
					},
					indexMeta: &indexMeta{
						segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
							1: genSegIndex(1, indexID, 20),
							2: genSegIndex(2, indexID, 20),
							3: genSegIndex(3, indexID, 20),
							4: genSegIndex(4, indexID, 20),
							5: genSegIndex(5, indexID, 20),
							6: genSegIndex(6, indexID, 20),
							7: genSegIndex(7, indexID, 20),
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
				mock0Allocator,
				make(chan *compactionSignal, 1),
				&spyCompactionHandler{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
			},
			args{
				2,
				&compactTime{},
			},
			false,
			nil,
		},
	}
	for _, tt := range tests {
		(tt.fields.compactionHandler).(*spyCompactionHandler).meta = tt.fields.meta
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.meta.channelCPs.checkpoints["ch1"] = &msgpb.MsgPosition{
				Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
				MsgID:     []byte{1, 2, 3, 4},
			}
			tr := &compactionTrigger{
				meta:                         tt.fields.meta,
				handler:                      newMockHandlerWithMeta(tt.fields.meta),
				allocator:                    tt.fields.allocator,
				signals:                      tt.fields.signals,
				compactionHandler:            tt.fields.compactionHandler,
				globalTrigger:                tt.fields.globalTrigger,
				indexEngineVersionManager:    newMockVersionManager(),
				estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
				estimateNonDiskSegmentPolicy: calBySchemaPolicy,
				closeCh:                      lifetime.NewSafeChan(),
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
		allocator         allocator.Allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
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
						{EntriesNum: 5, LogPath: "log1", LogSize: numRows * 1024 * 1024, MemorySize: numRows * 1024 * 1024},
					},
				},
			},
			IsSorted: true,
		}
	}

	genSegIndex := func(segID, indexID UniqueID, numRows int64) map[UniqueID]*model.SegmentIndex {
		return map[UniqueID]*model.SegmentIndex{
			indexID: {
				SegmentID:    segID,
				CollectionID: 2,
				PartitionID:  1,
				NumRows:      numRows,
				IndexID:      indexID,
				BuildID:      segID,
				NodeID:       0,
				IndexVersion: 1,
				IndexState:   commonpb.IndexState_Finished,
			},
		}
	}
	mock0Allocator := newMockAllocator(t)
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
					channelCPs: newChannelCps(),

					// 4 small segments
					segments: &SegmentsInfo{
						segments: map[int64]*SegmentInfo{
							1: {
								SegmentInfo:   genSeg(1, 600),
								lastFlushTime: time.Now().Add(-100 * time.Minute),
							},
							2: {
								SegmentInfo:   genSeg(2, 600),
								lastFlushTime: time.Now(),
							},
							3: {
								SegmentInfo:   genSeg(3, 600),
								lastFlushTime: time.Now(),
							},
							4: {
								SegmentInfo:   genSeg(4, 600),
								lastFlushTime: time.Now(),
							},
							5: {
								SegmentInfo:   genSeg(5, 260),
								lastFlushTime: time.Now(),
							},
							6: {
								SegmentInfo:   genSeg(6, 260),
								lastFlushTime: time.Now(),
							},
						},
					},
					indexMeta: &indexMeta{
						segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
							1: genSegIndex(1, indexID, 20),
							2: genSegIndex(2, indexID, 20),
							3: genSegIndex(3, indexID, 20),
							4: genSegIndex(4, indexID, 20),
							5: genSegIndex(5, indexID, 20),
							6: genSegIndex(6, indexID, 20),
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
				mock0Allocator,
				make(chan *compactionSignal, 1),
				&spyCompactionHandler{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
			},
			args{
				2,
				&compactTime{},
			},
			false,
			nil,
		},
	}
	for _, tt := range tests {
		(tt.fields.compactionHandler).(*spyCompactionHandler).meta = tt.fields.meta
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.meta.channelCPs.checkpoints["ch1"] = &msgpb.MsgPosition{
				Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
				MsgID:     []byte{1, 2, 3, 4},
			}
			tr := &compactionTrigger{
				meta:                         tt.fields.meta,
				handler:                      newMockHandlerWithMeta(tt.fields.meta),
				allocator:                    tt.fields.allocator,
				signals:                      tt.fields.signals,
				compactionHandler:            tt.fields.compactionHandler,
				globalTrigger:                tt.fields.globalTrigger,
				indexEngineVersionManager:    newMockVersionManager(),
				estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
				estimateNonDiskSegmentPolicy: calBySchemaPolicy,
				closeCh:                      lifetime.NewSafeChan(),
				testingOnly:                  true,
			}
			tr.start()
			defer tr.stop()
			err := tr.triggerCompaction()
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.compactionHandler).(*spyCompactionHandler)
			select {
			case val := <-spy.spyChan:
				// max size == 1000, expansion rate == 1.25.
				// segment 5 and 6 are squeezed into a non-planned segment. Total size: 600 + 260 + 260 == 1120,
				// which is greater than 1000 but smaller than 1000 * 1.25
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
		allocator         allocator.Allocator
		signals           chan *compactionSignal
		compactionHandler compactionPlanContext
		globalTrigger     *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}

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

	indexMeta := newSegmentIndexMeta(nil)
	indexMeta.indexes = map[UniqueID]map[UniqueID]*model.Index{
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
							{EntriesNum: 5, LogPath: "log1", LogSize: size[i] * 2 * 1024 * 1024, MemorySize: size[i] * 2 * 1024 * 1024},
						},
					},
				},
				IsSorted: true,
			},
			lastFlushTime: time.Now(),
		}

		indexMeta.updateSegmentIndex(&model.SegmentIndex{
			SegmentID:    i,
			CollectionID: 2,
			PartitionID:  1,
			NumRows:      100,
			IndexID:      indexID,
			BuildID:      i,
			NodeID:       0,
			IndexVersion: 1,
			IndexState:   commonpb.IndexState_Finished,
		})

		segmentInfos.segments[i] = info
	}

	mock0Allocator := newMockAllocator(t)

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
					channelCPs: newChannelCps(),

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
					indexMeta: indexMeta,
				},
				mock0Allocator,
				make(chan *compactionSignal, 1),
				&spyCompactionHandler{t: t, spyChan: make(chan *datapb.CompactionPlan, 10)},
				nil,
			},
			args{
				2,
				&compactTime{},
			},
			false,
			nil,
		},
	}
	for _, tt := range tests {
		(tt.fields.compactionHandler).(*spyCompactionHandler).meta = tt.fields.meta
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.meta.channelCPs.checkpoints["ch1"] = &msgpb.MsgPosition{
				Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
				MsgID:     []byte{1, 2, 3, 4},
			}
			tr := &compactionTrigger{
				meta:                      tt.fields.meta,
				handler:                   newMockHandlerWithMeta(tt.fields.meta),
				allocator:                 tt.fields.allocator,
				signals:                   tt.fields.signals,
				compactionHandler:         tt.fields.compactionHandler,
				globalTrigger:             tt.fields.globalTrigger,
				indexEngineVersionManager: newMockVersionManager(),
				closeCh:                   lifetime.NewSafeChan(),
				testingOnly:               true,
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

			assert.Equal(t, 4, len(plans))
			// plan 1: 250 + 20 * 10 + 3 * 20
			// plan 2: 200 + 7 * 20 + 4 * 40
			// plan 3: 128 + 6 * 40 + 127
			// plan 4: 300 + 128 + 128  ( < 512 * 1.25)
			// assert.Equal(t, 24, len(plans[0].GetInputSegments()))
			// assert.Equal(t, 12, len(plans[1].GetInputSegments()))
			// assert.Equal(t, 8, len(plans[2].GetInputSegments()))
			// assert.Equal(t, 3, len(plans[3].GetInputSegments()))
		})
	}
}

// Test shouldDoSingleCompaction
func Test_compactionTrigger_shouldDoSingleCompaction(t *testing.T) {
	indexMeta := newSegmentIndexMeta(nil)
	mock0Allocator := newMockAllocator(t)
	trigger := newCompactionTrigger(&meta{
		indexMeta:  indexMeta,
		channelCPs: newChannelCps(),
	}, &compactionPlanHandler{}, mock0Allocator, newMockHandler(), newIndexEngineVersionManager())

	// Test too many deltalogs.
	var binlogs []*datapb.FieldBinlog
	for i := UniqueID(0); i < 1000; i++ {
		binlogs = append(binlogs, &datapb.FieldBinlog{
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 5, LogPath: "log1", LogSize: 100, MemorySize: 100},
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

	couldDo := trigger.ShouldDoSingleCompaction(info, &compactTime{})
	assert.True(t, couldDo)

	// Test too many stats log
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

	couldDo = trigger.ShouldDoSingleCompaction(info, &compactTime{})
	assert.False(t, couldDo)

	// Test expire triggered  compaction
	var binlogs2 []*datapb.FieldBinlog
	for i := UniqueID(0); i < 100; i++ {
		binlogs2 = append(binlogs2, &datapb.FieldBinlog{
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 5, LogPath: "log1", LogSize: 100000, TimestampFrom: 300, TimestampTo: 500, MemorySize: 100000},
			},
		})
	}

	for i := UniqueID(0); i < 100; i++ {
		binlogs2 = append(binlogs2, &datapb.FieldBinlog{
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 5, LogPath: "log1", LogSize: 1000000, TimestampFrom: 300, TimestampTo: 1000, MemorySize: 1000000},
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
	couldDo = trigger.ShouldDoSingleCompaction(info2, &compactTime{expireTime: 300})
	assert.False(t, couldDo)

	// didn't reach single compaction size 10 * 1024 * 1024
	couldDo = trigger.ShouldDoSingleCompaction(info2, &compactTime{expireTime: 600})
	assert.False(t, couldDo)

	// expire time < Timestamp False
	couldDo = trigger.ShouldDoSingleCompaction(info2, &compactTime{expireTime: 1200})
	assert.True(t, couldDo)

	// Test Delete triggered compaction
	var binlogs3 []*datapb.FieldBinlog
	for i := UniqueID(0); i < 100; i++ {
		binlogs3 = append(binlogs2, &datapb.FieldBinlog{
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 5, LogPath: "log1", LogSize: 100000, TimestampFrom: 300, TimestampTo: 500, MemorySize: 100000},
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

	// deltalog is large enough, should do compaction
	couldDo = trigger.ShouldDoSingleCompaction(info3, &compactTime{})
	assert.True(t, couldDo)

	mockVersionManager := NewMockVersionManager(t)
	mockVersionManager.On("GetCurrentIndexEngineVersion", mock.Anything).Return(int32(2), nil)
	trigger.indexEngineVersionManager = mockVersionManager
	info4 := &SegmentInfo{
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

	info5 := &SegmentInfo{
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

	info6 := &SegmentInfo{
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

	indexMeta.updateSegmentIndex(&model.SegmentIndex{
		SegmentID:           1,
		IndexID:             101,
		CurrentIndexVersion: 1,
		IndexFileKeys:       []string{"index1"},
	})

	indexMeta.indexes = map[UniqueID]map[UniqueID]*model.Index{
		2: {
			101: {
				CollectionID: 2,
				IndexID:      101,
			},
		},
	}

	// expire time < Timestamp To, but index engine version is 2 which is larger than CurrentIndexVersion in segmentIndex
	Params.Save(Params.DataCoordCfg.AutoUpgradeSegmentIndex.Key, "true")
	couldDo = trigger.ShouldDoSingleCompaction(info4, &compactTime{expireTime: 300})
	assert.True(t, couldDo)

	indexMeta.updateSegmentIndex(&model.SegmentIndex{
		SegmentID:           1,
		IndexID:             101,
		CurrentIndexVersion: 2,
		IndexFileKeys:       []string{"index1"},
	})
	// expire time < Timestamp To, and index engine version is 2 which is equal CurrentIndexVersion in segmentIndex
	couldDo = trigger.ShouldDoSingleCompaction(info5, &compactTime{expireTime: 300})
	assert.False(t, couldDo)

	indexMeta.updateSegmentIndex(&model.SegmentIndex{
		SegmentID:           1,
		IndexID:             101,
		CurrentIndexVersion: 1,
		IndexFileKeys:       nil,
	})
	// expire time < Timestamp To, and index engine version is 2 which is larger than CurrentIndexVersion in segmentIndex but indexFileKeys is nil
	couldDo = trigger.ShouldDoSingleCompaction(info6, &compactTime{expireTime: 300})
	assert.False(t, couldDo)
}

func Test_compactionTrigger_new(t *testing.T) {
	type args struct {
		meta              *meta
		compactionHandler compactionPlanContext
		allocator         allocator.Allocator
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
				allocator.NewMockAllocator(t),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newCompactionTrigger(tt.args.meta, tt.args.compactionHandler, tt.args.allocator, newMockHandler(), newMockVersionManager())
			assert.Equal(t, tt.args.meta, got.meta)
			assert.Equal(t, tt.args.compactionHandler, got.compactionHandler)
			assert.Equal(t, tt.args.allocator, got.allocator)
		})
	}
}

func Test_compactionTrigger_getCompactTime(t *testing.T) {
	coll := &collectionInfo{
		ID:         1,
		Schema:     newTestSchema(),
		Partitions: []UniqueID{1},
		Properties: map[string]string{
			common.CollectionTTLConfigKey: "10",
		},
	}
	now := tsoutil.GetCurrentTime()
	ct, err := getCompactTime(now, coll)
	assert.NoError(t, err)
	assert.NotNil(t, ct)
}

func Test_triggerSingleCompaction(t *testing.T) {
	originValue := Params.DataCoordCfg.EnableAutoCompaction.GetValue()
	Params.Save(Params.DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer func() {
		Params.Save(Params.DataCoordCfg.EnableAutoCompaction.Key, originValue)
	}()
	m := &meta{
		channelCPs: newChannelCps(),
		segments:   NewSegmentsInfo(), collections: make(map[UniqueID]*collectionInfo),
	}
	got := newCompactionTrigger(m, &compactionPlanHandler{}, newMockAllocator(t),
		&ServerHandler{
			&Server{
				meta: m,
			},
		}, newMockVersionManager())
	got.signals = make(chan *compactionSignal, 1)
	{
		err := got.triggerSingleCompaction(1, 1, 1, "a", false)
		assert.NoError(t, err)
	}
	{
		err := got.triggerSingleCompaction(2, 2, 2, "b", false)
		assert.NoError(t, err)
	}
	var i satomic.Value
	i.Store(0)
	check := func() {
		for {
			select {
			case signal := <-got.signals:
				x := i.Load().(int)
				i.Store(x + 1)
				assert.EqualValues(t, 1, signal.collectionID)
			default:
				return
			}
		}
	}
	check()
	assert.Equal(t, 1, i.Load().(int))

	{
		err := got.triggerSingleCompaction(3, 3, 3, "c", true)
		assert.NoError(t, err)
	}
	var j satomic.Value
	j.Store(0)
	go func() {
		timeoutCtx, cancelFunc := context.WithTimeout(context.Background(), time.Second)
		defer cancelFunc()
		for {
			select {
			case signal := <-got.signals:
				x := j.Load().(int)
				j.Store(x + 1)
				if x == 0 {
					assert.EqualValues(t, 3, signal.collectionID)
				} else if x == 1 {
					assert.EqualValues(t, 4, signal.collectionID)
				}
			case <-timeoutCtx.Done():
				return
			}
		}
	}()
	{
		err := got.triggerSingleCompaction(4, 4, 4, "d", true)
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool {
		return j.Load().(int) == 2
	}, 2*time.Second, 500*time.Millisecond)
}

type CompactionTriggerSuite struct {
	suite.Suite

	collectionID int64
	partitionID  int64
	channel      string

	indexID    int64
	vecFieldID int64

	meta              *meta
	tr                *compactionTrigger
	allocator         *allocator.MockAllocator
	handler           *NMockHandler
	compactionHandler *MockCompactionPlanContext
	versionManager    *MockVersionManager
}

func (s *CompactionTriggerSuite) SetupSuite() {
	paramtable.Init()
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
					{EntriesNum: 5, LogPath: "log1", LogSize: 100, MemorySize: 100},
				},
			},
		},
		IsSorted: true,
	}
}

func (s *CompactionTriggerSuite) genSegIndex(segID, indexID UniqueID, numRows int64) map[UniqueID]*model.SegmentIndex {
	return map[UniqueID]*model.SegmentIndex{
		indexID: {
			SegmentID:    segID,
			CollectionID: s.collectionID,
			PartitionID:  s.partitionID,
			NumRows:      numRows,
			IndexID:      indexID,
			BuildID:      segID,
			NodeID:       0,
			IndexVersion: 1,
			IndexState:   commonpb.IndexState_Finished,
		},
	}
}

func (s *CompactionTriggerSuite) SetupTest() {
	s.collectionID = 100
	s.partitionID = 200
	s.indexID = 300
	s.vecFieldID = 400
	s.channel = "dml_0_100v0"
	catalog := mocks.NewDataCoordCatalog(s.T())
	catalog.EXPECT().SaveChannelCheckpoint(mock.Anything, s.channel, mock.Anything).Return(nil)

	seg1 := &SegmentInfo{
		SegmentInfo:   s.genSeg(1, 60),
		lastFlushTime: time.Now().Add(-100 * time.Minute),
	}
	seg2 := &SegmentInfo{
		SegmentInfo:   s.genSeg(2, 60),
		lastFlushTime: time.Now(),
	}
	seg3 := &SegmentInfo{
		SegmentInfo:   s.genSeg(3, 60),
		lastFlushTime: time.Now(),
	}
	seg4 := &SegmentInfo{
		SegmentInfo:   s.genSeg(4, 60),
		lastFlushTime: time.Now(),
	}
	seg5 := &SegmentInfo{
		SegmentInfo:   s.genSeg(5, 60),
		lastFlushTime: time.Now(),
	}
	seg6 := &SegmentInfo{
		SegmentInfo:   s.genSeg(6, 60),
		lastFlushTime: time.Now(),
	}

	s.meta = &meta{
		channelCPs: newChannelCps(),
		catalog:    catalog,
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{
				1: seg1,
				2: seg2,
				3: seg3,
				4: seg4,
				5: seg5,
				6: seg6,
			},
			secondaryIndexes: segmentInfoIndexes{
				coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
					s.collectionID: {
						1: seg1,
						2: seg2,
						3: seg3,
						4: seg4,
						5: seg5,
						6: seg6,
					},
				},
				channel2Segments: map[string]map[UniqueID]*SegmentInfo{
					s.channel: {
						1: seg1,
						2: seg2,
						3: seg3,
						4: seg4,
						5: seg5,
						6: seg6,
					},
				},
			},
		},
		indexMeta: &indexMeta{
			segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
				1: s.genSegIndex(1, indexID, 60),
				2: s.genSegIndex(2, indexID, 60),
				3: s.genSegIndex(3, indexID, 60),
				4: s.genSegIndex(4, indexID, 60),
				5: s.genSegIndex(5, indexID, 26),
				6: s.genSegIndex(6, indexID, 26),
			},
			indexes: map[UniqueID]map[UniqueID]*model.Index{
				s.collectionID: {
					s.indexID: {
						TenantID:     "",
						CollectionID: s.collectionID,
						FieldID:      s.vecFieldID,
						IndexID:      s.indexID,
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
			},
		},
		collections: map[int64]*collectionInfo{
			s.collectionID: {
				ID: s.collectionID,
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:  s.vecFieldID,
							DataType: schemapb.DataType_FloatVector,
						},
					},
				},
			},
		},
	}
	s.meta.UpdateChannelCheckpoint(s.channel, &msgpb.MsgPosition{
		ChannelName: s.channel,
		Timestamp:   tsoutil.ComposeTSByTime(time.Now(), 0),
		MsgID:       []byte{1, 2, 3, 4},
	})
	s.allocator = allocator.NewMockAllocator(s.T())
	s.compactionHandler = NewMockCompactionPlanContext(s.T())
	s.handler = NewNMockHandler(s.T())
	s.versionManager = NewMockVersionManager(s.T())
	s.tr = newCompactionTrigger(
		s.meta,
		s.compactionHandler,
		s.allocator,
		s.handler,
		s.versionManager,
	)
	s.tr.testingOnly = true
}

func (s *CompactionTriggerSuite) TestHandleSignal() {
	s.Run("getCompaction_failed", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		// s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(nil, errors.New("mocked"))
		tr.handleSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check compactionHandler.enqueueCompaction never called
	})

	s.Run("collectionAutoCompactionConfigError", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		// s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "bad_value",
			},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:  s.vecFieldID,
						DataType: schemapb.DataType_FloatVector,
					},
				},
			},
		}, nil)
		tr.handleSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check compactionHandler.enqueueCompaction never called
	})

	s.Run("collectionAutoCompactionDisabled", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		// s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "false",
			},
			ID: s.collectionID,
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:  s.vecFieldID,
						DataType: schemapb.DataType_FloatVector,
					},
				},
			},
		}, nil)
		tr.handleSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check compactionHandler.enqueueCompaction never called
	})

	s.Run("collectionAutoCompactionDisabled_force", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		// s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		// s.allocator.EXPECT().AllocID(mock.Anything).Return(20000, nil)
		start := int64(20000)
		s.allocator.EXPECT().AllocN(mock.Anything).RunAndReturn(func(i int64) (int64, int64, error) {
			return start, start + i, nil
		})
		s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "false",
			},
			Schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:  s.vecFieldID,
						DataType: schemapb.DataType_FloatVector,
					},
				},
			},
		}, nil)
		s.compactionHandler.EXPECT().enqueueCompaction(mock.Anything).Return(nil)
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
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.StartOfUserFieldID,
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
			{
				FieldID:  common.StartOfUserFieldID + 1,
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "128",
					},
				},
			},
		},
	}
	s.Run("getCompaction_failed", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		// s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(nil, errors.New("mocked"))
		tr.handleGlobalSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check compactionHandler.enqueueCompaction never called
	})

	s.Run("collectionAutoCompactionConfigError", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Schema: schema,
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

		// suite shall check compactionHandler.enqueueCompaction never called
	})

	s.Run("collectionAutoCompactionDisabled", func() {
		defer s.SetupTest()
		tr := s.tr
		s.compactionHandler.EXPECT().isFull().Return(false)
		// s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Schema: schema,
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

		// suite shall check compactionHandler.enqueueCompaction never called
	})

	s.Run("collectionAutoCompactionDisabled_force", func() {
		defer s.SetupTest()
		tr := s.tr
		// s.compactionHandler.EXPECT().isFull().Return(false)
		// s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		// s.allocator.EXPECT().AllocID(mock.Anything).Return(20000, nil).Maybe()
		start := int64(20000)
		s.allocator.EXPECT().AllocN(mock.Anything).RunAndReturn(func(i int64) (int64, int64, error) {
			return start, start + i, nil
		}).Maybe()
		s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Schema: schema,
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "false",
			},
		}, nil)
		// s.compactionHandler.EXPECT().enqueueCompaction(mock.Anything).Return(nil)
		tr.handleGlobalSignal(&compactionSignal{
			segmentID:    1,
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      true,
		})
	})
}

func (s *CompactionTriggerSuite) TestSqueezeSmallSegments() {
	expectedSize := int64(70000)
	smallsegments := []*SegmentInfo{
		{SegmentInfo: &datapb.SegmentInfo{ID: 3}, size: *atomic.NewInt64(69999)},
		{SegmentInfo: &datapb.SegmentInfo{ID: 1}, size: *atomic.NewInt64(100)},
	}

	largeSegment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 2}, size: *atomic.NewInt64(expectedSize)}
	buckets := [][]*SegmentInfo{{largeSegment}}
	s.Require().Equal(1, len(buckets))
	s.Require().Equal(1, len(buckets[0]))

	remaining := s.tr.squeezeSmallSegmentsToBuckets(smallsegments, buckets, expectedSize)
	s.Equal(1, len(remaining))
	s.EqualValues(3, remaining[0].ID)

	s.Equal(1, len(buckets))
	s.Equal(2, len(buckets[0]))
	log.Info("buckets", zap.Any("buckets", buckets))
}

//func Test_compactionTrigger_clustering(t *testing.T) {
//	paramtable.Init()
//	catalog := mocks.NewDataCoordCatalog(t)
//	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()
//	vecFieldID := int64(201)
//	meta := &meta{
//		catalog: catalog,
//		collections: map[int64]*collectionInfo{
//			1: {
//				ID: 1,
//				Schema: &schemapb.CollectionSchema{
//					Fields: []*schemapb.FieldSchema{
//						{
//							FieldID:  vecFieldID,
//							DataType: schemapb.DataType_FloatVector,
//							TypeParams: []*commonpb.KeyValuePair{
//								{
//									Key:   common.DimKey,
//									Value: "128",
//								},
//							},
//						},
//					},
//				},
//			},
//		},
//	}
//
//	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ClusteringCompactionEnable.Key, "false")
//	allocator := &MockAllocator0{}
//	tr := &compactionTrigger{
//		handler:                      newMockHandlerWithMeta(meta),
//		allocator:                    allocator,
//		estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
//		estimateNonDiskSegmentPolicy: calBySchemaPolicy,
//		testingOnly:                  true,
//	}
//	_, err := tr.triggerManualCompaction(1, true)
//	assert.Error(t, err)
//	assert.True(t, errors.Is(err, merr.ErrClusteringCompactionClusterNotSupport))
//	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ClusteringCompactionEnable.Key, "true")
//	_, err2 := tr.triggerManualCompaction(1, true)
//	assert.Error(t, err2)
//	assert.True(t, errors.Is(err2, merr.ErrClusteringCompactionCollectionNotSupport))
//}

func TestCompactionTriggerSuite(t *testing.T) {
	suite.Run(t, new(CompactionTriggerSuite))
}
