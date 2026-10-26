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
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// wantBucket is a lightweight assertion helper describing the expected
// output of a single compactionBucket produced by generatePlans.
type wantBucket struct {
	segmentIDs []int64
	maxSize    int64
}

type spyCompactionInspector struct {
	t       *testing.T
	spyChan chan *datapb.CompactionPlan
	meta    *meta
}

// getCompactionTasksNum implements CompactionInspector.
func (h *spyCompactionInspector) getCompactionTasksNum(filters ...compactionTaskFilter) int {
	return 0
}

func (h *spyCompactionInspector) getCompactionTasksNumBySignalID(signalID int64) int {
	return 0
}

func (h *spyCompactionInspector) getCompactionInfo(ctx context.Context, signalID int64) *compactionInfo {
	return nil
}

var _ CompactionInspector = (*spyCompactionInspector)(nil)

func (h *spyCompactionInspector) removeTasksByChannel(channel string) {}

// enqueueCompaction start to execute plan and return immediately
func (h *spyCompactionInspector) enqueueCompaction(task *datapb.CompactionTask) error {
	t := newMixCompactionTask(task, nil, h.meta, newMockVersionManager())
	alloc := newMock0Allocator(h.t)
	t.allocator = alloc
	plan, err := t.BuildCompactionRequest()
	h.spyChan <- plan
	return err
}

func (h *spyCompactionInspector) checkAndSetSegmentStating(channel string, segmentID int64) bool {
	return false
}

// isFull return true if the task pool is full
func (h *spyCompactionInspector) isFull() bool {
	return false
}

func (h *spyCompactionInspector) start() {}

func (h *spyCompactionInspector) stop() {}

func newMockVersionManager() IndexEngineVersionManager {
	return &versionManagerImpl{}
}

var _ CompactionInspector = (*spyCompactionInspector)(nil)

func Test_compactionTrigger_force_without_index(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterSegments(mock.Anything, mock.Anything).Return(nil).Maybe()

	collectionID := int64(11)
	binlogs := []*datapb.FieldBinlog{
		{
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 5, LogID: 1},
			},
		},
	}
	deltaLogs := []*datapb.FieldBinlog{
		{
			Binlogs: []*datapb.Binlog{
				{EntriesNum: 5, LogID: 1},
			},
		},
	}

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
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

	segInfo := &datapb.SegmentInfo{
		ID:             1,
		CollectionID:   collectionID,
		PartitionID:    1,
		LastExpireTime: 100,
		NumOfRows:      100,
		MaxRowNum:      300,
		InsertChannel:  "ch1",
		State:          commonpb.SegmentState_Flushed,
		Binlogs:        binlogs,
		Deltalogs:      deltaLogs,
		IsSorted:       true,
	}
	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(collectionID, &collectionInfo{
		ID:     collectionID,
		Schema: schema,
	})
	m := &meta{
		catalog:    catalog,
		channelCPs: newChannelCps(),
		segments: &SegmentsInfo{
			segments: map[int64]*SegmentInfo{
				1: {
					SegmentInfo: segInfo,
				},
			},
			secondaryIndexes: segmentInfoIndexes{
				coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
					collectionID: {
						1: {
							SegmentInfo: segInfo,
						},
					},
				},
			},
		},
		indexMeta: &indexMeta{
			segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
			indexes:        map[UniqueID]map[UniqueID]*model.Index{},
		},
		collections: collections,
	}

	inspector := &spyCompactionInspector{t: t, spyChan: make(chan *datapb.CompactionPlan, 1), meta: m}
	tr := &compactionTrigger{
		meta:          m,
		handler:       newMockHandlerWithMeta(m),
		allocator:     newMock0Allocator(t),
		signals:       make(chan *compactionSignal, 100),
		manualSignals: make(chan *compactionSignal, 100),
		inspector:     inspector,
		globalTrigger: nil,
		closeCh:       lifetime.NewSafeChan(),
		testingOnly:   true,
	}
	tr.closeWaiter.Add(1)
	go func() {
		defer tr.closeWaiter.Done()
		tr.work()
	}()
	defer tr.stop()

	_, err := tr.TriggerCompaction(context.TODO(), NewCompactionSignal().WithCollectionID(collectionID).WithIsForce(true))
	assert.NoError(t, err)

	select {
	case val := <-inspector.spyChan:
		assert.Equal(t, 1, len(val.SegmentBinlogs))
		return
	case <-time.After(3 * time.Second):
		assert.Fail(t, "failed to get plan")
		return
	}
}

func Test_compactionTrigger_force(t *testing.T) {
	paramtable.Init()
	type fields struct {
		meta          *meta
		allocator     allocator.Allocator
		signals       chan *compactionSignal
		inspector     CompactionInspector
		globalTrigger *time.Ticker
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

	seg1 := &SegmentInfo{
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
	}

	seg2 := &SegmentInfo{
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
	}

	seg3 := &SegmentInfo{
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
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(2, &collectionInfo{
		ID:     2,
		Schema: schema,
		Properties: map[string]string{
			common.CollectionTTLConfigKey: "0",
		},
	})
	collections.Insert(1111, &collectionInfo{
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
	})
	collections.Insert(1000, &collectionInfo{
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
	})
	// error (has no vector field)
	collections.Insert(2000, &collectionInfo{
		ID: 2000,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  vecFieldID,
					DataType: schemapb.DataType_Int16,
				},
			},
		},
	})
	// error (has no dim)
	collections.Insert(3000, &collectionInfo{
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
	})
	// error (dim parse fail)
	collections.Insert(4000, &collectionInfo{
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
	})
	collections.Insert(10000, &collectionInfo{
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
	})
	im := &indexMeta{
		segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
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
	}
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           1,
		CollectionID:        2,
		PartitionID:         1,
		NumRows:             100,
		IndexID:             indexID,
		BuildID:             1,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx2 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx2.Insert(indexID, &model.SegmentIndex{
		SegmentID:           2,
		CollectionID:        2,
		PartitionID:         1,
		NumRows:             100,
		IndexID:             indexID,
		BuildID:             2,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIdx3 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx3.Insert(indexID, &model.SegmentIndex{
		SegmentID:           3,
		CollectionID:        1111,
		PartitionID:         1,
		NumRows:             100,
		IndexID:             indexID,
		BuildID:             3,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	im.segmentIndexes.Insert(1, segIdx1)
	im.segmentIndexes.Insert(2, segIdx2)
	im.segmentIndexes.Insert(3, segIdx3)

	params, err := compaction.GenerateJSONParams()
	if err != nil {
		panic(err)
	}

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
							1: seg1,
							2: seg2,
							3: seg3,
						},
						secondaryIndexes: segmentInfoIndexes{
							coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
								2: {
									seg1.GetID(): seg1,
									seg2.GetID(): seg2,
								},
								1111: {
									seg3.GetID(): seg3,
								},
							},
						},
					},
					indexMeta:   im,
					collections: collections,
				},
				mock0Allocator,
				nil,
				&spyCompactionInspector{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
			},
			2,
			false,
			[]int64{
				1, 2,
			},
			[]*datapb.CompactionPlan{
				{
					PlanID: 100,
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
					BeginLogID:             100,
					Type:                   datapb.CompactionType_MixCompaction,
					Channel:                "ch1",
					TotalRows:              200,
					Schema:                 schema,
					PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 101, End: 200},
					PreAllocatedLogIDs:     &datapb.IDRange{Begin: 100, End: 200},
					MaxSize:                1073741824,
					SlotUsage:              paramtable.Get().DataCoordCfg.MixCompactionSlotUsage.GetAsInt64(),
					JsonParams:             params,
				},
			},
		},
	}
	for _, tt := range tests {
		tt.fields.inspector.(*spyCompactionInspector).meta = tt.fields.meta
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				meta:          tt.fields.meta,
				handler:       newMockHandlerWithMeta(tt.fields.meta),
				allocator:     tt.fields.allocator,
				signals:       make(chan *compactionSignal, 100),
				manualSignals: make(chan *compactionSignal, 100),
				inspector:     tt.fields.inspector,
				globalTrigger: tt.fields.globalTrigger,
				closeCh:       lifetime.NewSafeChan(),
				testingOnly:   true,
			}
			tr.closeWaiter.Add(1)
			go func() {
				defer tr.closeWaiter.Done()
				tr.work()
			}()
			defer tr.stop()
			_, err := tr.TriggerCompaction(context.TODO(), NewCompactionSignal().WithCollectionID(tt.collectionID).WithIsForce(true))
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.inspector).(*spyCompactionInspector)
			select {
			case plan := <-spy.spyChan:
				assert.Equal(t, 2, len(plan.SegmentBinlogs))
			case <-time.After(3 * time.Second):
				assert.Fail(t, "timeout waiting for plan")
				return
			}
		})

		t.Run(tt.name+" with DiskANN index", func(t *testing.T) {
			for _, segment := range tt.fields.meta.segments.GetSegments() {
				// Collection 1000 means it has DiskANN index
				delete(tt.fields.meta.segments.secondaryIndexes.coll2Segments[segment.GetCollectionID()], segment.GetID())
				segment.CollectionID = 1000
				_, ok := tt.fields.meta.segments.secondaryIndexes.coll2Segments[segment.GetCollectionID()]
				if !ok {
					tt.fields.meta.segments.secondaryIndexes.coll2Segments[segment.GetCollectionID()] = make(map[UniqueID]*SegmentInfo)
				}
				tt.fields.meta.segments.secondaryIndexes.coll2Segments[segment.GetCollectionID()][segment.GetID()] = segment
			}
			tr := &compactionTrigger{
				meta:          tt.fields.meta,
				handler:       newMockHandlerWithMeta(tt.fields.meta),
				allocator:     tt.fields.allocator,
				signals:       make(chan *compactionSignal, 100),
				manualSignals: make(chan *compactionSignal, 100),
				inspector:     tt.fields.inspector,
				globalTrigger: tt.fields.globalTrigger,
				closeCh:       lifetime.NewSafeChan(),
				testingOnly:   true,
			}
			tr.closeWaiter.Add(1)
			go func() {
				defer tr.closeWaiter.Done()
				tr.work()
			}()
			defer tr.stop()
			tt.collectionID = 1000
			_, err := tr.TriggerCompaction(context.TODO(), NewCompactionSignal().WithCollectionID(tt.collectionID).WithIsForce(true))
			assert.Equal(t, tt.wantErr, err != nil)
			// expect max row num =  2048*1024*1024/(128*4) = 4194304
			// assert.EqualValues(t, 4194304, tt.fields.meta.segments.GetSegments()[0].MaxRowNum)
			spy := (tt.fields.inspector).(*spyCompactionInspector)
			select {
			case plan := <-spy.spyChan:
				assert.NotNil(t, plan)
				assert.Equal(t, 3, len(plan.SegmentBinlogs))
			case <-time.After(3 * time.Second):
				assert.Fail(t, "timeout waiting for plans")
			}
		})

		t.Run(tt.name+" with getCompact error", func(t *testing.T) {
			for _, segment := range tt.fields.meta.segments.GetSegments() {
				segment.CollectionID = 1111
			}
			tr := &compactionTrigger{
				meta:          tt.fields.meta,
				handler:       newMockHandlerWithMeta(tt.fields.meta),
				allocator:     tt.fields.allocator,
				signals:       make(chan *compactionSignal, 100),
				manualSignals: make(chan *compactionSignal, 100),
				inspector:     tt.fields.inspector,
				globalTrigger: tt.fields.globalTrigger,
				closeCh:       lifetime.NewSafeChan(),
				testingOnly:   true,
			}
			tr.closeWaiter.Add(1)
			go func() {
				defer tr.closeWaiter.Done()
				tr.work()
			}()
			defer tr.stop()

			{
				// test getCompactTime fail for handle global signal
				signal := NewCompactionSignal().WithCollectionID(1111).WithIsForce(true)
				tr.TriggerCompaction(context.TODO(), signal)

				spy := (tt.fields.inspector).(*spyCompactionInspector)
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
				signal := NewCompactionSignal().WithCollectionID(1111).WithIsForce(true).WithSegmentIDs(3)
				tr.TriggerCompaction(context.TODO(), signal)

				spy := (tt.fields.inspector).(*spyCompactionInspector)
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
		meta          *meta
		allocator     allocator.Allocator
		signals       chan *compactionSignal
		inspector     CompactionInspector
		globalTrigger *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	vecFieldID := int64(201)
	segmentInfos := &SegmentsInfo{
		segments: make(map[UniqueID]*SegmentInfo),
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments: make(map[UniqueID]map[UniqueID]*SegmentInfo),
		},
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

	segmentInfos.secondaryIndexes.coll2Segments[2] = make(map[UniqueID]*SegmentInfo)
	nSegments := 50
	for i := UniqueID(0); i < UniqueID(nSegments); i++ {
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
		segmentInfos.secondaryIndexes.coll2Segments[2][i] = info
	}

	mock0Allocator := newMockAllocator(t)

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(2, &collectionInfo{
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
	})

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
					segments:    segmentInfos,
					channelCPs:  newChannelCps(),
					collections: collections,
					indexMeta:   indexMeta,
				},
				mock0Allocator,
				nil,
				&spyCompactionInspector{t: t, spyChan: make(chan *datapb.CompactionPlan, nSegments)},
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
					BeginLogID:         100,
					PreAllocatedLogIDs: &datapb.IDRange{Begin: 200, End: 2000},
					StartTime:          3,
					Type:               datapb.CompactionType_MixCompaction,
					Channel:            "ch1",
					MaxSize:            1342177280,
				},
			},
		},
	}
	for _, tt := range tests {
		(tt.fields.inspector).(*spyCompactionInspector).meta = tt.fields.meta
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				meta:          tt.fields.meta,
				handler:       newMockHandlerWithMeta(tt.fields.meta),
				allocator:     tt.fields.allocator,
				signals:       make(chan *compactionSignal, 100),
				manualSignals: make(chan *compactionSignal, 100),
				inspector:     tt.fields.inspector,
				globalTrigger: tt.fields.globalTrigger,
				closeCh:       lifetime.NewSafeChan(),
				testingOnly:   true,
			}
			tr.closeWaiter.Add(1)
			go func() {
				defer tr.closeWaiter.Done()
				tr.work()
			}()
			defer tr.stop()
			_, err := tr.TriggerCompaction(context.TODO(), NewCompactionSignal().WithCollectionID(tt.args.collectionID).WithIsForce(true))
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.inspector).(*spyCompactionInspector)

			select {
			case plan := <-spy.spyChan:
				assert.NotEmpty(t, plan)
				assert.Equal(t, nSegments, len(plan.SegmentBinlogs))
			case <-time.After(2 * time.Second):
				assert.Fail(t, "timeout")
			}
		})
	}
}

// Test no compaction selection
func Test_compactionTrigger_noplan(t *testing.T) {
	type fields struct {
		meta          *meta
		allocator     allocator.Allocator
		signals       chan *compactionSignal
		inspector     CompactionInspector
		globalTrigger *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	vecFieldID := int64(201)
	mock0Allocator := newMockAllocator(t)
	im := newSegmentIndexMeta(nil)
	im.indexes[2] = make(map[UniqueID]*model.Index)

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(2, &collectionInfo{
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
	})

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
					indexMeta: im,
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
					collections: collections,
				},
				mock0Allocator,
				make(chan *compactionSignal, 1),
				&spyCompactionInspector{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
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
				meta:          tt.fields.meta,
				handler:       newMockHandlerWithMeta(tt.fields.meta),
				allocator:     tt.fields.allocator,
				signals:       make(chan *compactionSignal, 100),
				inspector:     tt.fields.inspector,
				globalTrigger: tt.fields.globalTrigger,
				closeCh:       lifetime.NewSafeChan(),
				testingOnly:   true,
			}
			tr.start()
			defer tr.stop()
			_, err := tr.TriggerCompaction(context.TODO(), NewCompactionSignal())
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.inspector).(*spyCompactionInspector)
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

func mockSegment(segID, rows, deleteRows, sizeInMB int64) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:             segID,
		CollectionID:   2,
		PartitionID:    1,
		LastExpireTime: 100,
		NumOfRows:      sizeInMB,
		MaxRowNum:      150,
		InsertChannel:  "ch1",
		State:          commonpb.SegmentState_Flushed,
		Binlogs: []*datapb.FieldBinlog{
			{
				Binlogs: []*datapb.Binlog{
					{EntriesNum: rows, LogPath: "log1", LogSize: 100 * 1024 * 1024, MemorySize: sizeInMB * 1024 * 1024},
				},
			},
		},
		Deltalogs: []*datapb.FieldBinlog{
			{
				Binlogs: []*datapb.Binlog{
					{EntriesNum: deleteRows, LogPath: "deltalog1"},
				},
			},
		},
		IsSorted: true,
	}
}

func mockSegmentsInfo(sizeInMB ...int64) *SegmentsInfo {
	segments := make(map[int64]*SegmentInfo, len(sizeInMB))
	collectionID := int64(2)
	channel := "ch1"
	coll2Segments := make(map[UniqueID]map[UniqueID]*SegmentInfo)
	coll2Segments[collectionID] = make(map[UniqueID]*SegmentInfo)
	channel2Segments := make(map[string]map[UniqueID]*SegmentInfo)
	channel2Segments[channel] = make(map[UniqueID]*SegmentInfo)
	for i, size := range sizeInMB {
		segId := int64(i + 1)
		info := &SegmentInfo{
			SegmentInfo:   mockSegment(segId, size, 1, size),
			lastFlushTime: time.Now().Add(-100 * time.Minute),
		}
		segments[segId] = info
		coll2Segments[collectionID][segId] = info
		channel2Segments[channel][segId] = info
	}
	return &SegmentsInfo{
		segments: segments,
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments:    coll2Segments,
			channel2Segments: channel2Segments,
		},
	}
}

// Test compaction with prioritized candi
func Test_compactionTrigger_PrioritizedCandi(t *testing.T) {
	Params.Save(Params.DataCoordCfg.TwoTierCompaction.Key, "true")
	defer Params.Save(Params.DataCoordCfg.TwoTierCompaction.Key, "false")

	type fields struct {
		meta          *meta
		allocator     allocator.Allocator
		signals       chan *compactionSignal
		inspector     CompactionInspector
		globalTrigger *time.Ticker
	}
	vecFieldID := int64(201)

	mock0Allocator := newMockAllocator(t)

	genSegIndex := func(segID, indexID UniqueID, numRows int64) *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex] {
		segIdx := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdx.Insert(indexID, &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: 2,
			PartitionID:  1,
			NumRows:      numRows,
			IndexID:      indexID,
			BuildID:      segID,
			NodeID:       0,
			IndexVersion: 1,
			IndexState:   commonpb.IndexState_Finished,
		})
		return segIdx
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(2, &collectionInfo{
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
	})

	im := &indexMeta{
		segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
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
	}
	im.segmentIndexes.Insert(1, genSegIndex(1, indexID, 20))
	im.segmentIndexes.Insert(2, genSegIndex(2, indexID, 20))
	im.segmentIndexes.Insert(3, genSegIndex(3, indexID, 20))
	im.segmentIndexes.Insert(4, genSegIndex(4, indexID, 20))
	im.segmentIndexes.Insert(5, genSegIndex(5, indexID, 20))
	im.segmentIndexes.Insert(6, genSegIndex(6, indexID, 20))
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

					segments:    mockSegmentsInfo(20, 20, 20, 20, 20, 20),
					indexMeta:   im,
					collections: collections,
				},
				mock0Allocator,
				make(chan *compactionSignal, 1),
				&spyCompactionInspector{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
				nil,
			},
			false,
			nil,
		},
	}
	for _, tt := range tests {
		(tt.fields.inspector).(*spyCompactionInspector).meta = tt.fields.meta
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.meta.channelCPs.checkpoints["ch1"] = &msgpb.MsgPosition{
				Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
				MsgID:     []byte{1, 2, 3, 4},
			}
			tr := &compactionTrigger{
				meta:          tt.fields.meta,
				handler:       newMockHandlerWithMeta(tt.fields.meta),
				allocator:     tt.fields.allocator,
				signals:       make(chan *compactionSignal, 100),
				inspector:     tt.fields.inspector,
				globalTrigger: tt.fields.globalTrigger,
				closeCh:       lifetime.NewSafeChan(),
				testingOnly:   true,
			}
			tr.start()
			defer tr.stop()
			_, err := tr.TriggerCompaction(context.TODO(), NewCompactionSignal())
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.inspector).(*spyCompactionInspector)
			select {
			case val := <-spy.spyChan:
				assert.Fail(t, "we expect no compaction generated", val)
				return
			case <-time.After(3 * time.Second):
				// All 6 segments are fragments (residual size far below
				// the fragment threshold), but their count stays under
				// MaxFragmentsPerGroup, so the two-tier algorithm emits
				// no bucket for them.
				return
			}
		})
	}
}

// Test compaction with small candi
func Test_compactionTrigger_SmallCandi(t *testing.T) {
	Params.Save(Params.DataCoordCfg.TwoTierCompaction.Key, "true")
	defer Params.Save(Params.DataCoordCfg.TwoTierCompaction.Key, "false")

	type fields struct {
		meta          *meta
		allocator     allocator.Allocator
		signals       chan *compactionSignal
		inspector     CompactionInspector
		globalTrigger *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}
	vecFieldID := int64(201)
	mock0Allocator := newMockAllocator(t)

	genSegIndex := func(segID, indexID UniqueID, numRows int64) *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex] {
		segIdx := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		segIdx.Insert(indexID, &model.SegmentIndex{
			SegmentID:    segID,
			CollectionID: 2,
			PartitionID:  1,
			NumRows:      numRows,
			IndexID:      indexID,
			BuildID:      segID,
			NodeID:       0,
			IndexVersion: 1,
			IndexState:   commonpb.IndexState_Finished,
		})
		return segIdx
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(2, &collectionInfo{
		ID: 2,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  vecFieldID,
					DataType: schemapb.DataType_FloatVector,
				},
			},
		},
	})

	im := &indexMeta{
		segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
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
	}
	im.segmentIndexes.Insert(1, genSegIndex(1, indexID, 20))
	im.segmentIndexes.Insert(2, genSegIndex(2, indexID, 20))
	im.segmentIndexes.Insert(3, genSegIndex(3, indexID, 20))
	im.segmentIndexes.Insert(4, genSegIndex(4, indexID, 20))
	im.segmentIndexes.Insert(5, genSegIndex(5, indexID, 20))
	im.segmentIndexes.Insert(6, genSegIndex(6, indexID, 20))
	im.segmentIndexes.Insert(7, genSegIndex(7, indexID, 20))
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
					// 7 segments with 200MB each. The first 5 clear the
					// full-tier fill-rate gate and are packed into one
					// bucket; the remaining 2 stay below
					// MaxFragmentsPerGroup and are left uncompacted.
					segments:    mockSegmentsInfo(200, 200, 200, 200, 200, 200, 200),
					indexMeta:   im,
					collections: collections,
				},
				mock0Allocator,
				make(chan *compactionSignal, 1),
				&spyCompactionInspector{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
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
		(tt.fields.inspector).(*spyCompactionInspector).meta = tt.fields.meta
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.meta.channelCPs.checkpoints["ch1"] = &msgpb.MsgPosition{
				Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
				MsgID:     []byte{1, 2, 3, 4},
			}
			tr := &compactionTrigger{
				meta:                      tt.fields.meta,
				handler:                   newMockHandlerWithMeta(tt.fields.meta),
				allocator:                 tt.fields.allocator,
				signals:                   make(chan *compactionSignal, 100),
				inspector:                 tt.fields.inspector,
				globalTrigger:             tt.fields.globalTrigger,
				indexEngineVersionManager: newMockVersionManager(),
				closeCh:                   lifetime.NewSafeChan(),
				testingOnly:               true,
			}
			tr.start()
			defer tr.stop()
			_, err := tr.TriggerCompaction(context.TODO(), NewCompactionSignal())
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.inspector).(*spyCompactionInspector)
			select {
			case val := <-spy.spyChan:
				// 5 of the 7 segments clear the full-tier fill-rate gate
				// and are packed into a single bucket; the remaining 2
				// stay below MaxFragmentsPerGroup and are left uncompacted.
				assert.Equal(t, 5, len(val.SegmentBinlogs))
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
	Params.Save(Params.DataCoordCfg.TwoTierCompaction.Key, "true")
	defer Params.Save(Params.DataCoordCfg.TwoTierCompaction.Key, "false")

	type fields struct {
		meta          *meta
		allocator     allocator.Allocator
		signals       chan *compactionSignal
		inspector     CompactionInspector
		globalTrigger *time.Ticker
	}
	type args struct {
		collectionID int64
		compactTime  *compactTime
	}

	segmentInfos := &SegmentsInfo{
		segments: make(map[UniqueID]*SegmentInfo),
		secondaryIndexes: segmentInfoIndexes{
			coll2Segments:    map[UniqueID]map[UniqueID]*SegmentInfo{2: {}},
			channel2Segments: map[string]map[UniqueID]*SegmentInfo{"ch1": {}},
		},
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
		segmentInfos.secondaryIndexes.coll2Segments[2][i] = info
		segmentInfos.secondaryIndexes.channel2Segments["ch1"][i] = info
	}

	mock0Allocator := newMockAllocator(t)

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(2, &collectionInfo{
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
	})

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

					segments:    segmentInfos,
					collections: collections,
					indexMeta:   indexMeta,
				},
				mock0Allocator,
				make(chan *compactionSignal, 1),
				&spyCompactionInspector{t: t, spyChan: make(chan *datapb.CompactionPlan, 10)},
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
		(tt.fields.inspector).(*spyCompactionInspector).meta = tt.fields.meta
		t.Run(tt.name, func(t *testing.T) {
			tt.fields.meta.channelCPs.checkpoints["ch1"] = &msgpb.MsgPosition{
				Timestamp: tsoutil.ComposeTSByTime(time.Now(), 0),
				MsgID:     []byte{1, 2, 3, 4},
			}
			tr := &compactionTrigger{
				meta:                      tt.fields.meta,
				handler:                   newMockHandlerWithMeta(tt.fields.meta),
				allocator:                 tt.fields.allocator,
				signals:                   make(chan *compactionSignal, 100),
				inspector:                 tt.fields.inspector,
				globalTrigger:             tt.fields.globalTrigger,
				indexEngineVersionManager: newMockVersionManager(),
				closeCh:                   lifetime.NewSafeChan(),
				testingOnly:               true,
			}
			tr.start()
			defer tr.stop()
			_, err := tr.TriggerCompaction(context.TODO(), NewCompactionSignal())
			assert.Equal(t, tt.wantErr, err != nil)
			spy := (tt.fields.inspector).(*spyCompactionInspector)

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

			// Two-tier algorithm with expectedSize=1GiB (HNSW index):
			// 3 segments (510,500,480 MB residual) are full → excluded
			// Full-tier: {300,200} and {250,128,128} clear fill-rate gate
			// Fragment-tier: 40 fragments (10×40MB, 10×20MB, 20×10MB)
			//   exceed maxFragments=8, packed toward middleSize=256MB
			assert.GreaterOrEqual(t, len(plans), 2, "at least 2 full-tier plans")
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
	}, &compactionInspector{}, mock0Allocator, newMockHandler(), newIndexEngineVersionManager())

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
	mockVersionManager.On("GetCurrentIndexEngineVersion").Return(int32(2)).Maybe()
	mockVersionManager.On("ResolveVecIndexVersion").Return(int32(5)).Maybe()
	mockVersionManager.On("GetCurrentScalarIndexEngineVersion").Return(int32(2)).Maybe()
	mockVersionManager.On("ResolveScalarIndexVersion").Return(int32(2)).Maybe()
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
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: "HNSW"},
				},
			},
		},
	}

	// expire time < Timestamp To, but index engine version is 2 which is larger than CurrentIndexVersion in segmentIndex
	Params.Save(Params.DataCoordCfg.AutoUpgradeSegmentIndex.Key, "true")
	defer Params.Save(Params.DataCoordCfg.AutoUpgradeSegmentIndex.Key, "false")
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

	Params.Save(Params.DataCoordCfg.ForceRebuildSegmentIndex.Key, "true")
	defer Params.Save(Params.DataCoordCfg.ForceRebuildSegmentIndex.Key, "false")
	Params.Save(Params.DataCoordCfg.TargetVecIndexVersion.Key, "5")
	defer Params.Save(Params.DataCoordCfg.TargetVecIndexVersion.Key, "-1")
	couldDo = trigger.ShouldDoSingleCompaction(info5, &compactTime{expireTime: 300})
	assert.True(t, couldDo)

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

func Test_compactionTrigger_ShouldStrictCompactExpiry(t *testing.T) {
	trigger := &compactionTrigger{}

	now := time.Now()
	expireTS := tsoutil.ComposeTSByTime(now, 0)
	compact := &compactTime{
		expireTime: expireTS,
	}

	segment := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            10,
			CollectionID:  20,
			PartitionID:   30,
			InsertChannel: "test-channel",
		},
	}

	t.Run("no tolerance, fromTs before expire time => should compact", func(t *testing.T) {
		Params.Save(Params.DataCoordCfg.CompactionExpiryTolerance.Key, "0")
		defer Params.Save(Params.DataCoordCfg.CompactionExpiryTolerance.Key, "-1") // reset
		fromTs := tsoutil.ComposeTSByTime(now.Add(-time.Hour), 0)
		shouldCompact := trigger.ShouldCompactExpiry(fromTs, compact, segment)
		assert.True(t, shouldCompact)
	})

	t.Run("negative tolerance, disable force expiry compaction => should not compact", func(t *testing.T) {
		fromTs := tsoutil.ComposeTSByTime(now.Add(-time.Hour), 0)
		shouldCompact := trigger.ShouldCompactExpiry(fromTs, compact, segment)
		assert.False(t, shouldCompact)
	})

	t.Run("with tolerance, fromTs within tolerance => should NOT compact", func(t *testing.T) {
		Params.Save(Params.DataCoordCfg.CompactionExpiryTolerance.Key, "2")
		defer Params.Save(Params.DataCoordCfg.CompactionExpiryTolerance.Key, "-1") // reset

		fromTs := tsoutil.ComposeTSByTime(now.Add(-time.Hour), 0) // within 2h tolerance
		shouldCompact := trigger.ShouldCompactExpiry(fromTs, compact, segment)
		assert.False(t, shouldCompact)
	})

	t.Run("with tolerance, fromTs before expireTime - tolerance => should compact", func(t *testing.T) {
		Params.Save(Params.DataCoordCfg.CompactionExpiryTolerance.Key, "2")
		defer Params.Save(Params.DataCoordCfg.CompactionExpiryTolerance.Key, "-1") // reset
		fromTs := tsoutil.ComposeTSByTime(now.Add(-3*time.Hour), 0)                // earlier than expireTime - 30m
		shouldCompact := trigger.ShouldCompactExpiry(fromTs, compact, segment)
		assert.True(t, shouldCompact)
	})
}

func Test_compactionTrigger_new(t *testing.T) {
	type args struct {
		meta      *meta
		inspector CompactionInspector
		allocator allocator.Allocator
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"test new trigger",
			args{
				&meta{},
				&compactionInspector{},
				allocator.NewMockAllocator(t),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newCompactionTrigger(tt.args.meta, tt.args.inspector, tt.args.allocator, newMockHandler(), newMockVersionManager())
			assert.Equal(t, tt.args.meta, got.meta)
			assert.Equal(t, tt.args.inspector, got.inspector)
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

func Test_TirggerCompaction_WaitResult(t *testing.T) {
	originValue := Params.DataCoordCfg.EnableAutoCompaction.GetValue()
	Params.Save(Params.DataCoordCfg.EnableAutoCompaction.Key, "true")
	defer func() {
		Params.Save(Params.DataCoordCfg.EnableAutoCompaction.Key, originValue)
	}()
	m := &meta{
		channelCPs: newChannelCps(),
		segments:   NewSegmentsInfo(), collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
	}
	got := newCompactionTrigger(m, &compactionInspector{}, newMockAllocator(t),
		&ServerHandler{
			&Server{
				meta: m,
			},
		}, newMockVersionManager())
	got.signals = make(chan *compactionSignal, 1)
	{
		_, err := got.TriggerCompaction(context.TODO(), NewCompactionSignal().
			WithCollectionID(1).
			WithPartitionID(1).
			WithSegmentIDs(1).
			WithChannel("a").
			WithWaitResult(false))
		assert.NoError(t, err)
	}
	{
		_, err := got.TriggerCompaction(context.TODO(), NewCompactionSignal().
			WithCollectionID(2).
			WithPartitionID(2).
			WithSegmentIDs(2).
			WithChannel("b").
			WithWaitResult(false))
		assert.Error(t, err)
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
				switch x {
				case 0:
					assert.EqualValues(t, 3, signal.collectionID)
				case 1:
					assert.EqualValues(t, 4, signal.collectionID)
				}
				signal.Notify(nil)
			case <-timeoutCtx.Done():
				return
			}
		}
	}()

	{
		_, err := got.TriggerCompaction(context.TODO(), NewCompactionSignal().
			WithCollectionID(3).
			WithPartitionID(3).
			WithSegmentIDs(3).
			WithChannel("c").
			WithWaitResult(false))
		assert.NoError(t, err)
	}

	{
		_, err := got.TriggerCompaction(context.TODO(), NewCompactionSignal().
			WithCollectionID(4).
			WithPartitionID(4).
			WithSegmentIDs(4).
			WithChannel("d").
			WithWaitResult(true))
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

	meta           *meta
	tr             *compactionTrigger
	allocator      *allocator.MockAllocator
	handler        *NMockHandler
	inspector      *MockCompactionInspector
	versionManager *MockVersionManager
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

func (s *CompactionTriggerSuite) genSegIndex(segID, indexID UniqueID, numRows int64) *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex] {
	segIdx := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx.Insert(indexID, &model.SegmentIndex{
		SegmentID:    segID,
		CollectionID: s.collectionID,
		PartitionID:  s.partitionID,
		NumRows:      numRows,
		IndexID:      indexID,
		BuildID:      segID,
		NodeID:       0,
		IndexVersion: 1,
		IndexState:   commonpb.IndexState_Finished,
	})
	return segIdx
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

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(s.collectionID, &collectionInfo{
		ID: s.collectionID,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  s.vecFieldID,
					DataType: schemapb.DataType_FloatVector,
				},
			},
		},
	})

	im := &indexMeta{
		segmentIndexes: typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
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
	}
	im.segmentIndexes.Insert(1, s.genSegIndex(1, indexID, 60))
	im.segmentIndexes.Insert(2, s.genSegIndex(2, indexID, 60))
	im.segmentIndexes.Insert(3, s.genSegIndex(3, indexID, 60))
	im.segmentIndexes.Insert(4, s.genSegIndex(4, indexID, 60))
	im.segmentIndexes.Insert(5, s.genSegIndex(5, indexID, 60))
	im.segmentIndexes.Insert(6, s.genSegIndex(6, indexID, 60))
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
		indexMeta:   im,
		collections: collections,
	}
	s.meta.UpdateChannelCheckpoint(context.TODO(), s.channel, &msgpb.MsgPosition{
		ChannelName: s.channel,
		Timestamp:   tsoutil.ComposeTSByTime(time.Now(), 0),
		MsgID:       []byte{1, 2, 3, 4},
	})
	s.allocator = allocator.NewMockAllocator(s.T())
	s.inspector = NewMockCompactionInspector(s.T())
	s.handler = NewNMockHandler(s.T())
	s.versionManager = NewMockVersionManager(s.T())
	s.tr = newCompactionTrigger(
		s.meta,
		s.inspector,
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
		s.inspector.EXPECT().isFull().Return(false)
		// s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(nil, errors.New("mocked"))
		tr.handleSignal(&compactionSignal{
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check inspector.enqueueCompaction never called
	})

	s.Run("collectionAutoCompactionConfigError", func() {
		defer s.SetupTest()
		tr := s.tr
		s.inspector.EXPECT().isFull().Return(false)
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
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check inspector.enqueueCompaction never called
	})

	s.Run("collectionAutoCompactionDisabled", func() {
		defer s.SetupTest()
		tr := s.tr
		s.inspector.EXPECT().isFull().Return(false)
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
			collectionID: s.collectionID,
			partitionID:  s.partitionID,
			channel:      s.channel,
			isForce:      false,
		})

		// suite shall check inspector.enqueueCompaction never called
	})

	s.Run("collectionAutoCompactionDisabled_force", func() {
		defer s.SetupTest()
		tr := s.tr
		s.inspector.EXPECT().isFull().Return(false)
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
		s.inspector.EXPECT().enqueueCompaction(mock.Anything).Return(nil)
		tr.handleSignal(&compactionSignal{
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

	s.Run("GetCollection_failed", func() {
		defer s.SetupTest()
		tr := s.tr
		s.inspector.EXPECT().isFull().Return(false)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(nil, errors.New("mocked"))
		err := tr.handleSignal(NewCompactionSignal().
			WithCollectionID(s.collectionID).
			WithPartitionID(s.partitionID).
			WithChannel(s.channel))
		s.Error(err)
	})

	s.Run("collectionAutoCompactionConfigError", func() {
		defer s.SetupTest()
		tr := s.tr
		s.inspector.EXPECT().isFull().Return(false)
		s.allocator.EXPECT().AllocTimestamp(mock.Anything).Return(10000, nil)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Schema: schema,
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "bad_value",
			},
		}, nil)
		s.NotPanics(func() {
			err := tr.handleSignal(NewCompactionSignal().
				WithCollectionID(s.collectionID).
				WithPartitionID(s.partitionID).
				WithChannel(s.channel))
			s.NoError(err)
		}, "bad configuration shall not cause panicking")
	})

	s.Run("collectionAutoCompactionDisabled", func() {
		defer s.SetupTest()
		tr := s.tr
		s.inspector.EXPECT().isFull().Return(false)
		s.handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{
			Schema: schema,
			Properties: map[string]string{
				common.CollectionAutoCompactionKey: "false",
			},
		}, nil)
		err := tr.handleSignal(NewCompactionSignal().
			WithCollectionID(s.collectionID).
			WithPartitionID(s.partitionID).
			WithChannel(s.channel))
		s.NoError(err)
	})

	s.Run("collectionAutoCompactionDisabled_force", func() {
		defer s.SetupTest()
		tr := s.tr
		s.inspector.EXPECT().isFull().Return(false)
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
		err := tr.handleSignal(NewCompactionSignal().
			WithCollectionID(s.collectionID).
			WithPartitionID(s.partitionID).
			WithChannel(s.channel))
		s.NoError(err)
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

func TestCompactionTriggerSuite(t *testing.T) {
	suite.Run(t, new(CompactionTriggerSuite))
}

func Test_compactionTrigger_generatePlans(t *testing.T) {
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

	seg1 := &SegmentInfo{
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
						{
							EntriesNum:    5,
							LogID:         1,
							TimestampFrom: 1000,
							TimestampTo:   2000,
						},
					},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{EntriesNum: 5, LogID: 1, MemorySize: 1 * 1024 * 1024},
					},
				},
			},
			IsSorted: true,
		},
	}

	seg2 := &SegmentInfo{
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
						{
							EntriesNum:    5,
							LogID:         2,
							MemorySize:    3 * 1024 * 1024 * 1024,
							TimestampFrom: 2000,
							TimestampTo:   3000,
						},
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
	}

	type fields struct {
		meta          *meta
		allocator     allocator.Allocator
		signals       chan *compactionSignal
		inspector     CompactionInspector
		globalTrigger *time.Ticker
	}
	type args struct {
		segments     []*SegmentInfo
		signal       *compactionSignal
		compactTime  *compactTime
		expectedSize int64
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(2, &collectionInfo{
		ID:     2,
		Schema: schema,
		Properties: map[string]string{
			common.CollectionTTLConfigKey: "0",
		},
	})

	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	segIdx0 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx0.Insert(indexID, &model.SegmentIndex{
		SegmentID:           1,
		CollectionID:        2,
		PartitionID:         1,
		NumRows:             100,
		IndexID:             indexID,
		BuildID:             1,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIndexes.Insert(1, segIdx0)
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           2,
		CollectionID:        2,
		PartitionID:         1,
		NumRows:             100,
		IndexID:             indexID,
		BuildID:             2,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIndexes.Insert(2, segIdx1)

	// makeSeg builds a minimal flushed segment for two-tier composition
	// tests. size is used both as the row count and as the binlog memory
	// size, for simplicity.
	makeSeg := func(id int64, size int64) *SegmentInfo {
		return &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            id,
				CollectionID:  2,
				PartitionID:   1,
				NumOfRows:     size,
				InsertChannel: "ch1",
				State:         commonpb.SegmentState_Flushed,
				Binlogs: []*datapb.FieldBinlog{
					{Binlogs: []*datapb.Binlog{{EntriesNum: 5, LogID: id, MemorySize: size}}},
				},
				IsSorted: true,
			},
		}
	}

	// makeMetaWithSegs registers segs in a fresh meta so generatePlans'
	// helper methods (e.g. ShouldRebuildSegmentIndex) can look them up.
	makeMetaWithSegs := func(segs ...*SegmentInfo) *meta {
		segMap := make(map[int64]*SegmentInfo, len(segs))
		collMap := make(map[UniqueID]*SegmentInfo, len(segs))
		sIdxMap := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
		for _, s := range segs {
			segMap[s.GetID()] = s
			collMap[s.GetID()] = s
			si := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
			si.Insert(indexID, &model.SegmentIndex{
				SegmentID: s.GetID(), CollectionID: 2, PartitionID: 1,
				IndexID: indexID, BuildID: s.GetID(), IndexVersion: 1,
				IndexState: commonpb.IndexState_Finished,
			})
			sIdxMap.Insert(s.GetID(), si)
		}
		return &meta{
			catalog:    catalog,
			channelCPs: newChannelCps(),
			segments: &SegmentsInfo{
				segments: segMap,
				secondaryIndexes: segmentInfoIndexes{
					coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{2: collMap},
				},
			},
			indexMeta: &indexMeta{
				segmentIndexes: sIdxMap,
				indexes: map[UniqueID]map[UniqueID]*model.Index{
					2: {indexID: {
						CollectionID: 2, FieldID: vecFieldID, IndexID: indexID,
						IndexName: "_default_idx", IndexParams: []*commonpb.KeyValuePair{
							{Key: common.IndexTypeKey, Value: "HNSW"},
						},
					}},
				},
			},
			collections: collections,
		}
	}

	// zeroCompactTime is a non-nil, zero-value compactTime. generatePlans
	// evaluates ShouldDoSingleCompaction for every non-force segment, which
	// dereferences compactTime unconditionally, so a nil compactTime would
	// panic whenever a candidate carries any binlog (as makeSeg's segments
	// always do). Tests that are not exercising the force/TTL paths must
	// pass this instead of nil.
	zeroCompactTime := &compactTime{}

	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []wantBucket
		twoTier bool
	}{
		{
			name: "force trigger on large segments",
			fields: fields{
				meta: &meta{
					catalog:    catalog,
					channelCPs: newChannelCps(),
					segments: &SegmentsInfo{
						segments: map[int64]*SegmentInfo{
							1: seg1,
							2: seg2,
						},
						secondaryIndexes: segmentInfoIndexes{
							coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
								2: {
									seg1.GetID(): seg1,
									seg2.GetID(): seg2,
								},
							},
						},
					},
					indexMeta: &indexMeta{
						segmentIndexes: segIndexes,
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
					collections: collections,
				},
				allocator:     mock0Allocator,
				signals:       nil,
				inspector:     &spyCompactionInspector{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
				globalTrigger: nil,
			},
			args: args{
				segments:     []*SegmentInfo{seg1, seg2},
				signal:       &compactionSignal{collectionID: 2, partitionID: 1, channel: "ch1", isForce: true},
				compactTime:  nil,
				expectedSize: 1024 * 1024 * 1024,
			},
			want: []wantBucket{
				{segmentIDs: []int64{1}, maxSize: 1024 * 1024 * 1024},
				{segmentIDs: []int64{2}, maxSize: 1024 * 1024 * 1024},
			},
		},
		{
			// Five 200-sized segments clear the 85%-of-1024=870 fill-rate
			// gate on their own (1000 >= 870), so they compose into one
			// full-tier bucket. The five smaller segments are all below
			// the fragment threshold (256*0.85=217) but their count (5)
			// stays under MaxFragmentsPerGroup (8), so no fragment-tier
			// bucket is emitted for them: cascading compaction is avoided.
			name:    "full-tier packs five 200-sized segments leaving fragments below maxFragments",
			twoTier: true,
			fields: fields{
				meta:      makeMetaWithSegs(makeSeg(1, 200), makeSeg(2, 200), makeSeg(3, 200), makeSeg(4, 200), makeSeg(5, 200), makeSeg(6, 50), makeSeg(7, 45), makeSeg(8, 40), makeSeg(9, 35), makeSeg(10, 30)),
				allocator: mock0Allocator,
			},
			args: args{
				segments: []*SegmentInfo{
					makeSeg(1, 200), makeSeg(2, 200), makeSeg(3, 200),
					makeSeg(4, 200), makeSeg(5, 200),
					makeSeg(6, 50), makeSeg(7, 45), makeSeg(8, 40),
					makeSeg(9, 35), makeSeg(10, 30),
				},
				signal:       &compactionSignal{collectionID: 2, partitionID: 1, channel: "ch1"},
				compactTime:  zeroCompactTime,
				expectedSize: 1024,
			},
			want: []wantBucket{
				{segmentIDs: []int64{1, 2, 3, 4, 5}, maxSize: 1024},
			},
		},
		{
			// 50 segments of residual size 10 total only 500 < 870, so no
			// full-tier bucket forms. But their count (50) exceeds
			// MaxFragmentsPerGroup (8), so they are packed towards
			// middleSize (1024/4=256) instead of being left to accumulate
			// unboundedly.
			name:    "fragment-tier triggers when fragment count exceeds maxFragments",
			twoTier: true,
			fields: fields{
				meta: makeMetaWithSegs(func() []*SegmentInfo {
					segs := make([]*SegmentInfo, 50)
					for i := range segs {
						segs[i] = makeSeg(int64(i+1), 10)
					}
					return segs
				}()...),
				allocator: mock0Allocator,
			},
			args: args{
				segments: func() []*SegmentInfo {
					segs := make([]*SegmentInfo, 50)
					for i := range segs {
						segs[i] = makeSeg(int64(i+1), 10)
					}
					return segs
				}(),
				signal:       &compactionSignal{collectionID: 2, partitionID: 1, channel: "ch1"},
				compactTime:  zeroCompactTime,
				expectedSize: 1024,
			},
			want: []wantBucket{
				{segmentIDs: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}, maxSize: 256},
				{segmentIDs: []int64{26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50}, maxSize: 256},
			},
		},
		{
			// The 900-sized segment already clears the full threshold
			// (870) on its own, so it is excluded from packing altogether
			// (neither prioritized nor compactable). The remaining two
			// 100-sized segments total 200 < 870 and their count (2) is
			// under maxFragments, so no bucket is emitted at all.
			name:    "full segments are excluded from packing",
			twoTier: true,
			fields: fields{
				meta:      makeMetaWithSegs(makeSeg(1, 900), makeSeg(2, 100), makeSeg(3, 100)),
				allocator: mock0Allocator,
			},
			args: args{
				segments: []*SegmentInfo{
					makeSeg(1, 900),
					makeSeg(2, 100), makeSeg(3, 100),
				},
				signal:       &compactionSignal{collectionID: 2, partitionID: 1, channel: "ch1"},
				compactTime:  zeroCompactTime,
				expectedSize: 1024,
			},
			want: nil,
		},
		{
			// Two 450-sized segments sum to 900 >= 870, clearing the
			// fill-rate gate without needing a minimum segment count
			// (the full-tier packer only requires >= 2 segments).
			name:    "two segments compose without a minimum segment count",
			twoTier: true,
			fields: fields{
				meta:      makeMetaWithSegs(makeSeg(1, 450), makeSeg(2, 450)),
				allocator: mock0Allocator,
			},
			args: args{
				segments:     []*SegmentInfo{makeSeg(1, 450), makeSeg(2, 450)},
				signal:       &compactionSignal{collectionID: 2, partitionID: 1, channel: "ch1"},
				compactTime:  zeroCompactTime,
				expectedSize: 1024,
			},
			want: []wantBucket{
				{segmentIDs: []int64{1, 2}, maxSize: 1024},
			},
		},
		{
			// An 800-sized segment blocks the greedy packer: it is
			// selected first but leaves only 224 capacity, too little
			// for any 300-sized segment. Without the skip-and-retry
			// logic the three 300s would never be tried. The packer
			// skips the 800, packs [300,300,300]=900 (>=870), and
			// leaves the 800 as a non-full leftover.
			name:    "skip blocking large segment to pack smaller ones",
			twoTier: true,
			fields: fields{
				meta:      makeMetaWithSegs(makeSeg(1, 800), makeSeg(2, 300), makeSeg(3, 300), makeSeg(4, 300)),
				allocator: mock0Allocator,
			},
			args: args{
				segments: []*SegmentInfo{
					makeSeg(1, 800), makeSeg(2, 300),
					makeSeg(3, 300), makeSeg(4, 300),
				},
				signal:       &compactionSignal{collectionID: 2, partitionID: 1, channel: "ch1"},
				compactTime:  zeroCompactTime,
				expectedSize: 1024,
			},
			want: []wantBucket{
				{segmentIDs: []int64{2, 3, 4}, maxSize: 1024},
			},
		},
		{
			// Similar to the previous case with a different shape:
			// [600,500,500]. The greedy packer selects 600 first,
			// leaving 424 — too little for 500. But 500+500=1000
			// (>=870) is valid. The packer skips 600 and packs the
			// two 500s.
			name:    "skip one large segment so two medium segments can pair",
			twoTier: true,
			fields: fields{
				meta:      makeMetaWithSegs(makeSeg(1, 600), makeSeg(2, 500), makeSeg(3, 500)),
				allocator: mock0Allocator,
			},
			args: args{
				segments: []*SegmentInfo{
					makeSeg(1, 600), makeSeg(2, 500), makeSeg(3, 500),
				},
				signal:       &compactionSignal{collectionID: 2, partitionID: 1, channel: "ch1"},
				compactTime:  zeroCompactTime,
				expectedSize: 1024,
			},
			want: []wantBucket{
				{segmentIDs: []int64{2, 3}, maxSize: 1024},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.twoTier {
				Params.Save(Params.DataCoordCfg.TwoTierCompaction.Key, "true")
				defer Params.Save(Params.DataCoordCfg.TwoTierCompaction.Key, "false")
			}

			tr := &compactionTrigger{
				meta:          tt.fields.meta,
				handler:       newMockHandlerWithMeta(tt.fields.meta),
				allocator:     tt.fields.allocator,
				signals:       tt.fields.signals,
				inspector:     tt.fields.inspector,
				globalTrigger: tt.fields.globalTrigger,
				closeCh:       lifetime.NewSafeChan(),
				testingOnly:   true,
			}

			got := tr.generatePlans(tt.args.segments, tt.args.signal, tt.args.compactTime, tt.args.expectedSize)
			require.Len(t, got, len(tt.want))
			for i, bucket := range got {
				gotIDs := lo.Map(bucket.segments, func(s *SegmentInfo, _ int) int64 { return s.GetID() })
				sort.Slice(gotIDs, func(a, b int) bool { return gotIDs[a] < gotIDs[b] })
				wantIDs := tt.want[i].segmentIDs
				sort.Slice(wantIDs, func(a, b int) bool { return wantIDs[a] < wantIDs[b] })
				assert.Equal(t, wantIDs, gotIDs, "bucket %d segment IDs", i)
				assert.Equal(t, tt.want[i].maxSize, bucket.maxSize, "bucket %d maxSize", i)
			}
		})
	}
}

func Test_compactionTrigger_generatePlansByTime(t *testing.T) {
	Params.Save(Params.DataCoordCfg.TwoTierCompaction.Key, "true")
	defer Params.Save(Params.DataCoordCfg.TwoTierCompaction.Key, "false")

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

	seg1 := &SegmentInfo{
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
						{
							EntriesNum:    5,
							LogID:         1,
							TimestampFrom: 1000,
							TimestampTo:   2000,
						},
					},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{EntriesNum: 5, LogID: 1, MemorySize: 1 * 1024 * 1024},
					},
				},
			},
			IsSorted: true,
		},
	}

	seg2 := &SegmentInfo{
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
						{
							EntriesNum:    5,
							LogID:         2,
							TimestampFrom: 1000,
							TimestampTo:   2000,
						},
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
	}

	seg3 := &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:             3,
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
						{
							EntriesNum:    5,
							LogID:         3,
							TimestampFrom: 3000,
							TimestampTo:   4000,
						},
					},
				},
			},
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{EntriesNum: 5, LogID: 3},
					},
				},
			},
			IsSorted: true,
		},
	}

	type fields struct {
		meta          *meta
		allocator     allocator.Allocator
		signals       chan *compactionSignal
		inspector     CompactionInspector
		globalTrigger *time.Ticker
	}
	type args struct {
		segments     []*SegmentInfo
		signal       *compactionSignal
		compactTime  *compactTime
		expectedSize int64
	}

	collections := typeutil.NewConcurrentMap[UniqueID, *collectionInfo]()
	collections.Insert(2, &collectionInfo{
		ID:     2,
		Schema: schema,
		Properties: map[string]string{
			common.CollectionTTLConfigKey: "0",
		},
	})

	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	segIdx0 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx0.Insert(indexID, &model.SegmentIndex{
		SegmentID:           1,
		CollectionID:        2,
		PartitionID:         1,
		NumRows:             100,
		IndexID:             indexID,
		BuildID:             1,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIndexes.Insert(1, segIdx0)
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           2,
		CollectionID:        2,
		PartitionID:         1,
		NumRows:             100,
		IndexID:             indexID,
		BuildID:             2,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIndexes.Insert(2, segIdx1)
	segIdx2 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx2.Insert(indexID, &model.SegmentIndex{
		SegmentID:           3,
		CollectionID:        2,
		PartitionID:         1,
		NumRows:             100,
		IndexID:             indexID,
		BuildID:             3,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 0,
		WriteHandoff:        false,
	})
	segIndexes.Insert(3, segIdx2)

	tests := []struct {
		name   string
		fields fields
		args   args
		want   []wantBucket
	}{
		{
			name: "test time-based compaction",
			fields: fields{
				meta: &meta{
					catalog:    catalog,
					channelCPs: newChannelCps(),
					segments: &SegmentsInfo{
						segments: map[int64]*SegmentInfo{
							1: seg1,
							2: seg2,
							3: seg3,
						},
						secondaryIndexes: segmentInfoIndexes{
							coll2Segments: map[UniqueID]map[UniqueID]*SegmentInfo{
								2: {
									seg1.GetID(): seg1,
									seg2.GetID(): seg2,
									seg3.GetID(): seg3,
								},
							},
						},
					},
					indexMeta: &indexMeta{
						segmentIndexes: segIndexes,
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
					collections: collections,
				},
				allocator:     mock0Allocator,
				signals:       nil,
				inspector:     &spyCompactionInspector{t: t, spyChan: make(chan *datapb.CompactionPlan, 1)},
				globalTrigger: nil,
			},
			args: args{
				segments:     []*SegmentInfo{seg1, seg2, seg3},
				signal:       &compactionSignal{collectionID: 2, partitionID: 1, channel: "ch1", isForce: false},
				compactTime:  &compactTime{startTime: 1000, collectionTTL: time.Hour},
				expectedSize: 1024 * 1024 * 1024,
			},
			// seg1..3 carry only a few KB of residual size against a 1GiB
			// expectedSize, so neither the full-tier fill-rate gate (85%
			// of 1GiB) nor the fragment-tier count gate (3 <= the default
			// MaxFragmentsPerGroup of 8) is cleared: no bucket is emitted.
			// The old algorithm merged these purely because count reached
			// MinSegmentToMerge regardless of actual size; that "squeeze"
			// behavior is exactly what the two-tier algorithm removes.
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &compactionTrigger{
				meta:          tt.fields.meta,
				handler:       newMockHandlerWithMeta(tt.fields.meta),
				allocator:     tt.fields.allocator,
				signals:       tt.fields.signals,
				inspector:     tt.fields.inspector,
				globalTrigger: tt.fields.globalTrigger,
				closeCh:       lifetime.NewSafeChan(),
				testingOnly:   true,
			}

			got := tr.generatePlans(tt.args.segments, tt.args.signal, tt.args.compactTime, tt.args.expectedSize)
			require.Len(t, got, len(tt.want))
			for i, bucket := range got {
				gotIDs := lo.Map(bucket.segments, func(s *SegmentInfo, _ int) int64 { return s.GetID() })
				sort.Slice(gotIDs, func(a, b int) bool { return gotIDs[a] < gotIDs[b] })
				wantIDs := tt.want[i].segmentIDs
				sort.Slice(wantIDs, func(a, b int) bool { return wantIDs[a] < wantIDs[b] })
				assert.Equal(t, wantIDs, gotIDs, "bucket %d segment IDs", i)
				assert.Equal(t, tt.want[i].maxSize, bucket.maxSize, "bucket %d maxSize", i)
			}
		})
	}
}

func newTestIndexMeta(collID, segID, indexID int64, indexType string, segIdx *model.SegmentIndex) *indexMeta {
	im := newSegmentIndexMeta(nil)
	im.indexes[collID] = map[UniqueID]*model.Index{
		indexID: {
			CollectionID: collID,
			IndexID:      indexID,
			IndexName:    "test_idx",
			IndexParams: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: indexType},
			},
		},
	}
	segIdxMap := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdxMap.Insert(indexID, segIdx)
	im.segmentIndexes.Insert(segID, segIdxMap)
	return im
}

func Test_ShouldRebuildSegmentIndex_AutoUpgrade_ScalarUsesCorrectField(t *testing.T) {
	paramtable.Init()
	Params.Save("dataCoord.autoUpgradeSegmentIndex", "true")

	collID, segID, indexID := int64(1), int64(100), int64(10)

	t.Run("scalar auto-upgrade compares CurrentScalarIndexVersion", func(t *testing.T) {
		// Scalar index with CurrentIndexVersion=5 (vector field) but CurrentScalarIndexVersion=1
		// Engine scalar version is 2, so 1 < 2 should trigger rebuild
		segIdx := &model.SegmentIndex{
			SegmentID:                 segID,
			CollectionID:              collID,
			IndexID:                   indexID,
			IndexFileKeys:             []string{"file1"},
			CurrentIndexVersion:       5, // vector version field - should NOT be used for scalar
			CurrentScalarIndexVersion: 1, // scalar version field - should be used
		}
		im := newTestIndexMeta(collID, segID, indexID, "INVERTED", segIdx)

		mockVM := NewMockVersionManager(t)
		mockVM.On("GetCurrentScalarIndexEngineVersion").Return(int32(2)).Maybe()
		mockVM.On("GetCurrentIndexEngineVersion").Return(int32(5)).Maybe()

		trigger := &compactionTrigger{
			meta:                      &meta{indexMeta: im, channelCPs: newChannelCps()},
			indexEngineVersionManager: mockVM,
		}

		segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: segID, CollectionID: collID}}
		assert.True(t, trigger.ShouldRebuildSegmentIndex(segment))
	})

	t.Run("scalar auto-upgrade no rebuild when version matches", func(t *testing.T) {
		segIdx := &model.SegmentIndex{
			SegmentID:                 segID,
			CollectionID:              collID,
			IndexID:                   indexID,
			IndexFileKeys:             []string{"file1"},
			CurrentIndexVersion:       1, // vector version - irrelevant
			CurrentScalarIndexVersion: 2, // matches engine version
		}
		im := newTestIndexMeta(collID, segID, indexID, "INVERTED", segIdx)

		mockVM := NewMockVersionManager(t)
		mockVM.On("GetCurrentScalarIndexEngineVersion").Return(int32(2)).Maybe()
		mockVM.On("GetCurrentIndexEngineVersion").Return(int32(5)).Maybe()

		trigger := &compactionTrigger{
			meta:                      &meta{indexMeta: im, channelCPs: newChannelCps()},
			indexEngineVersionManager: mockVM,
		}

		segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: segID, CollectionID: collID}}
		assert.False(t, trigger.ShouldRebuildSegmentIndex(segment))
	})

	Params.Save("dataCoord.autoUpgradeSegmentIndex", "false")
}

func Test_ShouldRebuildSegmentIndex_ForceRebuild_ScalarUsesCorrectField(t *testing.T) {
	paramtable.Init()
	Params.Save("dataCoord.autoUpgradeSegmentIndex", "false")

	collID, segID, indexID := int64(1), int64(100), int64(10)

	t.Run("scalar force-rebuild compares CurrentScalarIndexVersion", func(t *testing.T) {
		Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "true")
		Params.Save("dataCoord.targetScalarIndexVersion", "3")
		defer func() {
			Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "false")
			Params.Save("dataCoord.targetScalarIndexVersion", "-1")
		}()

		// CurrentScalarIndexVersion=2 != resolved target=3, should trigger
		segIdx := &model.SegmentIndex{
			SegmentID:                 segID,
			CollectionID:              collID,
			IndexID:                   indexID,
			IndexFileKeys:             []string{"file1"},
			CurrentIndexVersion:       3, // vector version - should NOT be used
			CurrentScalarIndexVersion: 2, // scalar version - should be used
		}
		im := newTestIndexMeta(collID, segID, indexID, "INVERTED", segIdx)

		mockVM := NewMockVersionManager(t)
		mockVM.On("ResolveScalarIndexVersion").Return(int32(3)).Maybe()

		trigger := &compactionTrigger{
			meta:                      &meta{indexMeta: im, channelCPs: newChannelCps()},
			indexEngineVersionManager: mockVM,
		}

		segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: segID, CollectionID: collID}}
		assert.True(t, trigger.ShouldRebuildSegmentIndex(segment))
	})

	t.Run("scalar force-rebuild no trigger when version matches resolved target", func(t *testing.T) {
		Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "true")
		Params.Save("dataCoord.targetScalarIndexVersion", "3")
		defer func() {
			Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "false")
			Params.Save("dataCoord.targetScalarIndexVersion", "-1")
		}()

		segIdx := &model.SegmentIndex{
			SegmentID:                 segID,
			CollectionID:              collID,
			IndexID:                   indexID,
			IndexFileKeys:             []string{"file1"},
			CurrentIndexVersion:       1, // irrelevant
			CurrentScalarIndexVersion: 3, // matches resolved target
		}
		im := newTestIndexMeta(collID, segID, indexID, "INVERTED", segIdx)

		mockVM := NewMockVersionManager(t)
		mockVM.On("ResolveScalarIndexVersion").Return(int32(3)).Maybe()

		trigger := &compactionTrigger{
			meta:                      &meta{indexMeta: im, channelCPs: newChannelCps()},
			indexEngineVersionManager: mockVM,
		}

		segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: segID, CollectionID: collID}}
		assert.False(t, trigger.ShouldRebuildSegmentIndex(segment))
	})
}

func Test_ShouldRebuildSegmentIndex_ForceRebuild_TargetExceedsMax_Converges(t *testing.T) {
	paramtable.Init()
	Params.Save("dataCoord.autoUpgradeSegmentIndex", "false")

	collID, segID, indexID := int64(1), int64(100), int64(10)

	t.Run("vec force-rebuild with target>max converges after clamp", func(t *testing.T) {
		Params.Save("dataCoord.forceRebuildSegmentIndex", "true")
		Params.Save("dataCoord.targetVecIndexVersion", "30")
		defer func() {
			Params.Save("dataCoord.forceRebuildSegmentIndex", "false")
			Params.Save("dataCoord.targetVecIndexVersion", "-1")
		}()

		// Index was already rebuilt to clamped version (20), should NOT trigger again
		segIdx := &model.SegmentIndex{
			SegmentID:           segID,
			CollectionID:        collID,
			IndexID:             indexID,
			IndexFileKeys:       []string{"file1"},
			CurrentIndexVersion: 20, // matches resolved (clamped) target
		}
		im := newTestIndexMeta(collID, segID, indexID, "HNSW", segIdx)

		mockVM := NewMockVersionManager(t)
		// ResolveVecIndexVersion clamps target=30 to max=20
		mockVM.On("ResolveVecIndexVersion").Return(int32(20)).Maybe()

		trigger := &compactionTrigger{
			meta:                      &meta{indexMeta: im, channelCPs: newChannelCps()},
			indexEngineVersionManager: mockVM,
		}

		segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: segID, CollectionID: collID}}
		assert.False(t, trigger.ShouldRebuildSegmentIndex(segment))
	})

	t.Run("scalar force-rebuild with target>max converges after clamp", func(t *testing.T) {
		Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "true")
		Params.Save("dataCoord.targetScalarIndexVersion", "10")
		defer func() {
			Params.Save("dataCoord.forceRebuildScalarSegmentIndex", "false")
			Params.Save("dataCoord.targetScalarIndexVersion", "-1")
		}()

		// Index was already rebuilt to clamped version (5), should NOT trigger again
		segIdx := &model.SegmentIndex{
			SegmentID:                 segID,
			CollectionID:              collID,
			IndexID:                   indexID,
			IndexFileKeys:             []string{"file1"},
			CurrentScalarIndexVersion: 5, // matches resolved (clamped) target
		}
		im := newTestIndexMeta(collID, segID, indexID, "INVERTED", segIdx)

		mockVM := NewMockVersionManager(t)
		// ResolveScalarIndexVersion clamps target=10 to max=5
		mockVM.On("ResolveScalarIndexVersion").Return(int32(5)).Maybe()

		trigger := &compactionTrigger{
			meta:                      &meta{indexMeta: im, channelCPs: newChannelCps()},
			indexEngineVersionManager: mockVM,
		}

		segment := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: segID, CollectionID: collID}}
		assert.False(t, trigger.ShouldRebuildSegmentIndex(segment))
	})
}
