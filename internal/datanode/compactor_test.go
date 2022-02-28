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

package datanode

import (
	"context"
	"math"
	"testing"
	"time"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactionTaskInnerMethods(t *testing.T) {
	t.Run("Test getSegmentMeta", func(t *testing.T) {
		rc := &RootCoordFactory{}
		replica, err := newReplica(context.TODO(), rc, 1)
		require.NoError(t, err)

		task := &compactionTask{
			Replica: replica,
		}

		_, _, _, err = task.getSegmentMeta(100)
		assert.Error(t, err)

		err = replica.addNewSegment(100, 1, 10, "a", new(internalpb.MsgPosition), nil)
		require.NoError(t, err)

		collID, partID, meta, err := task.getSegmentMeta(100)
		assert.NoError(t, err)
		assert.Equal(t, UniqueID(1), collID)
		assert.Equal(t, UniqueID(10), partID)
		assert.NotNil(t, meta)

		rc.setCollectionID(-2)
		task.Replica.(*SegmentReplica).collSchema = nil
		_, _, _, err = task.getSegmentMeta(100)
		assert.Error(t, err)
	})

	t.Run("Test.interface2FieldData", func(t *testing.T) {
		tests := []struct {
			isvalid bool

			tp      schemapb.DataType
			content []interface{}

			description string
		}{
			{true, schemapb.DataType_Bool, []interface{}{true, false}, "valid bool"},
			{true, schemapb.DataType_Int8, []interface{}{int8(1), int8(2)}, "valid int8"},
			{true, schemapb.DataType_Int16, []interface{}{int16(1), int16(2)}, "valid int16"},
			{true, schemapb.DataType_Int32, []interface{}{int32(1), int32(2)}, "valid int32"},
			{true, schemapb.DataType_Int64, []interface{}{int64(1), int64(2)}, "valid int64"},
			{true, schemapb.DataType_Float, []interface{}{float32(1), float32(2)}, "valid float32"},
			{true, schemapb.DataType_Double, []interface{}{float64(1), float64(2)}, "valid float64"},
			{true, schemapb.DataType_FloatVector, []interface{}{[]float32{1.0, 2.0}}, "valid floatvector"},
			{true, schemapb.DataType_BinaryVector, []interface{}{[]byte{255}}, "valid binaryvector"},
			{false, schemapb.DataType_Bool, []interface{}{1, 2}, "invalid bool"},
			{false, schemapb.DataType_Int8, []interface{}{nil, nil}, "invalid int8"},
			{false, schemapb.DataType_Int16, []interface{}{nil, nil}, "invalid int16"},
			{false, schemapb.DataType_Int32, []interface{}{nil, nil}, "invalid int32"},
			{false, schemapb.DataType_Int64, []interface{}{nil, nil}, "invalid int64"},
			{false, schemapb.DataType_Float, []interface{}{nil, nil}, "invalid float32"},
			{false, schemapb.DataType_Double, []interface{}{nil, nil}, "invalid float64"},
			{false, schemapb.DataType_FloatVector, []interface{}{nil, nil}, "invalid floatvector"},
			{false, schemapb.DataType_BinaryVector, []interface{}{nil, nil}, "invalid binaryvector"},
			{false, schemapb.DataType_String, nil, "invalid data type"},
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				if test.isvalid {
					fd, err := interface2FieldData(test.tp, test.content, 2)
					assert.NoError(t, err)
					assert.Equal(t, 2, fd.RowNum())
				} else {
					fd, err := interface2FieldData(test.tp, test.content, 2)
					assert.Error(t, err)
					assert.Nil(t, fd)
				}
			})
		}

	})

	t.Run("Test mergeDeltalogs", func(t *testing.T) {
		t.Run("One segment with timetravel", func(t *testing.T) {
			invalidBlobs := map[UniqueID][]*Blob{
				1: {},
			}

			blobs, err := getDeltaBlobs(
				100,
				[]UniqueID{
					1,
					2,
					3,
					4,
					5,
					1,
				},
				[]Timestamp{
					20000,
					20001,
					20002,
					30000,
					50000,
					50000,
				})
			require.NoError(t, err)

			validBlobs := map[UniqueID][]*Blob{
				100: blobs,
			}

			tests := []struct {
				isvalid bool

				dBlobs     map[UniqueID][]*Blob
				timetravel Timestamp

				description string
			}{
				{false, invalidBlobs, 0, "invalid dBlobs"},
				{true, validBlobs, 21000, "valid blobs"},
			}

			for _, test := range tests {
				task := &compactionTask{}
				t.Run(test.description, func(t *testing.T) {
					if test.isvalid {
						pk2ts, db, err := task.mergeDeltalogs(test.dBlobs, test.timetravel)
						assert.NoError(t, err)
						assert.Equal(t, 3, len(pk2ts))
						assert.Equal(t, int64(3), db.GetEntriesNum())
						assert.Equal(t, int64(3), db.delData.RowCount)
						assert.ElementsMatch(t, []UniqueID{1, 4, 5}, db.delData.Pks)
						assert.ElementsMatch(t, []Timestamp{30000, 50000, 50000}, db.delData.Tss)

					} else {

						pk2ts, db, err := task.mergeDeltalogs(test.dBlobs, test.timetravel)
						assert.Error(t, err)
						assert.Nil(t, pk2ts)
						assert.Nil(t, db)
					}
				})

			}
		})

		t.Run("Multiple segments with timetravel", func(t *testing.T) {
			tests := []struct {
				segIDA  UniqueID
				dataApk []UniqueID
				dataAts []Timestamp

				segIDB  UniqueID
				dataBpk []UniqueID
				dataBts []Timestamp

				segIDC  UniqueID
				dataCpk []UniqueID
				dataCts []Timestamp

				timetravel    Timestamp
				expectedpk2ts int
				expecteddb    int
				description   string
			}{
				{
					0, nil, nil,
					100, []UniqueID{1, 2, 3}, []Timestamp{20000, 30000, 20005},
					200, []UniqueID{4, 5, 6}, []Timestamp{50000, 50001, 50002},
					40000, 3, 3, "2 segments with timetravel 40000",
				},
				{
					300, []UniqueID{10, 20}, []Timestamp{20001, 40001},
					100, []UniqueID{1, 2, 3}, []Timestamp{20000, 30000, 20005},
					200, []UniqueID{4, 5, 6}, []Timestamp{50000, 50001, 50002},
					40000, 4, 4, "3 segments with timetravel 40000",
				},
			}

			for _, test := range tests {
				t.Run(test.description, func(t *testing.T) {
					dBlobs := make(map[UniqueID][]*Blob)
					if test.segIDA != UniqueID(0) {
						d, err := getDeltaBlobs(test.segIDA, test.dataApk, test.dataAts)
						require.NoError(t, err)
						dBlobs[test.segIDA] = d
					}
					if test.segIDB != UniqueID(0) {
						d, err := getDeltaBlobs(test.segIDB, test.dataBpk, test.dataBts)
						require.NoError(t, err)
						dBlobs[test.segIDB] = d
					}
					if test.segIDC != UniqueID(0) {
						d, err := getDeltaBlobs(test.segIDC, test.dataCpk, test.dataCts)
						require.NoError(t, err)
						dBlobs[test.segIDC] = d
					}

					task := &compactionTask{}
					pk2ts, db, err := task.mergeDeltalogs(dBlobs, test.timetravel)
					assert.NoError(t, err)
					assert.Equal(t, test.expectedpk2ts, len(pk2ts))
					assert.Equal(t, test.expecteddb, int(db.GetEntriesNum()))
				})
			}
		})

	})

	t.Run("Test merge", func(t *testing.T) {
		t.Run("Merge without expiration", func(t *testing.T) {
			Params.DataCoordCfg.CompactionEntityExpiration = math.MaxInt64
			iData := genInsertDataWithExpiredTS()
			meta := NewMetaFactory().GetCollectionMeta(1, "test")

			iblobs, err := getInsertBlobs(100, iData, meta)
			require.NoError(t, err)

			iitr, err := storage.NewInsertBinlogIterator(iblobs, 106)
			require.NoError(t, err)

			mitr := storage.NewMergeIterator([]iterator{iitr})

			dm := map[UniqueID]Timestamp{
				1: 10000,
			}

			ct := &compactionTask{}
			idata, numOfRow, err := ct.merge(mitr, dm, meta.GetSchema(), ct.GetCurrentTime())
			assert.NoError(t, err)
			assert.Equal(t, int64(1), numOfRow)
			assert.Equal(t, 1, len(idata))
			assert.NotEmpty(t, idata[0].Data)
		})
		t.Run("Merge with expiration", func(t *testing.T) {
			Params.DataCoordCfg.CompactionEntityExpiration = 864000 // 10 days in seconds
			iData := genInsertDataWithExpiredTS()
			meta := NewMetaFactory().GetCollectionMeta(1, "test")

			iblobs, err := getInsertBlobs(100, iData, meta)
			require.NoError(t, err)

			iitr, err := storage.NewInsertBinlogIterator(iblobs, 106)
			require.NoError(t, err)

			mitr := storage.NewMergeIterator([]iterator{iitr})

			dm := map[UniqueID]Timestamp{
				1: 10000,
			}

			ct := &compactionTask{}
			idata, numOfRow, err := ct.merge(mitr, dm, meta.GetSchema(), genTimestamp())
			assert.NoError(t, err)
			assert.Equal(t, int64(0), numOfRow)
			assert.Equal(t, 1, len(idata))
			assert.Empty(t, idata[0].Data)
		})
	})

	t.Run("Test isExpiredEntity", func(t *testing.T) {
		t.Run("When CompactionEntityExpiration is set math.MaxInt64", func(t *testing.T) {
			Params.DataCoordCfg.CompactionEntityExpiration = math.MaxInt64

			ct := &compactionTask{}
			res := ct.isExpiredEntity(0, genTimestamp())
			assert.Equal(t, false, res)

			res = ct.isExpiredEntity(math.MaxInt64, genTimestamp())
			assert.Equal(t, false, res)

			res = ct.isExpiredEntity(0, math.MaxInt64)
			assert.Equal(t, false, res)

			res = ct.isExpiredEntity(math.MaxInt64, math.MaxInt64)
			assert.Equal(t, false, res)

			res = ct.isExpiredEntity(math.MaxInt64, 0)
			assert.Equal(t, false, res)
		})
		t.Run("When CompactionEntityExpiration is set MAX_ENTITY_EXPIRATION = 9223372036", func(t *testing.T) {
			Params.DataCoordCfg.CompactionEntityExpiration = 9223372036 // math.MaxInt64 / time.Second

			ct := &compactionTask{}
			res := ct.isExpiredEntity(0, genTimestamp())
			assert.Equal(t, false, res)

			res = ct.isExpiredEntity(math.MaxInt64, genTimestamp())
			assert.Equal(t, false, res)

			res = ct.isExpiredEntity(0, math.MaxInt64)
			assert.Equal(t, true, res)

			res = ct.isExpiredEntity(math.MaxInt64, math.MaxInt64)
			assert.Equal(t, false, res)

			res = ct.isExpiredEntity(math.MaxInt64, 0)
			assert.Equal(t, false, res)
		})
		t.Run("When CompactionEntityExpiration is set 10 days", func(t *testing.T) {
			Params.DataCoordCfg.CompactionEntityExpiration = 864000 // 10 days in seconds

			ct := &compactionTask{}
			res := ct.isExpiredEntity(0, genTimestamp())
			assert.Equal(t, true, res)

			res = ct.isExpiredEntity(math.MaxInt64, genTimestamp())
			assert.Equal(t, false, res)

			res = ct.isExpiredEntity(0, math.MaxInt64)
			assert.Equal(t, true, res)

			res = ct.isExpiredEntity(math.MaxInt64, math.MaxInt64)
			assert.Equal(t, false, res)

			res = ct.isExpiredEntity(math.MaxInt64, 0)
			assert.Equal(t, false, res)
		})
	})
}

func getDeltaBlobs(segID UniqueID, pks []UniqueID, tss []Timestamp) ([]*Blob, error) {
	deltaData := &DeleteData{
		Pks:      pks,
		Tss:      tss,
		RowCount: int64(len(pks)),
	}

	dCodec := storage.NewDeleteCodec()
	blob, err := dCodec.Serialize(1, 10, segID, deltaData)
	return []*Blob{blob}, err
}

func getInsertBlobs(segID UniqueID, iData *InsertData, meta *etcdpb.CollectionMeta) ([]*Blob, error) {
	iCodec := storage.NewInsertCodec(meta)

	iblobs, _, err := iCodec.Serialize(10, segID, iData)
	return iblobs, err
}

func TestCompactorInterfaceMethods(t *testing.T) {
	notEmptySegmentBinlogs := []*datapb.CompactionSegmentBinlogs{{
		SegmentID:           100,
		FieldBinlogs:        nil,
		Field2StatslogPaths: nil,
		Deltalogs:           nil,
	}}
	Params.DataCoordCfg.CompactionEntityExpiration = math.MaxInt64 // Turn off auto expiration

	t.Run("Test compact invalid", func(t *testing.T) {
		invalidAlloc := NewAllocatorFactory(-1)
		ctx, cancel := context.WithCancel(context.TODO())
		emptyTask := &compactionTask{
			ctx:    ctx,
			cancel: cancel,
		}
		emptySegmentBinlogs := []*datapb.CompactionSegmentBinlogs{}

		plan := &datapb.CompactionPlan{
			PlanID:           999,
			SegmentBinlogs:   notEmptySegmentBinlogs,
			StartTime:        0,
			TimeoutInSeconds: 10,
			Type:             datapb.CompactionType_UndefinedCompaction,
			Channel:          "",
		}

		emptyTask.plan = plan
		err := emptyTask.compact()
		assert.Error(t, err)

		plan.Type = datapb.CompactionType_InnerCompaction
		plan.SegmentBinlogs = emptySegmentBinlogs
		err = emptyTask.compact()
		assert.Error(t, err)

		plan.Type = datapb.CompactionType_MergeCompaction
		emptyTask.allocatorInterface = invalidAlloc
		plan.SegmentBinlogs = notEmptySegmentBinlogs
		err = emptyTask.compact()
		assert.Error(t, err)

		emptyTask.stop()
	})

	t.Run("Test typeI compact valid", func(t *testing.T) {
		var collID, partID, segID UniqueID = 1, 10, 100

		alloc := NewAllocatorFactory(1)
		rc := &RootCoordFactory{}
		dc := &DataCoordFactory{}
		mockfm := &mockFlushManager{}
		mockKv := memkv.NewMemoryKV()
		mockbIO := &binlogIO{mockKv, alloc}
		replica, err := newReplica(context.TODO(), rc, collID)
		require.NoError(t, err)
		replica.addFlushedSegmentWithPKs(segID, collID, partID, "channelname", 2, []UniqueID{1, 2})

		iData := genInsertData()
		meta := NewMetaFactory().GetCollectionMeta(collID, "test_compact_coll_name")
		dData := &DeleteData{
			Pks:      []UniqueID{1},
			Tss:      []Timestamp{20000},
			RowCount: 1,
		}

		cpaths, err := mockbIO.upload(context.TODO(), segID, partID, []*InsertData{iData}, dData, meta)
		require.NoError(t, err)
		require.Equal(t, 11, len(cpaths.inPaths))
		segBinlogs := []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID:           segID,
				FieldBinlogs:        cpaths.inPaths,
				Field2StatslogPaths: cpaths.statsPaths,
				Deltalogs:           cpaths.deltaInfo,
			}}

		plan := &datapb.CompactionPlan{
			PlanID:           10080,
			SegmentBinlogs:   segBinlogs,
			StartTime:        0,
			TimeoutInSeconds: 1,
			Type:             datapb.CompactionType_InnerCompaction,
			Timetravel:       30000,
			Channel:          "channelname",
		}

		ctx, cancel := context.WithCancel(context.TODO())
		cancel()
		canceledTask := newCompactionTask(ctx, mockbIO, mockbIO, replica, mockfm, alloc, dc, plan)
		err = canceledTask.compact()
		assert.Error(t, err)

		task := newCompactionTask(context.TODO(), mockbIO, mockbIO, replica, mockfm, alloc, dc, plan)
		err = task.compact()
		assert.NoError(t, err)

		updates, err := replica.getSegmentStatisticsUpdates(segID)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), updates.GetNumRows())

		id := task.getCollection()
		assert.Equal(t, UniqueID(1), id)

		planID := task.getPlanID()
		assert.Equal(t, plan.GetPlanID(), planID)

		// Compact to delete the entire segment
		deleteAllData := &DeleteData{
			Pks:      []UniqueID{1, 2},
			Tss:      []Timestamp{20000, 20001},
			RowCount: 2,
		}

		err = mockKv.RemoveWithPrefix("/")
		require.NoError(t, err)
		cpaths, err = mockbIO.upload(context.TODO(), segID, partID, []*InsertData{iData}, deleteAllData, meta)
		require.NoError(t, err)
		plan.PlanID++

		err = task.compact()
		assert.NoError(t, err)
		// The segment should be removed
		assert.False(t, replica.hasSegment(segID, true))

		// re-add the segment
		replica.addFlushedSegmentWithPKs(segID, collID, partID, "channelname", 2, []UniqueID{1, 2})

		// Compact empty segment
		err = mockKv.RemoveWithPrefix("/")
		require.NoError(t, err)
		cpaths, err = mockbIO.upload(context.TODO(), segID, partID, []*InsertData{iData}, dData, meta)
		require.NoError(t, err)
		plan.PlanID = 999876
		segmentBinlogsWithEmptySegment := []*datapb.CompactionSegmentBinlogs{
			{
				SegmentID: segID,
			},
		}
		plan.SegmentBinlogs = segmentBinlogsWithEmptySegment
		err = task.compact()
		assert.Error(t, err)

		plan.SegmentBinlogs = segBinlogs
		// New test, remove all the binlogs in memkv
		//  Deltas in timetravel range
		err = mockKv.RemoveWithPrefix("/")
		require.NoError(t, err)
		cpaths, err = mockbIO.upload(context.TODO(), segID, partID, []*InsertData{iData}, dData, meta)
		require.NoError(t, err)
		plan.PlanID++

		plan.Timetravel = Timestamp(10000)
		err = task.compact()
		assert.NoError(t, err)

		updates, err = replica.getSegmentStatisticsUpdates(segID)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), updates.GetNumRows())

		// New test, remove all the binlogs in memkv
		//  Timeout
		err = mockKv.RemoveWithPrefix("/")
		require.NoError(t, err)
		cpaths, err = mockbIO.upload(context.TODO(), segID, partID, []*InsertData{iData}, dData, meta)
		require.NoError(t, err)
		plan.PlanID++

		mockfm.sleepSeconds = plan.TimeoutInSeconds + int32(1)
		err = task.compact()
		assert.Error(t, err)
	})

	t.Run("Test typeII compact valid", func(t *testing.T) {
		var collID, partID, segID1, segID2 UniqueID = 1, 10, 200, 201

		alloc := NewAllocatorFactory(1)
		rc := &RootCoordFactory{}
		dc := &DataCoordFactory{}
		mockfm := &mockFlushManager{}
		mockKv := memkv.NewMemoryKV()
		mockbIO := &binlogIO{mockKv, alloc}
		replica, err := newReplica(context.TODO(), rc, collID)
		require.NoError(t, err)

		replica.addFlushedSegmentWithPKs(segID1, collID, partID, "channelname", 2, []UniqueID{1})
		replica.addFlushedSegmentWithPKs(segID2, collID, partID, "channelname", 2, []UniqueID{9})
		require.True(t, replica.hasSegment(segID1, true))
		require.True(t, replica.hasSegment(segID2, true))

		meta := NewMetaFactory().GetCollectionMeta(collID, "test_compact_coll_name")
		iData1 := genInsertDataWithPKs([2]int64{1, 2})
		dData1 := &DeleteData{
			Pks:      []UniqueID{1},
			Tss:      []Timestamp{20000},
			RowCount: 1,
		}
		iData2 := genInsertDataWithPKs([2]int64{9, 10})
		dData2 := &DeleteData{
			Pks:      []UniqueID{9},
			Tss:      []Timestamp{30000},
			RowCount: 1,
		}

		cpaths1, err := mockbIO.upload(context.TODO(), segID1, partID, []*InsertData{iData1}, dData1, meta)
		require.NoError(t, err)
		require.Equal(t, 11, len(cpaths1.inPaths))

		cpaths2, err := mockbIO.upload(context.TODO(), segID2, partID, []*InsertData{iData2}, dData2, meta)
		require.NoError(t, err)
		require.Equal(t, 11, len(cpaths2.inPaths))

		plan := &datapb.CompactionPlan{
			PlanID: 10080,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:           segID1,
					FieldBinlogs:        cpaths1.inPaths,
					Field2StatslogPaths: cpaths1.statsPaths,
					Deltalogs:           cpaths1.deltaInfo,
				},
				{
					SegmentID:           segID2,
					FieldBinlogs:        cpaths2.inPaths,
					Field2StatslogPaths: cpaths2.statsPaths,
					Deltalogs:           cpaths2.deltaInfo,
				},
			},
			StartTime:        0,
			TimeoutInSeconds: 1,
			Type:             datapb.CompactionType_MergeCompaction,
			Timetravel:       40000,
			Channel:          "channelname",
		}

		alloc.random = false // generated ID = 19530
		task := newCompactionTask(context.TODO(), mockbIO, mockbIO, replica, mockfm, alloc, dc, plan)
		err = task.compact()
		assert.NoError(t, err)

		assert.False(t, replica.hasSegment(segID1, true))
		assert.False(t, replica.hasSegment(segID2, true))
		assert.True(t, replica.hasSegment(19530, true))
		updates, err := replica.getSegmentStatisticsUpdates(19530)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), updates.GetNumRows())

		// New test, remove all the binlogs in memkv
		//  Deltas in timetravel range
		err = mockKv.RemoveWithPrefix("/")
		require.NoError(t, err)
		plan.PlanID++

		plan.Timetravel = Timestamp(25000)
		replica.addFlushedSegmentWithPKs(segID1, collID, partID, "channelname", 2, []UniqueID{1})
		replica.addFlushedSegmentWithPKs(segID2, collID, partID, "channelname", 2, []UniqueID{9})
		replica.removeSegments(19530)
		require.True(t, replica.hasSegment(segID1, true))
		require.True(t, replica.hasSegment(segID2, true))
		require.False(t, replica.hasSegment(19530, true))

		err = task.compact()
		assert.NoError(t, err)

		assert.False(t, replica.hasSegment(segID1, true))
		assert.False(t, replica.hasSegment(segID2, true))
		assert.True(t, replica.hasSegment(19530, true))
		updates, err = replica.getSegmentStatisticsUpdates(19530)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), updates.GetNumRows())

		// New test, remove all the binlogs in memkv
		//  Deltas in timetravel range
		err = mockKv.RemoveWithPrefix("/")
		require.NoError(t, err)
		plan.PlanID++

		plan.Timetravel = Timestamp(10000)
		replica.addFlushedSegmentWithPKs(segID1, collID, partID, "channelname", 2, []UniqueID{1})
		replica.addFlushedSegmentWithPKs(segID2, collID, partID, "channelname", 2, []UniqueID{9})
		replica.removeSegments(19530)
		require.True(t, replica.hasSegment(segID1, true))
		require.True(t, replica.hasSegment(segID2, true))
		require.False(t, replica.hasSegment(19530, true))

		err = task.compact()
		assert.NoError(t, err)

		assert.False(t, replica.hasSegment(segID1, true))
		assert.False(t, replica.hasSegment(segID2, true))
		assert.True(t, replica.hasSegment(19530, true))
		updates, err = replica.getSegmentStatisticsUpdates(19530)
		assert.NoError(t, err)
		assert.Equal(t, int64(4), updates.GetNumRows())
	})

	t.Run("Test typeII compact 2 segments with the same pk", func(t *testing.T) {
		// Test merge compactions, two segments with the same pk, one deletion pk=1
		// The merged segment 19530 should only contain 2 rows and both pk=2
		// Both pk = 1 rows of the two segments are compacted.
		var collID, partID, segID1, segID2 UniqueID = 1, 10, 200, 201

		alloc := NewAllocatorFactory(1)
		rc := &RootCoordFactory{}
		dc := &DataCoordFactory{}
		mockfm := &mockFlushManager{}
		mockKv := memkv.NewMemoryKV()
		mockbIO := &binlogIO{mockKv, alloc}
		replica, err := newReplica(context.TODO(), rc, collID)
		require.NoError(t, err)

		replica.addFlushedSegmentWithPKs(segID1, collID, partID, "channelname", 2, []UniqueID{1})
		replica.addFlushedSegmentWithPKs(segID2, collID, partID, "channelname", 2, []UniqueID{1})
		require.True(t, replica.hasSegment(segID1, true))
		require.True(t, replica.hasSegment(segID2, true))

		meta := NewMetaFactory().GetCollectionMeta(collID, "test_compact_coll_name")
		// the same pk for segmentI and segmentII
		iData1 := genInsertDataWithPKs([2]int64{1, 2})
		iData2 := genInsertDataWithPKs([2]int64{1, 2})

		dData1 := &DeleteData{
			Pks:      []UniqueID{1},
			Tss:      []Timestamp{20000},
			RowCount: 1,
		}
		// empty dData2
		dData2 := &DeleteData{
			Pks:      []UniqueID{},
			Tss:      []Timestamp{},
			RowCount: 0,
		}

		cpaths1, err := mockbIO.upload(context.TODO(), segID1, partID, []*InsertData{iData1}, dData1, meta)
		require.NoError(t, err)
		require.Equal(t, 11, len(cpaths1.inPaths))

		cpaths2, err := mockbIO.upload(context.TODO(), segID2, partID, []*InsertData{iData2}, dData2, meta)
		require.NoError(t, err)
		require.Equal(t, 11, len(cpaths2.inPaths))

		plan := &datapb.CompactionPlan{
			PlanID: 20080,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:           segID1,
					FieldBinlogs:        cpaths1.inPaths,
					Field2StatslogPaths: cpaths1.statsPaths,
					Deltalogs:           cpaths1.deltaInfo,
				},
				{
					SegmentID:           segID2,
					FieldBinlogs:        cpaths2.inPaths,
					Field2StatslogPaths: cpaths2.statsPaths,
					Deltalogs:           cpaths2.deltaInfo,
				},
			},
			StartTime:        0,
			TimeoutInSeconds: 1,
			Type:             datapb.CompactionType_MergeCompaction,
			Timetravel:       40000,
			Channel:          "channelname",
		}

		alloc.random = false // generated ID = 19530
		task := newCompactionTask(context.TODO(), mockbIO, mockbIO, replica, mockfm, alloc, dc, plan)
		err = task.compact()
		assert.NoError(t, err)

		assert.False(t, replica.hasSegment(segID1, true))
		assert.False(t, replica.hasSegment(segID2, true))
		assert.True(t, replica.hasSegment(19530, true))
		updates, err := replica.getSegmentStatisticsUpdates(19530)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), updates.GetNumRows())
	})
}

type mockFlushManager struct {
	sleepSeconds int32
}

var _ flushManager = (*mockFlushManager)(nil)

func (mfm *mockFlushManager) flushBufferData(data *BufferData, segmentID UniqueID, flushed bool, dropped bool, pos *internalpb.MsgPosition) error {
	return nil
}

func (mfm *mockFlushManager) flushDelData(data *DelDataBuf, segmentID UniqueID, pos *internalpb.MsgPosition) error {
	return nil
}

func (mfm *mockFlushManager) injectFlush(injection *taskInjection, segments ...UniqueID) {
	go func() {
		time.Sleep(time.Second * time.Duration(mfm.sleepSeconds))
		//injection.injected <- struct{}{}
		close(injection.injected)
		<-injection.injectOver
	}()
}

func (mfm *mockFlushManager) notifyAllFlushed() {}

func (mfm *mockFlushManager) startDropping() {}

func (mfm *mockFlushManager) close() {}
