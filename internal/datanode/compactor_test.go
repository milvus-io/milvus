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
	"fmt"
	"math"

	//	"math"
	"testing"
	"time"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var compactTestDir = "/tmp/milvus_test/compact"

func TestCompactionTaskInnerMethods(t *testing.T) {
	cm := storage.NewLocalChunkManager(storage.RootPath(compactTestDir))
	defer cm.RemoveWithPrefix("")
	t.Run("Test getSegmentMeta", func(t *testing.T) {
		rc := &RootCoordFactory{
			pkType: schemapb.DataType_Int64,
		}
		replica, err := newReplica(context.TODO(), rc, cm, 1)
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
			{true, schemapb.DataType_VarChar, []interface{}{"test1", "test2"}, "valid varChar"},
			{true, schemapb.DataType_FloatVector, []interface{}{[]float32{1.0, 2.0}}, "valid floatvector"},
			{true, schemapb.DataType_BinaryVector, []interface{}{[]byte{255}}, "valid binaryvector"},
			{false, schemapb.DataType_Bool, []interface{}{1, 2}, "invalid bool"},
			{false, schemapb.DataType_Int8, []interface{}{nil, nil}, "invalid int8"},
			{false, schemapb.DataType_Int16, []interface{}{nil, nil}, "invalid int16"},
			{false, schemapb.DataType_Int32, []interface{}{nil, nil}, "invalid int32"},
			{false, schemapb.DataType_Int64, []interface{}{nil, nil}, "invalid int64"},
			{false, schemapb.DataType_Float, []interface{}{nil, nil}, "invalid float32"},
			{false, schemapb.DataType_Double, []interface{}{nil, nil}, "invalid float64"},
			{false, schemapb.DataType_VarChar, []interface{}{nil, nil}, "invalid varChar"},
			{false, schemapb.DataType_FloatVector, []interface{}{nil, nil}, "invalid floatvector"},
			{false, schemapb.DataType_BinaryVector, []interface{}{nil, nil}, "invalid binaryvector"},
			{false, schemapb.DataType_None, nil, "invalid data type"},
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

			blobs, err := getInt64DeltaBlobs(
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
						matchedPks := []primaryKey{newInt64PrimaryKey(1), newInt64PrimaryKey(4), newInt64PrimaryKey(5)}
						assert.ElementsMatch(t, matchedPks, db.delData.Pks)
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
						d, err := getInt64DeltaBlobs(test.segIDA, test.dataApk, test.dataAts)
						require.NoError(t, err)
						dBlobs[test.segIDA] = d
					}
					if test.segIDB != UniqueID(0) {
						d, err := getInt64DeltaBlobs(test.segIDB, test.dataBpk, test.dataBts)
						require.NoError(t, err)
						dBlobs[test.segIDB] = d
					}
					if test.segIDC != UniqueID(0) {
						d, err := getInt64DeltaBlobs(test.segIDC, test.dataCpk, test.dataCts)
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
			alloc := NewAllocatorFactory(1)
			mockbIO := &binlogIO{cm, alloc}
			Params.CommonCfg.EntityExpirationTTL = 0
			iData := genInsertDataWithExpiredTS()
			meta := NewMetaFactory().GetCollectionMeta(1, "test", schemapb.DataType_Int64)

			var allPaths [][]string
			inpath, err := mockbIO.uploadInsertLog(context.Background(), 1, 0, iData, meta)
			assert.NoError(t, err)
			assert.Equal(t, 12, len(inpath))
			binlogNum := len(inpath[0].GetBinlogs())
			assert.Equal(t, 1, binlogNum)

			for idx := 0; idx < binlogNum; idx++ {
				var ps []string
				for _, path := range inpath {
					ps = append(ps, path.GetBinlogs()[idx].GetLogPath())
				}
				allPaths = append(allPaths, ps)
			}

			dm := map[interface{}]Timestamp{
				1: 10000,
			}

			ct := &compactionTask{downloader: mockbIO, uploader: mockbIO}
			inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
			assert.NoError(t, err)
			assert.Equal(t, int64(2), numOfRow)
			assert.Equal(t, 1, len(inPaths[0].GetBinlogs()))
			assert.Equal(t, 1, len(statsPaths))
		})
		t.Run("Merge without expiration2", func(t *testing.T) {
			alloc := NewAllocatorFactory(1)
			mockbIO := &binlogIO{cm, alloc}
			Params.CommonCfg.EntityExpirationTTL = 0
			flushInsertBufferSize := Params.DataNodeCfg.FlushInsertBufferSize
			defer func() {
				Params.DataNodeCfg.FlushInsertBufferSize = flushInsertBufferSize
			}()
			Params.DataNodeCfg.FlushInsertBufferSize = 128
			iData := genInsertDataWithExpiredTS()
			meta := NewMetaFactory().GetCollectionMeta(1, "test", schemapb.DataType_Int64)

			var allPaths [][]string
			inpath, err := mockbIO.uploadInsertLog(context.Background(), 1, 0, iData, meta)
			assert.NoError(t, err)
			assert.Equal(t, 12, len(inpath))
			binlogNum := len(inpath[0].GetBinlogs())
			assert.Equal(t, 1, binlogNum)

			for idx := 0; idx < binlogNum; idx++ {
				var ps []string
				for _, path := range inpath {
					ps = append(ps, path.GetBinlogs()[idx].GetLogPath())
				}
				allPaths = append(allPaths, ps)
			}

			dm := map[interface{}]Timestamp{}

			ct := &compactionTask{downloader: mockbIO, uploader: mockbIO}
			inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
			assert.NoError(t, err)
			assert.Equal(t, int64(2), numOfRow)
			assert.Equal(t, 2, len(inPaths[0].GetBinlogs()))
			assert.Equal(t, 1, len(statsPaths))
			assert.Equal(t, 1, len(statsPaths[0].GetBinlogs()))
		})

		t.Run("Merge with expiration", func(t *testing.T) {
			alloc := NewAllocatorFactory(1)
			mockbIO := &binlogIO{cm, alloc}
			Params.CommonCfg.EntityExpirationTTL = 864000 // 10 days in seconds
			iData := genInsertDataWithExpiredTS()
			meta := NewMetaFactory().GetCollectionMeta(1, "test", schemapb.DataType_Int64)

			var allPaths [][]string
			inpath, err := mockbIO.uploadInsertLog(context.Background(), 1, 0, iData, meta)
			assert.NoError(t, err)
			assert.Equal(t, 12, len(inpath))
			binlogNum := len(inpath[0].GetBinlogs())
			assert.Equal(t, 1, binlogNum)

			for idx := 0; idx < binlogNum; idx++ {
				var ps []string
				for _, path := range inpath {
					ps = append(ps, path.GetBinlogs()[idx].GetLogPath())
				}
				allPaths = append(allPaths, ps)
			}

			dm := map[interface{}]Timestamp{
				1: 10000,
			}

			ct := &compactionTask{downloader: mockbIO, uploader: mockbIO}
			inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), numOfRow)
			assert.Equal(t, 0, len(inPaths))
			assert.Equal(t, 1, len(statsPaths))
		})

		t.Run("Merge with meta error", func(t *testing.T) {
			alloc := NewAllocatorFactory(1)
			mockbIO := &binlogIO{cm, alloc}
			Params.CommonCfg.EntityExpirationTTL = 0
			iData := genInsertDataWithExpiredTS()
			meta := NewMetaFactory().GetCollectionMeta(1, "test", schemapb.DataType_Int64)

			var allPaths [][]string
			inpath, err := mockbIO.uploadInsertLog(context.Background(), 1, 0, iData, meta)
			assert.NoError(t, err)
			assert.Equal(t, 12, len(inpath))
			binlogNum := len(inpath[0].GetBinlogs())
			assert.Equal(t, 1, binlogNum)

			for idx := 0; idx < binlogNum; idx++ {
				var ps []string
				for _, path := range inpath {
					ps = append(ps, path.GetBinlogs()[idx].GetLogPath())
				}
				allPaths = append(allPaths, ps)
			}

			dm := map[interface{}]Timestamp{
				1: 10000,
			}

			ct := &compactionTask{downloader: mockbIO, uploader: mockbIO}
			_, _, _, err = ct.merge(context.Background(), allPaths, 2, 0, &etcdpb.CollectionMeta{
				Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "64"},
					}},
				}},
			}, dm)
			assert.Error(t, err)
		})

		t.Run("Merge with meta type param error", func(t *testing.T) {
			alloc := NewAllocatorFactory(1)
			mockbIO := &binlogIO{cm, alloc}
			Params.CommonCfg.EntityExpirationTTL = 0
			iData := genInsertDataWithExpiredTS()
			meta := NewMetaFactory().GetCollectionMeta(1, "test", schemapb.DataType_Int64)

			var allPaths [][]string
			inpath, err := mockbIO.uploadInsertLog(context.Background(), 1, 0, iData, meta)
			assert.NoError(t, err)
			assert.Equal(t, 12, len(inpath))
			binlogNum := len(inpath[0].GetBinlogs())
			assert.Equal(t, 1, binlogNum)

			for idx := 0; idx < binlogNum; idx++ {
				var ps []string
				for _, path := range inpath {
					ps = append(ps, path.GetBinlogs()[idx].GetLogPath())
				}
				allPaths = append(allPaths, ps)
			}

			dm := map[interface{}]Timestamp{
				1: 10000,
			}

			ct := &compactionTask{downloader: mockbIO, uploader: mockbIO}

			_, _, _, err = ct.merge(context.Background(), allPaths, 2, 0, &etcdpb.CollectionMeta{
				Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "dim"},
					}},
				}}}, dm)
			assert.Error(t, err)
		})
	})

	t.Run("Test isExpiredEntity", func(t *testing.T) {
		t.Run("When CompactionEntityExpiration is set math.MaxInt64", func(t *testing.T) {
			Params.CommonCfg.EntityExpirationTTL = math.MaxInt64

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
		t.Run("When CompactionEntityExpiration is set MAX_ENTITY_EXPIRATION = 0", func(t *testing.T) {
			Params.CommonCfg.EntityExpirationTTL = 0 // 0 means expiration is not enabled

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
		t.Run("When CompactionEntityExpiration is set 10 days", func(t *testing.T) {
			Params.CommonCfg.EntityExpirationTTL = 864000 // 10 days in seconds

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

func getInt64DeltaBlobs(segID UniqueID, pks []UniqueID, tss []Timestamp) ([]*Blob, error) {
	primaryKeys := make([]primaryKey, len(pks))
	for index, v := range pks {
		primaryKeys[index] = newInt64PrimaryKey(v)
	}
	deltaData := &DeleteData{
		Pks:      primaryKeys,
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
	cm := storage.NewLocalChunkManager(storage.RootPath(compactTestDir))
	defer cm.RemoveWithPrefix("")
	notEmptySegmentBinlogs := []*datapb.CompactionSegmentBinlogs{{
		SegmentID:           100,
		FieldBinlogs:        nil,
		Field2StatslogPaths: nil,
		Deltalogs:           nil,
	}}
	Params.CommonCfg.EntityExpirationTTL = 0 // Turn off auto expiration

	t.Run("Test compact invalid", func(t *testing.T) {
		invalidAlloc := NewAllocatorFactory(-1)
		ctx, cancel := context.WithCancel(context.TODO())
		emptyTask := &compactionTask{
			ctx:    ctx,
			cancel: cancel,
		}

		plan := &datapb.CompactionPlan{
			PlanID:           999,
			SegmentBinlogs:   notEmptySegmentBinlogs,
			StartTime:        0,
			TimeoutInSeconds: 10,
			Type:             datapb.CompactionType_UndefinedCompaction,
			Channel:          "",
		}

		emptyTask.plan = plan
		_, err := emptyTask.compact()
		assert.Error(t, err)

		plan.Type = datapb.CompactionType_MergeCompaction
		emptyTask.allocatorInterface = invalidAlloc
		plan.SegmentBinlogs = notEmptySegmentBinlogs
		_, err = emptyTask.compact()
		assert.Error(t, err)

		emptyTask.stop()
	})

	t.Run("Test typeII compact valid", func(t *testing.T) {
		alloc := NewAllocatorFactory(1)
		type testCase struct {
			pkType schemapb.DataType
			iData1 storage.FieldData
			iData2 storage.FieldData
			pks1   [2]primaryKey
			pks2   [2]primaryKey
			colID  UniqueID
			parID  UniqueID
			segID1 UniqueID
			segID2 UniqueID
		}
		cases := []testCase{
			{
				pkType: schemapb.DataType_Int64,
				iData1: &storage.Int64FieldData{Data: []UniqueID{1}},
				iData2: &storage.Int64FieldData{Data: []UniqueID{9}},
				pks1:   [2]primaryKey{newInt64PrimaryKey(1), newInt64PrimaryKey(2)},
				pks2:   [2]primaryKey{newInt64PrimaryKey(9), newInt64PrimaryKey(10)},
				colID:  1,
				parID:  10,
				segID1: 100,
				segID2: 101,
			},
			{
				pkType: schemapb.DataType_VarChar,
				iData1: &storage.StringFieldData{Data: []string{"aaaa"}},
				iData2: &storage.StringFieldData{Data: []string{"milvus"}},
				pks1:   [2]primaryKey{newVarCharPrimaryKey("aaaa"), newVarCharPrimaryKey("bbbb")},
				pks2:   [2]primaryKey{newVarCharPrimaryKey("milvus"), newVarCharPrimaryKey("mmmm")},
				colID:  2,
				parID:  11,
				segID1: 102,
				segID2: 103,
			},
		}

		for _, c := range cases {
			rc := &RootCoordFactory{
				pkType: c.pkType,
			}
			mockfm := &mockFlushManager{}
			mockKv := memkv.NewMemoryKV()
			mockbIO := &binlogIO{cm, alloc}
			replica, err := newReplica(context.TODO(), rc, cm, c.colID)
			require.NoError(t, err)

			replica.addFlushedSegmentWithPKs(c.segID1, c.colID, c.parID, "channelname", 2, c.iData1)
			replica.addFlushedSegmentWithPKs(c.segID2, c.colID, c.parID, "channelname", 2, c.iData2)
			require.True(t, replica.hasSegment(c.segID1, true))
			require.True(t, replica.hasSegment(c.segID2, true))

			meta := NewMetaFactory().GetCollectionMeta(c.colID, "test_compact_coll_name", c.pkType)
			iData1 := genInsertDataWithPKs(c.pks1, c.pkType)
			dData1 := &DeleteData{
				Pks:      []primaryKey{c.pks1[0]},
				Tss:      []Timestamp{20000},
				RowCount: 1,
			}
			iData2 := genInsertDataWithPKs(c.pks2, c.pkType)
			dData2 := &DeleteData{
				Pks:      []primaryKey{c.pks2[0]},
				Tss:      []Timestamp{30000},
				RowCount: 1,
			}

			cpaths1, err := mockbIO.upload(context.TODO(), c.segID1, c.parID, []*InsertData{iData1}, []byte{}, dData1, meta)
			require.NoError(t, err)
			require.Equal(t, 12, len(cpaths1.inPaths))

			cpaths2, err := mockbIO.upload(context.TODO(), c.segID2, c.parID, []*InsertData{iData2}, []byte{}, dData2, meta)
			require.NoError(t, err)
			require.Equal(t, 12, len(cpaths2.inPaths))

			plan := &datapb.CompactionPlan{
				PlanID: 10080,
				SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
					{
						SegmentID:           c.segID1,
						FieldBinlogs:        cpaths1.inPaths,
						Field2StatslogPaths: cpaths1.statsPaths,
						Deltalogs:           cpaths1.deltaInfo,
					},
					{
						SegmentID:           c.segID2,
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
			task := newCompactionTask(context.TODO(), mockbIO, mockbIO, replica, mockfm, alloc, plan)
			_, err = task.compact()
			assert.NoError(t, err)

			assert.False(t, replica.hasSegment(c.segID1, true))
			assert.False(t, replica.hasSegment(c.segID2, true))
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
			replica.addFlushedSegmentWithPKs(c.segID1, c.colID, c.parID, "channelname", 2, c.iData1)
			replica.addFlushedSegmentWithPKs(c.segID2, c.colID, c.parID, "channelname", 2, c.iData2)
			replica.removeSegments(19530)
			require.True(t, replica.hasSegment(c.segID1, true))
			require.True(t, replica.hasSegment(c.segID2, true))
			require.False(t, replica.hasSegment(19530, true))

			_, err = task.compact()
			assert.NoError(t, err)

			assert.False(t, replica.hasSegment(c.segID1, true))
			assert.False(t, replica.hasSegment(c.segID2, true))
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
			replica.addFlushedSegmentWithPKs(c.segID1, c.colID, c.parID, "channelname", 2, c.iData1)
			replica.addFlushedSegmentWithPKs(c.segID2, c.colID, c.parID, "channelname", 2, c.iData2)
			replica.removeSegments(19530)
			require.True(t, replica.hasSegment(c.segID1, true))
			require.True(t, replica.hasSegment(c.segID2, true))
			require.False(t, replica.hasSegment(19530, true))

			_, err = task.compact()
			assert.NoError(t, err)

			assert.False(t, replica.hasSegment(c.segID1, true))
			assert.False(t, replica.hasSegment(c.segID2, true))
			assert.True(t, replica.hasSegment(19530, true))
			updates, err = replica.getSegmentStatisticsUpdates(19530)
			assert.NoError(t, err)
			assert.Equal(t, int64(4), updates.GetNumRows())
		}
	})

	t.Run("Test typeII compact 2 segments with the same pk", func(t *testing.T) {
		// Test merge compactions, two segments with the same pk, one deletion pk=1
		// The merged segment 19530 should only contain 2 rows and both pk=2
		// Both pk = 1 rows of the two segments are compacted.
		var collID, partID, segID1, segID2 UniqueID = 1, 10, 200, 201

		alloc := NewAllocatorFactory(1)
		rc := &RootCoordFactory{
			pkType: schemapb.DataType_Int64,
		}
		mockfm := &mockFlushManager{}
		mockbIO := &binlogIO{cm, alloc}
		replica, err := newReplica(context.TODO(), rc, cm, collID)
		require.NoError(t, err)

		replica.addFlushedSegmentWithPKs(segID1, collID, partID, "channelname", 2, &storage.Int64FieldData{Data: []UniqueID{1}})
		replica.addFlushedSegmentWithPKs(segID2, collID, partID, "channelname", 2, &storage.Int64FieldData{Data: []UniqueID{1}})
		require.True(t, replica.hasSegment(segID1, true))
		require.True(t, replica.hasSegment(segID2, true))

		meta := NewMetaFactory().GetCollectionMeta(collID, "test_compact_coll_name", schemapb.DataType_Int64)
		// the same pk for segmentI and segmentII
		pks := [2]primaryKey{newInt64PrimaryKey(1), newInt64PrimaryKey(2)}
		iData1 := genInsertDataWithPKs(pks, schemapb.DataType_Int64)
		iData2 := genInsertDataWithPKs(pks, schemapb.DataType_Int64)

		pk1 := newInt64PrimaryKey(1)
		dData1 := &DeleteData{
			Pks:      []primaryKey{pk1},
			Tss:      []Timestamp{20000},
			RowCount: 1,
		}
		// empty dData2
		dData2 := &DeleteData{
			Pks:      []primaryKey{},
			Tss:      []Timestamp{},
			RowCount: 0,
		}

		cpaths1, err := mockbIO.upload(context.TODO(), segID1, partID, []*InsertData{iData1}, []byte{}, dData1, meta)
		require.NoError(t, err)
		require.Equal(t, 12, len(cpaths1.inPaths))

		cpaths2, err := mockbIO.upload(context.TODO(), segID2, partID, []*InsertData{iData2}, []byte{}, dData2, meta)
		require.NoError(t, err)
		require.Equal(t, 12, len(cpaths2.inPaths))

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
		task := newCompactionTask(context.TODO(), mockbIO, mockbIO, replica, mockfm, alloc, plan)
		_, err = task.compact()
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
	returnError  bool
}

var _ flushManager = (*mockFlushManager)(nil)

func (mfm *mockFlushManager) flushBufferData(data *BufferData, segStats []byte, segmentID UniqueID, flushed bool, dropped bool, pos *internalpb.MsgPosition) error {
	if mfm.returnError {
		return fmt.Errorf("mock error")
	}
	return nil
}

func (mfm *mockFlushManager) flushDelData(data *DelDataBuf, segmentID UniqueID, pos *internalpb.MsgPosition) error {
	if mfm.returnError {
		return fmt.Errorf("mock error")
	}
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
