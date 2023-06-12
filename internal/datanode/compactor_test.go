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
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var compactTestDir = "/tmp/milvus_test/compact"

func TestCompactionTaskInnerMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(compactTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	t.Run("Test getSegmentMeta", func(t *testing.T) {
		rc := &RootCoordFactory{
			pkType: schemapb.DataType_Int64,
		}
		channel := newChannel("a", 1, nil, rc, cm)
		var err error

		task := &compactionTask{
			Channel: channel,
			done:    make(chan struct{}, 1),
		}

		_, _, _, err = task.getSegmentMeta(100)
		assert.Error(t, err)

		err = channel.addSegment(addSegmentReq{
			segType:     datapb.SegmentType_New,
			segID:       100,
			collID:      1,
			partitionID: 10,
			startPos:    new(msgpb.MsgPosition),
			endPos:      nil,
		})
		require.NoError(t, err)

		collID, partID, meta, err := task.getSegmentMeta(100)
		assert.NoError(t, err)
		assert.Equal(t, UniqueID(1), collID)
		assert.Equal(t, UniqueID(10), partID)
		assert.NotNil(t, meta)

		rc.setCollectionID(-2)
		task.Channel.(*ChannelMeta).collSchema = nil
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
			{true, schemapb.DataType_JSON, []interface{}{[]byte("{\"key\":\"value\"}"), []byte("{\"hello\":\"world\"}")}, "valid json"},
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
			{false, schemapb.DataType_JSON, []interface{}{nil, nil}, "invalid json"},
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
				task := &compactionTask{
					done: make(chan struct{}, 1),
				}
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

					task := &compactionTask{
						done: make(chan struct{}, 1),
					}
					pk2ts, db, err := task.mergeDeltalogs(dBlobs, test.timetravel)
					assert.NoError(t, err)
					assert.Equal(t, test.expectedpk2ts, len(pk2ts))
					assert.Equal(t, test.expecteddb, int(db.GetEntriesNum()))
				})
			}
		})

	})

	t.Run("Test merge", func(t *testing.T) {
		collectionID := int64(1)
		meta := NewMetaFactory().GetCollectionMeta(collectionID, "test", schemapb.DataType_Int64)

		rc := &mocks.RootCoord{}
		rc.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Schema: meta.GetSchema(),
			}, nil)
		channel := newChannel("a", collectionID, meta.GetSchema(), rc, nil)
		channel.segments[1] = &Segment{numRows: 10}

		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
		alloc.EXPECT().AllocOne().Return(0, nil)
		t.Run("Merge without expiration", func(t *testing.T) {

			mockbIO := &binlogIO{cm, alloc}
			paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0")
			iData := genInsertDataWithExpiredTS()

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

			ct := &compactionTask{
				Channel: channel, downloader: mockbIO, uploader: mockbIO, done: make(chan struct{}, 1),
				plan: &datapb.CompactionPlan{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1}},
				}}
			inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
			assert.NoError(t, err)
			assert.Equal(t, int64(2), numOfRow)
			assert.Equal(t, 1, len(inPaths[0].GetBinlogs()))
			assert.Equal(t, 1, len(statsPaths))
		})
		t.Run("Merge without expiration2", func(t *testing.T) {
			mockbIO := &binlogIO{cm, alloc}
			paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0")
			BinLogMaxSize := Params.DataNodeCfg.BinLogMaxSize
			defer func() {
				Params.DataNodeCfg.BinLogMaxSize = BinLogMaxSize
			}()
			paramtable.Get().Save(Params.DataNodeCfg.BinLogMaxSize.Key, "128")
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

			ct := &compactionTask{
				Channel: channel, downloader: mockbIO, uploader: mockbIO, done: make(chan struct{}, 1),
				plan: &datapb.CompactionPlan{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1}},
				}}
			inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
			assert.NoError(t, err)
			assert.Equal(t, int64(2), numOfRow)
			assert.Equal(t, 2, len(inPaths[0].GetBinlogs()))
			assert.Equal(t, 1, len(statsPaths))
			assert.Equal(t, 1, len(statsPaths[0].GetBinlogs()))
		})

		t.Run("Merge with expiration", func(t *testing.T) {
			mockbIO := &binlogIO{cm, alloc}

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

			// 10 days in seconds
			ct := &compactionTask{
				Channel:    channel,
				downloader: mockbIO,
				uploader:   mockbIO,
				plan: &datapb.CompactionPlan{
					CollectionTtl: 864000,
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1}},
				},
				done: make(chan struct{}, 1),
			}
			inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), numOfRow)
			assert.Equal(t, 0, len(inPaths))
			assert.Equal(t, 0, len(statsPaths))
		})

		t.Run("Merge with meta error", func(t *testing.T) {
			mockbIO := &binlogIO{cm, alloc}
			paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0")
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

			ct := &compactionTask{
				Channel: channel, downloader: mockbIO, uploader: mockbIO, done: make(chan struct{}, 1),
				plan: &datapb.CompactionPlan{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1}},
				}}
			_, _, _, err = ct.merge(context.Background(), allPaths, 2, 0, &etcdpb.CollectionMeta{
				Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "64"},
					}},
				}},
			}, dm)
			assert.Error(t, err)
		})

		t.Run("Merge with meta type param error", func(t *testing.T) {
			mockbIO := &binlogIO{cm, alloc}
			paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0")
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

			ct := &compactionTask{Channel: channel, downloader: mockbIO, uploader: mockbIO, done: make(chan struct{}, 1)}

			_, _, _, err = ct.merge(context.Background(), allPaths, 2, 0, &etcdpb.CollectionMeta{
				Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "bad_dim"},
					}},
				}}}, dm)
			assert.Error(t, err)
		})
	})

	t.Run("Test isExpiredEntity", func(t *testing.T) {
		t.Run("When CompactionEntityExpiration is set math.MaxInt64", func(t *testing.T) {
			ct := &compactionTask{
				plan: &datapb.CompactionPlan{
					CollectionTtl: math.MaxInt64,
				},
				done: make(chan struct{}, 1),
			}

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
			// 0 means expiration is not enabled
			ct := &compactionTask{
				plan: &datapb.CompactionPlan{
					CollectionTtl: 0,
				},
				done: make(chan struct{}, 1),
			}
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
			// 10 days in seconds
			ct := &compactionTask{
				plan: &datapb.CompactionPlan{
					CollectionTtl: 864000,
				},
				done: make(chan struct{}, 1),
			}
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

	t.Run("Test getNumRows error", func(t *testing.T) {
		rc := &RootCoordFactory{
			pkType: schemapb.DataType_Int64,
		}
		cm := &mockCm{}

		ct := &compactionTask{
			Channel: newChannel("channel", 1, nil, rc, cm),
			plan: &datapb.CompactionPlan{
				SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
					{
						SegmentID: 1,
					}},
			},
			done: make(chan struct{}, 1),
		}

		//segment not in channel
		_, err := ct.getNumRows()
		assert.Error(t, err)
	})

	t.Run("Test uploadRemainLog error", func(t *testing.T) {
		f := &MetaFactory{}

		t.Run("field not in field to type", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			ct := &compactionTask{
				done: make(chan struct{}, 1),
			}
			meta := f.GetCollectionMeta(UniqueID(10001), "test_upload_remain_log", schemapb.DataType_Int64)
			fid2C := make(map[int64][]interface{})
			fid2T := make(map[int64]schemapb.DataType)
			fid2C[1] = nil
			_, _, err := ct.uploadRemainLog(ctx, 1, 2, meta, nil, 0, fid2C, fid2T)
			assert.Error(t, err)
		})

		t.Run("transfer interface wrong", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			ct := &compactionTask{
				done: make(chan struct{}, 1),
			}
			meta := f.GetCollectionMeta(UniqueID(10001), "test_upload_remain_log", schemapb.DataType_Int64)
			fid2C := make(map[int64][]interface{})
			fid2T := make(map[int64]schemapb.DataType)
			fid2C[1] = nil
			_, _, err := ct.uploadRemainLog(ctx, 1, 2, meta, nil, 0, fid2C, fid2T)
			assert.Error(t, err)
		})

		t.Run("upload failed", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			alloc := allocator.NewMockAllocator(t)
			alloc.EXPECT().AllocOne().Call.Return(int64(11111), nil)

			meta := f.GetCollectionMeta(UniqueID(10001), "test_upload_remain_log", schemapb.DataType_Int64)
			stats := storage.NewPrimaryKeyStats(106, int64(schemapb.DataType_Int64), 10)

			ct := &compactionTask{
				uploader: &binlogIO{&mockCm{errMultiSave: true}, alloc},
				done:     make(chan struct{}, 1),
			}

			_, _, err := ct.uploadRemainLog(ctx, 1, 2, meta, stats, 10, nil, nil)
			assert.Error(t, err)
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

func TestCompactorInterfaceMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(compactTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
	notEmptySegmentBinlogs := []*datapb.CompactionSegmentBinlogs{{
		SegmentID:           100,
		FieldBinlogs:        nil,
		Field2StatslogPaths: nil,
		Deltalogs:           nil,
	}}
	paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0") // Turn off auto expiration

	t.Run("Test compact invalid", func(t *testing.T) {
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocOne().Call.Return(int64(11111), nil)
		ctx, cancel := context.WithCancel(context.TODO())
		emptyTask := &compactionTask{
			ctx:     ctx,
			cancel:  cancel,
			done:    make(chan struct{}, 1),
			Channel: &ChannelMeta{},
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
		emptyTask.Allocator = alloc
		plan.SegmentBinlogs = notEmptySegmentBinlogs
		_, err = emptyTask.compact()
		assert.Error(t, err)

		emptyTask.complete()
		emptyTask.stop()
	})

	t.Run("Test typeII compact valid", func(t *testing.T) {
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
		alloc.EXPECT().AllocOne().Call.Return(int64(19530), nil)
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
			channel := newChannel("a", c.colID, nil, rc, cm)

			channel.addFlushedSegmentWithPKs(c.segID1, c.colID, c.parID, 2, c.iData1)
			channel.addFlushedSegmentWithPKs(c.segID2, c.colID, c.parID, 2, c.iData2)
			require.True(t, channel.hasSegment(c.segID1, true))
			require.True(t, channel.hasSegment(c.segID2, true))

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

			stats1 := storage.NewPrimaryKeyStats(1, int64(c.pkType), 1)
			iPaths1, sPaths1, err := mockbIO.uploadStatsLog(context.TODO(), c.segID1, c.parID, iData1, stats1, 2, meta)
			require.NoError(t, err)
			dPaths1, err := mockbIO.uploadDeltaLog(context.TODO(), c.segID1, c.parID, dData1, meta)
			require.NoError(t, err)
			require.Equal(t, 12, len(iPaths1))

			stats2 := storage.NewPrimaryKeyStats(1, int64(c.pkType), 1)
			iPaths2, sPaths2, err := mockbIO.uploadStatsLog(context.TODO(), c.segID2, c.parID, iData2, stats2, 2, meta)
			require.NoError(t, err)
			dPaths2, err := mockbIO.uploadDeltaLog(context.TODO(), c.segID2, c.parID, dData2, meta)
			require.NoError(t, err)
			require.Equal(t, 12, len(iPaths2))

			plan := &datapb.CompactionPlan{
				PlanID: 10080,
				SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
					{
						SegmentID:           c.segID1,
						FieldBinlogs:        lo.Values(iPaths1),
						Field2StatslogPaths: lo.Values(sPaths1),
						Deltalogs:           dPaths1,
					},
					{
						SegmentID:           c.segID2,
						FieldBinlogs:        lo.Values(iPaths2),
						Field2StatslogPaths: lo.Values(sPaths2),
						Deltalogs:           dPaths2,
					},
				},
				StartTime:        0,
				TimeoutInSeconds: 10,
				Type:             datapb.CompactionType_MergeCompaction,
				Timetravel:       40000,
				Channel:          "channelname",
			}

			task := newCompactionTask(context.TODO(), mockbIO, mockbIO, channel, mockfm, alloc, plan, nil)
			result, err := task.compact()
			assert.NoError(t, err)
			assert.NotNil(t, result)

			assert.Equal(t, plan.GetPlanID(), result.GetPlanID())
			assert.Equal(t, UniqueID(19530), result.GetSegmentID())
			assert.Equal(t, int64(2), result.GetNumOfRows())
			assert.NotEmpty(t, result.InsertLogs)
			assert.NotEmpty(t, result.Field2StatslogPaths)

			// New test, remove all the binlogs in memkv
			//  Deltas in timetravel range
			err = mockKv.RemoveWithPrefix("/")
			require.NoError(t, err)
			plan.PlanID++

			plan.Timetravel = Timestamp(25000)
			channel.addFlushedSegmentWithPKs(c.segID1, c.colID, c.parID, 2, c.iData1)
			channel.addFlushedSegmentWithPKs(c.segID2, c.colID, c.parID, 2, c.iData2)
			channel.removeSegments(19530)
			require.True(t, channel.hasSegment(c.segID1, true))
			require.True(t, channel.hasSegment(c.segID2, true))
			require.False(t, channel.hasSegment(19530, true))

			result, err = task.compact()
			assert.NoError(t, err)
			assert.NotNil(t, result)

			assert.Equal(t, plan.GetPlanID(), result.GetPlanID())
			assert.Equal(t, UniqueID(19530), result.GetSegmentID())
			assert.Equal(t, int64(3), result.GetNumOfRows())
			assert.NotEmpty(t, result.InsertLogs)
			assert.NotEmpty(t, result.Field2StatslogPaths)

			// New test, remove all the binlogs in memkv
			//  Deltas in timetravel range
			err = mockKv.RemoveWithPrefix("/")
			require.NoError(t, err)
			plan.PlanID++

			plan.Timetravel = Timestamp(10000)
			channel.addFlushedSegmentWithPKs(c.segID1, c.colID, c.parID, 2, c.iData1)
			channel.addFlushedSegmentWithPKs(c.segID2, c.colID, c.parID, 2, c.iData2)
			channel.removeSegments(19530)
			require.True(t, channel.hasSegment(c.segID1, true))
			require.True(t, channel.hasSegment(c.segID2, true))
			require.False(t, channel.hasSegment(19530, true))

			result, err = task.compact()
			assert.NoError(t, err)
			assert.NotNil(t, result)

			assert.Equal(t, plan.GetPlanID(), result.GetPlanID())
			assert.Equal(t, UniqueID(19530), result.GetSegmentID())
			assert.Equal(t, int64(4), result.GetNumOfRows())
			assert.NotEmpty(t, result.InsertLogs)
			assert.NotEmpty(t, result.Field2StatslogPaths)

			assert.Equal(t, 0, mockfm.injectCount())
			task.injectDone()
			time.Sleep(500 * time.Millisecond)
			assert.Equal(t, 1, mockfm.injectCount())
		}
	})

	t.Run("Test typeII compact 2 segments with the same pk", func(t *testing.T) {
		// Test merge compactions, two segments with the same pk, one deletion pk=1
		// The merged segment 19530 should only contain 2 rows and both pk=2
		// Both pk = 1 rows of the two segments are compacted.
		var collID, partID, segID1, segID2 UniqueID = 1, 10, 200, 201

		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocOne().Call.Return(int64(19530), nil)
		alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
		rc := &RootCoordFactory{
			pkType: schemapb.DataType_Int64,
		}
		mockfm := &mockFlushManager{}
		mockbIO := &binlogIO{cm, alloc}
		channel := newChannel("channelname", collID, nil, rc, cm)

		channel.addFlushedSegmentWithPKs(segID1, collID, partID, 2, &storage.Int64FieldData{Data: []UniqueID{1}})
		channel.addFlushedSegmentWithPKs(segID2, collID, partID, 2, &storage.Int64FieldData{Data: []UniqueID{1}})
		require.True(t, channel.hasSegment(segID1, true))
		require.True(t, channel.hasSegment(segID2, true))

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

		stats1 := storage.NewPrimaryKeyStats(1, int64(rc.pkType), 1)
		iPaths1, sPaths1, err := mockbIO.uploadStatsLog(context.TODO(), segID1, partID, iData1, stats1, 1, meta)
		require.NoError(t, err)
		dPaths1, err := mockbIO.uploadDeltaLog(context.TODO(), segID1, partID, dData1, meta)
		require.NoError(t, err)
		require.Equal(t, 12, len(iPaths1))

		stats2 := storage.NewPrimaryKeyStats(1, int64(rc.pkType), 1)
		iPaths2, sPaths2, err := mockbIO.uploadStatsLog(context.TODO(), segID2, partID, iData2, stats2, 1, meta)
		require.NoError(t, err)
		dPaths2, err := mockbIO.uploadDeltaLog(context.TODO(), segID2, partID, dData2, meta)
		require.NoError(t, err)
		require.Equal(t, 12, len(iPaths2))

		plan := &datapb.CompactionPlan{
			PlanID: 20080,
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{
					SegmentID:           segID1,
					FieldBinlogs:        lo.Values(iPaths1),
					Field2StatslogPaths: lo.Values(sPaths1),
					Deltalogs:           dPaths1,
				},
				{
					SegmentID:           segID2,
					FieldBinlogs:        lo.Values(iPaths2),
					Field2StatslogPaths: lo.Values(sPaths2),
					Deltalogs:           dPaths2,
				},
			},
			StartTime:        0,
			TimeoutInSeconds: 10,
			Type:             datapb.CompactionType_MergeCompaction,
			Timetravel:       40000,
			Channel:          "channelname",
		}

		task := newCompactionTask(context.TODO(), mockbIO, mockbIO, channel, mockfm, alloc, plan, nil)
		result, err := task.compact()
		assert.NoError(t, err)
		assert.NotNil(t, result)

		assert.Equal(t, plan.GetPlanID(), result.GetPlanID())
		assert.Equal(t, UniqueID(19530), result.GetSegmentID())
		assert.Equal(t, int64(2), result.GetNumOfRows())
		assert.NotEmpty(t, result.InsertLogs)
		assert.NotEmpty(t, result.Field2StatslogPaths)

		assert.Equal(t, 0, mockfm.injectCount())
		task.injectDone()
		time.Sleep(500 * time.Millisecond)
		assert.Equal(t, 1, mockfm.injectCount())
	})
}

type mockFlushManager struct {
	sleepSeconds     int32
	returnError      bool
	recordFlushedSeg bool
	flushedSegIDs    []UniqueID
	full             bool
	injectOverCount  struct {
		sync.RWMutex
		value int
	}
}

var _ flushManager = (*mockFlushManager)(nil)

func (mfm *mockFlushManager) flushBufferData(data *BufferData, segmentID UniqueID, flushed bool, dropped bool, pos *msgpb.MsgPosition) (*storage.PrimaryKeyStats, error) {
	if mfm.returnError {
		return nil, fmt.Errorf("mock error")
	}
	return nil, nil
}

func (mfm *mockFlushManager) flushDelData(data *DelDataBuf, segmentID UniqueID, pos *msgpb.MsgPosition) error {
	if mfm.returnError {
		return fmt.Errorf("mock error")
	}
	if mfm.recordFlushedSeg {
		mfm.flushedSegIDs = append(mfm.flushedSegIDs, segmentID)
	}
	return nil
}

func (mfm *mockFlushManager) isFull() bool {
	return mfm.full
}

func (mfm *mockFlushManager) injectFlush(injection *taskInjection, segments ...UniqueID) {
	go func() {
		time.Sleep(time.Second * time.Duration(mfm.sleepSeconds))
		//injection.injected <- struct{}{}
		close(injection.injected)
		<-injection.injectOver
		mfm.injectOverCount.Lock()
		defer mfm.injectOverCount.Unlock()
		mfm.injectOverCount.value++
	}()
}

func (mfm *mockFlushManager) injectCount() int {
	mfm.injectOverCount.RLock()
	defer mfm.injectOverCount.RUnlock()
	return mfm.injectOverCount.value
}

func (mfm *mockFlushManager) notifyAllFlushed() {}

func (mfm *mockFlushManager) startDropping() {}

func (mfm *mockFlushManager) close() {}
