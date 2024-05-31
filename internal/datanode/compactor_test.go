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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/allocator"
	"github.com/milvus-io/milvus/internal/datanode/io"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var compactTestDir = "/tmp/milvus_test/compact"

func TestCompactionTaskInnerMethods(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(compactTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())
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
			{true, schemapb.DataType_Array, []interface{}{
				&schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{1, 2},
						},
					},
				},
				&schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{3, 4},
						},
					},
				},
			}, "valid array"},
			{true, schemapb.DataType_FloatVector, []interface{}{[]float32{1.0, 2.0}}, "valid floatvector"},
			{true, schemapb.DataType_BinaryVector, []interface{}{[]byte{255}}, "valid binaryvector"},
			{true, schemapb.DataType_Float16Vector, []interface{}{[]byte{255, 255, 255, 255}}, "valid float16vector"},
			{true, schemapb.DataType_BFloat16Vector, []interface{}{[]byte{255, 255, 255, 255}}, "valid bfloat16vector"},

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
			{false, schemapb.DataType_Float16Vector, []interface{}{nil, nil}, "invalid float16vector"},
			{false, schemapb.DataType_BFloat16Vector, []interface{}{nil, nil}, "invalid bfloat16vector"},

			{false, schemapb.DataType_SparseFloatVector, []interface{}{nil, nil}, "invalid sparsefloatvector"},
			{false, schemapb.DataType_SparseFloatVector, []interface{}{[]byte{255}, []byte{15}}, "invalid sparsefloatvector"},
			{true, schemapb.DataType_SparseFloatVector, []interface{}{
				typeutil.CreateSparseFloatRow([]uint32{1, 2}, []float32{1.0, 2.0}),
				typeutil.CreateSparseFloatRow([]uint32{3, 4}, []float32{1.0, 2.0}),
			}, "valid sparsefloatvector"},
		}

		// make sure all new data types missed to handle would throw unexpected error
		for typeName, typeValue := range schemapb.DataType_value {
			tests = append(tests, struct {
				isvalid bool

				tp      schemapb.DataType
				content []interface{}

				description string
			}{false, schemapb.DataType(typeValue), []interface{}{nil, nil}, "invalid " + typeName})
		}

		for _, test := range tests {
			t.Run(test.description, func(t *testing.T) {
				if test.isvalid {
					fd, err := interface2FieldData(test.tp, test.content, 2)
					assert.NoError(t, err)
					assert.Equal(t, 2, fd.RowNum())
				} else {
					fd, err := interface2FieldData(test.tp, test.content, 2)
					assert.True(t, errors.Is(err, errTransferType) || errors.Is(err, errUnknownDataType))
					assert.Nil(t, fd)
				}
			})
		}
	})

	t.Run("Test mergeDeltalogs", func(t *testing.T) {
		t.Run("One segment", func(t *testing.T) {
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
					2,
				},
				[]Timestamp{
					20000,
					20001,
					20002,
					30000,
					50000,
					50000,
					10000,
				})
			require.NoError(t, err)

			validBlobs := map[UniqueID][]*Blob{
				100: blobs,
			}

			tests := []struct {
				isvalid bool

				dBlobs map[UniqueID][]*Blob

				description string
			}{
				{false, invalidBlobs, "invalid dBlobs"},
				{true, validBlobs, "valid blobs"},
			}

			for _, test := range tests {
				t.Run(test.description, func(t *testing.T) {
					pk2ts, err := MergeDeltalogs(test.dBlobs)
					if test.isvalid {
						assert.NoError(t, err)
						assert.Equal(t, 5, len(pk2ts))
						assert.EqualValues(t, 20001, pk2ts[UniqueID(2)])
					} else {
						assert.Error(t, err)
						assert.Nil(t, pk2ts)
					}
				})
			}
		})

		t.Run("Multiple segments", func(t *testing.T) {
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

				expectedpk2ts int
				description   string
			}{
				{
					0, nil, nil,
					100,
					[]UniqueID{1, 2, 3},
					[]Timestamp{20000, 30000, 20005},
					200,
					[]UniqueID{4, 5, 6},
					[]Timestamp{50000, 50001, 50002},
					6, "2 segments",
				},
				{
					300,
					[]UniqueID{10, 20},
					[]Timestamp{20001, 40001},
					100,
					[]UniqueID{1, 2, 3},
					[]Timestamp{20000, 30000, 20005},
					200,
					[]UniqueID{4, 5, 6},
					[]Timestamp{50000, 50001, 50002},
					8, "3 segments",
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

					pk2ts, err := MergeDeltalogs(dBlobs)
					assert.NoError(t, err)
					assert.Equal(t, test.expectedpk2ts, len(pk2ts))
				})
			}
		})
	})

	t.Run("Test merge", func(t *testing.T) {
		collectionID := int64(1)
		meta := NewMetaFactory().GetCollectionMeta(collectionID, "test", schemapb.DataType_Int64)

		metaCache := metacache.NewMockMetaCache(t)
		metaCache.EXPECT().Schema().Return(meta.GetSchema()).Maybe()
		metaCache.EXPECT().GetSegmentByID(mock.Anything).RunAndReturn(func(id int64, filters ...metacache.SegmentFilter) (*metacache.SegmentInfo, bool) {
			segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
				CollectionID: 1,
				PartitionID:  0,
				ID:           id,
				NumOfRows:    10,
			}, nil)
			return segment, true
		})

		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
		alloc.EXPECT().AllocOne().Return(0, nil)
		t.Run("Merge without expiration", func(t *testing.T) {
			mockbIO := io.NewBinlogIO(cm, getOrCreateIOPool())
			paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0")
			iData := genInsertDataWithExpiredTS()
			iCodec := storage.NewInsertCodecWithSchema(meta)
			var partId int64 = 0
			var segmentId int64 = 1
			blobs, err := iCodec.Serialize(partId, 0, iData)
			assert.NoError(t, err)
			var allPaths [][]string
			inpath, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), partId, segmentId, blobs)
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
				metaCache: metaCache,
				binlogIO:  mockbIO,
				Allocator: alloc,
				done:      make(chan struct{}, 1),
				plan: &datapb.CompactionPlan{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1},
					},
				},
			}
			inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
			assert.NoError(t, err)
			assert.Equal(t, int64(2), numOfRow)
			assert.Equal(t, 1, len(inPaths[0].GetBinlogs()))
			assert.Equal(t, 1, len(statsPaths))
			assert.NotEqual(t, -1, inPaths[0].GetBinlogs()[0].GetTimestampFrom())
			assert.NotEqual(t, -1, inPaths[0].GetBinlogs()[0].GetTimestampTo())
		})
		t.Run("Merge without expiration2", func(t *testing.T) {
			mockbIO := io.NewBinlogIO(cm, getOrCreateIOPool())
			iData := genInsertDataWithExpiredTS()
			iCodec := storage.NewInsertCodecWithSchema(meta)
			var partId int64 = 0
			var segmentId int64 = 1
			blobs, err := iCodec.Serialize(partId, 0, iData)
			assert.NoError(t, err)
			paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0")
			BinLogMaxSize := Params.DataNodeCfg.BinLogMaxSize.GetValue()
			defer func() {
				Params.Save(Params.DataNodeCfg.BinLogMaxSize.Key, BinLogMaxSize)
			}()
			paramtable.Get().Save(Params.DataNodeCfg.BinLogMaxSize.Key, "64")
			meta := NewMetaFactory().GetCollectionMeta(1, "test", schemapb.DataType_Int64)

			var allPaths [][]string
			inpath, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), partId, segmentId, blobs)
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
				metaCache: metaCache,
				binlogIO:  mockbIO,
				Allocator: alloc,
				done:      make(chan struct{}, 1),
				plan: &datapb.CompactionPlan{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1},
					},
				},
			}
			inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
			assert.NoError(t, err)
			assert.Equal(t, int64(2), numOfRow)
			assert.Equal(t, 1, len(inPaths[0].GetBinlogs()))
			assert.Equal(t, 1, len(statsPaths))
			assert.Equal(t, 1, len(statsPaths[0].GetBinlogs()))
			assert.NotEqual(t, -1, inPaths[0].GetBinlogs()[0].GetTimestampFrom())
			assert.NotEqual(t, -1, inPaths[0].GetBinlogs()[0].GetTimestampTo())
		})
		// set Params.DataNodeCfg.BinLogMaxSize.Key = 1 to generate multi binlogs, each has only one row
		t.Run("merge_with_more_than_100rows", func(t *testing.T) {
			mockbIO := io.NewBinlogIO(cm, getOrCreateIOPool())
			iCodec := storage.NewInsertCodecWithSchema(meta)
			paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0")
			BinLogMaxSize := Params.DataNodeCfg.BinLogMaxSize.GetAsInt()
			defer func() {
				paramtable.Get().Save(Params.DataNodeCfg.BinLogMaxSize.Key, fmt.Sprintf("%d", BinLogMaxSize))
			}()
			paramtable.Get().Save(Params.DataNodeCfg.BinLogMaxSize.Key, "1")
			iData := genInsertData(101)
			var partId int64 = 0
			var segmentId int64 = 1
			blobs, err := iCodec.Serialize(partId, segmentId, iData)
			assert.NoError(t, err)

			var allPaths [][]string
			inpath, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), partId, segmentId, blobs)
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
				metaCache: metaCache,
				binlogIO:  mockbIO,
				Allocator: alloc,
				done:      make(chan struct{}, 1),
				plan: &datapb.CompactionPlan{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1},
					},
				},
			}
			inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
			assert.NoError(t, err)
			assert.Equal(t, int64(101), numOfRow)
			assert.Equal(t, 2, len(inPaths[0].GetBinlogs()))
			assert.Equal(t, 1, len(statsPaths))
			for _, inpath := range inPaths {
				assert.NotEqual(t, -1, inpath.GetBinlogs()[0].GetTimestampFrom())
				assert.NotEqual(t, -1, inpath.GetBinlogs()[0].GetTimestampTo())
			}
		})

		t.Run("Merge with expiration", func(t *testing.T) {
			mockbIO := io.NewBinlogIO(cm, getOrCreateIOPool())
			iCodec := storage.NewInsertCodecWithSchema(meta)
			iData := genInsertDataWithExpiredTS()
			var partId int64 = 0
			var segmentId int64 = 1
			blobs, err := iCodec.Serialize(partId, 0, iData)
			assert.NoError(t, err)
			meta := NewMetaFactory().GetCollectionMeta(1, "test", schemapb.DataType_Int64)

			var allPaths [][]string
			inpath, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), partId, segmentId, blobs)
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
				metaCache: metaCache,
				binlogIO:  mockbIO,
				Allocator: alloc,
				plan: &datapb.CompactionPlan{
					CollectionTtl: 864000,
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1},
					},
				},
				done: make(chan struct{}, 1),
			}
			inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
			assert.NoError(t, err)
			assert.Equal(t, int64(0), numOfRow)
			assert.Equal(t, 0, len(inPaths))
			assert.Equal(t, 0, len(statsPaths))
		})

		t.Run("merge_with_rownum_zero", func(t *testing.T) {
			mockbIO := io.NewBinlogIO(cm, getOrCreateIOPool())
			iData := genInsertDataWithExpiredTS()
			iCodec := storage.NewInsertCodecWithSchema(meta)
			var partId int64 = 0
			var segmentId int64 = 1
			blobs, err := iCodec.Serialize(partId, 0, iData)
			assert.NoError(t, err)
			meta := NewMetaFactory().GetCollectionMeta(1, "test", schemapb.DataType_Int64)
			metaCache := metacache.NewMockMetaCache(t)
			metaCache.EXPECT().Schema().Return(meta.GetSchema()).Maybe()
			metaCache.EXPECT().GetSegmentByID(mock.Anything).RunAndReturn(func(id int64, filters ...metacache.SegmentFilter) (*metacache.SegmentInfo, bool) {
				segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
					CollectionID: 1,
					PartitionID:  0,
					ID:           id,
					NumOfRows:    0,
				}, nil)
				return segment, true
			})

			var allPaths [][]string
			inpath, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), partId, segmentId, blobs)
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
				metaCache: metaCache,
				binlogIO:  mockbIO,
				Allocator: alloc,
				done:      make(chan struct{}, 1),
				plan: &datapb.CompactionPlan{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1},
					},
				},
			}
			_, _, _, err = ct.merge(context.Background(), allPaths, 2, 0, &etcdpb.CollectionMeta{
				Schema: meta.GetSchema(),
			}, dm)
			assert.Error(t, err)
		})

		t.Run("Merge with meta error", func(t *testing.T) {
			mockbIO := io.NewBinlogIO(cm, getOrCreateIOPool())
			iCodec := storage.NewInsertCodecWithSchema(meta)
			paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0")
			iData := genInsertDataWithExpiredTS()
			var partId int64 = 0
			var segmentId int64 = 1
			blobs, err := iCodec.Serialize(partId, 0, iData)
			assert.NoError(t, err)
			meta := NewMetaFactory().GetCollectionMeta(1, "test", schemapb.DataType_Int64)

			var allPaths [][]string
			inpath, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), partId, segmentId, blobs)
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
				metaCache: metaCache,
				binlogIO:  mockbIO,
				Allocator: alloc,
				done:      make(chan struct{}, 1),
				plan: &datapb.CompactionPlan{
					SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
						{SegmentID: 1},
					},
				},
			}
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
			mockbIO := io.NewBinlogIO(cm, getOrCreateIOPool())
			iCodec := storage.NewInsertCodecWithSchema(meta)
			paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0")
			iData := genInsertDataWithExpiredTS()
			var partId int64 = 0
			var segmentId int64 = 1
			blobs, err := iCodec.Serialize(partId, 0, iData)
			assert.NoError(t, err)
			meta := NewMetaFactory().GetCollectionMeta(1, "test", schemapb.DataType_Int64)

			var allPaths [][]string
			inpath, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), partId, segmentId, blobs)
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
				metaCache: metaCache,
				binlogIO:  mockbIO,
				Allocator: alloc,
				done:      make(chan struct{}, 1),
			}

			_, _, _, err = ct.merge(context.Background(), allPaths, 2, 0, &etcdpb.CollectionMeta{
				Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "bad_dim"},
					}},
				}},
			}, dm)
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
		metaCache := metacache.NewMockMetaCache(t)
		metaCache.EXPECT().GetSegmentByID(mock.Anything).Return(nil, false)
		ct := &compactionTask{
			metaCache: metaCache,
			plan: &datapb.CompactionPlan{
				SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
					{
						SegmentID: 1,
					},
				},
			},
			done: make(chan struct{}, 1),
		}

		_, err := ct.getNumRows()
		assert.Error(t, err, "segment not found")
	})
}

func getInt64DeltaBlobs(segID UniqueID, pks []UniqueID, tss []Timestamp) ([]*Blob, error) {
	primaryKeys := make([]storage.PrimaryKey, len(pks))
	for index, v := range pks {
		primaryKeys[index] = storage.NewInt64PrimaryKey(v)
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
	t.Run("Test compact with all segment empty", func(t *testing.T) {
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocOne().Call.Return(int64(11111), nil)
		ctx, cancel := context.WithCancel(context.TODO())

		mockSyncmgr := syncmgr.NewMockSyncManager(t)
		mockSyncmgr.EXPECT().Block(mock.Anything).Return()
		task := &compactionTask{
			ctx:       ctx,
			cancel:    cancel,
			Allocator: alloc,
			done:      make(chan struct{}, 1),
			tr:        timerecord.NewTimeRecorder("test"),
			syncMgr:   mockSyncmgr,
			plan: &datapb.CompactionPlan{
				PlanID:           999,
				SegmentBinlogs:   []*datapb.CompactionSegmentBinlogs{{SegmentID: 100}},
				TimeoutInSeconds: 10,
				Type:             datapb.CompactionType_MixCompaction,
			},
		}

		_, err := task.compact()
		assert.ErrorIs(t, errIllegalCompactionPlan, err)
	})

	t.Run("Test compact invalid empty segment binlogs", func(t *testing.T) {
		plan := &datapb.CompactionPlan{
			PlanID:           999,
			SegmentBinlogs:   nil,
			TimeoutInSeconds: 10,
			Type:             datapb.CompactionType_MixCompaction,
		}
		ctx, cancel := context.WithCancel(context.Background())
		emptyTask := &compactionTask{
			ctx:    ctx,
			cancel: cancel,
			tr:     timerecord.NewTimeRecorder("test"),

			done: make(chan struct{}, 1),
			plan: plan,
		}

		_, err := emptyTask.compact()
		assert.Error(t, err)
		assert.ErrorIs(t, err, errIllegalCompactionPlan)

		emptyTask.complete()
		emptyTask.stop()
	})

	t.Run("Test compact invalid AllocOnce failed", func(t *testing.T) {
		mockAlloc := allocator.NewMockAllocator(t)
		mockAlloc.EXPECT().AllocOne().Call.Return(int64(0), errors.New("mock allocone error")).Once()
		plan := &datapb.CompactionPlan{
			PlanID:           999,
			SegmentBinlogs:   notEmptySegmentBinlogs,
			TimeoutInSeconds: 10,
			Type:             datapb.CompactionType_MixCompaction,
		}
		task := &compactionTask{
			ctx:       context.Background(),
			tr:        timerecord.NewTimeRecorder("test"),
			Allocator: mockAlloc,
			plan:      plan,
		}

		_, err := task.compact()
		assert.Error(t, err)
	})

	t.Run("Test typeII compact valid", func(t *testing.T) {
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
		alloc.EXPECT().AllocOne().Call.Return(int64(19530), nil)
		type testCase struct {
			pkType schemapb.DataType
			iData1 storage.FieldData
			iData2 storage.FieldData
			pks1   [2]storage.PrimaryKey
			pks2   [2]storage.PrimaryKey
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
				pks1:   [2]storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(2)},
				pks2:   [2]storage.PrimaryKey{storage.NewInt64PrimaryKey(9), storage.NewInt64PrimaryKey(10)},
				colID:  1,
				parID:  10,
				segID1: 100,
				segID2: 101,
			},
			{
				pkType: schemapb.DataType_VarChar,
				iData1: &storage.StringFieldData{Data: []string{"aaaa"}},
				iData2: &storage.StringFieldData{Data: []string{"milvus"}},
				pks1:   [2]storage.PrimaryKey{storage.NewVarCharPrimaryKey("aaaa"), storage.NewVarCharPrimaryKey("bbbb")},
				pks2:   [2]storage.PrimaryKey{storage.NewVarCharPrimaryKey("milvus"), storage.NewVarCharPrimaryKey("mmmm")},
				colID:  2,
				parID:  11,
				segID1: 102,
				segID2: 103,
			},
		}

		for _, c := range cases {
			collName := "test_compact_coll_name"
			meta := NewMetaFactory().GetCollectionMeta(c.colID, collName, c.pkType)

			mockbIO := io.NewBinlogIO(cm, getOrCreateIOPool())
			iCodec := storage.NewInsertCodecWithSchema(meta)
			mockKv := memkv.NewMemoryKV()
			metaCache := metacache.NewMockMetaCache(t)
			metaCache.EXPECT().Collection().Return(c.colID)
			metaCache.EXPECT().Schema().Return(meta.GetSchema())
			syncMgr := syncmgr.NewMockSyncManager(t)
			syncMgr.EXPECT().Block(mock.Anything).Return()

			bfs := metacache.NewBloomFilterSet()
			bfs.UpdatePKRange(c.iData1)
			seg1 := metacache.NewSegmentInfo(&datapb.SegmentInfo{
				CollectionID: c.colID,
				PartitionID:  c.parID,
				ID:           c.segID1,
				NumOfRows:    2,
			}, bfs)
			bfs = metacache.NewBloomFilterSet()
			bfs.UpdatePKRange(c.iData2)
			seg2 := metacache.NewSegmentInfo(&datapb.SegmentInfo{
				CollectionID: c.colID,
				PartitionID:  c.parID,
				ID:           c.segID2,
				NumOfRows:    2,
			}, bfs)

			bfs = metacache.NewBloomFilterSet()
			seg3 := metacache.NewSegmentInfo(&datapb.SegmentInfo{
				CollectionID: c.colID,
				PartitionID:  c.parID,
				ID:           99999,
			}, bfs)

			metaCache.EXPECT().GetSegmentByID(c.segID1).Return(seg1, true)
			metaCache.EXPECT().GetSegmentByID(c.segID2).Return(seg2, true)
			metaCache.EXPECT().GetSegmentByID(seg3.SegmentID()).Return(seg3, true)
			metaCache.EXPECT().GetSegmentByID(mock.Anything).Return(nil, false)

			iData1 := genInsertDataWithPKs(c.pks1, c.pkType)
			iblobs1, err := iCodec.Serialize(c.parID, 0, iData1)
			assert.NoError(t, err)
			dData1 := &DeleteData{
				Pks:      []storage.PrimaryKey{c.pks1[0]},
				Tss:      []Timestamp{20000},
				RowCount: 1,
			}
			iData2 := genInsertDataWithPKs(c.pks2, c.pkType)
			iblobs2, err := iCodec.Serialize(c.parID, 3, iData2)
			assert.NoError(t, err)
			dData2 := &DeleteData{
				Pks:      []storage.PrimaryKey{c.pks2[0]},
				Tss:      []Timestamp{30000},
				RowCount: 1,
			}

			stats1, err := storage.NewPrimaryKeyStats(1, int64(c.pkType), 1)
			require.NoError(t, err)
			iPaths1, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), c.parID, c.segID1, iblobs1)
			require.NoError(t, err)
			sPaths1, err := uploadStatsLog(context.Background(), mockbIO, alloc, meta.GetID(), c.parID, c.segID1, stats1, 2, iCodec)
			require.NoError(t, err)
			dPaths1, err := uploadDeltaLog(context.TODO(), mockbIO, alloc, meta.GetID(), c.parID, c.segID1, dData1)
			require.NoError(t, err)
			require.Equal(t, 12, len(iPaths1))

			stats2, err := storage.NewPrimaryKeyStats(1, int64(c.pkType), 1)
			require.NoError(t, err)
			iPaths2, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), c.parID, c.segID2, iblobs2)
			require.NoError(t, err)
			sPaths2, err := uploadStatsLog(context.Background(), mockbIO, alloc, meta.GetID(), c.parID, c.segID2, stats2, 2, iCodec)
			require.NoError(t, err)
			dPaths2, err := uploadDeltaLog(context.TODO(), mockbIO, alloc, meta.GetID(), c.parID, c.segID2, dData2)
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
					{
						SegmentID: seg3.SegmentID(), // empty segment
					},
				},
				StartTime:        0,
				TimeoutInSeconds: 10,
				Type:             datapb.CompactionType_MergeCompaction,
				Channel:          "channelname",
			}

			task := newCompactionTask(context.TODO(), mockbIO, metaCache, syncMgr, alloc, plan)
			result, err := task.compact()
			assert.NoError(t, err)
			assert.NotNil(t, result)

			assert.Equal(t, plan.GetPlanID(), result.GetPlanID())
			assert.Equal(t, 1, len(result.GetSegments()))

			segment := result.GetSegments()[0]
			assert.EqualValues(t, 19530, segment.GetSegmentID())
			assert.EqualValues(t, 2, segment.GetNumOfRows())
			assert.NotEmpty(t, segment.InsertLogs)
			assert.NotEmpty(t, segment.Field2StatslogPaths)

			// New test, remove all the binlogs in memkv
			err = mockKv.RemoveWithPrefix("/")
			require.NoError(t, err)
			plan.PlanID++

			result, err = task.compact()
			assert.NoError(t, err)
			assert.NotNil(t, result)

			assert.Equal(t, plan.GetPlanID(), result.GetPlanID())
			assert.Equal(t, 1, len(result.GetSegments()))

			segment = result.GetSegments()[0]
			assert.EqualValues(t, 19530, segment.GetSegmentID())
			assert.EqualValues(t, 2, segment.GetNumOfRows())
			assert.NotEmpty(t, segment.InsertLogs)
			assert.NotEmpty(t, segment.Field2StatslogPaths)
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

		meta := NewMetaFactory().GetCollectionMeta(collID, "test_compact_coll_name", schemapb.DataType_Int64)

		mockbIO := io.NewBinlogIO(cm, getOrCreateIOPool())
		iCodec := storage.NewInsertCodecWithSchema(meta)

		metaCache := metacache.NewMockMetaCache(t)
		metaCache.EXPECT().Collection().Return(collID)
		metaCache.EXPECT().Schema().Return(meta.GetSchema())
		syncMgr := syncmgr.NewMockSyncManager(t)
		syncMgr.EXPECT().Block(mock.Anything).Return()

		bfs := metacache.NewBloomFilterSet()
		bfs.UpdatePKRange(&storage.Int64FieldData{Data: []UniqueID{1}})
		seg1 := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			CollectionID: collID,
			PartitionID:  partID,
			ID:           segID1,
			NumOfRows:    2,
		}, bfs)
		bfs = metacache.NewBloomFilterSet()
		bfs.UpdatePKRange(&storage.Int64FieldData{Data: []UniqueID{1}})
		seg2 := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			CollectionID: collID,
			PartitionID:  partID,
			ID:           segID2,
			NumOfRows:    2,
		}, bfs)

		metaCache.EXPECT().GetSegmentByID(mock.Anything).RunAndReturn(func(id int64, filters ...metacache.SegmentFilter) (*metacache.SegmentInfo, bool) {
			switch id {
			case segID1:
				return seg1, true
			case segID2:
				return seg2, true
			default:
				return nil, false
			}
		})

		// the same pk for segmentI and segmentII
		pks := [2]storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(2)}
		iData1 := genInsertDataWithPKs(pks, schemapb.DataType_Int64)
		iblobs1, err := iCodec.Serialize(partID, 0, iData1)
		assert.NoError(t, err)
		iData2 := genInsertDataWithPKs(pks, schemapb.DataType_Int64)
		iblobs2, err := iCodec.Serialize(partID, 1, iData2)
		assert.NoError(t, err)

		pk1 := storage.NewInt64PrimaryKey(1)
		dData1 := &DeleteData{
			Pks:      []storage.PrimaryKey{pk1},
			Tss:      []Timestamp{20000},
			RowCount: 1,
		}
		// empty dData2
		dData2 := &DeleteData{
			Pks:      []storage.PrimaryKey{},
			Tss:      []Timestamp{},
			RowCount: 0,
		}

		stats1, err := storage.NewPrimaryKeyStats(1, int64(schemapb.DataType_Int64), 1)
		require.NoError(t, err)
		iPaths1, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), partID, segID1, iblobs1)
		require.NoError(t, err)
		sPaths1, err := uploadStatsLog(context.Background(), mockbIO, alloc, meta.GetID(), partID, segID1, stats1, 1, iCodec)
		require.NoError(t, err)
		dPaths1, err := uploadDeltaLog(context.TODO(), mockbIO, alloc, meta.GetID(), partID, segID1, dData1)
		require.NoError(t, err)
		require.Equal(t, 12, len(iPaths1))

		stats2, err := storage.NewPrimaryKeyStats(1, int64(schemapb.DataType_Int64), 1)
		require.NoError(t, err)
		iPaths2, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), partID, segID2, iblobs2)
		require.NoError(t, err)
		sPaths2, err := uploadStatsLog(context.Background(), mockbIO, alloc, meta.GetID(), partID, segID2, stats2, 1, iCodec)
		require.NoError(t, err)
		dPaths2, err := uploadDeltaLog(context.TODO(), mockbIO, alloc, meta.GetID(), partID, segID2, dData2)
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
			Channel:          "channelname",
		}

		task := newCompactionTask(context.TODO(), mockbIO, metaCache, syncMgr, alloc, plan)
		result, err := task.compact()
		assert.NoError(t, err)
		assert.NotNil(t, result)

		assert.Equal(t, plan.GetPlanID(), result.GetPlanID())
		assert.Equal(t, 1, len(result.GetSegments()))

		segment := result.GetSegments()[0]
		assert.EqualValues(t, 19530, segment.GetSegmentID())
		assert.EqualValues(t, 2, segment.GetNumOfRows())
		assert.NotEmpty(t, segment.InsertLogs)
		assert.NotEmpty(t, segment.Field2StatslogPaths)
	})
}

func TestInjectDone(t *testing.T) {
	syncMgr := syncmgr.NewMockSyncManager(t)

	segmentIDs := []int64{100, 200, 300}
	task := &compactionTask{
		plan: &datapb.CompactionPlan{
			SegmentBinlogs: lo.Map(segmentIDs, func(id int64, _ int) *datapb.CompactionSegmentBinlogs {
				return &datapb.CompactionSegmentBinlogs{SegmentID: id}
			}),
		},
		syncMgr: syncMgr,
	}

	for _, segmentID := range segmentIDs {
		syncMgr.EXPECT().Unblock(segmentID).Return().Once()
	}

	task.injectDone()
	task.injectDone()
}

func BenchmarkCompaction(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cm := storage.NewLocalChunkManager(storage.RootPath(compactTestDir))
	defer cm.RemoveWithPrefix(ctx, cm.RootPath())

	collectionID := int64(1)
	meta := NewMetaFactory().GetCollectionMeta(collectionID, "test", schemapb.DataType_Int64)
	mockbIO := io.NewBinlogIO(cm, getOrCreateIOPool())
	paramtable.Get().Save(Params.CommonCfg.EntityExpirationTTL.Key, "0")
	iData := genInsertDataWithExpiredTS()
	iCodec := storage.NewInsertCodecWithSchema(meta)
	var partId int64 = 0
	var segmentId int64 = 1
	blobs, err := iCodec.Serialize(partId, 0, iData)
	assert.NoError(b, err)
	var allPaths [][]string
	alloc := allocator.NewMockAllocator(b)
	alloc.EXPECT().GetGenerator(mock.Anything, mock.Anything).Call.Return(validGeneratorFn, nil)
	alloc.EXPECT().AllocOne().Call.Return(int64(19530), nil)
	inpath, err := uploadInsertLog(context.Background(), mockbIO, alloc, meta.GetID(), partId, segmentId, blobs)
	assert.NoError(b, err)
	assert.Equal(b, 12, len(inpath))
	binlogNum := len(inpath[0].GetBinlogs())
	assert.Equal(b, 1, binlogNum)

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

	metaCache := metacache.NewMockMetaCache(b)
	metaCache.EXPECT().Schema().Return(meta.GetSchema()).Maybe()
	metaCache.EXPECT().GetSegmentByID(mock.Anything).RunAndReturn(func(id int64, filters ...metacache.SegmentFilter) (*metacache.SegmentInfo, bool) {
		segment := metacache.NewSegmentInfo(&datapb.SegmentInfo{
			CollectionID: 1,
			PartitionID:  0,
			ID:           id,
			NumOfRows:    10,
		}, nil)
		return segment, true
	})

	ct := &compactionTask{
		metaCache: metaCache,
		binlogIO:  mockbIO,
		Allocator: alloc,
		done:      make(chan struct{}, 1),
		plan: &datapb.CompactionPlan{
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{
				{SegmentID: 1},
			},
		},
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		inPaths, statsPaths, numOfRow, err := ct.merge(context.Background(), allPaths, 2, 0, meta, dm)
		assert.NoError(b, err)
		assert.Equal(b, int64(2), numOfRow)
		assert.Equal(b, 1, len(inPaths[0].GetBinlogs()))
		assert.Equal(b, 1, len(statsPaths))
		assert.NotEqual(b, -1, inPaths[0].GetBinlogs()[0].GetTimestampFrom())
		assert.NotEqual(b, -1, inPaths[0].GetBinlogs()[0].GetTimestampTo())
	}
}
