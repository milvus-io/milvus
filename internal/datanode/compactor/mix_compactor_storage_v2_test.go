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

package compactor

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestMixCompactionTaskStorageV2Suite(t *testing.T) {
	suite.Run(t, new(MixCompactionTaskStorageV2Suite))
}

type MixCompactionTaskStorageV2Suite struct {
	MixCompactionTaskSuite
}

func (s *MixCompactionTaskStorageV2Suite) SetupTest() {
	s.setupTest()
	paramtable.Get().Save("common.storageType", "local")
	paramtable.Get().Save("common.storage.enableV2", "true")
	initcore.InitStorageV2FileSystem(paramtable.Get())
}

func (s *MixCompactionTaskStorageV2Suite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EntityExpirationTTL.Key)
	paramtable.Get().Reset("common.storageType")
	paramtable.Get().Reset("common.storage.enableV2")
	os.RemoveAll(paramtable.Get().LocalStorageCfg.Path.GetValue() + "insert_log")
	os.RemoveAll(paramtable.Get().LocalStorageCfg.Path.GetValue() + "delta_log")
	os.RemoveAll(paramtable.Get().LocalStorageCfg.Path.GetValue() + "stats_log")
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactDupPK() {
	s.prepareCompactDupPKSegments()
	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	s.Equal(1, len(result.GetSegments()[0].GetInsertLogs()))
	s.Equal(1, len(result.GetSegments()[0].GetField2StatslogPaths()))
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactDupPK_MixToV2Format() {
	segments := []int64{7, 8, 9}
	dblobs, err := getInt64DeltaBlobs(
		1,
		[]int64{100},
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second), 0)},
	)
	s.Require().NoError(err)

	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"1"}).
		Return([][]byte{dblobs.GetValue()}, nil).Times(len(segments))
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)

	// Clear original segments
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
	for _, segID := range segments {
		s.initSegBuffer(1, segID)
		kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
		s.Require().NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
			left, right := lo.Difference(keys, lo.Keys(kvs))
			return len(left) == 0 && len(right) == 0
		})).Return(lo.Values(kvs), nil).Once()

		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:    segID,
			FieldBinlogs: lo.Values(fBinlogs),
			Deltalogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "1"}}},
			},
		})
	}

	v2Segments := []int64{10, 11}
	for _, segID := range v2Segments {
		binlogs, _, _, _, _, err := s.initStorageV2Segments(1, segID, alloc)
		s.NoError(err)
		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:      segID,
			FieldBinlogs:   lo.Values(binlogs),
			Deltalogs:      []*datapb.FieldBinlog{},
			StorageVersion: storage.StorageV2,
		})
	}

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(5, segment.GetNumOfRows())
	s.NotEmpty(segment.InsertLogs)
	s.NotEmpty(segment.Field2StatslogPaths)
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactDupPK_V2ToV2Format() {
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)

	// Clear original segments
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)

	v2Segments := []int64{10, 11}
	for _, segID := range v2Segments {
		binlogs, _, _, _, _, err := s.initStorageV2Segments(1, segID, alloc)
		s.NoError(err)
		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:      segID,
			FieldBinlogs:   lo.Values(binlogs),
			Deltalogs:      []*datapb.FieldBinlog{},
			StorageVersion: storage.StorageV2,
		})
	}

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(2, segment.GetNumOfRows())
	s.NotEmpty(segment.InsertLogs)
	s.NotEmpty(segment.Field2StatslogPaths)
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactDupPK_V2ToV1Format() {
	paramtable.Get().Save("common.storage.enableV2", "false")
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)

	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)

	v2Segments := []int64{10, 11}
	for _, segID := range v2Segments {
		binlogs, _, _, _, _, err := s.initStorageV2Segments(1, segID, alloc)
		s.NoError(err)
		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:      segID,
			FieldBinlogs:   lo.Values(binlogs),
			Deltalogs:      []*datapb.FieldBinlog{},
			StorageVersion: storage.StorageV2,
		})
	}

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(2, segment.GetNumOfRows())
	// each field has only one insert log for storage v1
	s.EqualValues(len(s.task.plan.Schema.Fields), len(segment.GetInsertLogs()))
	s.NotEmpty(segment.Field2StatslogPaths)
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactTwoToOne() {
	s.prepareCompactTwoToOneSegments()
	result, err := s.task.Compact()
	s.NoError(err)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	s.Equal(1, len(result.GetSegments()[0].GetInsertLogs()))
	s.Equal(1, len(result.GetSegments()[0].GetField2StatslogPaths()))
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactTwoToOneWithBM25() {
	s.prepareCompactTwoToOneWithBM25Segments()
	result, err := s.task.Compact()
	s.NoError(err)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(3, segment.GetNumOfRows())
	s.Empty(segment.Deltalogs)
	s.Equal(1, len(segment.InsertLogs))
	s.Equal(1, len(segment.Bm25Logs))
	s.Equal(1, len(segment.Field2StatslogPaths))
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactSortedSegment() {
	s.prepareCompactSortedSegment()
	paramtable.Get().Save("dataNode.compaction.useMergeSort", "true")
	defer paramtable.Get().Reset("dataNode.compaction.useMergeSort")

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	s.True(result.GetSegments()[0].GetIsSorted())
	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(291, segment.GetNumOfRows())
	s.EqualValues(1, len(segment.InsertLogs))
	s.EqualValues(1, len(segment.Field2StatslogPaths))
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskStorageV2Suite) TestMergeNoExpiration_V1ToV2Format() {
	s.TestMergeNoExpiration()
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactSortedSegmentLackBinlog() {
	s.prepareCompactSortedSegmentLackBinlog()
	paramtable.Get().Save("dataNode.compaction.useMergeSort", "true")
	defer paramtable.Get().Reset("dataNode.compaction.useMergeSort")

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	s.True(result.GetSegments()[0].GetIsSorted())

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(291, segment.GetNumOfRows())
	s.EqualValues(1, len(segment.InsertLogs))
	s.EqualValues(1, len(segment.Field2StatslogPaths))
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskStorageV2Suite) TestSplitMergeEntityExpired_V1ToV2Format() {
	s.TestSplitMergeEntityExpired()
}

func (s *MixCompactionTaskStorageV2Suite) TestMergeNoExpirationLackBinlog_V1ToV2Format() {
	s.TestMergeNoExpirationLackBinlog()
}

func (s *MixCompactionTaskStorageV2Suite) initStorageV2Segments(rows int, seed int64, alloc allocator.Interface) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	size int64,
	err error,
) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
	metacache.UpdateNumOfRows(1000)(seg)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Collection().Return(CollectionID).Maybe()
	mc.EXPECT().Schema().Return(s.meta.Schema).Maybe()
	mc.EXPECT().GetSegmentByID(seed).Return(seg, true).Maybe()
	mc.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg}).Maybe()
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return().Maybe()

	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", CollectionID)
	pack := new(syncmgr.SyncPack).WithCollectionID(CollectionID).WithPartitionID(PartitionID).WithSegmentID(seed).WithChannelName(channelName).WithInsertData(getInsertData(rows, seed, s.meta.GetSchema()))
	bw := syncmgr.NewBulkPackWriterV2(mc, cm, alloc, packed.DefaultWriteBufferSize, 0)
	return bw.Write(context.Background(), pack)
}

func getRowWithoutNil(magic int64) map[int64]interface{} {
	ts := tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)
	return map[int64]interface{}{
		common.RowIDField:      magic,
		common.TimeStampField:  int64(ts), // should be int64 here
		BoolField:              true,
		Int8Field:              int8(magic),
		Int16Field:             int16(magic),
		Int32Field:             int32(magic),
		Int64Field:             magic,
		FloatField:             float32(magic),
		DoubleField:            float64(magic),
		StringField:            "str",
		VarCharField:           "varchar",
		BinaryVectorField:      []byte{0},
		FloatVectorField:       []float32{4, 5, 6, 7},
		Float16VectorField:     []byte{0, 0, 0, 0, 255, 255, 255, 255},
		BFloat16VectorField:    []byte{0, 0, 0, 0, 255, 255, 255, 255},
		SparseFloatVectorField: typeutil.CreateSparseFloatRow([]uint32{0, 1, 2}, []float32{4, 5, 6}),
		ArrayField: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
			},
		},
		JSONField:                    []byte(`{"batch":ok}`),
		BoolFieldWithDefaultValue:    true,
		Int8FieldWithDefaultValue:    int8(magic),
		Int16FieldWithDefaultValue:   int16(magic),
		Int32FieldWithDefaultValue:   int32(magic),
		Int64FieldWithDefaultValue:   magic,
		FloatFieldWithDefaultValue:   float32(magic),
		DoubleFieldWithDefaultValue:  float64(magic),
		StringFieldWithDefaultValue:  "str",
		VarCharFieldWithDefaultValue: "varchar",
	}
}

func getInsertData(size int, seed int64, schema *schemapb.CollectionSchema) []*storage.InsertData {
	buf, _ := storage.NewInsertData(schema)
	for i := 0; i < size; i++ {
		buf.Append(getRowWithoutNil(seed))
	}
	return []*storage.InsertData{buf}
}
