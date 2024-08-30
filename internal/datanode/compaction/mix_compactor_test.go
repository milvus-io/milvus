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

package compaction

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var compactTestDir = "/tmp/milvus_test/compact"

func TestMixCompactionTaskSuite(t *testing.T) {
	suite.Run(t, new(MixCompactionTaskSuite))
}

type MixCompactionTaskSuite struct {
	suite.Suite

	mockBinlogIO *io.MockBinlogIO

	meta      *etcdpb.CollectionMeta
	segWriter *SegmentWriter

	task *mixCompactionTask
	plan *datapb.CompactionPlan
}

func (s *MixCompactionTaskSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (s *MixCompactionTaskSuite) SetupTest() {
	s.mockBinlogIO = io.NewMockBinlogIO(s.T())

	s.meta = genTestCollectionMeta()

	paramtable.Get().Save(paramtable.Get().CommonCfg.EntityExpirationTTL.Key, "0")

	s.plan = &datapb.CompactionPlan{
		PlanID: 999,
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{
			SegmentID:           100,
			FieldBinlogs:        nil,
			Field2StatslogPaths: nil,
			Deltalogs:           nil,
		}},
		TimeoutInSeconds:     10,
		Type:                 datapb.CompactionType_MixCompaction,
		Schema:               s.meta.GetSchema(),
		BeginLogID:           19530,
		PreAllocatedSegments: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		MaxSize:              64 * 1024 * 1024,
	}

	s.task = NewMixCompactionTask(context.Background(), s.mockBinlogIO, s.plan)
	s.task.plan = s.plan
}

func (s *MixCompactionTaskSuite) SetupSubTest() {
	s.SetupTest()
}

func (s *MixCompactionTaskSuite) TearDownTest() {
	paramtable.Get().Reset(paramtable.Get().CommonCfg.EntityExpirationTTL.Key)
}

func getMilvusBirthday() time.Time {
	return time.Date(2019, time.Month(5), 30, 0, 0, 0, 0, time.UTC)
}

func (s *MixCompactionTaskSuite) TestCompactDupPK() {
	// Test merge compactions, two segments with the same pk, one deletion pk=1
	// The merged segment 19530 should remain 3 pk without pk=100
	segments := []int64{7, 8, 9}
	dblobs, err := getInt64DeltaBlobs(
		1,
		[]int64{100},
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second), 0)},
	)
	s.Require().NoError(err)

	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"1"}).
		Return([][]byte{dblobs.GetValue()}, nil).Times(3)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)

	// clear origial segments
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
	for _, segID := range segments {
		s.initSegBuffer(segID)
		row := getRow(100)
		v := &storage.Value{
			PK:        storage.NewInt64PrimaryKey(100),
			Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
			Value:     row,
		}
		err := s.segWriter.Write(v)
		s.segWriter.writer.Flush()
		s.Require().NoError(err)

		kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
		s.Require().NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
			left, right := lo.Difference(keys, lo.Keys(kvs))
			return len(left) == 0 && len(right) == 0
		})).Return(lo.Values(kvs), nil).Once()

		s.plan.SegmentBinlogs = append(s.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:    segID,
			FieldBinlogs: lo.Values(fBinlogs),
			Deltalogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "1"}}},
			},
		})
	}
	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(3, segment.GetNumOfRows())
	s.NotEmpty(segment.InsertLogs)
	s.NotEmpty(segment.Field2StatslogPaths)
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskSuite) TestCompactTwoToOne() {
	segments := []int64{5, 6, 7}
	alloc := allocator.NewLocalAllocator(7777777, math.MaxInt64)
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
	for _, segID := range segments {
		s.initSegBuffer(segID)
		kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
		s.Require().NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
			left, right := lo.Difference(keys, lo.Keys(kvs))
			return len(left) == 0 && len(right) == 0
		})).Return(lo.Values(kvs), nil).Once()

		s.plan.SegmentBinlogs = append(s.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			SegmentID:    segID,
			FieldBinlogs: lo.Values(fBinlogs),
		})
	}

	// append an empty segment
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{
		CollectionID: CollectionID,
		PartitionID:  PartitionID,
		ID:           99999,
		NumOfRows:    0,
	}, pkoracle.NewBloomFilterSet())

	s.plan.SegmentBinlogs = append(s.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
		SegmentID: seg.SegmentID(),
	})

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(3, segment.GetNumOfRows())
	s.NotEmpty(segment.InsertLogs)
	s.NotEmpty(segment.Field2StatslogPaths)
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskSuite) TestSplitMergeEntityExpired() {
	s.initSegBuffer(3)
	collTTL := 864000 // 10 days
	currTs := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second*(time.Duration(collTTL)+1)), 0)
	s.task.currentTs = currTs
	s.task.plan.CollectionTtl = int64(collTTL)
	alloc := allocator.NewLocalAllocator(888888, math.MaxInt64)

	kvs, _, err := serializeWrite(context.TODO(), alloc, s.segWriter)
	s.Require().NoError(err)
	s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, paths []string) ([][]byte, error) {
			s.Require().Equal(len(paths), len(kvs))
			return lo.Values(kvs), nil
		})
	s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

	s.task.collectionID = CollectionID
	s.task.partitionID = PartitionID
	s.task.maxRows = 1000

	compactionSegments, err := s.task.mergeSplit(s.task.ctx, [][]string{lo.Keys(kvs)}, nil)
	s.NoError(err)
	s.Equal(0, len(compactionSegments))
}

func (s *MixCompactionTaskSuite) TestMergeNoExpiration() {
	s.initSegBuffer(4)
	deleteTs := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(10*time.Second), 0)
	tests := []struct {
		description string
		deletions   map[interface{}]uint64
		expectedRes int
	}{
		{"no deletion", nil, 1},
		{"mismatch deletion", map[interface{}]uint64{int64(1): deleteTs}, 1},
		{"deleted pk=4", map[interface{}]uint64{int64(4): deleteTs}, 0},
	}

	alloc := allocator.NewLocalAllocator(888888, math.MaxInt64)
	kvs, _, err := serializeWrite(context.TODO(), alloc, s.segWriter)
	s.Require().NoError(err)
	for _, test := range tests {
		s.Run(test.description, func() {
			s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).RunAndReturn(
				func(ctx context.Context, paths []string) ([][]byte, error) {
					s.Require().Equal(len(paths), len(kvs))
					return lo.Values(kvs), nil
				})
			s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()

			s.task.collectionID = CollectionID
			s.task.partitionID = PartitionID
			s.task.maxRows = 1000
			res, err := s.task.mergeSplit(s.task.ctx, [][]string{lo.Keys(kvs)}, test.deletions)
			s.NoError(err)
			s.EqualValues(test.expectedRes, len(res))
			if test.expectedRes > 0 {
				s.EqualValues(1, res[0].GetNumOfRows())
			}
		})
	}
}

func (s *MixCompactionTaskSuite) TestMergeDeltalogsMultiSegment() {
	tests := []struct {
		segIDA  int64
		dataApk []int64
		dataAts []uint64

		segIDB  int64
		dataBpk []int64
		dataBts []uint64

		segIDC  int64
		dataCpk []int64
		dataCts []uint64

		expectedpk2ts map[int64]uint64
		description   string
	}{
		{
			0, nil, nil,
			100,
			[]int64{1, 2, 3},
			[]uint64{20000, 30000, 20005},
			200,
			[]int64{4, 5, 6},
			[]uint64{50000, 50001, 50002},
			map[int64]uint64{
				1: 20000,
				2: 30000,
				3: 20005,
				4: 50000,
				5: 50001,
				6: 50002,
			},
			"2 segments",
		},
		{
			300,
			[]int64{10, 20},
			[]uint64{20001, 40001},
			100,
			[]int64{1, 2, 3},
			[]uint64{20000, 30000, 20005},
			200,
			[]int64{4, 5, 6},
			[]uint64{50000, 50001, 50002},
			map[int64]uint64{
				10: 20001,
				20: 40001,
				1:  20000,
				2:  30000,
				3:  20005,
				4:  50000,
				5:  50001,
				6:  50002,
			},
			"3 segments",
		},
	}

	for _, test := range tests {
		s.Run(test.description, func() {
			dValues := make([][]byte, 0)
			if test.dataApk != nil {
				d, err := getInt64DeltaBlobs(test.segIDA, test.dataApk, test.dataAts)
				s.Require().NoError(err)
				dValues = append(dValues, d.GetValue())
			}
			if test.dataBpk != nil {
				d, err := getInt64DeltaBlobs(test.segIDB, test.dataBpk, test.dataBts)
				s.Require().NoError(err)
				dValues = append(dValues, d.GetValue())
			}
			if test.dataCpk != nil {
				d, err := getInt64DeltaBlobs(test.segIDC, test.dataCpk, test.dataCts)
				s.Require().NoError(err)
				dValues = append(dValues, d.GetValue())
			}

			s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.Anything).
				Return(dValues, nil)

			got, err := mergeDeltalogs(s.task.ctx, s.task.binlogIO, map[int64][]string{100: {"random"}})
			s.NoError(err)

			s.Equal(len(test.expectedpk2ts), len(got))
			gotKeys := lo.Map(lo.Keys(got), func(k interface{}, _ int) int64 {
				res, ok := k.(int64)
				s.Require().True(ok)
				return res
			})
			s.ElementsMatch(gotKeys, lo.Keys(test.expectedpk2ts))
			s.ElementsMatch(lo.Values(got), lo.Values(test.expectedpk2ts))
		})
	}
}

func (s *MixCompactionTaskSuite) TestMergeDeltalogsOneSegment() {
	blob, err := getInt64DeltaBlobs(
		100,
		[]int64{1, 2, 3, 4, 5, 1, 2},
		[]uint64{20000, 20001, 20002, 30000, 50000, 50000, 10000},
	)
	s.Require().NoError(err)

	expectedMap := map[int64]uint64{1: 50000, 2: 20001, 3: 20002, 4: 30000, 5: 50000}

	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"a"}).
		Return([][]byte{blob.GetValue()}, nil).Once()
	s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{"mock_error"}).
		Return(nil, errors.New("mock_error")).Once()

	invalidPaths := map[int64][]string{2000: {"mock_error"}}
	got, err := mergeDeltalogs(s.task.ctx, s.task.binlogIO, invalidPaths)
	s.Error(err)
	s.Nil(got)

	dpaths := map[int64][]string{1000: {"a"}}
	got, err = mergeDeltalogs(s.task.ctx, s.task.binlogIO, dpaths)
	s.NoError(err)
	s.NotNil(got)
	s.Equal(len(expectedMap), len(got))

	gotKeys := lo.Map(lo.Keys(got), func(k interface{}, _ int) int64 {
		res, ok := k.(int64)
		s.Require().True(ok)
		return res
	})
	s.ElementsMatch(gotKeys, lo.Keys(expectedMap))
	s.ElementsMatch(lo.Values(got), lo.Values(expectedMap))
}

func (s *MixCompactionTaskSuite) TestCompactFail() {
	s.Run("mock ctx done", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		s.task.ctx = ctx
		s.task.cancel = cancel
		_, err := s.task.Compact()
		s.Error(err)
		s.ErrorIs(err, context.Canceled)
	})

	s.Run("Test compact invalid empty segment binlogs", func() {
		s.plan.SegmentBinlogs = nil

		_, err := s.task.Compact()
		s.Error(err)
	})

	s.Run("Test compact failed maxSize zero", func() {
		s.plan.MaxSize = 0
		_, err := s.task.Compact()
		s.Error(err)
	})
}

func (s *MixCompactionTaskSuite) TestIsExpiredEntity() {
	milvusBirthdayTs := tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)

	tests := []struct {
		description string
		collTTL     int64
		nowTs       uint64
		entityTs    uint64

		expect bool
	}{
		{"ttl=maxInt64, nowTs-entityTs=ttl", math.MaxInt64, math.MaxInt64, 0, true},
		{"ttl=maxInt64, nowTs-entityTs < 0", math.MaxInt64, milvusBirthdayTs, 0, false},
		{"ttl=maxInt64, 0<nowTs-entityTs<ttl", math.MaxInt64, 0, milvusBirthdayTs, false},
		{"ttl=maxInt64, nowTs-entityTs>ttl v2", math.MaxInt64, math.MaxInt64, milvusBirthdayTs, true},
		// entityTs==currTs will never happen
		// {"ttl=maxInt64, curTs-entityTs=0", math.MaxInt64, milvusBirthdayTs, milvusBirthdayTs, true},
		{"ttl=0, nowTs>entityTs", 0, milvusBirthdayTs + 1, milvusBirthdayTs, false},
		{"ttl=0, nowTs==entityTs", 0, milvusBirthdayTs, milvusBirthdayTs, false},
		{"ttl=0, nowTs<entityTs", 0, milvusBirthdayTs, milvusBirthdayTs + 1, false},
		{"ttl=10days, nowTs-entityTs>10days", 864000, milvusBirthdayTs + 864001, milvusBirthdayTs, true},
		{"ttl=10days, nowTs-entityTs==10days", 864000, milvusBirthdayTs + 864000, milvusBirthdayTs, true},
		{"ttl=10days, nowTs-entityTs<10days", 864000, milvusBirthdayTs + 10, milvusBirthdayTs, false},
	}
	for _, test := range tests {
		s.Run(test.description, func() {
			t := &mixCompactionTask{
				plan: &datapb.CompactionPlan{
					CollectionTtl: test.collTTL,
				},
				currentTs: test.nowTs,
			}
			got := isExpiredEntity(t.plan.GetCollectionTtl(), t.currentTs, test.entityTs)
			s.Equal(test.expect, got)
		})
	}
}

func getRow(magic int64) map[int64]interface{} {
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
		JSONField: []byte(`{"batch":ok}`),
	}
}

func (s *MixCompactionTaskSuite) initSegBuffer(magic int64) {
	segWriter, err := NewSegmentWriter(s.meta.GetSchema(), 100, magic, PartitionID, CollectionID)
	s.Require().NoError(err)

	v := storage.Value{
		PK:        storage.NewInt64PrimaryKey(magic),
		Timestamp: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
		Value:     getRow(magic),
	}
	err = segWriter.Write(&v)
	s.Require().NoError(err)
	segWriter.writer.Flush()

	s.segWriter = segWriter
}

const (
	CollectionID           = 1
	PartitionID            = 1
	SegmentID              = 1
	BoolField              = 100
	Int8Field              = 101
	Int16Field             = 102
	Int32Field             = 103
	Int64Field             = 104
	FloatField             = 105
	DoubleField            = 106
	StringField            = 107
	BinaryVectorField      = 108
	FloatVectorField       = 109
	ArrayField             = 110
	JSONField              = 111
	Float16VectorField     = 112
	BFloat16VectorField    = 113
	SparseFloatVectorField = 114
	VarCharField           = 115
)

func getInt64DeltaBlobs(segID int64, pks []int64, tss []uint64) (*storage.Blob, error) {
	primaryKeys := make([]storage.PrimaryKey, len(pks))
	for index, v := range pks {
		primaryKeys[index] = storage.NewInt64PrimaryKey(v)
	}
	deltaData := storage.NewDeleteData(primaryKeys, tss)

	dCodec := storage.NewDeleteCodec()
	blob, err := dCodec.Serialize(1, 10, segID, deltaData)
	return blob, err
}

func genTestCollectionMeta() *etcdpb.CollectionMeta {
	return &etcdpb.CollectionMeta{
		ID:            CollectionID,
		PartitionTags: []string{"partition_0", "partition_1"},
		Schema: &schemapb.CollectionSchema{
			Name:        "schema",
			Description: "schema",
			AutoID:      true,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  common.RowIDField,
					Name:     "row_id",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  common.TimeStampField,
					Name:     "Timestamp",
					DataType: schemapb.DataType_Int64,
				},
				{
					FieldID:  BoolField,
					Name:     "field_bool",
					DataType: schemapb.DataType_Bool,
				},
				{
					FieldID:  Int8Field,
					Name:     "field_int8",
					DataType: schemapb.DataType_Int8,
				},
				{
					FieldID:  Int16Field,
					Name:     "field_int16",
					DataType: schemapb.DataType_Int16,
				},
				{
					FieldID:  Int32Field,
					Name:     "field_int32",
					DataType: schemapb.DataType_Int32,
				},
				{
					FieldID:      Int64Field,
					Name:         "field_int64",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  FloatField,
					Name:     "field_float",
					DataType: schemapb.DataType_Float,
				},
				{
					FieldID:  DoubleField,
					Name:     "field_double",
					DataType: schemapb.DataType_Double,
				},
				{
					FieldID:  StringField,
					Name:     "field_string",
					DataType: schemapb.DataType_String,
				},
				{
					FieldID:  VarCharField,
					Name:     "field_varchar",
					DataType: schemapb.DataType_VarChar,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.MaxLengthKey,
							Value: "128",
						},
					},
				},
				{
					FieldID:     ArrayField,
					Name:        "field_int32_array",
					Description: "int32 array",
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int32,
				},
				{
					FieldID:     JSONField,
					Name:        "field_json",
					Description: "json",
					DataType:    schemapb.DataType_JSON,
				},
				{
					FieldID:     BinaryVectorField,
					Name:        "field_binary_vector",
					Description: "binary_vector",
					DataType:    schemapb.DataType_BinaryVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "8",
						},
					},
				},
				{
					FieldID:     FloatVectorField,
					Name:        "field_float_vector",
					Description: "float_vector",
					DataType:    schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "4",
						},
					},
				},
				{
					FieldID:     Float16VectorField,
					Name:        "field_float16_vector",
					Description: "float16_vector",
					DataType:    schemapb.DataType_Float16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "4",
						},
					},
				},
				{
					FieldID:     BFloat16VectorField,
					Name:        "field_bfloat16_vector",
					Description: "bfloat16_vector",
					DataType:    schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "4",
						},
					},
				},
				{
					FieldID:     SparseFloatVectorField,
					Name:        "field_sparse_float_vector",
					Description: "sparse_float_vector",
					DataType:    schemapb.DataType_SparseFloatVector,
					TypeParams:  []*commonpb.KeyValuePair{},
				},
			},
		},
	}
}
