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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestMixCompactionTaskStorageV2Suite(t *testing.T) {
	suite.Run(t, new(MixCompactionTaskStorageV2Suite))
}

type MixCompactionTaskStorageV2Suite struct {
	MixCompactionTaskStorageV1Suite
}

func (s *MixCompactionTaskStorageV2Suite) SetupTest() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "false")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, s.T().TempDir())
	initcore.InitStorageV2FileSystem(paramtable.Get())
	s.setupTest()
	s.task.compactionParams = compaction.GenParams()
}

func (s *MixCompactionTaskStorageV2Suite) TearDownTest() {
	localStoragePath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	os.RemoveAll(filepath.Join(localStoragePath, "insert_log"))
	os.RemoveAll(filepath.Join(localStoragePath, "delta_log"))
	os.RemoveAll(filepath.Join(localStoragePath, "stats_log"))
	paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
	paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
	initcore.CleanArrowFileSystem()
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactDupPK() {
	s.prepareCompactDupPKSegments()
	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	s.Equal(7, len(result.GetSegments()[0].GetInsertLogs()))
	s.Equal(1, len(result.GetSegments()[0].GetField2StatslogPaths()))
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactMetrics() {
	s.prepareCompactDupPKSegments()

	storageConfig := s.task.GetStorageConfig()
	beforeMetrics, err := storagev2.GetFilesystemMetricsWithConfig(storageConfig)
	s.NoError(err)
	s.Contains(beforeMetrics, "origin")

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	afterMetrics, err := storagev2.GetFilesystemMetricsWithConfig(storageConfig)
	s.NoError(err)
	s.Contains(afterMetrics, "origin")

	s.Greater(afterMetrics["origin"].WriteBytes, beforeMetrics["origin"].WriteBytes,
		"write bytes should increase after compaction")
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactDupPK_MixToV2Format() {
	segments := []int64{7, 8, 9}
	dblobs, err := getInt64DeltaBlobs(
		1,
		[]int64{100},
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second))},
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
			CollectionID: 1,
			SegmentID:    segID,
			FieldBinlogs: lo.Values(fBinlogs),
			Deltalogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{LogID: 1, LogPath: "1"}}},
			},
		})
	}

	v2Segments := []int64{10, 11}
	for _, segID := range v2Segments {
		binlogs, _, _, _, _, _, _, err := s.initStorageV2Segments(1, segID, alloc)
		s.NoError(err)
		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			CollectionID:   1,
			SegmentID:      segID,
			FieldBinlogs:   storage.SortFieldBinlogs(binlogs),
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
		binlogs, _, _, _, _, _, _, err := s.initStorageV2Segments(1, segID, alloc)
		s.NoError(err)
		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			CollectionID:   1,
			SegmentID:      segID,
			FieldBinlogs:   storage.SortFieldBinlogs(binlogs),
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

// TestCompactSizesBloomFilterByOutputRows deletes most input rows, so the output segment has far fewer rows than
// the input. Its PK bloom filter must be sized by the output rows, not by all input rows.
func (s *MixCompactionTaskStorageV2Suite) TestCompactSizesBloomFilterByOutputRows() {
	s.testCompactSizesBloomFilterByOutputRows(storage.StorageV2)
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactSizesBloomFilterByOutputRowsV3() {
	s.testCompactSizesBloomFilterByOutputRows(storage.StorageV3)
}

func (s *MixCompactionTaskStorageV2Suite) testCompactSizesBloomFilterByOutputRows(version int64) {
	s.task.compactionParams.StorageVersion = version
	s.task.compactionParams.UseLoonFFI = version == storage.StorageV3
	const rowsPerSegment, deletedPerSegment = 100, 90
	segments := []int64{1001, 1002, 1003}
	statsBlobs := make(map[string][]byte)
	if version == storage.StorageV2 {
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, kvs map[string][]byte) error {
			for k, v := range kvs {
				if strings.Contains(k, common.SegmentStatslogPath) {
					statsBlobs[k] = v
				}
			}
			return nil
		})
	}
	alloc := allocator.NewLocalAllocator(100, math.MaxInt64)
	deleteTs := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(10 * time.Second))
	s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
	for _, segID := range segments {
		s.initMultiRowsSegBuffer(segID, rowsPerSegment, 3)
		kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
		s.Require().NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
			left, right := lo.Difference(keys, lo.Keys(kvs))
			return len(left) == 0 && len(right) == 0
		})).Return(lo.Values(kvs), nil).Once()

		pks := make([]int64, 0, deletedPerSegment)
		tss := make([]uint64, 0, deletedPerSegment)
		for i := int64(0); i < deletedPerSegment; i++ {
			pks = append(pks, segID+i*3)
			tss = append(tss, deleteTs)
		}
		blob, err := getInt64DeltaBlobs(segID, pks, tss)
		s.Require().NoError(err)
		deltaPath := fmt.Sprintf("deltalog/%d", segID)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, []string{deltaPath}).
			Return([][]byte{blob.GetValue()}, nil).Once()

		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			CollectionID: 1,
			SegmentID:    segID,
			FieldBinlogs: lo.Values(fBinlogs),
			Deltalogs: []*datapb.FieldBinlog{
				{Binlogs: []*datapb.Binlog{{LogPath: deltaPath}}},
			},
		})
	}

	result, err := s.task.Compact()
	s.Require().NoError(err)

	s.Require().Len(result.GetSegments(), 1)
	outputRows := int64(len(segments) * (rowsPerSegment - deletedPerSegment))
	s.Equal(outputRows, result.GetSegments()[0].GetNumOfRows())
	if version == storage.StorageV2 {
		s.Require().Len(statsBlobs, 1)
	}
	stats := s.readOutputPkStats(result.GetSegments()[0], statsBlobs, version)
	fp := paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat()
	inputRows := int64(len(segments) * rowsPerSegment)
	s.Equal(bloomfilter.NewBloomFilterWithType(uint(outputRows), fp, bloomfilter.BlockBFName).Cap(), stats.BF.Cap())
	s.Less(stats.BF.Cap(), bloomfilter.NewBloomFilterWithType(uint(inputRows), fp, bloomfilter.BlockBFName).Cap())
	for _, segID := range segments {
		for i := int64(deletedPerSegment); i < rowsPerSegment; i++ {
			var key [8]byte
			common.Endian.PutUint64(key[:], uint64(segID+i*3))
			s.True(stats.BF.Test(key[:]), "missing retained PK %d", segID+i*3)
		}
	}
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactSizesBloomFilterPerOutputSegment() {
	s.testCompactSizesBloomFilterPerOutputSegment(storage.StorageV2)
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactSizesBloomFilterPerOutputSegmentV3() {
	s.testCompactSizesBloomFilterPerOutputSegment(storage.StorageV3)
}

func (s *MixCompactionTaskStorageV2Suite) testCompactSizesBloomFilterPerOutputSegment(version int64) {
	s.task.compactionParams.StorageVersion = version
	s.task.compactionParams.UseLoonFFI = version == storage.StorageV3
	// Each input fits in one record. A one-byte target forces rotation before
	// the next record, producing differently sized output segments.
	rows := []int64{100, 200, 30}
	const inputRows = 330
	s.task.plan.MaxSize = 1
	s.task.plan.SegmentBinlogs = nil
	statsBlobs := make(map[string][]byte)
	if version == storage.StorageV2 {
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, kvs map[string][]byte) error {
			for k, v := range kvs {
				if strings.Contains(k, common.SegmentStatslogPath) {
					statsBlobs[k] = v
				}
			}
			return nil
		})
	}
	alloc := allocator.NewLocalAllocator(100, math.MaxInt64)
	for i, n := range rows {
		seed := int64(1000 * (i + 1))
		s.initMultiRowsSegBuffer(seed, n, 1)
		kvs, fBinlogs, err := serializeWrite(context.TODO(), alloc, s.segWriter)
		s.Require().NoError(err)
		s.mockBinlogIO.EXPECT().Download(mock.Anything, mock.MatchedBy(func(keys []string) bool {
			left, right := lo.Difference(keys, lo.Keys(kvs))
			return len(left) == 0 && len(right) == 0
		})).Return(lo.Values(kvs), nil).Once()
		s.task.plan.SegmentBinlogs = append(s.task.plan.SegmentBinlogs, &datapb.CompactionSegmentBinlogs{
			CollectionID: 1,
			SegmentID:    seed,
			FieldBinlogs: lo.Values(fBinlogs),
		})
	}

	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().Len(result.GetSegments(), len(rows))
	if version == storage.StorageV2 {
		s.Require().Len(statsBlobs, len(rows))
	}
	fp := paramtable.Get().CommonCfg.MaxBloomFalsePositive.GetAsFloat()
	var totalRows int64
	for i, segment := range result.GetSegments() {
		s.Require().Equal(rows[i], segment.GetNumOfRows())
		totalRows += segment.GetNumOfRows()
		stats := s.readOutputPkStats(segment, statsBlobs, version)
		s.Equal(bloomfilter.NewBloomFilterWithType(uint(rows[i]), fp, bloomfilter.BlockBFName).Cap(), stats.BF.Cap())
		s.Less(stats.BF.Cap(), bloomfilter.NewBloomFilterWithType(inputRows, fp, bloomfilter.BlockBFName).Cap())
		seed := int64(1000 * (i + 1))
		s.Equal(seed, stats.MinPk.GetValue())
		s.Equal(seed+rows[i]-1, stats.MaxPk.GetValue())
		for pk := seed; pk < seed+rows[i]; pk++ {
			var key [8]byte
			common.Endian.PutUint64(key[:], uint64(pk))
			s.True(stats.BF.Test(key[:]), "segment %d missing PK %d", segment.GetSegmentID(), pk)
		}
	}
	s.EqualValues(inputRows, totalRows)
}

// readOutputPkStats follows the output metadata, including the V3 manifest,
// so the assertions cover the persisted BF that downstream readers will load.
func (s *MixCompactionTaskStorageV2Suite) readOutputPkStats(
	segment *datapb.CompactionSegment,
	statsBlobs map[string][]byte,
	version int64,
) *storage.PrimaryKeyStats {
	s.T().Helper()
	s.Require().Equal(version, segment.GetStorageVersion())
	var blob []byte
	if version == storage.StorageV3 {
		s.Require().NotEmpty(segment.GetManifest())
		for _, fieldLogs := range segment.GetField2StatslogPaths() {
			s.Empty(fieldLogs.GetBinlogs(), "V3 must not expose legacy PK stats files")
		}
		s.Empty(statsBlobs, "V3 PK stats must be written through the manifest")
		manifestStats, err := packed.GetManifestStats(segment.GetManifest(), s.task.compactionParams.StorageConfig)
		s.Require().NoError(err)
		pkField, err := typeutil.GetPrimaryFieldSchema(s.task.plan.GetSchema())
		s.Require().NoError(err)
		stat, ok := manifestStats[fmt.Sprintf("bloom_filter.%d", pkField.GetFieldID())]
		s.Require().True(ok, "missing BF entry for segment %d", segment.GetSegmentID())
		s.Require().Len(stat.Paths, 1)
		blob, err = packed.ReadFile(s.task.compactionParams.StorageConfig, stat.Paths[0])
		s.Require().NoError(err)
		s.Equal(fmt.Sprint(len(blob)), stat.Metadata["memory_size"])
	} else {
		s.Require().Len(segment.GetField2StatslogPaths(), 1)
		logs := segment.GetField2StatslogPaths()[0].GetBinlogs()
		s.Require().Len(logs, 1)
		var ok bool
		blob, ok = statsBlobs[logs[0].GetLogPath()]
		s.Require().True(ok, "missing stats for segment %d", segment.GetSegmentID())
	}
	stats, err := storage.DeserializeStats([]*storage.Blob{{Value: blob}})
	s.Require().NoError(err)
	s.Require().Len(stats, 1)
	s.Equal(bloomfilter.BlockedBF, stats[0].BFType)
	return stats[0]
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactTwoToOne() {
	s.prepareCompactTwoToOneSegments()
	result, err := s.task.Compact()
	s.NoError(err)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	s.Equal(7, len(result.GetSegments()[0].GetInsertLogs()))
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
	s.Equal(3, len(segment.InsertLogs))
	s.Equal(1, len(segment.Bm25Logs))
	s.Equal(1, len(segment.Field2StatslogPaths))
}

func (s *MixCompactionTaskStorageV2Suite) TestCompactSortedSegment() {
	s.prepareCompactSortedSegment()
	paramtable.Get().Save("dataNode.compaction.useMergeSort", "true")
	defer paramtable.Get().Reset("dataNode.compaction.useMergeSort")
	s.task.compactionParams = compaction.GenParams()

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	s.True(result.GetSegments()[0].GetIsSorted())
	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(291, segment.GetNumOfRows())
	s.EqualValues(7, len(segment.InsertLogs))
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
	s.task.compactionParams = compaction.GenParams()
	fmt.Println(s.task.compactionParams)

	result, err := s.task.Compact()
	s.NoError(err)
	s.NotNil(result)

	s.Equal(s.task.plan.GetPlanID(), result.GetPlanID())
	s.Equal(1, len(result.GetSegments()))
	s.True(result.GetSegments()[0].GetIsSorted())

	segment := result.GetSegments()[0]
	s.EqualValues(19531, segment.GetSegmentID())
	s.EqualValues(291, segment.GetNumOfRows())
	s.EqualValues(7, len(segment.InsertLogs))
	s.EqualValues(1, len(segment.Field2StatslogPaths))
	s.Empty(segment.Deltalogs)
}

func (s *MixCompactionTaskStorageV2Suite) TestSplitMergeEntityExpired_V1ToV2Format() {
	s.TestSplitMergeEntityExpired()
}

func (s *MixCompactionTaskStorageV2Suite) TestMergeNoExpirationLackBinlog_V1ToV2Format() {
	s.TestMergeNoExpirationLackBinlog()
}

func (s *MixCompactionTaskStorageV2Suite) TestProjectedReaderPreservesPackedGroupPathIndexes() {
	const segmentID = int64(1001)
	fieldBinlogs, _, _, _, _, _, _, err := s.initStorageV2Segments(
		1,
		segmentID,
		allocator.NewLocalAllocator(1000, math.MaxInt64),
	)
	s.Require().NoError(err)

	fullSchema := s.meta.GetSchema()
	selectedFields := lo.Filter(fullSchema.GetFields(), func(field *schemapb.FieldSchema, _ int) bool {
		switch field.GetFieldID() {
		case common.RowIDField, common.TimeStampField, Int64Field, FloatVectorField:
			return true
		default:
			return false
		}
	})
	readSchema := &schemapb.CollectionSchema{
		Name:   fullSchema.GetName(),
		Fields: selectedFields,
	}
	storageConfig := &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    paramtable.Get().LocalStorageCfg.Path.GetValue(),
	}
	segment := &datapb.CompactionSegmentBinlogs{
		CollectionID:   CollectionID,
		PartitionID:    PartitionID,
		SegmentID:      segmentID,
		StorageVersion: storage.StorageV2,
	}
	modernFieldBinlogs := storage.SortFieldBinlogs(fieldBinlogs)
	legacyFieldBinlogs := lo.Map(modernFieldBinlogs, func(fieldBinlog *datapb.FieldBinlog, _ int) *datapb.FieldBinlog {
		legacy := proto.Clone(fieldBinlog).(*datapb.FieldBinlog)
		legacy.ChildFields = nil
		return legacy
	})
	for _, testCase := range []struct {
		name         string
		fieldBinlogs []*datapb.FieldBinlog
	}{
		{name: "modern_v2", fieldBinlogs: modernFieldBinlogs},
		{name: "legacy_v2", fieldBinlogs: legacyFieldBinlogs},
	} {
		s.Run(testCase.name, func() {
			segment.FieldBinlogs = append([]*datapb.FieldBinlog(nil), testCase.fieldBinlogs...)
			reader, _, err := newCompactionSegmentRecordReader(context.Background(), segment, readSchema, storageConfig,
				storage.WithCollectionID(CollectionID),
				storage.WithVersion(storage.StorageV2),
				storage.WithStorageConfig(storageConfig),
			)
			s.Require().NoError(err)

			record, err := reader.Next()
			s.Require().NoError(err)
			values := make([]*storage.Value, record.Len())
			s.Require().NoError(storage.ValueDeserializerWithSelectedFields(record, values, selectedFields, true))
			s.Require().Len(values, 1)
			s.EqualValues(segmentID, values[0].Value.(map[int64]interface{})[Int64Field])
			s.Equal([]float32{4, 5, 6, 7}, values[0].Value.(map[int64]interface{})[FloatVectorField])
			s.Require().NoError(reader.Close())
		})
	}
}

func (s *MixCompactionTaskStorageV2Suite) initStorageV2Segments(rows int, seed int64, alloc allocator.Interface) (
	inserts map[int64]*datapb.FieldBinlog,
	deltas *datapb.FieldBinlog,
	stats map[int64]*datapb.FieldBinlog,
	bm25Stats map[int64]*datapb.FieldBinlog,
	manifest string,
	size int64,
	segmentStats *datapb.Statistics,
	err error,
) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
	bfs := pkoracle.NewBloomFilterSet()
	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil, metacache.NewEmptySegmentStats())
	metacache.UpdateNumOfRows(1000)(seg)
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().Collection().Return(CollectionID).Maybe()
	mc.EXPECT().GetSchema(mock.Anything).Return(s.meta.Schema).Maybe()
	mc.EXPECT().GetSegmentByID(seed).Return(seg, true).Maybe()
	mc.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg}).Maybe()
	mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
		action(seg)
	}).Return().Maybe()

	channelName := fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", CollectionID)
	pack := new(syncmgr.SyncPack).WithCollectionID(CollectionID).WithPartitionID(PartitionID).WithSegmentID(seed).WithChannelName(channelName).WithInsertData(getInsertData(rows, seed, s.meta.GetSchema()))
	sch := s.meta.Schema
	fields := typeutil.GetAllFieldSchemas(sch)
	columnGroups := storagecommon.SplitColumns(fields, map[int64]storagecommon.ColumnStats{}, storagecommon.NewSelectedDataTypePolicy(), storagecommon.NewRemanentShortPolicy(-1))
	bw := syncmgr.NewBulkPackWriterV2(mc, s.meta.Schema, cm, alloc, packed.DefaultWriteBufferSize, 0, &indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    rootPath,
	}, columnGroups)
	return bw.Write(context.Background(), pack)
}

func getRowWithoutNil(magic int64) map[int64]interface{} {
	ts := tsoutil.ComposeTSByTime(getMilvusBirthday())
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
