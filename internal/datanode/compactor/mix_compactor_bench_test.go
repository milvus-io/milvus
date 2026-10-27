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
	"strconv"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	binlogio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/bloomfilter"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func BenchmarkMixCompactor(b *testing.B) {
	// Setup
	s := new(MixCompactionTaskStorageV1Suite)

	s.SetT(&testing.T{})
	s.SetupSuite()
	s.SetupTest()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		seq := int64(i * 100000)
		segments := []int64{seq, seq + 1, seq + 2}
		alloc := allocator.NewLocalAllocator(seq+3, math.MaxInt64)
		s.mockBinlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
		s.task.plan.SegmentBinlogs = make([]*datapb.CompactionSegmentBinlogs, 0)
		for _, segID := range segments {
			s.initSegBuffer(100000, segID)
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
			})
		}

		b.StartTimer()

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

	s.TearDownTest()
}

// Only Compact is timed; input generation and output validation are excluded.
// BF_BENCH_ROWS controls rows per input (default 1000000); BF_BENCH_VARCHAR=1
// selects string PKs. BF_BENCH_MERGE_SORT=0 selects mergeSplit instead of mergeSort.
// BF_BENCH_BATCH_BYTES overrides the write batch size for small fixtures.
// For an old-implementation comparison, disable UseDeferredBloomFilter with a
// Go overlay and set BF_BENCH_EXPECT_DEFERRED=false. Inputs use V2 in both cases.
func BenchmarkMixCompactorBloomFilterSizing(b *testing.B) {
	paramtable.Get().Init(paramtable.NewBaseTable())
	rows := int64(1000000)
	if value := os.Getenv("BF_BENCH_ROWS"); value != "" {
		var err error
		rows, err = strconv.ParseInt(value, 10, 64)
		require.NoError(b, err)
	}
	require.GreaterOrEqual(b, rows, int64(10000))
	for _, version := range []int64{storage.StorageV2, storage.StorageV3} {
		b.Run(fmt.Sprintf("V%d", version), func(b *testing.B) {
			for _, scenario := range []struct {
				name          string
				inputs        int
				deletePercent int
			}{
				{"single", 1, 0}, {"split13", 13, 0}, {"delete50", 1, 50}, {"delete90", 1, 90},
			} {
				if os.Getenv("BF_BENCH_VARCHAR") == "1" && scenario.deletePercent != 0 {
					continue // Delta fixtures below use Int64 PKs.
				}
				b.Run(scenario.name, func(b *testing.B) {
					b.StopTimer()
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						benchmarkMixBloomSizing(b, version, rows, scenario.inputs, scenario.deletePercent)
					}
				})
			}
		})
	}
}

func benchmarkMixBloomSizing(b *testing.B, version, rows int64, inputs, deletePercent int) {
	ctx := context.Background()
	oldLevel := mlog.GetLevel()
	mlog.SetLevel(mlog.ErrorLevel)
	defer mlog.SetLevel(oldLevel)
	pt := paramtable.Get()
	root := b.TempDir()
	defer os.RemoveAll(root)
	for key, value := range map[string]string{
		pt.CommonCfg.StorageType.Key:           "local",
		pt.CommonCfg.UseLoonFFI.Key:            "false",
		pt.LocalStorageCfg.Path.Key:            root,
		pt.CommonCfg.BloomFilterType.Key:       bloomfilter.BlockBFName,
		pt.CommonCfg.MaxBloomFalsePositive.Key: "0.001",
	} {
		require.NoError(b, pt.Save(key, value))
		defer pt.Reset(key)
	}
	initcore.InitStorageV2FileSystem(pt)
	defer initcore.CleanArrowFileSystem()
	params := compaction.GenParams()
	params.StorageVersion = version
	params.UseLoonFFI = version == storage.StorageV3
	params.UseMergeSort = os.Getenv("BF_BENCH_MERGE_SORT") != "0"
	if value := os.Getenv("BF_BENCH_BATCH_BYTES"); value != "" {
		batchBytes, err := strconv.ParseUint(value, 10, 64)
		require.NoError(b, err)
		params.BinLogMaxSize = batchBytes
	}
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	io := binlogio.NewBinlogIO(cm)
	schema := &schemapb.CollectionSchema{Name: "bloom_sizing_bench", Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vector", DataType: schemapb.DataType_BinaryVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "64"}}},
	}}
	varchar := os.Getenv("BF_BENCH_VARCHAR") == "1"
	if varchar {
		schema.Fields[2].DataType = schemapb.DataType_VarChar
		schema.Fields[2].TypeParams = []*commonpb.KeyValuePair{{Key: "max_length", Value: "20"}}
		require.Zero(b, deletePercent)
	}
	input := prepareBloomBenchInputs(b, schema, io, params, root, rows, inputs, deletePercent)
	totalRows := rows * int64(inputs)
	// Int64 rows use 32 bytes; fixed-width VarChar rows use 44. A single-output case has room to spare;
	// split cases deliberately cross the target and exercise actual rotation.
	rowBytes := int64(32)
	if varchar {
		rowBytes = 44
	}
	maxSize := rows * rowBytes
	if inputs == 1 {
		maxSize *= 2
	}
	plan := &datapb.CompactionPlan{
		PlanID: 999, Type: datapb.CompactionType_MixCompaction, Schema: schema,
		SegmentBinlogs: input, TotalRows: totalRows, MaxSize: maxSize,
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 10000, End: 100000},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 100000, End: math.MaxInt64},
	}
	require.Equal(b, params.UseMergeSort, canMergeSort(plan, params), "compaction branch must match the requested path")
	b.Logf("merge_sort=%t batch_bytes=%d target_bytes=%d", params.UseMergeSort, params.BinLogMaxSize, maxSize)
	task := NewMixCompactionTask(ctx, io, cm, plan, params, []int64{100})
	defer task.cancel()
	b.StartTimer()
	result, err := task.Compact()
	b.StopTimer()
	require.NoError(b, err)
	if inputs == 1 {
		require.Len(b, result.GetSegments(), 1)
	} else {
		require.Greater(b, len(result.GetSegments()), 1, "fixture must exercise output rotation")
	}
	deferred := os.Getenv("BF_BENCH_EXPECT_DEFERRED") != "false"
	fp := pt.CommonCfg.MaxBloomFalsePositive.GetAsFloat()
	var outputRows, bfBytes, idealBytes int64
	for _, seg := range result.GetSegments() {
		require.Equal(b, version, seg.GetStorageVersion())
		var data []byte
		if version == storage.StorageV3 {
			stats, err := packed.GetManifestStats(seg.GetManifest(), params.StorageConfig)
			require.NoError(b, err)
			stat, ok := stats["bloom_filter.100"]
			require.True(b, ok)
			require.Len(b, stat.Paths, 1)
			data, err = packed.ReadFile(params.StorageConfig, stat.Paths[0])
			require.NoError(b, err)
		} else {
			require.Len(b, seg.GetField2StatslogPaths(), 1)
			logs := seg.GetField2StatslogPaths()[0].GetBinlogs()
			require.Len(b, logs, 1)
			data, err = cm.Read(ctx, logs[0].GetLogPath())
			require.NoError(b, err)
		}
		stats, err := storage.DeserializeStats([]*storage.Blob{{Value: data}})
		require.NoError(b, err)
		require.Len(b, stats, 1)
		actual := stats[0].BF.Cap()
		ideal := bloomfilter.NewBloomFilterWithType(uint(seg.GetNumOfRows()), fp, bloomfilter.BlockBFName).Cap()
		expected := ideal
		if !deferred {
			expected = bloomfilter.NewBloomFilterWithType(uint(totalRows), fp, bloomfilter.BlockBFName).Cap()
		}
		require.Equal(b, expected, actual, "segment %d BF capacity", seg.GetSegmentID())
		// Both paths produce contiguous PK ranges for these fixtures.
		var minPK, maxPK int64
		if varchar {
			minPK, err = strconv.ParseInt(stats[0].MinPk.GetValue().(string), 10, 64)
			require.NoError(b, err)
			maxPK, err = strconv.ParseInt(stats[0].MaxPk.GetValue().(string), 10, 64)
			require.NoError(b, err)
		} else {
			minPK = stats[0].MinPk.GetValue().(int64)
			maxPK = stats[0].MaxPk.GetValue().(int64)
		}
		require.Equal(b, seg.GetNumOfRows(), maxPK-minPK+1)
		for pk := minPK; pk <= maxPK; pk++ {
			var key [8]byte
			common.Endian.PutUint64(key[:], uint64(pk))
			present := stats[0].BF.Test(key[:])
			if varchar {
				present = stats[0].BF.TestString(fmt.Sprintf("%020d", pk))
			}
			if !present {
				b.Fatalf("segment %d missing PK %d", seg.GetSegmentID(), pk)
			}
		}
		outputRows += seg.GetNumOfRows()
		bfBytes += int64(actual / 8)
		idealBytes += int64(ideal / 8)
		b.Logf("segment=%d rows=%d bf_bytes=%d ideal_bytes=%d", seg.GetSegmentID(), seg.GetNumOfRows(), actual/8, ideal/8)
	}
	require.Equal(b, totalRows-rows*int64(deletePercent)/100*int64(inputs), outputRows)
	b.ReportMetric(float64(totalRows), "input-rows")
	b.ReportMetric(float64(outputRows), "output-rows")
	b.ReportMetric(float64(len(result.GetSegments())), "output-segments")
	b.ReportMetric(float64(bfBytes), "bf-B")
	b.ReportMetric(float64(bfBytes)/float64(idealBytes), "bf/ideal")
}

// Prepare inputs in bounded batches and persist them to real local storage.
// No map retains all input binlog bytes during the measured compaction.
func prepareBloomBenchInputs(b *testing.B, schema *schemapb.CollectionSchema, io binlogio.BinlogIO,
	params compaction.Params, root string, rows int64, inputs, deletePercent int,
) []*datapb.CompactionSegmentBinlogs {
	b.Helper()
	ctx := context.Background()
	alloc := allocator.NewLocalAllocator(1, 99999)
	segments := make([]*datapb.CompactionSegmentBinlogs, 0, inputs)
	const insertTS = uint64(400000000000000000)
	for i := 0; i < inputs; i++ {
		segmentID := int64(i + 1)
		writer, err := storage.NewBinlogRecordWriter(ctx, 1, 10, segmentID, schema, alloc,
			params.BinLogMaxSize, rows,
			storage.WithVersion(storage.StorageV2), storage.WithUseLoonFFI(false),
			storage.WithStorageConfig(params.StorageConfig), storage.WithUploader(io.Upload))
		require.NoError(b, err)
		for offset := int64(0); offset < rows; offset += 8192 {
			n := min(int64(8192), rows-offset)
			values := make([]*storage.Value, n)
			for j := int64(0); j < n; j++ {
				pk := int64(i)*rows + offset + j
				if params.UseMergeSort {
					pk = (offset+j)*int64(inputs) + int64(i)
				}
				var vector [8]byte
				common.Endian.PutUint64(vector[:], uint64(pk)*0x9e3779b97f4a7c15)
				var primaryKey storage.PrimaryKey = storage.NewInt64PrimaryKey(pk)
				var fieldPK interface{} = pk
				if schema.Fields[2].GetDataType() == schemapb.DataType_VarChar {
					value := fmt.Sprintf("%020d", pk)
					primaryKey = storage.NewVarCharPrimaryKey(value)
					fieldPK = value
				}
				values[j] = &storage.Value{PK: primaryKey, Timestamp: int64(insertTS),
					Value: map[int64]interface{}{common.RowIDField: pk, common.TimeStampField: int64(insertTS), 100: fieldPK, 101: vector[:]}}
			}
			record, err := storage.ValueSerializer(values, schema)
			require.NoError(b, err)
			err = writer.Write(record)
			record.Release()
			require.NoError(b, err)
		}
		require.NoError(b, writer.Close())
		fields, _, _, _, _ := writer.GetLogs()
		segment := &datapb.CompactionSegmentBinlogs{
			CollectionID: 1, PartitionID: 10, SegmentID: segmentID,
			StorageVersion: storage.StorageV2, FieldBinlogs: storage.SortFieldBinlogs(fields),
			IsSorted: params.UseMergeSort,
		}
		if deleted := rows * int64(deletePercent) / 100; deleted > 0 {
			// Delete cases have one input; bounded files avoid a large setup heap.
			logs := &datapb.FieldBinlog{}
			for offset := int64(0); offset < deleted; offset += 8192 {
				n := min(int64(8192), deleted-offset)
				pks := make([]int64, n)
				tss := make([]uint64, n)
				for j := int64(0); j < n; j++ {
					pks[j] = int64(i)*rows + offset + j
					tss[j] = insertTS + 1
				}
				blob, err := getInt64DeltaBlobs(segmentID, pks, tss)
				require.NoError(b, err)
				path := filepath.Join(root, "delta_log", fmt.Sprintf("%d-%d", segmentID, offset))
				require.NoError(b, io.Upload(ctx, map[string][]byte{path: blob.Value}))
				logs.Binlogs = append(logs.Binlogs, &datapb.Binlog{LogPath: path, EntriesNum: n})
			}
			segment.Deltalogs = []*datapb.FieldBinlog{logs}
		}
		segments = append(segments, segment)
	}
	return segments
}
