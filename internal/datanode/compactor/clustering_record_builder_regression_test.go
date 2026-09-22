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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	binlogio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestClusterBufferWideReadback(t *testing.T) {
	for _, version := range []int64{storage.StorageV1, storage.StorageV2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			const rows = 9
			schema := clusteringWideSchema()
			schema.Fields[4].DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "fallback"}}
			schema.Fields[6] = &schemapb.FieldSchema{
				FieldID: 104, Name: "double", DataType: schemapb.DataType_Double, Nullable: true,
				DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_DoubleData{DoubleData: 3.5}},
			}
			const defaultTimestamp int64 = 1_700_000_000_000_000
			schema.Fields[7] = &schemapb.FieldSchema{
				FieldID: 105, Name: "timestamp", DataType: schemapb.DataType_Timestamptz, Nullable: true,
				DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_TimestamptzData{TimestamptzData: defaultTimestamp}},
			}
			buffer, observer := newClusteringTestBuffer(t, schema, 16<<10, version)
			for row := 0; row < rows; row++ {
				record := clusteringTestRecord(t, schema, row, 1)
				err := buffer.WriteRecord(record, 0)
				record.Release()
				require.NoError(t, err)
			}
			require.NoError(t, buffer.Close())
			segments := buffer.GetCompactionSegments()
			require.Len(t, segments, 1)
			segment := segments[0]
			require.EqualValues(t, rows, segment.NumOfRows)
			reader, err := storage.NewBinlogRecordReader(context.Background(), segment.InsertLogs, schema,
				storage.WithVersion(version), storage.WithStorageConfig(buffer.writer.params.StorageConfig),
				storage.WithDownloader(func(_ context.Context, paths []string) ([][]byte, error) {
					data := make([][]byte, len(paths))
					for i, key := range paths {
						require.Contains(t, observer.blobs, key)
						data[i] = observer.blobs[key]
					}
					return data, nil
				}))
			require.NoError(t, err)
			defer reader.Close()
			rowID := 0
			for {
				record, err := reader.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				for row := 0; row < record.Len(); row++ {
					for _, field := range schema.Fields {
						column := record.Column(field.FieldID)
						null := field.Nullable && rowID%3 == 0 && field.DefaultValue == nil
						require.Equal(t, null, column.IsNull(row), "field %d row %d", field.FieldID, rowID)
						if null {
							continue
						}
						switch column := column.(type) {
						case *array.Int64:
							want := int64(rowID)
							if field.GetDataType() == schemapb.DataType_Timestamptz && rowID%3 == 0 {
								want = defaultTimestamp
							}
							require.Equal(t, want, column.Value(row))
						case *array.Int32:
							require.EqualValues(t, rowID, column.Value(row))
						case *array.Float64:
							want := float64(rowID) + 0.25
							if rowID%3 == 0 {
								want = 3.5
							}
							require.Equal(t, want, column.Value(row))
						case *array.String:
							want := strings.Repeat("x", 128+rowID%17) + fmt.Sprint(rowID)
							if rowID%3 == 0 {
								want = "fallback"
							}
							require.Equal(t, want, column.Value(row))
						case *array.FixedSizeBinary:
							for dim := 0; dim < 2048; dim++ {
								require.Equal(t, float32(rowID+dim), math.Float32frombits(binary.LittleEndian.Uint32(column.Value(row)[dim*4:])))
							}
						default:
							t.Fatalf("unexpected column %T", column)
						}
					}
					rowID++
				}
			}
			require.Equal(t, rows, rowID)
			for _, group := range segment.InsertLogs {
				var entries int64
				for _, log := range group.Binlogs {
					require.EqualValues(t, entries, log.TimestampFrom)
					entries += log.EntriesNum
					require.EqualValues(t, entries-1, log.TimestampTo)
				}
				require.EqualValues(t, rows, entries)
			}
			if version == storage.StorageV2 {
				for _, field := range schema.Fields {
					want := int64(0)
					if field.Nullable && field.DefaultValue == nil {
						want = 3
					}
					require.Contains(t, segment.GetStats().GetNullCounts(), field.FieldID)
					require.Equal(t, want, segment.GetStats().GetNullCounts()[field.FieldID], "field %d", field.FieldID)
				}
			}
			require.NotEmpty(t, segment.Field2StatslogPaths)
			for _, field := range segment.Field2StatslogPaths {
				for _, log := range field.Binlogs {
					stats, err := storage.DeserializeStats([]*storage.Blob{{Value: observer.blobs[log.LogPath]}})
					require.NoError(t, err)
					require.Len(t, stats, 1)
					require.EqualValues(t, 0, stats[0].MinPk.GetValue())
					require.EqualValues(t, rows-1, stats[0].MaxPk.GetValue())
				}
			}
		})
	}
}

func TestClusteringTextRewriteAcrossPartitionNamespaces(t *testing.T) {
	setupBumpUTEnv(t)
	const textID = int64(105)
	const rows = 6
	fixture := buildBumpFixture(t, withRows(rows),
		withSourceFields(&schemapb.FieldSchema{FieldID: textID, Name: "text_lob", DataType: schemapb.DataType_Text}),
		withFillValue(func(i int, _ uint64, values map[int64]any) { values[textID] = bumpFxLobText(i) }),
		withTextLOBSource(textID), withLegacySourceNamespace())
	plan := proto.Clone(fixture.task.plan).(*datapb.CompactionPlan)
	plan.Type = datapb.CompactionType_ClusteringCompaction
	plan.ClusteringKeyField = bumpFxPKField
	plan.PreferSegmentRows, plan.MaxSegmentRows = 128, 128
	plan.AnalyzeResultPath = fixture.cfg.RootPath + "/analyze_stats/999"
	task := NewClusteringCompactionTask(context.Background(), binlogio.NewBinlogIO(fixture.task.chunkManager), plan, fixture.task.compactionParams)
	result, err := task.Compact()
	require.NoError(t, err)
	require.Len(t, result.Segments, 1)
	segment := result.Segments[0]
	require.EqualValues(t, rows, segment.NumOfRows)
	base, _, err := packed.UnmarshalManifestPath(segment.Manifest)
	require.NoError(t, err)
	require.Equal(t, storage.SegmentManifestBasePath(fixture.cfg.RootPath, CollectionID, PartitionID, segment.SegmentID), base)
	lobFiles, err := packed.GetManifestLobFiles(segment.Manifest, fixture.cfg)
	require.NoError(t, err)
	require.NotEmpty(t, lobFiles)
	require.NotNil(t, task.lobContext)
	require.True(t, task.lobContext.DecodeTextFromSource)
	configs, err := task.lobContext.GetSourceTextColumnConfigs(segment.Manifest)
	require.NoError(t, err)
	reader, err := storage.NewTextDecodedManifestRecordReader(context.Background(), segment.Manifest, plan.Schema, configs,
		storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(fixture.cfg))
	require.NoError(t, err)
	defer reader.Close()
	readRows := 0
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		for row := 0; row < record.Len(); row++ {
			require.EqualValues(t, readRows, record.Column(bumpFxPKField).(*array.Int64).Value(row))
			require.Equal(t, bumpFxLobText(readRows), record.Column(textID).(*array.String).Value(row))
			readRows++
		}
	}
	require.Equal(t, rows, readRows)
}

// The second Next releases the input and pauses while the bucket still owns an
// underfilled batch. This exercises Compact's cleanup after real mapping work.
type pausedClusteringReader struct {
	ctx    context.Context
	record storage.Record
	read   bool
	ready  chan<- struct{}
	resume <-chan struct{}
	closed chan struct{}
	err    error
}

func (r *pausedClusteringReader) Next() (storage.Record, error) {
	if !r.read {
		r.read = true
		return r.record, nil
	}
	r.record.Release()
	r.record = nil
	r.ready <- struct{}{}
	select {
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	case <-r.resume:
		return nil, r.err
	}
}

func (r *pausedClusteringReader) Close() error {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}
	close(r.closed)
	return nil
}

func TestClusteringReleasesPendingBuildersOnFailure(t *testing.T) {
	for _, cancelTask := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%v", cancelTask), func(t *testing.T) {
			schema := clusteringWideSchema()
			buffer, observer := newClusteringTestBuffer(t, schema, 64<<20, storage.StorageV1)
			workers := &paramtable.Get().DataNodeCfg.ClusteringCompactionWorkerPoolSize
			previousWorkers := workers.GetValue()
			require.NoError(t, paramtable.Get().Save(workers.Key, "2"))
			t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(workers.Key, previousWorkers)) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			checked := memory.NewCheckedAllocator(memory.DefaultAllocator)
			originalAllocator := memory.DefaultAllocator
			memory.DefaultAllocator = checked
			defer func() { memory.DefaultAllocator = originalAllocator }()
			ready := make(chan struct{}, 2)
			gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
			readFailure := errors.New("injected read failure after a buffered record")
			readers := make([]*pausedClusteringReader, 2)
			for i := range readers {
				readers[i] = &pausedClusteringReader{
					ctx: ctx, record: clusteringTestRecord(t, schema, i, 1),
					ready: ready, resume: gates[i], closed: make(chan struct{}), err: readFailure,
				}
			}
			plan := &datapb.CompactionPlan{
				Type: datapb.CompactionType_ClusteringCompaction, Schema: schema, ClusteringKeyField: 100,
				SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{CollectionID: CollectionID, SegmentID: 0}, {CollectionID: CollectionID, SegmentID: 1}},
			}
			task := NewClusteringCompactionTask(ctx, buffer.writer.binlogIO, plan, buffer.writer.params)
			analyze := mockey.Mock((*clusteringCompactionTask).getScalarAnalyzeResult).To(func(task *clusteringCompactionTask, _ context.Context) error {
				task.clusterBuffers = []*ClusterBuffer{buffer}
				task.keyToBufferFunc = func(any) *ClusterBuffer { return buffer }
				return nil
			}).Build()
			defer analyze.UnPatch()
			readerFactory := mockey.Mock(newTextDecodedCompactionSegmentRecordReader).To(func(_ context.Context, segment *datapb.CompactionSegmentBinlogs,
				_ *schemapb.CollectionSchema, _ *indexpb.StorageConfig, _ []packed.TextColumnConfig, _ ...storage.RwOption,
			) (storage.RecordReader, map[int64]struct{}, error) {
				fields := make(map[int64]struct{})
				for _, field := range schema.Fields {
					fields[field.FieldID] = struct{}{}
				}
				return readers[segment.SegmentID], fields, nil
			}).Build()
			defer readerFactory.UnPatch()
			finished := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, err := task.Compact()
				finished <- err
			}()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Error("mapping workers did not stop during test cleanup")
				}
			}()
			for range readers {
				select {
				case <-ready:
				case <-time.After(10 * time.Second):
					t.Fatal("mapping did not reach the pending batch")
				}
			}
			require.Greater(t, checked.CurrentAlloc(), 0)
			require.Equal(t, 2, buffer.builder.GetRowNum())
			require.Empty(t, observer.rows)
			if cancelTask {
				cancel()
			} else {
				close(gates[0])
				select {
				case <-readers[0].closed:
				case <-time.After(10 * time.Second):
					t.Fatal("failed reader did not close")
				}
				select {
				case err := <-finished:
					t.Fatalf("Compact returned while another mapping worker still owned a pending batch: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
				close(gates[1])
			}
			select {
			case err := <-finished:
				if cancelTask {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					require.ErrorIs(t, err, readFailure)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("Compact did not finish after readers stopped")
			}
			require.Nil(t, buffer.builder)
			checked.AssertSize(t, 0)
			require.Empty(t, observer.rows)
		})
	}
}

func TestClusteringTextAddedAfterAllSourceSegments(t *testing.T) {
	setupBumpUTEnv(t)
	const textID = int64(105)
	const rows = 6
	fixture := buildBumpFixture(t, withRows(rows), withLegacySourceNamespace(),
		withTargetAddedField(&schemapb.FieldSchema{FieldID: textID, Name: "added_text", DataType: schemapb.DataType_Text, Nullable: true}))
	plan := proto.Clone(fixture.task.plan).(*datapb.CompactionPlan)
	plan.Type = datapb.CompactionType_ClusteringCompaction
	plan.ClusteringKeyField = bumpFxPKField
	plan.PreferSegmentRows, plan.MaxSegmentRows = 128, 128
	plan.AnalyzeResultPath = fixture.cfg.RootPath + "/analyze_stats/999"
	params := fixture.task.compactionParams
	params.BinLogMaxSize = 1 // Every source row becomes a separate all-null TEXT output batch.
	task := NewClusteringCompactionTask(context.Background(), binlogio.NewBinlogIO(fixture.task.chunkManager), plan, params)
	result, err := task.Compact()
	require.NoError(t, err)
	require.True(t, task.lobContext.DecodeTextFromSource)
	require.Len(t, result.Segments, 1)
	segment := result.Segments[0]
	require.EqualValues(t, rows, segment.NumOfRows)
	configs, err := task.lobContext.GetSourceTextColumnConfigs(segment.Manifest)
	require.NoError(t, err)
	reader, err := storage.NewTextDecodedManifestRecordReader(context.Background(), segment.Manifest, plan.Schema, configs,
		storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(fixture.cfg))
	require.NoError(t, err)
	defer reader.Close()
	readRows := 0
	for {
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, record.Len(), record.Column(textID).NullN())
		for row := 0; row < record.Len(); row++ {
			require.EqualValues(t, readRows, record.Column(bumpFxPKField).(*array.Int64).Value(row))
			readRows++
		}
	}
	require.Equal(t, rows, readRows)
}
