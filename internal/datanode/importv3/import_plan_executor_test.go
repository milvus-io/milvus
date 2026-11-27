// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package importv3

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func importPlanTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "note", DataType: schemapb.DataType_VarChar},
	}}
}

// defaultImportPlanWriterSpec builds a valid Storage V3 WriterSpec whose single
// column group covers the whole segment schema (user fields + RowID + timestamp).
func defaultImportPlanWriterSpec(schema *schemapb.CollectionSchema) *importv3pb.WriterSpec {
	segmentSchema := typeutil.AppendSystemFields(schema)
	fieldIDs := make([]int64, 0, len(segmentSchema.GetFields()))
	for _, field := range segmentSchema.GetFields() {
		fieldIDs = append(fieldIDs, field.GetFieldID())
	}
	return &importv3pb.WriterSpec{
		StorageVersion: storage.StorageV3,
		Format:         "parquet",
		BloomType:      "BlockedBloomFilter",
		BloomFpp:       0.001,
		Groups:         []*importv3pb.ColumnGroupSpec{{Id: 0, FieldIds: fieldIDs, Format: "parquet"}},
	}
}

func newImportPlanRequest(schema *schemapb.CollectionSchema, fragments []*importv3pb.FragmentRef, rows int64, spec *importv3pb.WriterSpec) *datapb.ImportTaskV3Request {
	return &datapb.ImportTaskV3Request{
		JobId: 1, TaskId: 2, RunId: 3, SegmentId: 1000, Slot: 1,
		StorageConfig: &indexpb.StorageConfig{RootPath: "root", BucketName: "bucket"},
		Plan: &importv3pb.ImportTaskPlan{
			PartitionId: 10, CollectionId: 1, Rows: rows, FanIn: 16, DataTs: 12345,
			CollectionSchema: schema, Writer: spec, Fragments: fragments,
		},
	}
}

// planFragmentSource builds a merge Source that replays one fragment batch as a
// real arrow record over the fragment schema.
func planFragmentSource(t *testing.T, id string, ids []int64, notes []string, schema *schemapb.CollectionSchema) Source {
	t.Helper()
	rowIDs := make([]int64, len(ids))
	for i := range rowIDs {
		rowIDs[i] = int64(1000 + i)
	}
	data := &storage.InsertData{Data: map[int64]storage.FieldData{
		100:               &storage.Int64FieldData{Data: append([]int64(nil), ids...)},
		101:               &storage.StringFieldData{Data: append([]string(nil), notes...)},
		common.RowIDField: &storage.Int64FieldData{Data: rowIDs},
	}}
	return Source{
		ID: id, Rows: int64(data.GetRowNum()),
		Open: func(context.Context) (storage.RecordReader, error) {
			return storage.NewInsertDataRecordReader(data, schema)
		},
	}
}

// planFakeBinlogWriter is the final segment writer stub. It records how many
// records it received and lets a test choose GetLogs' manifest-vs-binlog shape
// and the row count the writer reports, so the executor's branch selection and
// its row-match guard can be pinned without touching object storage.
type planFakeBinlogWriter struct {
	rows          int64
	fieldBinlogs  map[storage.FieldID]*datapb.FieldBinlog
	statsLog      *datapb.FieldBinlog
	bm25StatsLog  map[storage.FieldID]*datapb.FieldBinlog
	manifest      string
	expirQuantile []int64
	statsBlobSize int64

	writeCount int
	written    []int
}

func (w *planFakeBinlogWriter) Write(record storage.Record) error {
	w.writeCount++
	w.written = append(w.written, record.Len())
	return nil
}

func (w *planFakeBinlogWriter) GetWrittenUncompressed() uint64 { return 0 }
func (w *planFakeBinlogWriter) Close() error                   { return nil }
func (w *planFakeBinlogWriter) GetRowNum() int64               { return w.rows }
func (w *planFakeBinlogWriter) GetStatsBlobSize() int64        { return w.statsBlobSize }
func (w *planFakeBinlogWriter) FlushChunk() error              { return nil }
func (w *planFakeBinlogWriter) GetBufferUncompressed() uint64  { return 0 }
func (w *planFakeBinlogWriter) Schema() *schemapb.CollectionSchema {
	return nil
}

func (w *planFakeBinlogWriter) GetLogs() (
	map[storage.FieldID]*datapb.FieldBinlog,
	*datapb.FieldBinlog,
	map[storage.FieldID]*datapb.FieldBinlog,
	string,
	[]int64,
) {
	return w.fieldBinlogs, w.statsLog, w.bm25StatsLog, w.manifest, w.expirQuantile
}

// runExecuteImportPlan runs executeImportPlan against a set of in-memory
// fragment sources and a stub final writer, so the real merge and final
// transform run while the read and write boundaries are controlled.
func runExecuteImportPlan(t *testing.T, fake *planFakeBinlogWriter, sources map[string]Source, req *datapb.ImportTaskV3Request) ([]*datapb.ImportTaskV3Result, error) {
	t.Helper()
	srcMock := mockey.Mock(SourceFromFragment).To(
		func(ref *importv3pb.FragmentRef, _ *schemapb.CollectionSchema, _ int64, _ *indexpb.StorageConfig, _ *indexcgopb.StoragePluginContext) (Source, error) {
			source, ok := sources[ref.GetPath()]
			require.True(t, ok, "unexpected fragment ref %q", ref.GetPath())
			return source, nil
		}).Build()
	defer srcMock.UnPatch()

	writerMock := mockey.Mock(storage.NewBinlogRecordWriter).To(
		func(context.Context, storage.UniqueID, storage.UniqueID, storage.UniqueID, *schemapb.CollectionSchema, allocator.Interface, uint64, int64, ...storage.RwOption) (storage.BinlogRecordWriter, error) {
			return fake, nil
		}).Build()
	defer writerMock.UnPatch()

	return executeImportPlan(context.Background(), nil, req, req.GetPlan(), nil)
}

// TestExecuteImportPlanManifestLastWrite pins the Storage V3 happy path: two
// fragments merge into one formal segment whose only boundary artifact is the
// manifest path (never the FieldBinlog arrays), and the merged rows match the
// written rows.
func TestExecuteImportPlanManifestLastWrite(t *testing.T) {
	initReshardPipelineParams(t)
	schema := importPlanTestSchema()
	fragmentSchema := importutilv2.FragmentSchema(schema, false)
	sources := map[string]Source{
		"f1": planFragmentSource(t, "f1", []int64{1, 2}, []string{"a", "b"}, fragmentSchema),
		"f2": planFragmentSource(t, "f2", []int64{3}, []string{"c"}, fragmentSchema),
	}
	fake := &planFakeBinlogWriter{rows: 3, manifest: "import_v3/1/reshard/2/manifests/3.pb"}
	req := newImportPlanRequest(schema,
		[]*importv3pb.FragmentRef{{Path: "f1", Rows: 2}, {Path: "f2", Rows: 1}},
		3, defaultImportPlanWriterSpec(schema))

	results, err := runExecuteImportPlan(t, fake, sources, req)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, int64(3), results[0].GetRows())
	require.Equal(t, "import_v3/1/reshard/2/manifests/3.pb", results[0].GetManifestPath())
	require.Empty(t, results[0].GetInsertLogs(), "Storage V3 crosses the boundary by manifest only")
	require.Nil(t, results[0].GetPkLog())
	require.Empty(t, results[0].GetBm25Logs())
	// The merged record reached the formal writer exactly once.
	require.Equal(t, 1, fake.writeCount)
	require.Equal(t, []int{3}, fake.written)
}

// TestExecuteImportPlanBinlogBranch pins the Storage V2 branch: with no manifest
// path the result carries the sorted FieldBinlogs, the PK stats log and the
// BM25 logs instead.
func TestExecuteImportPlanBinlogBranch(t *testing.T) {
	initReshardPipelineParams(t)
	schema := importPlanTestSchema()
	fragmentSchema := importutilv2.FragmentSchema(schema, false)
	sources := map[string]Source{
		"f1": planFragmentSource(t, "f1", []int64{1, 2}, []string{"a", "b"}, fragmentSchema),
	}
	statsLog := &datapb.FieldBinlog{FieldID: common.RowIDField}
	bm25 := &datapb.FieldBinlog{FieldID: 102}
	fake := &planFakeBinlogWriter{
		rows: 2,
		fieldBinlogs: map[storage.FieldID]*datapb.FieldBinlog{
			100: {FieldID: 100},
		},
		statsLog:     statsLog,
		bm25StatsLog: map[storage.FieldID]*datapb.FieldBinlog{102: bm25},
	}
	req := newImportPlanRequest(schema,
		[]*importv3pb.FragmentRef{{Path: "f1", Rows: 2}}, 2, defaultImportPlanWriterSpec(schema))

	results, err := runExecuteImportPlan(t, fake, sources, req)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, int64(2), results[0].GetRows())
	require.Empty(t, results[0].GetManifestPath())
	require.Len(t, results[0].GetInsertLogs(), 1)
	require.Equal(t, int64(100), results[0].GetInsertLogs()[0].GetFieldID())
	require.Same(t, statsLog, results[0].GetPkLog())
	require.Len(t, results[0].GetBm25Logs(), 1)
	require.Equal(t, int64(102), results[0].GetBm25Logs()[0].GetFieldID())
}

// TestExecuteImportPlanRejectsWriterRowMismatch pins the integrity guard: a
// writer that reports a different row count than the merge produced must fail
// the run instead of publishing a segment for the wrong number of rows.
func TestExecuteImportPlanRejectsWriterRowMismatch(t *testing.T) {
	initReshardPipelineParams(t)
	schema := importPlanTestSchema()
	fragmentSchema := importutilv2.FragmentSchema(schema, false)
	sources := map[string]Source{
		"f1": planFragmentSource(t, "f1", []int64{1, 2}, []string{"a", "b"}, fragmentSchema),
	}
	fake := &planFakeBinlogWriter{rows: 999} // merged rows are 2
	req := newImportPlanRequest(schema,
		[]*importv3pb.FragmentRef{{Path: "f1", Rows: 2}}, 2, defaultImportPlanWriterSpec(schema))

	_, err := runExecuteImportPlan(t, fake, sources, req)
	require.Error(t, err)
	require.ErrorContains(t, err, "writer/result row mismatch")
}

func TestBuildImportV3WriterOptionsValidation(t *testing.T) {
	schema := importPlanTestSchema()
	segmentSchema := typeutil.AppendSystemFields(schema)
	storageConfig := &indexpb.StorageConfig{RootPath: "root", BucketName: "bucket"}
	validGroups := []*importv3pb.ColumnGroupSpec{
		{Id: 0, FieldIds: []int64{100}, Format: "parquet"},
		{Id: 1, FieldIds: []int64{101, common.RowIDField, common.TimeStampField}, Format: "parquet"},
	}
	valid := func(mutate func(spec *importv3pb.WriterSpec)) *importv3pb.WriterSpec {
		spec := &importv3pb.WriterSpec{
			StorageVersion: storage.StorageV3,
			Format:         "parquet",
			BloomType:      "BasicBloomFilter",
			BloomFpp:       0.01,
			Groups:         validGroups,
		}
		if mutate != nil {
			mutate(spec)
		}
		return spec
	}

	t.Run("valid", func(t *testing.T) {
		options, err := buildImportV3WriterOptions(storageConfig, 1, 10, 5, segmentSchema, valid(nil))
		require.NoError(t, err)
		require.NotEmpty(t, options)
	})

	t.Run("unsupported bloom type", func(t *testing.T) {
		_, err := buildImportV3WriterOptions(storageConfig, 1, 10, 5, segmentSchema, valid(func(s *importv3pb.WriterSpec) {
			s.BloomType = "AlwaysTrueBloomFilter"
		}))
		require.ErrorContains(t, err, "Bloom filter config is invalid")
	})

	t.Run("bloom fpp out of range", func(t *testing.T) {
		for _, fpp := range []float64{0, -1, 1, 2} {
			_, err := buildImportV3WriterOptions(storageConfig, 1, 10, 5, segmentSchema, valid(func(s *importv3pb.WriterSpec) {
				s.BloomFpp = fpp
			}))
			require.ErrorContains(t, err, "Bloom filter config is invalid", "fpp=%v", fpp)
		}
	})

	t.Run("empty writer format", func(t *testing.T) {
		_, err := buildImportV3WriterOptions(storageConfig, 1, 10, 5, segmentSchema, valid(func(s *importv3pb.WriterSpec) {
			s.Format = ""
		}))
		require.ErrorContains(t, err, "writer format is empty")
	})

	t.Run("non-positive V2 sizes", func(t *testing.T) {
		_, err := buildImportV3WriterOptions(storageConfig, 1, 10, 5, segmentSchema, valid(func(s *importv3pb.WriterSpec) {
			s.V2 = &importv3pb.V2PackedIOConfig{BufferSize: 0, MultipartSize: 1}
		}))
		require.ErrorContains(t, err, "V2 IO sizes must be positive")

		_, err = buildImportV3WriterOptions(storageConfig, 1, 10, 5, segmentSchema, valid(func(s *importv3pb.WriterSpec) {
			s.V2 = &importv3pb.V2PackedIOConfig{BufferSize: 1, MultipartSize: -1}
		}))
		require.ErrorContains(t, err, "V2 IO sizes must be positive")
	})

	t.Run("invalid TEXT config", func(t *testing.T) {
		cases := []*importv3pb.TextColumnWriteSpec{
			nil,
			{FieldId: common.StartOfUserFieldID - 1, InlineLimit: 1, LobLimit: 1, FlushLimit: 1},
			{FieldId: 100, InlineLimit: -1, LobLimit: 1, FlushLimit: 1},
			{FieldId: 100, InlineLimit: 1, LobLimit: 0, FlushLimit: 1},
			{FieldId: 100, InlineLimit: 1, LobLimit: 1, FlushLimit: 0},
		}
		for i, text := range cases {
			_, err := buildImportV3WriterOptions(storageConfig, 1, 10, 5, segmentSchema, valid(func(s *importv3pb.WriterSpec) {
				s.Text = []*importv3pb.TextColumnWriteSpec{text}
			}))
			require.ErrorContains(t, err, "TEXT config is invalid", "case=%d", i)
		}
	})

	t.Run("valid TEXT config is accepted", func(t *testing.T) {
		_, err := buildImportV3WriterOptions(storageConfig, 1, 10, 5, segmentSchema, valid(func(s *importv3pb.WriterSpec) {
			s.Text = []*importv3pb.TextColumnWriteSpec{{FieldId: 100, InlineLimit: 1, LobLimit: 2, FlushLimit: 3}}
		}))
		require.NoError(t, err)
	})

	t.Run("invalid column groups propagate", func(t *testing.T) {
		_, err := buildImportV3WriterOptions(storageConfig, 1, 10, 5, segmentSchema, valid(func(s *importv3pb.WriterSpec) {
			s.Groups = nil
		}))
		require.ErrorContains(t, err, "column groups are empty")
	})
}

func TestImportV3GroupsValidation(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "note", DataType: schemapb.DataType_VarChar},
	}}

	t.Run("empty specs", func(t *testing.T) {
		_, err := importV3Groups(schema, nil)
		require.ErrorContains(t, err, "column groups are empty")
	})

	t.Run("incomplete group", func(t *testing.T) {
		_, err := importV3Groups(schema, []*importv3pb.ColumnGroupSpec{
			{Id: 0, FieldIds: nil, Format: "parquet"},
		})
		require.ErrorContains(t, err, "column group is incomplete")
		_, err = importV3Groups(schema, []*importv3pb.ColumnGroupSpec{
			{Id: 0, FieldIds: []int64{100}, Format: ""},
		})
		require.ErrorContains(t, err, "column group is incomplete")
	})

	t.Run("unknown column", func(t *testing.T) {
		_, err := importV3Groups(schema, []*importv3pb.ColumnGroupSpec{
			{Id: 0, FieldIds: []int64{100, 101, 999}, Format: "parquet"},
		})
		require.ErrorContains(t, err, "references unknown field 999")
	})

	t.Run("overlapping groups", func(t *testing.T) {
		_, err := importV3Groups(schema, []*importv3pb.ColumnGroupSpec{
			{Id: 0, FieldIds: []int64{100}, Format: "parquet"},
			{Id: 1, FieldIds: []int64{100, 101}, Format: "parquet"},
		})
		require.ErrorContains(t, err, "overlap at field 100")
	})

	t.Run("incomplete cover", func(t *testing.T) {
		_, err := importV3Groups(schema, []*importv3pb.ColumnGroupSpec{
			{Id: 0, FieldIds: []int64{100}, Format: "parquet"},
		})
		require.ErrorContains(t, err, "do not cover target schema")
	})

	t.Run("full cover succeeds", func(t *testing.T) {
		groups, err := importV3Groups(schema, []*importv3pb.ColumnGroupSpec{
			{Id: 0, FieldIds: []int64{100}, Format: "parquet"},
			{Id: 1, FieldIds: []int64{101}, Format: "parquet"},
		})
		require.NoError(t, err)
		require.Len(t, groups, 2)
		require.Equal(t, []int64{100}, groups[0].Fields)
		require.Equal(t, []int64{101}, groups[1].Fields)
	})
}
