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

package binlog

import (
	"context"
	"fmt"
	"io"
	"math"
	"path"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"
	"golang.org/x/exp/slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSnapshotBitmapTextAndLocalDeletes(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	root := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
	cm := storage.NewLocalChunkManager()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 0, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: 1, Name: "timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
	}}
	segmentPath := storage.SegmentManifestBasePath(root, 1, 2, 3)
	w, err := storage.NewPackedTextBatchWriter("", segmentPath, schema, 0, 0,
		[]storagecommon.ColumnGroup{
			{GroupID: 0, Columns: []int{0, 1, 2}, Fields: []int64{0, 1, 100}},
			{GroupID: 1, Columns: []int{3}, Fields: []int64{101}},
		}, cfg, []packed.TextColumnConfig{{
			FieldID:         101,
			LobBasePath:     path.Join(storage.SegmentPartitionBasePath(root, 1, 2), "lobs", "101"),
			InlineThreshold: 1, MaxLobFileBytes: 1 << 20, FlushThresholdBytes: 1,
		}}, "parquet", []string{"parquet", "parquet"})
	require.NoError(t, err)
	var values []*storage.Value
	for i := int64(0); i < 130; i++ {
		values = append(values, &storage.Value{Value: map[int64]any{0: i, 1: int64(100), 100: i, 101: strings.Repeat(fmt.Sprint(i), 100)}})
	}
	// Separate writes exercise multiple underlying chunks/file ranges.
	for _, batch := range [][]*storage.Value{values[:63], values[63:]} {
		record, err := storage.ValueSerializer(batch, schema)
		require.NoError(t, err)
		require.NoError(t, w.Write(record))
		record.Release()
	}
	output, err := w.Close()
	require.NoError(t, err)
	defer output.Destroy()
	manifest, err := packed.CommitManifestUpdates(segmentPath, packed.ManifestEarliest, cfg, &packed.ManifestUpdates{NewFiles: output})
	require.NoError(t, err)
	paths := []string{path.Join(root, "shared.delta"), path.Join(root, "own.delta")}
	for i, file := range paths {
		var pks []storage.PrimaryKey
		var timestamps []uint64
		for pk := int64(0); pk < 130; pk += 2 {
			if i == 1 {
				pk = 129
			}
			pks = append(pks, storage.NewInt64PrimaryKey(pk))
			timestamps = append(timestamps, 200)
		}
		record, _, _, err := storage.BuildDeleteRecord(pks, timestamps)
		require.NoError(t, err)
		dw, err := storage.NewDeltalogWriter(ctx, 1, 2, 3, int64(i+1), schemapb.DataType_Int64, file,
			storage.WithVersion(storage.StorageV2), storage.WithStorageConfig(cfg))
		require.NoError(t, err)
		require.NoError(t, dw.Write(record))
		record.Release()
		require.NoError(t, dw.Close())
	}
	manifest, err = packed.AddDeltaLogsToManifest(manifest, cfg, []packed.DeltaLogEntry{{Path: paths[1], NumEntries: 1}})
	require.NoError(t, err)
	l0, err := packed.AddDeltaLogsToManifest(packed.MarshalManifestPath(path.Join(root, "l0"), packed.ManifestLatest), cfg,
		[]packed.DeltaLogEntry{{Path: paths[0], NumEntries: 65}})
	require.NoError(t, err)
	input := &internalpb.SnapshotImportL0Source{ManifestL0Paths: []string{l0, l0}}
	source := &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest}
	for _, batchBudget := range []int64{int64(len(paths[0])) + 64 + 256, 1 << 20} {
		opens := make(map[string]int)
		patch := mockey.Mock((*DeleteMerger).mergeFile).When(func(_ *DeleteMerger, _ context.Context, path string, _, _ uint64, _ int64) bool {
			opens[path]++
			return false
		}).Return(false, nil).Build()
		masks, err := BuildSnapshotDeleteMasks(ctx, cm, schema, cfg, []*internalpb.SnapshotImportSource{source}, input,
			0, 300, batchBudget, 1<<20, SourceEncryption{}, nil)
		patch.UnPatch()
		require.NoError(t, err)
		require.Equal(t, map[string]int{paths[0]: 1, paths[1]: 1}, opens)
		r, err := NewStorageV3ManifestReaderWithSharedL0(ctx, cm, schema, cfg, manifest, 0, 300, 1,
			SourceEncryption{}, source, batchBudget, masks, nil)
		require.NoError(t, err)
		var got []int64
		for {
			data, err := r.Read()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			for i := 0; i < data.GetRowNum(); i++ {
				pk := data.Data[100].GetRow(i).(int64)
				got = append(got, pk)
				require.Equal(t, strings.Repeat(fmt.Sprint(pk), 100), data.Data[101].GetRow(i))
			}
		}
		r.Close()
		var want []int64
		for pk := int64(1); pk < 129; pk += 2 {
			want = append(want, pk)
		}
		require.Equal(t, want, got)
	}
}

func TestSnapshotBitmapBoundsAndBatchFailure(t *testing.T) {
	budget := &maskBudget{limit: maskBlockWords*8 + 64}
	mask := &rowDeleteMask{rows: -1}
	require.NoError(t, mask.prepareRow(0, budget))
	require.False(t, mask.deleted(0))
	require.ErrorIs(t, mask.prepareRow(maskBlockWords*64, budget), merr.ErrServiceResourceInsufficient)
	require.NoError(t, mask.finish(1))
	require.NoError(t, mask.prepareRow(0, budget))
	require.ErrorIs(t, mask.prepareRow(1, budget), merr.ErrDataIntegrity)
	require.ErrorIs(t, mask.finish(0), merr.ErrDataIntegrity)

	for _, mode := range []string{"success", "consume_error", "oversized_key"} {
		t.Run(mode, func(t *testing.T) {
			m, err := NewDeleteMerger(nil, nil, schemapb.DataType_VarChar, 1, 130)
			require.NoError(t, err)
			var seen []map[any]uint64
			m.consume = func(batch map[any]uint64) error {
				if mode == "consume_error" {
					return merr.ErrIoPermissionDenied
				}
				copy := make(map[any]uint64)
				for k, v := range batch {
					copy[k] = v
				}
				seen = append(seen, copy)
				return nil
			}
			require.NoError(t, m.merge("a", 10))
			require.NoError(t, m.merge("a", 20))
			if mode == "oversized_key" {
				require.ErrorIs(t, m.merge("long", 30), merr.ErrServiceResourceInsufficient)
				return
			}
			err = m.merge("b", 30)
			if mode == "consume_error" {
				require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
				return
			}
			require.NoError(t, err)
			require.NoError(t, m.merge("a", 40))
			require.NoError(t, m.flush())
			require.NoError(t, m.flush())
			require.Equal(t, []map[any]uint64{{"a": 20}, {"b": 30}, {"a": 40}}, seen)
		})
	}
}

func TestSnapshotBitmapScanFailures(t *testing.T) {
	for _, mode := range []string{"empty", "open", "read", "cancel", "cancel_after_open", "commit", "bitmap_budget", "shorter", "longer", "null_ts", "wrong_pk"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 1, DataType: schemapb.DataType_Int64},
			}}
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
				{Name: "pk", Type: arrow.PrimitiveTypes.Int64}, {Name: "ts", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil))
			builder.Field(0).(*array.Int64Builder).Append(1)
			if mode == "null_ts" {
				builder.Field(1).AppendNull()
			} else {
				builder.Field(1).(*array.Int64Builder).Append(100)
			}
			owner := &storageV3DeltaRecordReader{record: storage.NewSimpleArrowRecord(builder.NewRecord(), map[int64]int{100: 0, 1: 1})}
			builder.Release()
			defer owner.Close()
			if mode == "empty" {
				owner.read = true
			}
			var openErr error
			if mode == "open" {
				openErr = merr.ErrIoPermissionDenied
			}
			open := mockey.Mock(storage.NewManifestRecordReader).To(func(context.Context, string, *schemapb.CollectionSchema, ...storage.RwOption) (storage.RecordReader, error) {
				if mode == "cancel_after_open" {
					cancel()
				}
				return owner, openErr
			}).Build()
			defer open.UnPatch()
			if mode == "read" {
				p := mockey.Mock((*storageV3DeltaRecordReader).Next).Return(nil, merr.ErrIoKeyNotFound).Build()
				defer p.UnPatch()
			}
			scan := snapshotMaskScan{source: &internalpb.SnapshotImportSource{}, schema: schema, mask: &rowDeleteMask{rows: -1}}
			budget := &maskBudget{limit: 1 << 20}
			var want error = merr.ErrDataIntegrity
			switch mode {
			case "cancel":
				cancel()
				want = context.Canceled
			case "cancel_after_open":
				want = context.Canceled
			case "open":
				want = openErr
			case "read":
				want = merr.ErrIoKeyNotFound
			case "commit":
				scan.source.SourceCommitTimestamp = 99
			case "bitmap_budget":
				budget.limit = 1
				want = merr.ErrServiceResourceInsufficient
			case "shorter":
				scan.mask.rows = 2
			case "longer":
				scan.mask.rows = 0
			case "wrong_pk":
				schema.Fields[0].DataType = schemapb.DataType_VarChar
			}
			err := scan.apply(ctx, map[any]uint64{int64(1): 200}, budget)
			if mode == "empty" {
				require.NoError(t, err)
				require.Zero(t, scan.mask.rows)
			} else {
				require.ErrorIs(t, err, want)
			}
		})
	}
}

func TestSnapshotBitmapFinalReaderCountGuard(t *testing.T) {
	for _, mode := range []string{"shorter", "longer", "missing_mask", "empty_container", "nil_masks", "nil_source", "inline_l0"} {
		t.Run(mode, func(t *testing.T) {
			paramtable.Init()
			schema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}})
			patchStorageV3TestFieldIDs(t, 100)
			fragments := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
			defer fragments.UnPatch()
			record, err := storage.ValueSerializer([]*storage.Value{{Value: map[int64]any{0: int64(1), 1: int64(100), 100: int64(1)}}}, schema)
			require.NoError(t, err)
			owner := &storageV3DeltaRecordReader{record: record}
			defer owner.Close()
			open := mockey.Mock(storage.NewManifestRecordReader).Return(owner, nil).Build()
			defer open.UnPatch()
			source := &internalpb.SnapshotImportSource{Version: 1, ManifestPath: packed.MarshalManifestPath("source", 1)}
			masks := &SnapshotL0Deletes{masks: make(map[snapshotMaskKey]*rowDeleteMask)}
			if mode != "missing_mask" {
				rows := int64(0)
				if mode == "shorter" {
					rows = 2
				}
				masks.masks[maskKey(source)] = &rowDeleteMask{rows: rows, blocks: []*[maskBlockWords]uint64{new([maskBlockWords]uint64)}}
			}
			manifest := source.ManifestPath
			switch mode {
			case "empty_container":
				masks = &SnapshotL0Deletes{}
			case "nil_masks":
				masks = nil
			case "nil_source":
				source = nil
			case "inline_l0":
				source.LegacyL0Deltalogs = []string{"inline.delta"}
			}
			r, err := NewStorageV3ManifestReaderWithSharedL0(context.Background(), nil, schema, nil, manifest,
				0, math.MaxUint64, 1024, SourceEncryption{}, source, 1024, masks, nil)
			if mode != "shorter" && mode != "longer" {
				want := merr.ErrServiceInternal
				if mode == "inline_l0" {
					want = merr.ErrServiceUnimplemented
				}
				require.ErrorIs(t, err, want)
				require.Zero(t, open.Times(), "invalid shared state must fail before opening storage")
				return
			}
			require.NoError(t, err)
			_, err = r.Read()
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			_, again := r.Read()
			require.Same(t, err, again)
			require.Nil(t, r.dr)
			require.Nil(t, r.deleteMask)
		})
	}
}

func TestSnapshotBitmapPreparationFailures(t *testing.T) {
	for _, mode := range []string{"empty", "duplicate", "pk", "cancel", "manifest", "bitmap_budget", "prepare", "shared", "shared_scan", "own_paths", "own_validate", "own_merger", "own_read", "own_flush", "empty_scan"} {
		t.Run(mode, func(t *testing.T) {
			paramtable.Init()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}}
			source := &internalpb.SnapshotImportSource{Version: 1, ManifestPath: packed.MarshalManifestPath("source", 1)}
			sources := []*internalpb.SnapshotImportSource{source}
			if mode == "duplicate" {
				sources = append(sources, source)
			}
			limit := int64(1 << 20)
			switch mode {
			case "pk":
				schema.Fields = nil
			case "cancel":
				cancel()
			case "manifest":
				source.ManifestPath = ""
			case "bitmap_budget":
				limit = 1
			case "own_merger":
				schema.Fields[0].DataType = schemapb.DataType_Float
			}
			var prepareErr error
			if mode == "prepare" {
				prepareErr = merr.ErrIoPermissionDenied
			}
			fields := mockey.Mock(packed.GetManifestFieldIDs).Return(map[int64]struct{}{0: {}, 1: {}, 100: {}}, prepareErr).Build()
			defer fields.UnPatch()
			fragments := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
			defer fragments.UnPatch()
			var deltaPaths []string
			if strings.HasPrefix(mode, "own_") {
				deltaPaths = []string{"own.delta"}
			}
			var pathsErr error
			if mode == "own_paths" {
				pathsErr = merr.ErrIoKeyNotFound
			}
			paths := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(deltaPaths, pathsErr).Build()
			defer paths.UnPatch()
			open := mockey.Mock(storage.NewManifestRecordReader).To(func(context.Context, string, *schemapb.CollectionSchema, ...storage.RwOption) (storage.RecordReader, error) {
				if mode == "shared_scan" || mode == "own_flush" || mode == "empty_scan" {
					return nil, merr.ErrIoPermissionDenied
				}
				record, err := storage.ValueSerializer([]*storage.Value{{Value: map[int64]any{0: int64(1), 1: int64(100), 100: int64(1)}}}, typeutil.AppendSystemFields(schema))
				return &storageV3DeltaRecordReader{record: record}, err
			}).Build()
			defer open.UnPatch()
			load := mockey.Mock(loadSnapshotL0Deletes).To(func(ctx context.Context, _ storage.ChunkManager, _ *schemapb.CollectionSchema,
				_ *indexpb.StorageConfig, _ *internalpb.SnapshotImportL0Source, _, _ uint64, _ int64,
				consume func(map[any]uint64) error, _ func(string) error,
			) (*DeleteMerger, error) {
				if mode == "shared" {
					return nil, merr.ErrIoKeyNotFound
				}
				if mode == "shared_scan" {
					if err := consume(map[any]uint64{int64(1): 200}); err != nil {
						return nil, err
					}
				}
				return NewDeleteMerger(nil, nil, schemapb.DataType_Int64, 1, 1024)
			}).Build()
			defer load.UnPatch()
			delta := mockey.Mock(storage.NewDeltalogReader).To(func(context.Context, schemapb.DataType, []string, ...storage.RwOption) (storage.RecordReader, error) {
				if mode == "own_read" {
					return nil, merr.ErrIoPermissionDenied
				}
				record, _, _, err := storage.BuildDeleteRecord([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)}, []uint64{200})
				return &storageV3DeltaRecordReader{record: record}, err
			}).Build()
			defer delta.UnPatch()
			validate := func(path string) error {
				if mode == "own_validate" && path == "own.delta" {
					return merr.ErrIoPermissionDenied
				}
				return nil
			}
			result, err := BuildSnapshotDeleteMasks(ctx, nil, schema, nil, sources, &internalpb.SnapshotImportL0Source{},
				0, 300, 1024, limit, SourceEncryption{}, validate)
			if mode == "empty" || mode == "duplicate" {
				require.NoError(t, err)
				require.Len(t, result.masks, 1)
				require.EqualValues(t, 1, result.masks[maskKey(source)].rows)
			} else {
				require.Error(t, err)
				require.Nil(t, result, "never publish masks from a failed preparation")
				if mode == "cancel" {
					require.ErrorIs(t, err, context.Canceled)
				}
			}
		})
	}
}

func TestSnapshotL0Batches(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_VarChar}}}
	cm := storage.NewLocalChunkManager()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	file := path.Join(cfg.RootPath, "shared.delta")
	record, _, _, err := storage.BuildDeleteRecord([]storage.PrimaryKey{storage.NewVarCharPrimaryKey("key"), storage.NewVarCharPrimaryKey("key")}, []uint64{200, 500})
	require.NoError(t, err)
	writer, err := storage.NewDeltalogWriter(ctx, 1, 10, 30, 1, schemapb.DataType_VarChar, file,
		storage.WithVersion(storage.StorageV1), storage.WithStorageConfig(cfg), storage.WithUploader(cm.MultiWrite))
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	record.Release()
	require.NoError(t, writer.Close())
	inventory := &internalpb.SnapshotImportL0Source{LegacyL0Deltalogs: []string{file, file}}
	pathCharge := int64(len(file)) + 64
	budget := int64(259) + pathCharge
	batches := 0
	consume := func(batch map[any]uint64) error {
		batches++
		require.Equal(t, map[any]uint64{"key": 200}, batch, "range filtering must happen before max aggregation")
		return nil
	}
	_, err = loadSnapshotL0Deletes(ctx, cm, schema, cfg, inventory, 0, 450, pathCharge+130, consume, nil)
	require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient, "one key and the retained inventory must fit the batch budget")
	require.Zero(t, batches)
	merger, err := loadSnapshotL0Deletes(ctx, cm, schema, cfg, inventory, 0, 450, budget, consume, nil)
	require.NoError(t, err)
	require.Equal(t, 1, batches, "duplicate paths must not be decoded twice")
	require.Empty(t, merger.data, "no timestamp map is retained after consuming the final batch")
	require.EqualValues(t, pathCharge, merger.used)
	require.EqualValues(t, pathCharge, merger.fixed)
	require.Len(t, merger.loadedPaths, 1)

	// Segment-local loading reuses the scratch map and skips objects already
	// folded into every mask, even if the object is no longer readable.
	require.NoError(t, cm.Remove(ctx, file))
	_, err = merger.Merge(ctx, []string{file}, 0, 450, true)
	require.NoError(t, err)
	require.Empty(t, merger.data)
	_, err = merger.Merge(ctx, []string{file}, 0, 450, false)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)

	for _, mode := range []string{"empty", "missing", "budget", "canceled", "conflict", "missing_consumer"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			budget := int64(259)
			input := &internalpb.SnapshotImportL0Source{}
			consume := func(map[any]uint64) error { return nil }
			switch mode {
			case "missing":
				input.LegacyL0Deltalogs = []string{file}
			case "budget":
				budget = 0
			case "canceled":
				cancel()
			case "missing_consumer":
				consume = nil
			case "conflict":
				// An empty real object is enough to reach cross-list validation.
				w, err := storage.NewDeltalogWriter(ctx, 1, 10, 30, 1, schemapb.DataType_VarChar, file,
					storage.WithVersion(storage.StorageV1), storage.WithUploader(cm.MultiWrite))
				require.NoError(t, err)
				require.NoError(t, w.Close())
				input.LegacyL0Deltalogs = []string{file}
				input.ManifestL0Paths = []string{packed.MarshalManifestPath("l0", 1)}
				patch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string{file}, nil).Build()
				defer patch.UnPatch()
			}
			result, err := loadSnapshotL0Deletes(ctx, cm, schema, cfg, input, 0, 450, budget, consume, nil)
			if mode == "empty" {
				require.NoError(t, err)
				require.Empty(t, result.data)
			} else {
				require.Error(t, err)
				require.Nil(t, result, "never return a partially consumed merger")
				if mode == "conflict" {
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
				}
				if mode == "missing_consumer" {
					require.ErrorIs(t, err, merr.ErrServiceInternal)
				}
			}
		})
	}
}

func TestSnapshotL0DeferredManifest(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}}
	manifest := packed.MarshalManifestPath("root/l0", 7)
	for _, mode := range []string{"dedup", "manifest_error", "manifest_boundary", "delete_boundary", "latest", "inventory_budget", "canceled_before_manifest", "canceled_after_manifest"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := &internalpb.SnapshotImportL0Source{ManifestL0Paths: []string{manifest, packed.MarshalManifestPath("s3://source/root/l0", 7)}}
			if mode == "latest" {
				source.ManifestL0Paths = []string{packed.MarshalManifestPath("root/l0", packed.ManifestLatest)}
			}
			if mode == "canceled_after_manifest" {
				source.ManifestL0Paths[1] = packed.MarshalManifestPath("root/l0", 8)
			}
			resolve := mockey.Mock(packed.GetDeltaLogPathsFromManifest).To(func(got string, _ *indexpb.StorageConfig) ([]string, error) {
				require.Equal(t, manifest, got)
				if mode == "manifest_error" {
					return nil, merr.ErrIoKeyNotFound
				}
				if mode == "canceled_after_manifest" {
					cancel()
				}
				return []string{"s3://source/root/delete", "root/delete"}, nil
			}).Build()
			defer resolve.UnPatch()
			open := mockey.Mock(storage.NewDeltalogReader).To(func(_ context.Context, _ schemapb.DataType, paths []string, _ ...storage.RwOption) (storage.RecordReader, error) {
				require.Equal(t, []string{"root/delete"}, paths)
				return &storageV3DeltaRecordReader{read: true}, nil
			}).Build()
			defer open.UnPatch()
			validate := func(path string) error {
				if mode == "canceled_before_manifest" {
					cancel()
				}
				if mode == "manifest_boundary" && path == "root/l0" || mode == "delete_boundary" && path == "s3://source/root/delete" {
					return merr.ErrParameterInvalid
				}
				return nil
			}
			budget := int64(1024)
			if mode == "inventory_budget" {
				budget = 1
			}
			merger, err := loadSnapshotL0Deletes(ctx, nil, schema, nil, source, 0, math.MaxUint64, budget, func(map[any]uint64) error { return nil }, validate)
			if mode == "dedup" {
				require.NoError(t, err)
				require.EqualValues(t, 1, resolve.Times())
				require.EqualValues(t, 1, open.Times())
				require.Equal(t, map[string]bool{"root/delete": false}, merger.loadedPaths)
				require.EqualValues(t, len("root/delete")+64, merger.used, "aliases charge retained path memory once")
			} else {
				require.Error(t, err)
				require.Nil(t, merger)
				require.Zero(t, open.Times(), "invalid input must fail before opening a delete reader")
				if mode == "manifest_error" {
					require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
				}
			}
		})
	}
}

func TestSnapshotReaderDeferredPhysicalValidation(t *testing.T) {
	paramtable.Init()
	patchStorageV3TestFieldIDs(t, 100, 101, 102)
	type sourceCM struct{ storage.ChunkManager }
	for _, mode := range []string{"manifest", "fragment", "delete", "lob_path", "lob_missing", "lob_error", "valid", "shared", "unprojected_lob", "partially_projected_lob", "shared_partially_projected_lob"} {
		t.Run(mode, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}, {FieldID: 101, DataType: schemapb.DataType_Text},
			}}
			if mode == "unprojected_lob" {
				schema.Fields = schema.Fields[:1]
			}
			fragments := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment{{FilePath: "root/data"}}, nil).Build()
			defer fragments.UnPatch()
			lobFiles := []packed.LobFileInfo{{FieldID: 101, Path: "s3://source/root/lob", FileSizeBytes: 123}}
			partialProjection := strings.HasSuffix(mode, "partially_projected_lob")
			if partialProjection {
				// Source TEXT 102 is absent from the target. None of its LOB
				// metadata or objects may be validated or counted for Import.
				lobFiles = append(lobFiles,
					packed.LobFileInfo{FieldID: 102, Path: "outside/dropped-lob", FileSizeBytes: -1},
					packed.LobFileInfo{FieldID: 102, Path: "root/missing-lob", FileSizeBytes: 456},
					packed.LobFileInfo{FieldID: 102},
				)
			}
			lob := mockey.Mock(packed.GetManifestLobFiles).Return(lobFiles, nil).Build()
			defer lob.UnPatch()
			delta := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string{"root/delete"}, nil).Build()
			defer delta.UnPatch()
			exist := mockey.Mock((*sourceCM).Exist).To(func(_ *sourceCM, _ context.Context, key string) (bool, error) {
				require.Equal(t, "root/lob", key)
				if mode == "lob_error" {
					return false, merr.ErrIoFailed
				}
				return mode != "lob_missing", nil
			}).Build()
			defer exist.UnPatch()
			open := mockey.Mock(storage.NewManifestRecordReader).Return(&storageV3DeltaRecordReader{read: true}, nil).Build()
			defer open.UnPatch()
			deletes := mockey.Mock(storage.NewDeltalogReader).Return(&storageV3DeltaRecordReader{read: true}, nil).Build()
			defer deletes.UnPatch()
			validate := func(path string) error {
				if path == "outside/dropped-lob" {
					return merr.ErrParameterInvalid
				}
				if mode == "manifest" && path == "root/segment" || mode == "fragment" && path == "root/data" || mode == "delete" && path == "root/delete" || mode == "lob_path" && path == "s3://source/root/lob" {
					return merr.ErrParameterInvalid
				}
				return nil
			}
			manifest := packed.MarshalManifestPath("root/segment", 1)
			var r *reader
			var err error
			if mode == "shared" || mode == "shared_partially_projected_lob" {
				source := &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest}
				masks := &SnapshotL0Deletes{masks: map[snapshotMaskKey]*rowDeleteMask{maskKey(source): {rows: 0}}}
				r, err = NewStorageV3ManifestReaderWithSharedL0(context.Background(), &sourceCM{}, schema, nil, manifest, 0, math.MaxUint64, 1024, SourceEncryption{},
					source, 1024, masks, validate)
			} else {
				r, err = NewStorageV3ManifestReader(context.Background(), &sourceCM{}, schema, nil, manifest, 0, math.MaxUint64, 1024, SourceEncryption{}, nil, 0, validate)
			}
			if mode == "valid" || mode == "shared" || mode == "unprojected_lob" || partialProjection {
				require.NoError(t, err)
				if partialProjection {
					require.EqualValues(t, 1, exist.Times(), "only the retained TEXT field needs a LOB existence check")
					require.EqualValues(t, 123, r.storageV3LobSize)
					require.Equal(t, []string{"root/data"}, r.storageV3Files, "dropped LOBs must not reach Size lookups")
				}
				r.Close()
				if mode == "unprojected_lob" {
					require.Zero(t, lob.Times())
					require.Zero(t, exist.Times())
				}
			} else {
				require.Error(t, err)
				require.Zero(t, open.Times())
				if mode == "lob_missing" {
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
				}
				if mode == "lob_error" {
					require.ErrorIs(t, err, merr.ErrIoFailed)
				}
			}
		})
	}
}

func TestSnapshotDeleteMapBudget(t *testing.T) {
	m, err := NewDeleteMerger(nil, nil, schemapb.DataType_Int64, 1, 260)
	require.NoError(t, err)
	require.NoError(t, m.merge(int64(1), 300))
	require.NoError(t, m.merge(int64(1), 200))
	require.EqualValues(t, 300, m.data[int64(1)])
	buffer := []byte("key")
	borrowed := unsafe.String(unsafe.SliceData(buffer), len(buffer))
	require.NoError(t, m.merge(borrowed, 100))
	require.NoError(t, m.merge(borrowed, 200))
	buffer[0] = 'x'
	require.EqualValues(t, 200, m.data["key"], "updating an equal key must not retain borrowed Arrow memory")
	require.EqualValues(t, 259, m.used, "replacement must not charge another key")
	err = m.merge(int64(2), 100)
	require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
	require.Contains(t, err.Error(), "260 byte budget (accounted=259, next key=128)")
	require.Contains(t, err.Error(), "dataNode.import.readDeleteBufferSizeInMB")
	require.Contains(t, err.Error(), "dataNode.import.memoryLimitPercentage")
	require.Len(t, m.data, 2)
}

func TestDeleteMergerInvalidDependencies(t *testing.T) {
	for _, tc := range []struct {
		name   string
		pkType schemapb.DataType
		budget int64
		want   error
	}{
		{"budget", schemapb.DataType_Int64, 0, merr.ErrServiceResourceInsufficient},
		{"pk_type", schemapb.DataType_Float, 1024, merr.ErrDataIntegrity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewDeleteMerger(nil, nil, tc.pkType, 1, tc.budget)
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, m)
		})
	}
	for _, schema := range []*schemapb.CollectionSchema{
		{}, {Fields: []*schemapb.FieldSchema{{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Float}}},
	} {
		shared, err := loadSnapshotL0Deletes(context.Background(), nil, schema, nil, nil, 0, math.MaxUint64, 1024, func(map[any]uint64) error { return nil }, nil)
		require.Error(t, err)
		require.Nil(t, shared)
	}
	m, err := NewDeleteMerger(nil, nil, schemapb.DataType_Int64, 1, 1024)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	data, err := m.Merge(ctx, []string{"unopened"}, 0, math.MaxUint64, false)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, data)
}

func TestSnapshotDeleteReadErrors(t *testing.T) {
	type failingDeltaReader struct{ storage.RecordReader }
	for _, mode := range []string{"open", "read", "terminal_after_record", "bad_timestamp", "bad_pk", "cancel_after_record", "cancel_before_read", "budget"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			budget := int64(1024)
			if mode == "budget" {
				budget = 1
			}
			m, err := NewDeleteMerger(nil, nil, schemapb.DataType_Int64, 1, budget)
			require.NoError(t, err)
			pkType, tsType := arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int64
			if mode == "bad_pk" {
				pkType = arrow.BinaryTypes.String
			}
			if mode == "bad_timestamp" {
				tsType = arrow.BinaryTypes.String
			}
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{{Name: "pk", Type: pkType}, {Name: "ts", Type: tsType}}, nil))
			for i := 0; i < 2; i++ {
				if b, ok := builder.Field(i).(*array.Int64Builder); ok {
					b.Append(1)
				} else {
					builder.Field(i).(*array.StringBuilder).Append("bad")
				}
			}
			record := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{0: 0, 1: 1})
			builder.Release()
			defer record.Release()
			cause := merr.ErrIoKeyNotFound
			reads, opens, closes := 0, 0, 0
			openPatch := mockey.Mock(storage.NewDeltalogReader).To(func(context.Context, schemapb.DataType, []string, ...storage.RwOption) (storage.RecordReader, error) {
				opens++
				if mode == "open" {
					return nil, cause
				}
				return &failingDeltaReader{}, nil
			}).Build()
			defer openPatch.UnPatch()
			nextPatch := mockey.Mock((*failingDeltaReader).Next).To(func(*failingDeltaReader) (storage.Record, error) {
				reads++
				if mode == "read" || reads > 1 {
					return nil, cause
				}
				if mode == "cancel_after_record" {
					cancel()
				}
				return record, nil
			}).Build()
			defer nextPatch.UnPatch()
			closePatch := mockey.Mock((*failingDeltaReader).Close).To(func(*failingDeltaReader) error { closes++; return nil }).Build()
			defer closePatch.UnPatch()
			if mode == "cancel_before_read" {
				cancel()
				_, err = m.mergeFile(ctx, "delta", 0, math.MaxUint64, storage.StorageV3)
			} else {
				var data map[any]uint64
				data, err = m.Merge(ctx, []string{"delta"}, 0, math.MaxUint64, true)
				require.Nil(t, data, "never publish partially decoded deletes")
			}
			require.Error(t, err)
			switch mode {
			case "open", "read":
				require.ErrorIs(t, err, cause)
				require.Equal(t, 2, opens)
			case "terminal_after_record":
				require.ErrorIs(t, err, cause)
				require.Equal(t, 1, opens)
			case "cancel_after_record", "cancel_before_read":
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 1, opens)
			case "budget":
				require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
				require.Equal(t, 1, opens)
			case "bad_pk", "bad_timestamp":
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
				require.Equal(t, 1, opens)
			}
			if mode != "open" {
				require.Equal(t, opens, closes)
			}
		})
	}
}

func TestSnapshotL0RealFiles(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(pkType.String(), func(t *testing.T) {
			root := t.TempDir()
			cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: root}
			cm := storage.NewLocalChunkManager()
			schema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{
					FieldID: 100, Name: "pk", DataType: pkType, IsPrimaryKey: true,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "100"}},
				},
			}})
			// Use real storage writers and readers. The data manifest has no own
			// deltas: successful filtering must come from the attached L0 files.
			writer, err := storage.NewBinlogRecordWriter(ctx, 1, 10, 20, schema, allocator.NewLocalAllocator(1, 1000), 1024*1024, 100,
				storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(cfg),
				storage.WithColumnGroups([]storagecommon.ColumnGroup{
					{GroupID: 0, Columns: []int{0}, Fields: []int64{100}},
					{GroupID: 1, Columns: []int{1, 2}, Fields: []int64{0, 1}},
				}),
				storage.WithUploader(cm.MultiWrite))
			require.NoError(t, err)
			var values []*storage.Value
			for i, row := range [][2]int64{{1, 100}, {1, 300}, {2, 100}, {3, 300}, {4, 100}} {
				var pk any = row[0]
				if pkType == schemapb.DataType_VarChar {
					pk = fmt.Sprint(row[0])
				}
				values = append(values, &storage.Value{Value: map[int64]any{100: pk, 0: int64(i + 1), 1: row[1]}})
			}
			record, err := storage.ValueSerializer(values, schema)
			require.NoError(t, err)
			require.NoError(t, writer.Write(record))
			record.Release()
			require.NoError(t, writer.Close())
			_, _, _, manifest, _ := writer.GetLogs()
			require.NotEmpty(t, manifest)
			paths := []string{path.Join(root, "legacy.delta"), path.Join(root, "packed.delta")}
			for i, version := range []int64{storage.StorageV1, storage.StorageV2} {
				deltaWriter, err := storage.NewDeltalogWriter(ctx, 1, 10, 30, int64(i+1), pkType, paths[i],
					storage.WithVersion(version), storage.WithStorageConfig(cfg), storage.WithUploader(cm.MultiWrite))
				require.NoError(t, err)
				arrowPK := arrow.PrimitiveTypes.Int64
				if pkType == schemapb.DataType_VarChar {
					arrowPK = arrow.BinaryTypes.String
				}
				builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
					{Name: "pk", Type: arrowPK}, {Name: "ts", Type: arrow.PrimitiveTypes.Int64},
				}, nil))
				rows := [][][2]int64{{{1, 200}, {2, 100}}, {{1, 150}, {1, 500}, {3, 400}}}[i]
				for _, row := range rows {
					if pkType == schemapb.DataType_Int64 {
						builder.Field(0).(*array.Int64Builder).Append(row[0])
					} else {
						builder.Field(0).(*array.StringBuilder).Append(fmt.Sprint(row[0]))
					}
					builder.Field(1).(*array.Int64Builder).Append(row[1])
				}
				deltaRecord := storage.NewSimpleArrowRecord(builder.NewRecord(), map[storage.FieldID]int{0: 0, 1: 1})
				builder.Release()
				require.NoError(t, deltaWriter.Write(deltaRecord))
				deltaRecord.Release()
				require.NoError(t, deltaWriter.Close())
			}
			l0Manifest, err := packed.AddDeltaLogsToManifest(packed.MarshalManifestPath(path.Join(root, "l0"), packed.ManifestLatest), cfg,
				[]packed.DeltaLogEntry{{Path: paths[1], NumEntries: 3}})
			require.NoError(t, err)
			budget := int64(512)
			for _, file := range paths {
				budget += int64(len(file)) + 64
			}
			// The same decoder/merger is usable without an insert reader or a
			// schema. Incremental calls retain max timestamps and own string keys.
			merger, err := NewDeleteMerger(cm, cfg, pkType, 1, budget)
			require.NoError(t, err)
			first, err := merger.Merge(ctx, paths[:1], 0, 450, true)
			require.NoError(t, err)
			require.Len(t, first, 2)
			merged, err := merger.Merge(ctx, paths[1:], 0, 450, true) // V2 fallback
			require.NoError(t, err)
			require.Len(t, merged, 3)
			require.Len(t, first, 3, "incremental calls share one owned map, not a copy")
			expectedBytes := int64(3 * 128)
			if pkType == schemapb.DataType_VarChar {
				expectedBytes += 3 // three one-byte owned string keys
			}
			require.EqualValues(t, expectedBytes, merger.used)
			merged, err = merger.Merge(ctx, paths[1:], 0, 450, false) // direct V3 packed path
			require.NoError(t, err)
			var consumed map[any]uint64
			_, err = loadSnapshotL0Deletes(ctx, cm, schema, cfg, &internalpb.SnapshotImportL0Source{
				LegacyL0Deltalogs: paths[:1], ManifestL0Paths: []string{l0Manifest},
			}, 0, 450, budget, func(batch map[any]uint64) error {
				consumed = make(map[any]uint64, len(batch))
				for pk, ts := range batch {
					consumed[pk] = ts
				}
				return nil
			}, nil)
			require.NoError(t, err)
			require.Equal(t, merged, consumed)
			for _, boundary := range []string{"next_manifest", "final_batch"} {
				input := &internalpb.SnapshotImportL0Source{LegacyL0Deltalogs: paths[:1]}
				keyCharge := int64(128)
				if pkType == schemapb.DataType_VarChar {
					keyCharge++
				}
				batchBudget := int64(len(paths[0])) + 64 + 2*keyCharge
				if boundary == "next_manifest" {
					input.ManifestL0Paths = []string{l0Manifest}
				}
				partial, err := loadSnapshotL0Deletes(ctx, cm, schema, cfg, input, 0, 450, batchBudget,
					func(map[any]uint64) error { return merr.ErrIoPermissionDenied }, nil)
				require.ErrorIs(t, err, merr.ErrIoPermissionDenied)
				require.Nil(t, partial)
			}
			// Real column-group readers must preserve physical row positions
			// across PK/TS projection, full reads and different delete budgets.
			bitmapSources := []*internalpb.SnapshotImportSource{
				{Version: 1, ManifestPath: manifest},
				{Version: 1, ManifestPath: manifest, SourceCommitTimestamp: 300},
			}
			for _, batchBudget := range []int64{budget - 512 + 129, budget} {
				masks, err := BuildSnapshotDeleteMasks(ctx, cm, schema, cfg, bitmapSources,
					&internalpb.SnapshotImportL0Source{LegacyL0Deltalogs: paths[:1], ManifestL0Paths: []string{l0Manifest}},
					0, 450, batchBudget, 1<<20, SourceEncryption{}, nil)
				require.NoError(t, err)
				for i, source := range bitmapSources {
					r, err := NewStorageV3ManifestReaderWithSharedL0(ctx, cm, schema, cfg, manifest, 0, 450, 1, SourceEncryption{}, source, batchBudget, masks, nil)
					require.NoError(t, err)
					var ids []int64
					for {
						data, err := r.Read()
						if err == io.EOF {
							break
						}
						require.NoError(t, err)
						for j := 0; j < data.GetRowNum(); j++ {
							ids = append(ids, data.Data[common.RowIDField].GetRow(j).(int64))
						}
					}
					r.Close()
					want := [][]int64{{2, 3, 5}, {1, 2, 3, 5}}[i]
					require.Equal(t, want, ids)
				}
			}
			for _, commit := range []uint64{0, 300} {
				source := &internalpb.SnapshotImportSource{
					Version: 1, ManifestPath: manifest, SourceCommitTimestamp: commit,
				}
				shared, err := BuildSnapshotDeleteMasks(ctx, cm, schema, cfg, []*internalpb.SnapshotImportSource{source},
					&internalpb.SnapshotImportL0Source{LegacyL0Deltalogs: paths[:1], ManifestL0Paths: []string{l0Manifest}},
					0, 450, budget, 1<<20, SourceEncryption{}, nil)
				require.NoError(t, err)
				// Re-open twice to model the identical persisted input used by
				// PreImport and Import; exact row IDs prove reinsert ordering.
				for phase := 0; phase < 2; phase++ {
					r, err := NewStorageV3ManifestReaderWithSharedL0(ctx, cm, schema, cfg, manifest, 0, 450, 1024, SourceEncryption{}, source, budget, shared, nil)
					require.NoError(t, err)
					var ids []int64
					for {
						batch, err := r.Read()
						if err == io.EOF {
							break
						}
						require.NoError(t, err)
						for i := 0; i < batch.GetRowNum(); i++ {
							ids = append(ids, batch.Data[common.RowIDField].GetRow(i).(int64))
						}
					}
					want := []int64{2, 3, 5}
					if commit != 0 {
						want = []int64{1, 2, 3, 5}
					}
					require.Equal(t, want, ids)
					require.Empty(t, r.deleteData, "bitmap readers do not retain a delete map")
					r.Close()
					require.Nil(t, r.deleteData)
					require.NotNil(t, shared.masks[maskKey(source)], "closing a borrower must not clear task-owned masks")
				}
			}
			source := &internalpb.SnapshotImportSource{
				Version: 1, ManifestPath: manifest,
			}
			// V2 metadata can reference either legacy or packed files; the
			// legacy inventory must retain its established V1/V2 fallback.
			shared, err := BuildSnapshotDeleteMasks(ctx, cm, schema, cfg, []*internalpb.SnapshotImportSource{source},
				&internalpb.SnapshotImportL0Source{LegacyL0Deltalogs: paths}, 0, 450, budget, 1<<20, SourceEncryption{}, nil)
			require.NoError(t, err)
			r, err := NewStorageV3ManifestReaderWithSharedL0(ctx, cm, schema, cfg, manifest, 0, 450, 1024, SourceEncryption{}, source, budget, shared, nil)
			require.NoError(t, err)
			r.Close()
			_, err = NewStorageV3ManifestReaderWithSharedL0(ctx, cm, schema, cfg, manifest, 0, 450, 1024, SourceEncryption{}, source, 0, shared, nil)
			require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
			r, err = NewStorageV3ManifestReaderWithSharedL0(ctx, cm, schema, cfg, manifest, 0, 450, 1024, SourceEncryption{}, source, 1, shared, nil)
			require.NoError(t, err, "prepared bitmaps must not be charged as another delete map")
			r.Close()
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = loadSnapshotL0Deletes(canceled, cm, schema, cfg, &internalpb.SnapshotImportL0Source{LegacyL0Deltalogs: paths}, 0, 450, 512, func(map[any]uint64) error { return nil }, nil)
			require.ErrorIs(t, err, context.Canceled)
			canceled, cancel = context.WithCancel(ctx)
			defer cancel()
			r, err = NewStorageV3ManifestReaderWithSharedL0(canceled, cm, schema, cfg, manifest, 0, 450, 1024, SourceEncryption{}, source, budget, shared, nil)
			require.NoError(t, err)
			cancel()
			_, err = r.Read()
			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, r.dr)
			require.Nil(t, r.deleteData)
		})
	}
}

const (
	insertPrefix = "mock-insert-binlog-prefix"
	deltaPrefix  = "mock-delta-binlog-prefix"
)

type ReaderSuite struct {
	suite.Suite

	schema  *schemapb.CollectionSchema
	numRows int

	pkDataType  schemapb.DataType
	vecDataType schemapb.DataType

	deletePKs []storage.PrimaryKey
	deleteTss []int64

	tsStart uint64
	tsEnd   uint64
}

type storageV3DeltaRecordReader struct {
	record storage.Record
	read   bool
}

func (r *storageV3DeltaRecordReader) Next() (storage.Record, error) {
	if r.read {
		if r.record != nil {
			r.record.Release()
			r.record = nil
		}
		return nil, io.EOF
	}
	r.read = true
	return r.record, nil
}

func (r *storageV3DeltaRecordReader) Close() error {
	if r.record != nil {
		r.record.Release()
		r.record = nil
	}
	return nil
}

func (suite *ReaderSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
}

func (suite *ReaderSuite) SetupTest() {
	// default suite params
	suite.numRows = 100
	suite.tsStart = 0
	suite.tsEnd = math.MaxUint64
	suite.pkDataType = schemapb.DataType_Int64
	suite.vecDataType = schemapb.DataType_FloatVector
}

func genBinlogPath(fieldID int64) string {
	return fmt.Sprintf("backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/%d/6666", fieldID)
}

func genBinlogPaths(fieldIDs []int64) map[int64][]string {
	binlogPaths := make(map[int64][]string)
	for _, fieldID := range fieldIDs {
		binlogPaths[fieldID] = []string{genBinlogPath(fieldID)}
	}
	return binlogPaths
}

func createBinlogBuf(t *testing.T, field *schemapb.FieldSchema, data storage.FieldData) []byte {
	dataType := field.GetDataType()
	w := storage.NewInsertBinlogWriter(dataType, 1, 1, 1, field.GetFieldID(), field.GetNullable())
	assert.NotNil(t, w)
	defer w.Close()

	var dim int64
	var err error
	dim, err = typeutil.GetDim(field)
	if err != nil || dim == 0 {
		dim = 1
	}

	evt, err := w.NextInsertEventWriter(storage.WithDim(int(dim)), storage.WithNullable(field.GetNullable()), storage.WithElementType(field.GetElementType()))
	assert.NoError(t, err)

	evt.SetEventTimestamp(1, math.MaxInt64)
	w.SetEventTimeStamp(1, math.MaxInt64)

	// without the two lines, the case will crash at here.
	// the "original_size" is come from storage.originalSizeKey
	sizeTotal := data.GetMemorySize()
	w.AddExtra("original_size", fmt.Sprintf("%v", sizeTotal))

	switch dataType {
	case schemapb.DataType_Bool:
		err = evt.AddBoolToPayload(data.(*storage.BoolFieldData).Data, data.(*storage.BoolFieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int8:
		err = evt.AddInt8ToPayload(data.(*storage.Int8FieldData).Data, data.(*storage.Int8FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int16:
		err = evt.AddInt16ToPayload(data.(*storage.Int16FieldData).Data, data.(*storage.Int16FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int32:
		err = evt.AddInt32ToPayload(data.(*storage.Int32FieldData).Data, data.(*storage.Int32FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Int64:
		err = evt.AddInt64ToPayload(data.(*storage.Int64FieldData).Data, data.(*storage.Int64FieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Float:
		err = evt.AddFloatToPayload(data.(*storage.FloatFieldData).Data, data.(*storage.FloatFieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_Double:
		err = evt.AddDoubleToPayload(data.(*storage.DoubleFieldData).Data, data.(*storage.DoubleFieldData).ValidData)
		assert.NoError(t, err)
	case schemapb.DataType_VarChar:
		values := data.(*storage.StringFieldData).Data
		validValues := data.(*storage.StringFieldData).ValidData
		for i, val := range values {
			valid := true
			if len(validValues) > 0 {
				valid = validValues[i]
			}
			err = evt.AddOneStringToPayload(val, valid)
			assert.NoError(t, err)
		}
	case schemapb.DataType_JSON:
		rows := data.(*storage.JSONFieldData).Data
		validValues := data.(*storage.JSONFieldData).ValidData
		for i := 0; i < len(rows); i++ {
			valid := true
			if len(validValues) > 0 {
				valid = validValues[i]
			}
			err = evt.AddOneJSONToPayload(rows[i], valid)
			assert.NoError(t, err)
		}
	case schemapb.DataType_Array:
		rows := data.(*storage.ArrayFieldData).Data
		validValues := data.(*storage.ArrayFieldData).ValidData
		for i := 0; i < len(rows); i++ {
			valid := true
			if len(validValues) > 0 {
				valid = validValues[i]
			}
			err = evt.AddOneArrayToPayload(rows[i], valid)
			assert.NoError(t, err)
		}
	case schemapb.DataType_BinaryVector:
		vectors := data.(*storage.BinaryVectorFieldData).Data
		err = evt.AddBinaryVectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_FloatVector:
		vectors := data.(*storage.FloatVectorFieldData).Data
		err = evt.AddFloatVectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_Float16Vector:
		vectors := data.(*storage.Float16VectorFieldData).Data
		err = evt.AddFloat16VectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_BFloat16Vector:
		vectors := data.(*storage.BFloat16VectorFieldData).Data
		err = evt.AddBFloat16VectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_SparseFloatVector:
		vectors := data.(*storage.SparseFloatVectorFieldData)
		err = evt.AddSparseFloatVectorToPayload(vectors)
		assert.NoError(t, err)
	case schemapb.DataType_Int8Vector:
		vectors := data.(*storage.Int8VectorFieldData).Data
		err = evt.AddInt8VectorToPayload(vectors, int(dim), nil)
		assert.NoError(t, err)
	case schemapb.DataType_ArrayOfVector:
		elementType := field.GetElementType()
		switch elementType {
		case schemapb.DataType_FloatVector:
			vectors := data.(*storage.VectorArrayFieldData)
			err = evt.AddVectorArrayFieldDataToPayload(vectors)
			assert.NoError(t, err)
		default:
			assert.True(t, false)
			return nil
		}
	default:
		assert.True(t, false)
		return nil
	}

	err = w.Finish()
	assert.NoError(t, err)
	buf, err := w.GetBuffer()
	assert.NoError(t, err)
	return buf
}

func createDeltaBuf(t *testing.T, deletePKs []storage.PrimaryKey, deleteTss []int64) []byte {
	assert.Equal(t, len(deleteTss), len(deletePKs))
	deleteData := storage.NewDeleteData(nil, nil)
	for i := range deletePKs {
		deleteData.Append(deletePKs[i], uint64(deleteTss[i]))
	}
	deleteCodec := storage.NewDeleteCodec()
	blob, err := deleteCodec.Serialize(1, 1, 1, deleteData)
	assert.NoError(t, err)
	return blob.Value
}

func (suite *ReaderSuite) createMockChunk(schema *schemapb.CollectionSchema, insertBinlogs map[int64][]string, expectRead bool) (*mocks.ChunkManager, *storage.InsertData) {
	var deltaLogs []string
	if len(suite.deletePKs) != 0 {
		deltaLogs = []string{
			"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415105",
		}
	}

	cm := mocks.NewChunkManager(suite.T())

	originalInsertData, err := testutil.CreateInsertData(schema, suite.numRows)
	suite.NoError(err)

	insertLogs := lo.Flatten(lo.Values(insertBinlogs))

	cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
			for _, filePath := range insertLogs {
				if !cowf(&storage.ChunkObjectInfo{FilePath: filePath, ModifyTime: time.Now()}) {
					return nil
				}
			}
			return nil
		})

	if expectRead {
		cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, filePath := range deltaLogs {
					if !cowf(&storage.ChunkObjectInfo{FilePath: filePath, ModifyTime: time.Now()}) {
						return nil
					}
				}
				return nil
			})

		var (
			paths = make([]string, 0)
			bytes = make([][]byte, 0)
		)
		allFields := typeutil.GetAllFieldSchemas(schema)
		for _, field := range allFields {
			fieldID := field.GetFieldID()
			logs, ok := insertBinlogs[fieldID]
			if ok && len(logs) > 0 {
				paths = append(paths, insertBinlogs[fieldID][0])

				// the testutil.CreateInsertData() doesn't create data for function output field
				// add data here to avoid crash
				if field.IsFunctionOutput {
					data, dim := testutils.GenerateSparseFloatVectorsData(suite.numRows)
					originalInsertData.Data[fieldID] = &storage.SparseFloatVectorFieldData{
						SparseFloatArray: schemapb.SparseFloatArray{
							Contents: data,
							Dim:      dim,
						},
					}
				}
				bytes = append(bytes, createBinlogBuf(suite.T(), field, originalInsertData.Data[fieldID]))
			}
		}
		cm.EXPECT().MultiRead(mock.Anything, paths).Return(bytes, nil)

		if len(suite.deletePKs) != 0 {
			for _, path := range deltaLogs {
				buf := createDeltaBuf(suite.T(), suite.deletePKs, suite.deleteTss)
				cm.EXPECT().MultiRead(mock.Anything, []string{path}).Return([][]byte{buf}, nil)
			}
		}
	}

	return cm, originalInsertData
}

func (suite *ReaderSuite) run(dataType schemapb.DataType, elemType schemapb.DataType, nullable bool) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      int64(common.RowIDField),
				Name:         common.RowIDFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      int64(common.TimeStampField),
				Name:         common.TimeStampFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     suite.pkDataType,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: suite.vecDataType,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:     102,
				Name:        dataType.String(),
				DataType:    dataType,
				ElementType: elemType,
				Nullable:    nullable,
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{
			{
				FieldID: 103,
				Fields: []*schemapb.FieldSchema{
					{
						FieldID:     104,
						Name:        "struct_str",
						DataType:    schemapb.DataType_Array,
						ElementType: schemapb.DataType_VarChar,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   common.MaxLengthKey,
								Value: "256",
							},
							{
								Key:   common.MaxCapacityKey,
								Value: "20",
							},
						},
					},
					{
						FieldID:     105,
						Name:        "struct_float_vector",
						DataType:    schemapb.DataType_ArrayOfVector,
						ElementType: schemapb.DataType_FloatVector,
						TypeParams: []*commonpb.KeyValuePair{
							{
								Key:   common.MaxCapacityKey,
								Value: "20",
							},
							{
								Key:   common.DimKey,
								Value: "8",
							},
						},
					},
				},
			},
		},
	}
	allFields := typeutil.GetAllFieldSchemas(schema)
	insertBinlogs := genBinlogPaths(lo.Map(allFields, func(fieldSchema *schemapb.FieldSchema, _ int) int64 {
		return fieldSchema.GetFieldID()
	}))
	cm, originalInsertData := suite.createMockChunk(schema, insertBinlogs, true)
	cm.EXPECT().Size(mock.Anything, mock.Anything).Return(128, nil)

	reader, err := NewReader(context.Background(), cm, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
	suite.NoError(err)
	insertData, err := reader.Read()
	suite.NoError(err)
	size, err := reader.Size()
	suite.NoError(err)
	suite.Equal(int64(128*len(lo.Flatten(lo.Values(insertBinlogs)))), size)
	size2, err := reader.Size() // size is cached
	suite.NoError(err)
	suite.Equal(size, size2)

	pks, err := storage.GetPkFromInsertData(schema, originalInsertData)
	suite.NoError(err)
	tss, err := storage.GetTimestampFromInsertData(originalInsertData)
	suite.NoError(err)
	expectInsertData, err := storage.NewInsertData(schema)
	suite.NoError(err)
	for _, field := range schema.GetFields() {
		expectInsertData.Data[field.GetFieldID()], err = storage.NewFieldData(field.GetDataType(), field, suite.numRows)
		suite.NoError(err)
	}
OUTER:
	for i := 0; i < suite.numRows; i++ {
		if uint64(tss.Data[i]) < suite.tsStart || uint64(tss.Data[i]) > suite.tsEnd {
			continue
		}
		for j := 0; j < len(suite.deletePKs); j++ {
			if suite.deletePKs[j].GetValue() == pks.GetRow(i) && suite.deleteTss[j] > tss.Data[i] {
				continue OUTER
			}
		}
		err = expectInsertData.Append(originalInsertData.GetRow(i))
		suite.NoError(err)
	}

	expectRowCount := expectInsertData.GetRowNum()
	for fieldID, data := range insertData.Data {
		suite.Equal(expectRowCount, data.RowNum())
		fieldData := expectInsertData.Data[fieldID]
		fieldDataType := typeutil.GetField(schema, fieldID).GetDataType()
		for i := 0; i < expectRowCount; i++ {
			expect := fieldData.GetRow(i)
			actual := data.GetRow(i)
			switch fieldDataType {
			case schemapb.DataType_Array:
				if expect == nil {
					suite.Nil(expect)
				} else {
					suite.True(slices.Equal(expect.(*schemapb.ScalarField).GetIntData().GetData(), actual.(*schemapb.ScalarField).GetIntData().GetData()))
				}
			case schemapb.DataType_ArrayOfVector:
				suite.True(slices.Equal(expect.(*schemapb.VectorField).GetFloatVector().GetData(), actual.(*schemapb.VectorField).GetFloatVector().GetData()))
			default:
				suite.Equal(expect, actual)
			}
		}
	}
}

func (suite *ReaderSuite) TestReadScalarFields() {
	suite.run(schemapb.DataType_Bool, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int8, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int16, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Int64, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Float, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_Double, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_VarChar, schemapb.DataType_None, false)
	suite.run(schemapb.DataType_JSON, schemapb.DataType_None, false)

	suite.run(schemapb.DataType_Array, schemapb.DataType_Bool, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int8, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int16, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int32, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int64, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Float, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Double, false)
	suite.run(schemapb.DataType_Array, schemapb.DataType_String, false)

	suite.run(schemapb.DataType_Bool, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int8, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int16, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Int64, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Float, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_Double, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_VarChar, schemapb.DataType_None, true)
	suite.run(schemapb.DataType_JSON, schemapb.DataType_None, true)

	suite.run(schemapb.DataType_Array, schemapb.DataType_Bool, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int8, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int16, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int32, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Int64, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Float, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_Double, true)
	suite.run(schemapb.DataType_Array, schemapb.DataType_String, true)
}

func (suite *ReaderSuite) TestWithTSRangeAndDelete() {
	suite.numRows = 10
	suite.tsStart = 2
	suite.tsEnd = 8
	suite.deletePKs = []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(4),
		storage.NewInt64PrimaryKey(6),
		storage.NewInt64PrimaryKey(8),
	}
	suite.deleteTss = []int64{
		8, 8, 1, 8,
	}
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
}

func (suite *ReaderSuite) TestStringPK() {
	suite.pkDataType = schemapb.DataType_VarChar
	suite.numRows = 10
	suite.tsStart = 2
	suite.tsEnd = 8
	suite.deletePKs = []storage.PrimaryKey{
		storage.NewVarCharPrimaryKey("1"),
		storage.NewVarCharPrimaryKey("4"),
		storage.NewVarCharPrimaryKey("6"),
		storage.NewVarCharPrimaryKey("8"),
	}
	suite.deleteTss = []int64{
		8, 8, 1, 8,
	}
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
}

func (suite *ReaderSuite) TestVector() {
	suite.pkDataType = schemapb.DataType_Int64
	suite.tsStart = 2
	suite.tsEnd = 8
	suite.deletePKs = []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(4),
		storage.NewInt64PrimaryKey(6),
		storage.NewInt64PrimaryKey(8),
	}
	suite.deleteTss = []int64{
		8, 8, 1, 8,
	}
	suite.vecDataType = schemapb.DataType_BinaryVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_FloatVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_Float16Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_BFloat16Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_SparseFloatVector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
	suite.vecDataType = schemapb.DataType_Int8Vector
	suite.run(schemapb.DataType_Int32, schemapb.DataType_None, false)
}

func (suite *ReaderSuite) TestVerify() {
	suite.deletePKs = []storage.PrimaryKey{}

	pkFieldID := int64(100)
	vecFieldID := int64(101)
	nullableFieldID := int64(102)
	functionFieldID := int64(103)
	dynamicFieldID := int64(104)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      pkFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  vecFieldID,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:  nullableFieldID,
				Name:     "nullable",
				DataType: schemapb.DataType_Double,
				Nullable: true,
			},
			{
				FieldID:          functionFieldID,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
			{
				FieldID:   dynamicFieldID,
				Name:      "dynamic",
				DataType:  schemapb.DataType_JSON,
				IsDynamic: true,
			},
		},
	}
	insertBinlogs := genBinlogPaths(lo.Map(schema.Fields, func(fieldSchema *schemapb.FieldSchema, _ int) int64 {
		return fieldSchema.GetFieldID()
	}))

	checkFunc := func() {
		cm, _ := suite.createMockChunk(schema, insertBinlogs, false)
		reader, err := NewReader(context.Background(), cm, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
		suite.Error(err)
		suite.Nil(reader)
	}

	// no insert binlogs to import
	reader, err := NewReader(context.Background(), nil, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
	suite.Error(err)
	suite.Nil(reader)

	// too many input paths
	reader, err = NewReader(context.Background(), nil, schema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix, "dummy"}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
	suite.Error(err)
	suite.Nil(reader)

	// no binlog for RowID
	insertBinlogs[common.RowIDField] = []string{}
	checkFunc()

	// no binlog for RowID
	insertBinlogs[common.RowIDField] = []string{genBinlogPath(common.RowIDField)}
	insertBinlogs[common.TimeStampField] = []string{}
	checkFunc()

	// binlog count not equal
	insertBinlogs[common.TimeStampField] = []string{genBinlogPath(common.TimeStampField)}
	insertBinlogs[vecFieldID] = []string{genBinlogPath(vecFieldID), genBinlogPath(vecFieldID)}
	checkFunc()

	// vector field is required
	insertBinlogs[vecFieldID] = []string{}
	checkFunc()

	// primary key is required
	insertBinlogs[vecFieldID] = []string{genBinlogPath(vecFieldID)}
	insertBinlogs[pkFieldID] = []string{}
	checkFunc()

	// function output field is required
	insertBinlogs[pkFieldID] = []string{genBinlogPath(pkFieldID)}
	insertBinlogs[functionFieldID] = []string{}
	checkFunc()
}

func (suite *ReaderSuite) TestZeroDeltaRead() {
	suite.deletePKs = []storage.PrimaryKey{}

	mockChunkFunc := func(sourceSchema *schemapb.CollectionSchema, expectReadBinlogs map[int64][]string) *mocks.ChunkManager {
		sourceBinlogs := genBinlogPaths(lo.Map(sourceSchema.Fields, func(fieldSchema *schemapb.FieldSchema, _ int) int64 {
			return fieldSchema.GetFieldID()
		}))

		cm := mocks.NewChunkManager(suite.T())

		sourceInsertData, err := testutil.CreateInsertData(sourceSchema, suite.numRows)
		suite.NoError(err)

		sourceInsertLogs := lo.Flatten(lo.Values(sourceBinlogs))

		cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				for _, filePath := range sourceInsertLogs {
					if !cowf(&storage.ChunkObjectInfo{FilePath: filePath, ModifyTime: time.Now()}) {
						return nil
					}
				}
				return nil
			})

		var (
			paths = make([]string, 0)
			bytes = make([][]byte, 0)
		)
		for _, field := range sourceSchema.Fields {
			fieldID := field.GetFieldID()
			logs, ok := expectReadBinlogs[fieldID]
			if ok && len(logs) > 0 {
				paths = append(paths, expectReadBinlogs[fieldID][0])

				// the testutil.CreateInsertData() doesn't create data for function output field
				// add data here to avoid crash
				if field.IsFunctionOutput {
					data, dim := testutils.GenerateSparseFloatVectorsData(suite.numRows)
					sourceInsertData.Data[fieldID] = &storage.SparseFloatVectorFieldData{
						SparseFloatArray: schemapb.SparseFloatArray{
							Contents: data,
							Dim:      dim,
						},
					}
				}
				bytes = append(bytes, createBinlogBuf(suite.T(), field, sourceInsertData.Data[fieldID]))
			}
		}
		cm.EXPECT().MultiRead(mock.Anything, paths).Return(bytes, nil)

		cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, s string, b bool, cowf storage.ChunkObjectWalkFunc) error {
				return nil
			})

		return cm
	}

	rowID := int64(common.RowIDField)
	tsID := int64(common.TimeStampField)
	pkFieldID := int64(100)
	vecFieldID := int64(101)
	functionFieldID := int64(102)
	nullableFieldID := int64(103)
	dynamicFieldID := int64(104)
	sourceSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      rowID,
				Name:         common.RowIDFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      tsID,
				Name:         common.TimeStampFieldName,
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      pkFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  vecFieldID,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.DimKey,
						Value: "8",
					},
				},
			},
			{
				FieldID:          functionFieldID,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
			},
			{
				FieldID:      nullableFieldID,
				Name:         "nullable",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:   dynamicFieldID,
				Name:      "dynamic",
				DataType:  schemapb.DataType_JSON,
				IsDynamic: true,
			},
		},
	}

	checkFunc := func(targetSchema *schemapb.CollectionSchema, expectReadBinlogs map[int64][]string) {
		cm := mockChunkFunc(sourceSchema, expectReadBinlogs)
		reader, err := NewReader(context.Background(), cm, targetSchema, &indexpb.StorageConfig{}, storage.StorageV1, []string{insertPrefix, deltaPrefix}, suite.tsStart, suite.tsEnd, 64*1024*1024, "")
		suite.NoError(err)
		suite.NotNil(reader)

		readData, err := reader.Read()
		suite.NoError(err)
		suite.Equal(suite.numRows, readData.GetRowNum())

		for _, field := range targetSchema.Fields {
			fieldID := field.GetFieldID()
			fieldData, ok := readData.Data[fieldID]
			if !ok {
				// if this field has no data, it must be nullable/default or dynamic
				suite.True(field.GetIsDynamic() || field.GetNullable() || field.GetDefaultValue() != nil)
			} else {
				suite.Equal(suite.numRows, fieldData.RowNum())
			}
		}
	}

	targetSchemaFunc := func(from int, to int, newFields ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
		fields := make([]*schemapb.FieldSchema, 0)
		fields = append(fields, sourceSchema.Fields[from:to]...)
		fields = append(fields, newFields...)
		return &schemapb.CollectionSchema{Fields: fields}
	}

	// the target schema lacks some fields(not required field), can import
	checkFunc(targetSchemaFunc(0, 3), map[int64][]string{
		rowID:     {genBinlogPath(rowID)},
		tsID:      {genBinlogPath(tsID)},
		pkFieldID: {genBinlogPath(pkFieldID)},
	})

	// the target schema has a new nullable field, can import
	checkFunc(targetSchemaFunc(0, len(sourceSchema.Fields), &schemapb.FieldSchema{
		FieldID:  200,
		Name:     "new",
		DataType: schemapb.DataType_Double,
		Nullable: true,
	}), map[int64][]string{
		rowID:           {genBinlogPath(rowID)},
		tsID:            {genBinlogPath(tsID)},
		pkFieldID:       {genBinlogPath(pkFieldID)},
		vecFieldID:      {genBinlogPath(vecFieldID)},
		functionFieldID: {genBinlogPath(functionFieldID)},
		nullableFieldID: {genBinlogPath(nullableFieldID)},
		dynamicFieldID:  {genBinlogPath(dynamicFieldID)},
	})

	// the target schema has a new dynamic field, can import
	checkFunc(targetSchemaFunc(0, len(sourceSchema.Fields), &schemapb.FieldSchema{
		FieldID:   200,
		Name:      "new",
		DataType:  schemapb.DataType_JSON,
		IsDynamic: true,
	}), map[int64][]string{
		rowID:           {genBinlogPath(rowID)},
		tsID:            {genBinlogPath(tsID)},
		pkFieldID:       {genBinlogPath(pkFieldID)},
		vecFieldID:      {genBinlogPath(vecFieldID)},
		functionFieldID: {genBinlogPath(functionFieldID)},
		nullableFieldID: {genBinlogPath(nullableFieldID)},
		dynamicFieldID:  {genBinlogPath(dynamicFieldID)},
	})
}

func TestBinlogReader(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}

func TestSelectImportFields_BackupCompatibility(t *testing.T) {
	base := typeutil.AppendSystemFields(&schemapb.CollectionSchema{
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "renamed", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "nullable", DataType: schemapb.DataType_Int64, Nullable: true},
			{
				FieldID: 103, Name: "default", DataType: schemapb.DataType_Int64,
				DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}},
			},
			{FieldID: 104, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	})
	for _, tc := range []struct {
		name    string
		present []int64
		mutate  func(*schemapb.CollectionSchema)
		want    []int64
		errText string
	}{
		{name: "omit_optional_and_extra", present: []int64{100, 101, 999}, want: []int64{0, 1, 100, 101}},
		{name: "retain_present_optional", present: []int64{100, 101, 102, 103, 104}, want: []int64{0, 1, 100, 101, 102, 103, 104}},
		{name: "required_missing", present: []int64{100}, errText: "no binlog for field:renamed"},
		{name: "autoid_still_requires_pk", present: []int64{101}, mutate: func(s *schemapb.CollectionSchema) {
			typeutil.GetField(s, 100).AutoID = true
		}, errText: "no binlog for field:pk"},
		{name: "function_output_required", present: []int64{100}, mutate: func(s *schemapb.CollectionSchema) {
			typeutil.GetField(s, 101).IsFunctionOutput = true
		}, errText: "no binlog for field:renamed"},
		{name: "nullable_struct_absent", present: []int64{100, 101}, mutate: func(s *schemapb.CollectionSchema) {
			s.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Nullable: true, Fields: []*schemapb.FieldSchema{
				{FieldID: 201, Name: "a"}, {FieldID: 202, Name: "b"},
			}}}
		}, want: []int64{0, 1, 100, 101}},
		{name: "struct_present", present: []int64{100, 101, 201, 202}, mutate: func(s *schemapb.CollectionSchema) {
			s.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Fields: []*schemapb.FieldSchema{
				{FieldID: 201, Name: "a"}, {FieldID: 202, Name: "b"},
			}}}
		}, want: []int64{0, 1, 100, 101, 201, 202}},
		{name: "partial_struct_rejected", present: []int64{100, 101, 201}, mutate: func(s *schemapb.CollectionSchema) {
			s.StructArrayFields = []*schemapb.StructArrayFieldSchema{{FieldID: 200, Nullable: true, Fields: []*schemapb.FieldSchema{
				{FieldID: 201, Name: "a"}, {FieldID: 202, Name: "b"},
			}}}
		}, errText: "no binlog for struct field:b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := typeutil.Clone(base)
			if tc.mutate != nil {
				tc.mutate(schema)
			}
			before := typeutil.Clone(schema)
			logs := map[int64][]string{0: {"rowid"}, 1: {"timestamp"}}
			for _, id := range tc.present {
				logs[id] = []string{fmt.Sprint(id)}
			}
			readSchema, err := selectImportFields(schema, func(id int64) bool { _, ok := logs[id]; return ok })
			validLogs, legacySchema, legacyErr := verify(schema, storage.StorageV1, logs)
			require.Equal(t, before, schema, "selection must not change the task's target schema")
			if tc.errText != "" {
				require.ErrorIs(t, err, merr.ErrImportFailed)
				require.ErrorContains(t, err, tc.errText)
				require.ErrorIs(t, legacyErr, merr.ErrImportFailed)
				require.ErrorContains(t, legacyErr, tc.errText)
				return
			}
			require.NoError(t, err)
			require.NoError(t, legacyErr)
			require.Equal(t, legacySchema, readSchema)
			require.ElementsMatch(t, tc.want, lo.Keys(validLogs))
			_, dynamicPresent := logs[104]
			require.Equal(t, dynamicPresent, readSchema.GetEnableDynamicField())
		})
	}
}

func patchStorageV3TestFieldIDs(t *testing.T, ids ...int64) {
	t.Helper()
	present := map[int64]struct{}{common.RowIDField: {}, common.TimeStampField: {}}
	for _, id := range ids {
		present[id] = struct{}{}
	}
	p := mockey.Mock(packed.GetManifestFieldIDs).Return(present, nil).Build()
	t.Cleanup(func() { p.UnPatch() })
}

func TestStorageV3Reader_BackupFieldSelection(t *testing.T) {
	paramtable.Init()
	manifest := packed.MarshalManifestPath("snapshot/segment/10", 7)
	sourceSchema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "source_name", DataType: schemapb.DataType_Int64},
		{FieldID: 999, Name: "source_only", DataType: schemapb.DataType_Int64},
	}})
	target := &schemapb.CollectionSchema{EnableDynamicField: true, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "target_name", DataType: schemapb.DataType_Int64},
		// A matching name at source ID 999 must not remap data into target ID 102.
		{FieldID: 102, Name: "source_only", DataType: schemapb.DataType_Int64, Nullable: true},
		{
			FieldID: 103, Name: "default", DataType: schemapb.DataType_Int64,
			DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}},
		},
		{FieldID: 104, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
	}}
	patchStorageV3TestFieldIDs(t, 100, 101, 999)
	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
	defer fragmentsPatch.UnPatch()
	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
	defer lobPatch.UnPatch()
	deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
	defer deltaPatch.UnPatch()
	record, err := storage.ValueSerializer([]*storage.Value{
		{Value: map[int64]any{0: int64(1), 1: int64(100), 100: int64(1), 101: int64(111), 999: int64(9)}},
		{Value: map[int64]any{0: int64(2), 1: int64(200), 100: int64(2), 101: int64(222), 999: int64(9)}},
	}, sourceSchema)
	require.NoError(t, err)
	owner := &storageV3DeltaRecordReader{record: record}
	defer owner.Close()
	openPatch := mockey.Mock(storage.NewManifestRecordReader).To(func(_ context.Context, _ string,
		schema *schemapb.CollectionSchema, _ ...storage.RwOption,
	) (storage.RecordReader, error) {
		require.Equal(t, "target_name", typeutil.GetField(schema, 101).GetName())
		require.Len(t, typeutil.GetAllFieldSchemas(schema), 4)
		return owner, nil
	}).Build()
	defer openPatch.UnPatch()
	r, err := NewStorageV3ManifestReader(context.Background(), nil, target, &indexpb.StorageConfig{},
		manifest, 0, math.MaxUint64, 1024, SourceEncryption{},
		&internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest, SourceCommitTimestamp: 300}, 1024, nil)
	require.NoError(t, err)
	defer r.Close()
	r.deleteData[int64(1)] = 400
	data, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, 1, data.GetRowNum())
	require.Equal(t, int64(2), data.Data[100].GetRow(0))
	require.Equal(t, int64(222), data.Data[101].GetRow(0), "read by ID even when the field name changes")
	for _, id := range []int64{102, 103, 104, 999} {
		require.NotContains(t, data.Data, id, "optional target fields are filled by Import; source-only fields are ignored")
	}
	require.Len(t, target.Fields, 5, "keep the full target schema for downstream filling")
	require.True(t, target.GetEnableDynamicField())
}

func TestStorageV3Reader_ManifestFieldSelectionErrors(t *testing.T) {
	paramtable.Init()
	for _, tc := range []struct {
		name    string
		readErr error
		want    error
	}{
		{"read_error", merr.WrapErrIoKeyNotFound("manifest unavailable"), merr.ErrIoKeyNotFound},
		{"missing_required", nil, merr.ErrImportFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := mockey.Mock(packed.GetManifestFieldIDs).
				Return(map[int64]struct{}{common.RowIDField: {}, common.TimeStampField: {}}, tc.readErr).Build()
			defer p.UnPatch()
			r, err := NewStorageV3ManifestReader(context.Background(), nil,
				&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}}},
				nil, packed.MarshalManifestPath("snapshot/segment/10", 7), 0, math.MaxUint64, 1024, SourceEncryption{}, nil, 0, nil)
			require.Nil(t, r)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestStorageV3Reader_UsesManifestDeltalogPaths(t *testing.T) {
	patchStorageV3TestFieldIDs(t, 100)
	segmentPath := "backup/insert_log/1/2/3"
	manifestPath := packed.MarshalManifestPath(segmentPath, 7)
	readablePath := segmentPath + "/_delta/2"

	manifestReaderPatch := mockey.Mock(storage.NewManifestRecordReader).
		Return(&storageV3DeltaRecordReader{read: true}, nil).Build()
	defer manifestReaderPatch.UnPatch()
	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
		Return([]packed.Fragment(nil), nil).Build()
	defer fragmentsPatch.UnPatch()
	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
	defer lobPatch.UnPatch()
	deltaManifestPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).
		Return([]string{readablePath}, nil).Build()
	defer deltaManifestPatch.UnPatch()

	var readerPaths []string
	deltaReaderPatch := mockey.Mock(storage.NewDeltalogReader).To(
		func(_ context.Context, _ schemapb.DataType, paths []string, _ ...storage.RwOption) (storage.RecordReader, error) {
			readerPaths = append([]string(nil), paths...)
			return &storageV3DeltaRecordReader{read: true}, nil
		}).Build()
	defer deltaReaderPatch.UnPatch()

	r := &reader{
		ctx: context.Background(),
		schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
		storageConfig:  &indexpb.StorageConfig{},
		storageVersion: storage.StorageV3,
		retryAttempts:  1,
	}
	assert.NoError(t, r.initStorageV3Manifest(manifestPath, 0, math.MaxUint64))
	assert.Equal(t, []string{readablePath}, readerPaths)
	assert.Len(t, r.filters, 1)
	r.Close()
}

func TestStorageV3Reader_UsesExactSnapshotManifest(t *testing.T) {
	patchStorageV3TestFieldIDs(t, 100)
	segmentPath := "snapshot/files/segment/10"
	exactManifest := packed.MarshalManifestPath(segmentPath, 7)
	var openedManifest string

	manifestReaderPatch := mockey.Mock(storage.NewManifestRecordReader).To(
		func(_ context.Context, manifestPath string, _ *schemapb.CollectionSchema, _ ...storage.RwOption) (storage.RecordReader, error) {
			openedManifest = manifestPath
			return &storageV3DeltaRecordReader{read: true}, nil
		}).Build()
	defer manifestReaderPatch.UnPatch()
	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
		Return([]packed.Fragment(nil), nil).Build()
	defer fragmentsPatch.UnPatch()
	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
	defer lobPatch.UnPatch()
	deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
	defer deltaPatch.UnPatch()

	r, err := NewStorageV3ManifestReader(
		context.Background(),
		nil,
		&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
		&indexpb.StorageConfig{},
		exactManifest,
		0,
		math.MaxUint64,
		1024,
		SourceEncryption{},
		nil,
		0,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, exactManifest, openedManifest)
	r.Close()

	_, err = NewStorageV3ManifestReader(
		context.Background(),
		nil,
		&schemapb.CollectionSchema{},
		&indexpb.StorageConfig{},
		packed.MarshalManifestPath(segmentPath, packed.ManifestLatest),
		0,
		math.MaxUint64,
		1024,
		SourceEncryption{},
		nil,
		0,
		nil,
	)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
	assert.ErrorContains(t, err, "exact StorageV3 manifest version")
}

func TestStorageV3Reader_CMEKWithoutTextUsesPackedReaderContext(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("snapshot/files/segment/10", 7)
	expectedContext := &indexcgopb.StoragePluginContext{
		EncryptionZoneId: 10,
		EncryptionKey:    "encoded-source-key",
	}

	// The constructor consumes explicit dependencies; neither EZK parsing nor
	// source/target key lookup belongs to a per-segment reader anymore.
	parsePatch := mockey.Mock(hookutil.GetEzIDByImportEzk).Return(int64(0), merr.ErrServiceInternal).Build()
	defer parsePatch.UnPatch()
	contextPatch := mockey.Mock(hookutil.GetCPluginContextByEzID).Return(nil, merr.ErrServiceInternal).Build()
	defer contextPatch.UnPatch()
	encryptionPatch := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(true).Build()
	defer encryptionPatch.UnPatch()
	fieldIDsPatch := mockey.Mock(packed.GetManifestFieldIDs).
		Return(map[int64]struct{}{common.RowIDField: {}, common.TimeStampField: {}, 100: {}}, nil).Build()
	defer fieldIDsPatch.UnPatch()

	var capturedContext *indexcgopb.StoragePluginContext
	innerReaderPatch := mockey.Mock(storage.NewRecordReaderFromManifest).To(
		func(_ string,
			schema *schemapb.CollectionSchema,
			_ int64,
			_ *indexpb.StorageConfig,
			pluginContext *indexcgopb.StoragePluginContext,
			_ ...storage.RwOption,
		) (storage.RecordReader, error) {
			capturedContext = pluginContext
			require.False(t, typeutil.HasTextField(schema), "absent target-only TEXT must not select the LOB reader")
			record, err := storage.ValueSerializer([]*storage.Value{{Value: map[int64]any{
				common.RowIDField: int64(1), common.TimeStampField: int64(100), 100: int64(42),
			}}}, schema)
			return &storageV3DeltaRecordReader{record: record}, err
		}).Build()
	defer innerReaderPatch.UnPatch()
	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
		Return([]packed.Fragment(nil), nil).Build()
	defer fragmentsPatch.UnPatch()
	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
	defer lobPatch.UnPatch()
	deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
	defer deltaPatch.UnPatch()

	for _, tc := range []struct {
		name       string
		encryption SourceEncryption
		addedText  bool
	}{
		{"cmek", SourceEncryption{Encrypted: true, PluginContext: expectedContext}, false},
		{"cmek_nullable_text", SourceEncryption{Encrypted: true, PluginContext: expectedContext}, true},
		{"cmek_disabled_plugin", SourceEncryption{Encrypted: true}, false},
		{"plaintext", SourceEncryption{}, false},
		{"plaintext_nullable_text", SourceEncryption{}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			}, Properties: []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "99"}}}
			if tc.addedText {
				schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
					FieldID: 101, Name: "target_only_text", DataType: schemapb.DataType_Text, Nullable: true,
				})
			}
			capturedContext = &indexcgopb.StoragePluginContext{EncryptionZoneId: 99}
			r, err := NewStorageV3ManifestReader(context.Background(), nil, schema, &indexpb.StorageConfig{},
				manifestPath, 0, math.MaxUint64, 1024, tc.encryption, nil, 0, nil)
			require.NoError(t, err)
			defer r.Close()
			require.Equal(t, tc.encryption.PluginContext, capturedContext, "an explicit nil must not fall back to the target key")
			data, err := r.Read()
			require.NoError(t, err)
			require.Equal(t, int64(42), data.Data[100].GetRow(0))
			require.NotContains(t, data.Data, int64(101), "Import fills the absent nullable field later")
			require.Equal(t, tc.addedText, typeutil.HasTextField(schema), "keep the target schema for NULL filling")
		})
	}
	require.Zero(t, parsePatch.Times())
	require.Zero(t, contextPatch.Times())
}

func TestStorageV3Reader_CMEKWithTextIsUnsupported(t *testing.T) {
	patchStorageV3TestFieldIDs(t, 100, 101)
	openPatch := mockey.Mock(storage.NewManifestRecordReader).To(func(_ context.Context, _ string,
		_ *schemapb.CollectionSchema, _ ...storage.RwOption,
	) (storage.RecordReader, error) {
		t.Fatal("encrypted TEXT must be rejected before opening the data reader")
		return nil, nil
	}).Build()
	defer openPatch.UnPatch()
	for _, nullable := range []bool{false, true} {
		t.Run(fmt.Sprintf("present_text_nullable=%t", nullable), func(t *testing.T) {
			_, err := NewStorageV3ManifestReader(
				context.Background(),
				nil,
				&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
					{FieldID: 101, DataType: schemapb.DataType_Text, Nullable: nullable},
				}},
				&indexpb.StorageConfig{},
				packed.MarshalManifestPath("snapshot/files/segment/10", 7),
				0,
				math.MaxUint64,
				1024,
				SourceEncryption{Encrypted: true},
				nil,
				0,
				nil,
			)
			assert.ErrorIs(t, err, merr.ErrOperationNotSupported)
			assert.ErrorContains(t, err, "TEXT/LOB")
		})
	}
}

func TestStorageV3Reader_RejectsLegacyPathInput(t *testing.T) {
	r := &reader{storageVersion: storage.StorageV3}
	err := r.init([]string{"backup/insert_log/1/2/3"}, 0, math.MaxUint64)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
	assert.ErrorContains(t, err, "exact manifest from a snapshot source")
}

func TestStorageV3Reader_CollectsManifestReferencedFiles(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	partitionPath := path.Join(root, "backup/insert_log/1/2")
	segmentPath := path.Join(partitionPath, "10")
	manifestPath := packed.MarshalManifestPath(segmentPath, 7)
	segmentDataPath := path.Join(segmentPath, "_data/0.parquet")
	orphanDataPath := path.Join(segmentPath, "_data/orphan.parquet")
	lobWithSizePath := path.Join(partitionPath, "lobs/101/_data/a.vx")
	lobWithoutSizePath := path.Join(partitionPath, "lobs/101/_data/b.vx")
	cm := storage.NewLocalChunkManager()
	assert.NoError(t, cm.Write(ctx, segmentDataPath, []byte("segment-10")))
	assert.NoError(t, cm.Write(ctx, orphanDataPath, []byte("orphan")))
	assert.NoError(t, cm.Write(ctx, lobWithoutSizePath, []byte("lob")))

	fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).To(
		func(gotManifest string, _ *indexpb.StorageConfig, columns []string) ([]packed.Fragment, error) {
			assert.Equal(t, manifestPath, gotManifest)
			assert.Nil(t, columns)
			return []packed.Fragment{{FilePath: segmentDataPath}}, nil
		}).Build()
	defer fragmentsPatch.UnPatch()

	lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo{
		{FieldID: 101, Path: lobWithSizePath, FileSizeBytes: 64},
		{FieldID: 101, Path: lobWithoutSizePath},
	}, nil).Build()
	defer lobPatch.UnPatch()

	r := &reader{
		ctx:            ctx,
		cm:             cm,
		schema:         &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, DataType: schemapb.DataType_Text}}},
		storageVersion: storage.StorageV3,
		fileSize:       atomic.NewInt64(0),
	}
	assert.NoError(t, r.collectStorageV3Files(manifestPath))
	assert.ElementsMatch(t, []string{
		segmentDataPath,
		lobWithoutSizePath,
	}, r.storageV3Files)
	assert.NotContains(t, r.storageV3Files, orphanDataPath)
	assert.Equal(t, int64(64), r.storageV3LobSize)
	size, err := r.Size()
	assert.NoError(t, err)
	assert.Equal(t, int64(len("segment-10")+len("lob"))+64, size)
}

func TestStorageV3Reader_RejectsInvalidManifestSizeMetadata(t *testing.T) {
	manifestPath := packed.MarshalManifestPath("backup/insert_log/1/2/3", 7)
	tests := []struct {
		name          string
		fragments     []packed.Fragment
		lobFiles      []packed.LobFileInfo
		fragmentErr   error
		lobErr        error
		wantError     string
		dataIntegrity bool
	}{
		{
			name:        "data fragment read failure",
			fragmentErr: errors.New("fragment read failed"),
			wantError:   "failed to read StorageV3 data files from manifest",
		},
		{
			name:          "data fragment without path",
			fragments:     []packed.Fragment{{}},
			wantError:     "data fragment without a path",
			dataIntegrity: true,
		},
		{
			name:      "LOB metadata read failure",
			lobErr:    errors.New("LOB metadata read failed"),
			wantError: "failed to read StorageV3 LOB files from manifest",
		},
		{
			name:          "LOB file without path",
			lobFiles:      []packed.LobFileInfo{{FieldID: 101, FileSizeBytes: 1}},
			wantError:     "LOB file without a path",
			dataIntegrity: true,
		},
		{
			name:          "LOB file with negative size",
			lobFiles:      []packed.LobFileInfo{{FieldID: 101, Path: "lobs/101", FileSizeBytes: -1}},
			wantError:     "with negative size",
			dataIntegrity: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).
				Return(test.fragments, test.fragmentErr).Build()
			defer fragmentsPatch.UnPatch()
			lobPatch := mockey.Mock(packed.GetManifestLobFiles).
				Return(test.lobFiles, test.lobErr).Build()
			defer lobPatch.UnPatch()

			r := &reader{ctx: context.Background(), schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, DataType: schemapb.DataType_Text}}}}
			err := r.collectStorageV3Files(manifestPath)
			assert.ErrorContains(t, err, test.wantError)
			if test.dataIntegrity {
				assert.ErrorIs(t, err, merr.ErrDataIntegrity)
			}
		})
	}
}

func TestStorageV3Reader_ReadsManifestDeletes(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ts", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 2, 3}, nil)
	builder.Field(1).(*array.Int64Builder).AppendValues([]int64{5, 12, 18, 25}, nil)
	rawRecord := builder.NewRecord()
	builder.Release()
	record := storage.NewSimpleArrowRecord(rawRecord, map[storage.FieldID]int{
		0:                     0,
		common.TimeStampField: 1,
	})

	readerPatch := mockey.Mock(storage.NewDeltalogReader).To(
		func(_ context.Context, pkType schemapb.DataType, paths []string, _ ...storage.RwOption) (storage.RecordReader, error) {
			assert.Equal(t, schemapb.DataType_Int64, pkType)
			assert.Equal(t, []string{"delta.parquet"}, paths)
			return &storageV3DeltaRecordReader{record: record}, nil
		}).Build()
	defer readerPatch.UnPatch()

	r := &reader{
		ctx: context.Background(),
		schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		}},
	}
	deletes, err := r.readDeleteV3([]string{"delta.parquet"}, 10, 20)
	assert.NoError(t, err)
	assert.Equal(t, map[any]typeutil.Timestamp{int64(2): 18}, deletes)
}

func TestStorageV3Reader_DeleteMergerBoundary(t *testing.T) {
	for _, mode := range []string{"missing_pk", "invalid_budget", "open", "read", "unsupported_pk", "string_max"} {
		t.Run(mode, func(t *testing.T) {
			r := &reader{ctx: context.Background(), schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_VarChar}},
			}}
			switch mode {
			case "missing_pk":
				r.schema.Fields = nil
			case "invalid_budget":
				r.snapshotSource = &internalpb.SnapshotImportSource{}
			case "unsupported_pk":
				r.schema.Fields[0].DataType = schemapb.DataType_Float
			}
			record, _, _, err := storage.BuildDeleteRecord([]storage.PrimaryKey{storage.NewVarCharPrimaryKey("key"), storage.NewVarCharPrimaryKey("key")}, []uint64{20, 10})
			require.NoError(t, err)
			owner := &storageV3DeltaRecordReader{record: record}
			defer owner.Close()
			var openErr error
			if mode == "open" {
				openErr = merr.ErrIoPermissionDenied
			}
			open := mockey.Mock(storage.NewDeltalogReader).Return(owner, openErr).Build()
			defer open.UnPatch()
			if mode == "read" {
				next := mockey.Mock((*storageV3DeltaRecordReader).Next).Return(nil, merr.ErrIoKeyNotFound).Build()
				defer next.UnPatch()
			}
			data, err := r.readDeleteV3([]string{"delta"}, 0, math.MaxUint64)
			if mode == "string_max" {
				require.NoError(t, err)
				require.Equal(t, map[any]uint64{"key": 20}, data)
			} else {
				require.Error(t, err)
				require.Nil(t, data)
				if mode == "invalid_budget" {
					require.ErrorIs(t, err, merr.ErrServiceResourceInsufficient)
				}
				if openErr != nil {
					require.ErrorIs(t, err, openErr)
				}
			}
		})
	}
}

func TestStorageV3Reader_InitializationCleanup(t *testing.T) {
	paramtable.Init()
	patchStorageV3TestFieldIDs(t, 100)
	for _, mode := range []string{"manifest", "delete", "filter"} {
		t.Run(mode, func(t *testing.T) {
			var manifestErr error
			if mode == "manifest" {
				manifestErr = merr.ErrIoKeyNotFound
			}
			fragments := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
			defer fragments.UnPatch()
			paths := []string{"delta"}
			if mode == "filter" {
				paths = nil
			}
			manifest := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return(paths, manifestErr).Build()
			defer manifest.UnPatch()
			open := mockey.Mock(storage.NewManifestRecordReader).Return(&storageV3DeltaRecordReader{}, nil).Build()
			defer open.UnPatch()
			closeReader := mockey.Mock((*storageV3DeltaRecordReader).Close).Return(nil).Build()
			defer closeReader.UnPatch()
			delta := mockey.Mock(storage.NewDeltalogReader).Return(nil, merr.ErrIoKeyNotFound).Build()
			defer delta.UnPatch()
			filter := mockey.Mock(FilterWithDelete).Return(nil, merr.ErrDataIntegrity).Build()
			defer filter.UnPatch()
			manifestPath := packed.MarshalManifestPath("root/segment", 1)
			r, err := NewStorageV3ManifestReader(context.Background(), nil,
				&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}}},
				nil, manifestPath, 0, math.MaxUint64, 1024, SourceEncryption{},
				&internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifestPath}, 1024, nil)
			require.Nil(t, r)
			if mode == "filter" {
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
			} else {
				require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
			}
			if mode == "manifest" {
				require.Zero(t, open.Times())
				require.Zero(t, closeReader.Times())
			} else {
				require.EqualValues(t, 1, closeReader.Times(), "failed initialization closes the opened data reader")
			}
		})
	}
}

func TestStorageV3Reader_SourceTimestampInvariant(t *testing.T) {
	paramtable.Init()
	manifest := packed.MarshalManifestPath("snapshot/files/segment/10", 7)
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		for _, tc := range []struct {
			name       string
			legacy     bool
			commitTs   uint64
			rowTs      int64
			deleteTs   uint64
			start, end uint64
			wantRows   int
			invalid    bool
		}{
			{name: "no_context_keeps_raw_delete_semantics", legacy: true, rowTs: 100, deleteTs: 200, end: math.MaxUint64},
			{name: "zero_commit_is_not_an_upper_bound", rowTs: 400, deleteTs: 300, end: math.MaxUint64, wantRows: 1},
			{name: "row_before_commit", commitTs: 300, rowTs: 100, deleteTs: 200, end: math.MaxUint64, wantRows: 1},
			{name: "row_equals_commit", commitTs: 300, rowTs: 300, deleteTs: 300, end: math.MaxUint64, wantRows: 1},
			{name: "delete_equals_commit", commitTs: 300, rowTs: 100, deleteTs: 300, end: math.MaxUint64, wantRows: 1},
			{name: "delete_after_commit", commitTs: 300, rowTs: 100, deleteTs: 400, end: math.MaxUint64},
			{name: "unsigned_commit_timestamp", commitTs: math.MaxUint64, rowTs: 100, deleteTs: 400, end: math.MaxUint64, wantRows: 1},
			{name: "unsigned_delete_timestamp", commitTs: 300, rowTs: 100, deleteTs: math.MaxUint64, end: math.MaxUint64},
			{name: "range_still_uses_raw_timestamp", commitTs: 300, rowTs: 100, start: 90, end: 110, wantRows: 1},
			{name: "row_after_commit", commitTs: 300, rowTs: 400, end: math.MaxUint64, invalid: true},
			{name: "range_cannot_hide_invalid_row", commitTs: 300, rowTs: 400, end: 200, invalid: true},
			{name: "delete_cannot_hide_invalid_row", commitTs: 300, rowTs: 400, deleteTs: 500, end: math.MaxUint64, invalid: true},
		} {
			t.Run(pkType.String()+"/"+tc.name, func(t *testing.T) {
				patchStorageV3TestFieldIDs(t, 100)
				schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: pkType, IsPrimaryKey: true},
				}}
				var pk any = int64(1)
				arrowPK := arrow.PrimitiveTypes.Int64
				if pkType == schemapb.DataType_VarChar {
					pk, arrowPK = "key", arrow.BinaryTypes.String
				}
				builder := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
					{Name: "row_id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "ts", Type: arrow.PrimitiveTypes.Int64},
					{Name: "pk", Type: arrowPK},
				}, nil))
				builder.Field(0).(*array.Int64Builder).Append(1)
				builder.Field(1).(*array.Int64Builder).Append(tc.rowTs)
				if pkType == schemapb.DataType_Int64 {
					builder.Field(2).(*array.Int64Builder).Append(pk.(int64))
				} else {
					builder.Field(2).(*array.StringBuilder).Append(pk.(string))
				}
				rawRecord := builder.NewRecord()
				builder.Release()
				owner := &storageV3DeltaRecordReader{record: storage.NewSimpleArrowRecord(rawRecord, map[storage.FieldID]int{
					common.RowIDField: 0, common.TimeStampField: 1, 100: 2,
				})}
				defer owner.Close()
				manifestPatch := mockey.Mock(storage.NewManifestRecordReader).Return(owner, nil).Build()
				defer manifestPatch.UnPatch()
				fragmentsPatch := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
				defer fragmentsPatch.UnPatch()
				lobPatch := mockey.Mock(packed.GetManifestLobFiles).Return([]packed.LobFileInfo(nil), nil).Build()
				defer lobPatch.UnPatch()
				deltaPatch := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
				defer deltaPatch.UnPatch()
				var source *internalpb.SnapshotImportSource
				if !tc.legacy {
					source = &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest, SourceCommitTimestamp: tc.commitTs}
				}
				r, err := NewStorageV3ManifestReader(context.Background(), nil, schema, &indexpb.StorageConfig{},
					manifest, tc.start, tc.end, 1024, SourceEncryption{}, source, 1024, nil)
				require.NoError(t, err)
				defer r.Close()
				r.deleteData = make(map[any]typeutil.Timestamp)
				if tc.deleteTs != 0 {
					r.deleteData[pk] = tc.deleteTs
				}
				deleteFilter, err := FilterWithDelete(r)
				require.NoError(t, err)
				r.filters = append(r.filters, deleteFilter)

				data, err := r.Read()
				if tc.invalid {
					require.ErrorIs(t, err, merr.ErrDataIntegrity)
					require.Nil(t, data)
					require.Nil(t, r.dr, "invalid source closes the deserializer immediately")
					require.Nil(t, owner.record, "borrowed Arrow record was released")
					_, nextErr := r.Read()
					require.Same(t, err, nextErr, "retry must not skip the invalid source row")
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.wantRows, data.GetRowNum())
				if tc.wantRows > 0 {
					require.Equal(t, tc.rowTs, data.Data[common.TimeStampField].GetRow(0), "do not rewrite raw timestamps")
				}
				_, err = r.Read()
				require.ErrorIs(t, err, io.EOF)
			})
		}
	}
}

func TestStorageV3Reader_RejectsUnsupportedSourceBeforeIO(t *testing.T) {
	paramtable.Init()
	manifest := packed.MarshalManifestPath("snapshot/files/segment/10", 7)
	for _, tc := range []struct {
		name   string
		source *internalpb.SnapshotImportSource
		want   error
	}{
		{"version", &internalpb.SnapshotImportSource{Version: 99, ManifestPath: manifest}, merr.ErrServiceUnimplemented},
		{"manifest_mismatch", &internalpb.SnapshotImportSource{Version: 1, ManifestPath: "other"}, merr.ErrServiceInternal},
		{"inline_legacy_l0", &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest, LegacyL0Deltalogs: []string{"delta"}}, merr.ErrServiceUnimplemented},
		{"inline_manifest_l0", &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest, ManifestL0Deltalogs: []string{"delta"}}, merr.ErrServiceUnimplemented},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewStorageV3ManifestReader(context.Background(), nil, &schemapb.CollectionSchema{}, nil,
				manifest, 0, math.MaxUint64, 1024, SourceEncryption{}, tc.source, 1024, nil)
			require.ErrorIs(t, err, tc.want)
			require.Nil(t, r)
		})
	}
}

func TestStorageV3Reader_InvalidSourceManifest(t *testing.T) {
	paramtable.Init()
	patchStorageV3TestFieldIDs(t)
	for _, manifest := range []string{"", "not-json", packed.MarshalManifestPath("snapshot/segment/10", packed.ManifestLatest)} {
		t.Run(manifest, func(t *testing.T) {
			r, err := NewStorageV3ManifestReader(context.Background(), nil, &schemapb.CollectionSchema{}, nil,
				manifest, 0, math.MaxUint64, 1024, SourceEncryption{}, &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest}, 1024, nil)
			require.ErrorIs(t, err, merr.ErrImportFailed)
			require.Nil(t, r)
		})
	}
	manifest := packed.MarshalManifestPath("snapshot/segment/10", 7)
	openErr := merr.WrapErrIoKeyNotFound("missing source object")
	fragments := mockey.Mock(packed.ReadFragmentsFromManifest).Return([]packed.Fragment(nil), nil).Build()
	defer fragments.UnPatch()
	delta := mockey.Mock(packed.GetDeltaLogPathsFromManifest).Return([]string(nil), nil).Build()
	defer delta.UnPatch()
	openPatch := mockey.Mock(storage.NewManifestRecordReader).Return(nil, openErr).Build()
	defer openPatch.UnPatch()
	r, err := NewStorageV3ManifestReader(context.Background(), nil, &schemapb.CollectionSchema{}, nil,
		manifest, 0, math.MaxUint64, 1024, SourceEncryption{}, &internalpb.SnapshotImportSource{Version: 1, ManifestPath: manifest}, 1024, nil)
	require.ErrorIs(t, err, merr.ErrIoKeyNotFound)
	require.Nil(t, r)
}

func TestStorageV3Reader_ReadFailurePaths(t *testing.T) {
	type timestampValueReader struct {
		storage.DeserializeReader[*storage.Value]
	}
	paramtable.Init()
	for _, name := range []string{"allocate_batch", "deserialize", "allocate_field", "append_field", "filter_batch", "missing_field", "buffer_boundary"} {
		t.Run(name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			}}
			r := newReader(context.Background(), nil, schema, nil, storage.StorageV3, 1, "")
			r.snapshotSource = &internalpb.SnapshotImportSource{Version: 1, SourceCommitTimestamp: 200}
			owner := &timestampValueReader{}
			r.dr = owner
			closePatch := mockey.Mock((*timestampValueReader).Close).Return(nil).Build()
			defer closePatch.UnPatch()
			defer r.Close()
			cause := merr.WrapErrIoKeyNotFound("source object")
			read := 0
			rows := 1
			if name == "buffer_boundary" {
				rows = 101
			}
			nextPatch := mockey.Mock((*timestampValueReader).NextValue).To(func(_ *timestampValueReader) (**storage.Value, error) {
				if name == "deserialize" {
					return nil, cause
				}
				if read == rows {
					return nil, io.EOF
				}
				read++
				row := map[int64]any{common.RowIDField: int64(read), common.TimeStampField: int64(100), 100: int64(read)}
				if name == "append_field" {
					row[100] = "not an int64"
				}
				v := &storage.Value{Timestamp: 100, Value: row}
				return &v, nil
			}).Build()
			defer nextPatch.UnPatch()

			data, err := storage.NewInsertDataWithFunctionOutputField(r.schema)
			require.NoError(t, err)
			if name == "missing_field" || name == "allocate_field" {
				delete(data.Data, 100)
			}
			allocations := 0
			batchPatch := mockey.Mock(storage.NewInsertDataWithFunctionOutputField).To(func(_ *schemapb.CollectionSchema) (*storage.InsertData, error) {
				allocations++
				if name == "allocate_batch" || (name == "filter_batch" && allocations == 2) {
					return nil, cause
				}
				return data, nil
			}).Build()
			defer batchPatch.UnPatch()
			if name == "allocate_field" {
				fieldPatch := mockey.Mock(storage.NewFieldData).Return(nil, cause).Build()
				defer fieldPatch.UnPatch()
			}
			if name == "filter_batch" {
				r.filters = []Filter{func(_ map[int64]interface{}) bool { return false }}
			}

			result, err := r.Read()
			switch name {
			case "missing_field":
				require.NoError(t, err)
				require.Equal(t, 1, result.GetRowNum())
			case "buffer_boundary":
				require.NoError(t, err)
				require.Equal(t, 100, result.GetRowNum())
				require.Equal(t, 100, read, "reader must return a bounded batch before EOF")
			case "append_field":
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Nil(t, result)
			default:
				require.ErrorIs(t, err, merr.ErrIoKeyNotFound, "preserve dependency errors instead of returning partial rows")
				require.Nil(t, result)
			}
		})
	}
}

func TestStorageV3Reader_DeleteFilterRequiresPrimaryKey(t *testing.T) {
	f, err := FilterWithDelete(&reader{schema: &schemapb.CollectionSchema{}})
	require.Error(t, err)
	require.Nil(t, f)
}

func TestBinlogReader_InitDependencyErrors(t *testing.T) {
	paramtable.Init()
	type chunkManager struct{ storage.ChunkManager }
	for _, name := range []string{"list_insert", "ez_id", "plugin_context", "open_reader", "walk_delta", "read_delta", "delete_filter", "no_delta"} {
		t.Run(name, func(t *testing.T) {
			cause := errors.New("injected dependency failure")
			r := &reader{
				ctx: context.Background(), cm: &chunkManager{}, retryAttempts: 1,
				storageVersion: storage.StorageV1,
				schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				}},
			}
			defer r.Close()
			var listErr error
			if name == "list_insert" {
				listErr = cause
			}
			listPatch := mockey.Mock(listInsertLogs).Return(map[int64][]string{0: {"row_id"}, 1: {"timestamp"}, 100: {"pk"}}, listErr).Build()
			defer listPatch.UnPatch()

			if name == "ez_id" || name == "plugin_context" || name == "no_delta" {
				r.importEz = "source-key"
				var ezErr, pluginErr error
				if name == "ez_id" {
					ezErr = cause
				}
				if name == "plugin_context" {
					pluginErr = cause
				}
				ezPatch := mockey.Mock(hookutil.GetEzIDByImportEzk).Return(int64(10), ezErr).Build()
				defer ezPatch.UnPatch()
				pluginPatch := mockey.Mock(hookutil.GetCPluginContextByEzID).Return(&indexcgopb.StoragePluginContext{}, pluginErr).Build()
				defer pluginPatch.UnPatch()
			}
			if name == "open_reader" {
				openPatch := mockey.Mock(storage.NewBinlogRecordReader).Return(nil, cause).Build()
				defer openPatch.UnPatch()
			}
			if name == "walk_delta" || name == "read_delta" || name == "delete_filter" {
				walkPatch := mockey.Mock((*chunkManager).WalkWithPrefix).To(
					func(_ *chunkManager, _ context.Context, _ string, _ bool, walk storage.ChunkObjectWalkFunc) error {
						if name == "walk_delta" {
							return cause
						}
						walk(&storage.ChunkObjectInfo{FilePath: "delta/1"})
						return nil
					}).Build()
				defer walkPatch.UnPatch()
			}
			if name == "read_delta" {
				deletePatch := mockey.Mock(storage.NewDeltalogReader).Return(nil, cause).Build()
				defer deletePatch.UnPatch()
			}
			if name == "delete_filter" {
				readPatch := mockey.Mock((*reader).readDelete).Return(map[any]typeutil.Timestamp{}, nil).Build()
				defer readPatch.UnPatch()
				filterPatch := mockey.Mock(FilterWithDelete).Return(nil, cause).Build()
				defer filterPatch.UnPatch()
			}
			paths := []string{"insert", "delta"}
			if name == "no_delta" {
				paths = paths[:1]
			}
			err := r.init(paths, 0, math.MaxUint64)
			if name == "no_delta" {
				require.NoError(t, err)
				require.NotNil(t, r.dr)
			} else if name == "walk_delta" {
				// The shared walk helper classifies raw backend failures as IO.
				require.ErrorIs(t, err, merr.ErrIoFailed)
				require.ErrorContains(t, err, cause.Error())
			} else {
				require.ErrorIs(t, err, cause)
			}
		})
	}
}

func TestBinlogReader_SizeError(t *testing.T) {
	cause := errors.New("object size unavailable")
	patch := mockey.Mock(storage.GetFilesSize).Return(int64(0), cause).Build()
	defer patch.UnPatch()
	r := &reader{ctx: context.Background(), fileSize: atomic.NewInt64(0)}
	size, err := r.Size()
	require.ErrorIs(t, err, cause)
	require.Zero(t, size)
	require.Zero(t, r.fileSize.Load(), "failed lookups must not populate the size cache")
}

func TestDeltaLogListing_RetryOnTransientError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	insertPrefix := "backup/insert_log/seg/"
	deltaPrefix := "backup/delta_log/seg/"

	// Insert walk succeeds immediately with one field
	cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "0/file1"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "1/file1"})
			return nil
		}).Once()

	// Delta walk: first call returns partial results + transient error, second call succeeds with empty result
	// Empty result triggers early return (len(deltaLogs) == 0) so readDelete is never called.
	deltaCallCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			deltaCallCount++
			if deltaCallCount == 1 {
				walkFunc(&storage.ChunkObjectInfo{FilePath: deltaPrefix + "partial"})
				return errors.New("net/http: timeout awaiting response headers")
			}
			// Second attempt: empty walk (no delta logs) — triggers early nil return
			return nil
		}).Times(2)

	r := &reader{
		ctx:            ctx,
		cm:             cm,
		schema:         &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 0}, {FieldID: 1}}},
		storageVersion: storage.StorageV1,
		retryAttempts:  5,
	}

	err := r.init([]string{insertPrefix, deltaPrefix}, 0, math.MaxUint64)
	assert.NoError(t, err)
	assert.Equal(t, 2, deltaCallCount, "delta log WalkWithPrefix should have retried on transient error")
}

func TestDeltaLogListing_NonRetryableErrorFailsFast(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	cm := mocks.NewChunkManager(t)

	insertPrefix := "backup/insert_log/seg/"
	deltaPrefix := "backup/delta_log/seg/"

	// Insert walk succeeds
	cm.EXPECT().WalkWithPrefix(mock.Anything, insertPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "0/file1"})
			walkFunc(&storage.ChunkObjectInfo{FilePath: insertPrefix + "1/file1"})
			return nil
		}).Once()

	// Delta walk: return non-retryable error
	deltaCallCount := 0
	cm.EXPECT().WalkWithPrefix(mock.Anything, deltaPrefix, true, mock.Anything).
		RunAndReturn(func(ctx context.Context, prefix string, recursive bool, walkFunc storage.ChunkObjectWalkFunc) error {
			deltaCallCount++
			return merr.WrapErrIoPermissionDenied(deltaPrefix, errors.New("access denied"))
		}).Once()

	r := &reader{
		ctx:            ctx,
		cm:             cm,
		schema:         &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 0}, {FieldID: 1}}},
		storageVersion: storage.StorageV1,
		retryAttempts:  5,
	}

	err := r.init([]string{insertPrefix, deltaPrefix}, 0, math.MaxUint64)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrIoPermissionDenied))
	assert.Equal(t, 1, deltaCallCount, "non-retryable error should not retry")
}

func TestMultiReadWithRetry_NonRetryableError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	cm := mocks.NewChunkManager(t)
	callCount := 0
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
			callCount++
			return nil, merr.WrapErrIoPermissionDenied("test/path", fmt.Errorf("access denied"))
		})

	r := &reader{ctx: ctx, cm: cm, retryAttempts: 3}
	_, err := r.multiReadWithRetry(ctx, []string{"test/path"})
	assert.Error(t, err)
	assert.True(t, merr.IsNonRetryableErr(err))
	assert.Equal(t, 1, callCount, "non-retryable error should not be retried")
}

func TestMultiReadWithRetry_RetryableError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	cm := mocks.NewChunkManager(t)
	callCount := 0
	cm.EXPECT().MultiRead(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, paths []string) ([][]byte, error) {
			callCount++
			if callCount < 3 {
				return nil, merr.WrapErrIoFailed("test/path", fmt.Errorf("transient error"))
			}
			return [][]byte{[]byte("data")}, nil
		})

	r := &reader{ctx: ctx, cm: cm, retryAttempts: 3}
	result, err := r.multiReadWithRetry(ctx, []string{"test/path"})
	assert.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte("data")}, result)
	assert.Equal(t, 3, callCount, "retryable error should be retried until success")
}
