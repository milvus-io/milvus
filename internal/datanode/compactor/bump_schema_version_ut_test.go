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
	"crypto/sha256"
	"fmt"
	sio "io"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ============================================================================
// Fixture: real StorageV3 source segments, deltalogs, golden helpers
// ============================================================================
// ---------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------

// setupBumpUTEnv points storage at a per-test local temp dir, mirroring the
// existing suite fixture (see BumpSchemaVersionCompactionTaskSuite.SetupTest).
func setupBumpUTEnv(t *testing.T) {
	paramtable.Get().Save("common.storage.enablev2", "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.StorageType.Key, "local")
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "false")
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())
	initcore.InitStorageV2FileSystem(paramtable.Get())
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().CommonCfg.StorageType.Key)
		paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
		paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key)
		paramtable.Get().Reset("common.storage.enablev2")
		initcore.CleanArrowFileSystem()
	})
}

// ---------------------------------------------------------------------------
// F1: fixture builder
// ---------------------------------------------------------------------------

const (
	bumpFxPKField   = int64(100)
	bumpFxTextField = int64(101)
)

type fixtureRow struct {
	pk     any
	ts     uint64
	values map[int64]any // by source field ID, excludes system fields
	kept   bool          // expected to survive full-rewrite filtering
}

type fixtureSpec struct {
	rows             int
	varCharPK        bool
	noSourceText     bool                    // exclude the base text field from the physically written source
	extraFields      []*schemapb.FieldSchema // physically written source fields beyond the base set
	fill             func(i int, ts uint64, v map[int64]any)
	rowTs            func(i int) uint64
	targetAdded      []*schemapb.FieldSchema
	targetDropped    []int64 // source field IDs removed from the target schema
	functions        []*schemapb.FunctionSchema
	collectionTTL    time.Duration
	ttlProperty      string
	deletedPKs       map[any]uint64 // pk -> delete ts, written as a real V3 deltalog
	commitTs         uint64
	fillTextAnalyzer bool  // decorate the base text field with multi_analyzer_params (by_field=lang)
	textLOBFieldID   int64 // >0: write this source TEXT field through the LOB-aware writer
}

type fixtureOpt func(*fixtureSpec)

func withRows(n int) fixtureOpt { return func(s *fixtureSpec) { s.rows = n } }

func withVarCharPK() fixtureOpt { return func(s *fixtureSpec) { s.varCharPK = true } }

func withoutSourceText() fixtureOpt { return func(s *fixtureSpec) { s.noSourceText = true } }

func withSourceFields(fs ...*schemapb.FieldSchema) fixtureOpt {
	return func(s *fixtureSpec) { s.extraFields = append(s.extraFields, fs...) }
}

func withFillValue(f func(i int, ts uint64, v map[int64]any)) fixtureOpt {
	return func(s *fixtureSpec) { s.fill = f }
}

func withRowTs(f func(i int) uint64) fixtureOpt { return func(s *fixtureSpec) { s.rowTs = f } }

func withTargetAddedField(fs ...*schemapb.FieldSchema) fixtureOpt {
	return func(s *fixtureSpec) { s.targetAdded = append(s.targetAdded, fs...) }
}

func withTargetDroppedField(ids ...int64) fixtureOpt {
	return func(s *fixtureSpec) { s.targetDropped = append(s.targetDropped, ids...) }
}

func withTargetFunctions(fns ...*schemapb.FunctionSchema) fixtureOpt {
	return func(s *fixtureSpec) { s.functions = append(s.functions, fns...) }
}

func withCollectionTTL(d time.Duration) fixtureOpt {
	return func(s *fixtureSpec) { s.collectionTTL = d }
}

func withTTLProperty(fieldName string) fixtureOpt {
	return func(s *fixtureSpec) { s.ttlProperty = fieldName }
}

func withDeletedPKs(pks map[any]uint64) fixtureOpt {
	return func(s *fixtureSpec) { s.deletedPKs = pks }
}

func withCommitTs(ts uint64) fixtureOpt { return func(s *fixtureSpec) { s.commitTs = ts } }

func withTextLOBSource(fieldID int64) fixtureOpt {
	return func(s *fixtureSpec) { s.textLOBFieldID = fieldID }
}

type bumpFixture struct {
	t            *testing.T
	task         *bumpSchemaVersionCompactionTask
	segment      *datapb.CompactionSegmentBinlogs
	sourceSchema *schemapb.CollectionSchema
	targetSchema *schemapb.CollectionSchema
	rows         []fixtureRow
	cfg          *indexpb.StorageConfig
	// manifest as written by the fixture, before Compact runs (source of truth
	// for verifySourceIntact).
	sourceManifest string
}

func bumpFxBaseFields(varCharPK bool) []*schemapb.FieldSchema {
	pk := &schemapb.FieldSchema{FieldID: bumpFxPKField, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
	if varCharPK {
		pk = &schemapb.FieldSchema{
			FieldID: bumpFxPKField, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}},
		}
	}
	return []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
		pk,
		{
			FieldID: bumpFxTextField, Name: "text", DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "2048"}},
		},
	}
}

// deterministic value formulas — golden rows are derived, never random.
func bumpFxPK(varCharPK bool, i int) any {
	if varCharPK {
		return fmt.Sprintf("pk-%06d", i)
	}
	return int64(i)
}

func bumpFxVarchar(fieldID int64, i int) string { return fmt.Sprintf("v-%d-%d", fieldID, i) }

func bumpFxTS(i int) uint64 {
	return tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Duration(i) * time.Second))
}

// buildBumpFixture writes a real StorageV3 source segment (and optional real
// deltalogs) and returns a ready-to-Compact task plus the golden row set.
func buildBumpFixture(t *testing.T, opts ...fixtureOpt) *bumpFixture {
	spec := &fixtureSpec{rows: 3, rowTs: bumpFxTS}
	for _, opt := range opts {
		opt(spec)
	}

	baseFields := bumpFxBaseFields(spec.varCharPK)
	if spec.noSourceText {
		baseFields = baseFields[:3] // row_id, timestamp, pk
	}
	sourceFields := append(baseFields, spec.extraFields...)
	if spec.fillTextAnalyzer {
		for _, f := range sourceFields {
			if f.GetFieldID() == bumpFxTextField {
				f.TypeParams = append(f.TypeParams,
					&commonpb.KeyValuePair{Key: common.EnableAnalyzerKey, Value: "true"},
					&commonpb.KeyValuePair{Key: "multi_analyzer_params", Value: `{"by_field":"lang","analyzers":{"default":{"type":"standard"},"english":{"type":"english"}}}`},
				)
			}
		}
	}
	sourceSchema := &schemapb.CollectionSchema{Fields: sourceFields}

	// Target schema: source logical fields minus dropped, plus added, plus functions.
	dropped := make(map[int64]struct{}, len(spec.targetDropped))
	for _, id := range spec.targetDropped {
		dropped[id] = struct{}{}
	}
	targetFields := make([]*schemapb.FieldSchema, 0, len(sourceFields)+len(spec.targetAdded))
	for _, f := range sourceFields {
		if _, ok := dropped[f.GetFieldID()]; ok {
			continue
		}
		targetFields = append(targetFields, f)
	}
	targetFields = append(targetFields, spec.targetAdded...)
	targetSchema := &schemapb.CollectionSchema{
		Name:      "bump_ut",
		Fields:    targetFields,
		Functions: spec.functions,
	}
	if spec.ttlProperty != "" {
		targetSchema.Properties = append(targetSchema.Properties, &commonpb.KeyValuePair{
			Key: common.CollectionTTLFieldKey, Value: spec.ttlProperty,
		})
	}

	// Write the source segment through the real V3 writer.
	binlogIO := mock_util.NewMockBinlogIO(t)
	binlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil).Maybe()
	segIDAlloc := allocator.NewLocalAllocator(100, math.MaxInt64)
	logIDAlloc := allocator.NewLocalAllocator(9530, 19530)
	compAlloc := NewCompactionAllocator(segIDAlloc, logIDAlloc)
	params := compaction.GenParams()
	params.StorageVersion = storage.StorageV3

	fixtureWriterOpts := []storage.RwOption{
		storage.WithStorageConfig(params.StorageConfig),
		storage.WithVersion(storage.StorageV3),
	}
	if spec.textLOBFieldID > 0 {
		lobBasePath := path.Join(params.StorageConfig.GetRootPath(),
			common.SegmentInsertLogPath, metautil.JoinIDPath(CollectionID, PartitionID))
		fixtureWriterOpts = append(fixtureWriterOpts, storage.WithTextColumnConfigs([]packed.TextColumnConfig{{
			FieldID:             spec.textLOBFieldID,
			LobBasePath:         lobBasePath,
			InlineThreshold:     params.TextInlineThreshold,
			MaxLobFileBytes:     params.TextMaxLobFileBytes,
			FlushThresholdBytes: params.TextFlushThresholdBytes,
		}}))
	}
	writer, err := NewMultiSegmentWriter(context.Background(), binlogIO, compAlloc,
		64*1024*1024, sourceSchema, params, int64(spec.rows)+1000, PartitionID, CollectionID,
		"bump_ut_channel", compactionBatchSize, fixtureWriterOpts...)
	require.NoError(t, err)

	rows := make([]fixtureRow, 0, spec.rows)
	for i := 0; i < spec.rows; i++ {
		ts := spec.rowTs(i)
		pk := bumpFxPK(spec.varCharPK, i)
		values := map[int64]any{
			common.RowIDField:     int64(i),
			common.TimeStampField: int64(ts),
			bumpFxPKField:         pk,
		}
		if !spec.noSourceText {
			values[bumpFxTextField] = bumpFxVarchar(bumpFxTextField, i)
		}
		if spec.fill != nil {
			spec.fill(i, ts, values)
		}
		var pkValue storage.PrimaryKey
		if spec.varCharPK {
			pkValue = storage.NewVarCharPrimaryKey(pk.(string))
		} else {
			pkValue = storage.NewInt64PrimaryKey(pk.(int64))
		}
		require.NoError(t, writer.WriteValue(&storage.Value{PK: pkValue, Timestamp: int64(ts), Value: values}))
		golden := make(map[int64]any, len(values))
		for k, v := range values {
			if k == common.RowIDField || k == common.TimeStampField {
				continue
			}
			golden[k] = v
		}
		rows = append(rows, fixtureRow{pk: pk, ts: ts, values: golden, kept: true})
	}
	require.NoError(t, writer.Close())
	segments := writer.GetCompactionSegments()
	require.Len(t, segments, 1)
	written := segments[0]

	segment := &datapb.CompactionSegmentBinlogs{
		CollectionID:    CollectionID,
		PartitionID:     PartitionID,
		SegmentID:       written.GetSegmentID(),
		FieldBinlogs:    written.GetInsertLogs(),
		InsertChannel:   "bump_ut_channel",
		StorageVersion:  written.GetStorageVersion(),
		Manifest:        written.GetManifest(),
		CommitTimestamp: spec.commitTs,
	}

	// Real V3 deltalog: production recipe from BulkPackWriterV3.writeDelta.
	if len(spec.deletedPKs) > 0 {
		segment.Deltalogs = writeFixtureDeltalog(t, spec, segment, params.StorageConfig)
	}

	jsonParams, err := compaction.GenerateJSONParams(targetSchema)
	require.NoError(t, err)
	plan := &datapb.CompactionPlan{
		PlanID:                 999,
		Type:                   datapb.CompactionType_BumpSchemaVersionCompaction,
		SegmentBinlogs:         []*datapb.CompactionSegmentBinlogs{segment},
		Schema:                 targetSchema,
		TotalRows:              int64(spec.rows),
		PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 19531, End: math.MaxInt64},
		PreAllocatedLogIDs:     &datapb.IDRange{Begin: 20000, End: 30000},
		MaxSize:                64 * 1024 * 1024,
		JsonParams:             jsonParams,
		CollectionTtl:          int64(spec.collectionTTL),
		Channel:                "bump_ut_channel",
	}

	cm, err := storage.NewChunkManagerFactoryWithParam(paramtable.Get()).NewPersistentStorageChunkManager(context.Background())
	require.NoError(t, err)
	task := NewBumpSchemaVersionCompactionTask(context.Background(), cm, plan, params)

	fix := &bumpFixture{
		t:              t,
		task:           task,
		segment:        segment,
		sourceSchema:   sourceSchema,
		targetSchema:   targetSchema,
		rows:           rows,
		cfg:            params.StorageConfig,
		sourceManifest: segment.GetManifest(),
	}
	fix.computeKept(spec)
	return fix
}

// writeFixtureDeltalog writes a real V3 delta file and commits it to the source
// manifest, mirroring syncmgr.BulkPackWriterV3.writeDelta.
func writeFixtureDeltalog(t *testing.T, spec *fixtureSpec, segment *datapb.CompactionSegmentBinlogs, cfg *indexpb.StorageConfig) []*datapb.FieldBinlog {
	const deltaLogID = int64(777001)
	pks := make([]storage.PrimaryKey, 0, len(spec.deletedPKs))
	tss := make([]uint64, 0, len(spec.deletedPKs))
	for pk, ts := range spec.deletedPKs {
		if spec.varCharPK {
			pks = append(pks, storage.NewVarCharPrimaryKey(pk.(string)))
		} else {
			pks = append(pks, storage.NewInt64PrimaryKey(pk.(int64)))
		}
		tss = append(tss, ts)
	}
	pkType := schemapb.DataType_Int64
	if spec.varCharPK {
		pkType = schemapb.DataType_VarChar
	}
	basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifest())
	require.NoError(t, err)
	deltaPath := metautil.BuildDeltaLogPathV3(basePath, deltaLogID)

	writer, err := storage.NewDeltalogWriter(context.Background(), segment.GetCollectionID(),
		segment.GetPartitionID(), segment.GetSegmentID(), deltaLogID, pkType, deltaPath,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(cfg),
	)
	require.NoError(t, err)
	record, tsFrom, tsTo, err := storage.BuildDeleteRecord(pks, tss)
	require.NoError(t, err)
	defer record.Release()
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	newManifest, err := packed.AddDeltaLogsToManifest(segment.GetManifest(), cfg,
		[]packed.DeltaLogEntry{{Path: deltaPath, NumEntries: int64(len(pks))}})
	require.NoError(t, err)
	segment.Manifest = newManifest

	return []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{
		LogID:         deltaLogID,
		EntriesNum:    int64(len(pks)),
		TimestampFrom: tsFrom,
		TimestampTo:   tsTo,
	}}}}
}

// computeKept replicates EntityFilterImpl's rules exactly (delete / collection
// TTL / TTL field) so golden expectations are derived, not asserted ad hoc.
func (f *bumpFixture) computeKept(spec *fixtureSpec) {
	ttlFieldID := int64(-1)
	if spec.ttlProperty != "" {
		for _, field := range f.targetSchema.GetFields() {
			if field.GetName() == spec.ttlProperty && field.GetDataType() == schemapb.DataType_Timestamptz {
				ttlFieldID = field.GetFieldID()
			}
		}
	}
	now := time.Now()
	for i := range f.rows {
		row := &f.rows[i]
		effective := row.ts
		if spec.commitTs > effective {
			effective = spec.commitTs
		}
		if deleteTs, ok := spec.deletedPKs[row.pk]; ok && effective < deleteTs {
			row.kept = false
			continue
		}
		if spec.collectionTTL > 0 {
			entityTime, _ := tsoutil.ParseTS(effective)
			if now.UnixMilli()-entityTime.UnixMilli() >= spec.collectionTTL.Milliseconds() {
				row.kept = false
				continue
			}
		}
		if ttlFieldID >= common.StartOfUserFieldID {
			if v, ok := row.values[ttlFieldID]; ok && v != nil {
				if expireMicros, ok := v.(int64); ok && expireMicros >= 0 && now.UnixMicro() >= expireMicros {
					row.kept = false
				}
			}
		}
	}
}

func (f *bumpFixture) keptRows() []fixtureRow {
	kept := make([]fixtureRow, 0, len(f.rows))
	for _, r := range f.rows {
		if r.kept {
			kept = append(kept, r)
		}
	}
	return kept
}

// ---------------------------------------------------------------------------
// F2: golden verifier
// ---------------------------------------------------------------------------

// readAllColumns reads every row of readSchema's fields from the manifest into
// per-field value slices (nil entry = NULL) and returns the row count.
func readAllColumns(t *testing.T, cfg *indexpb.StorageConfig, manifest string, readSchema *schemapb.CollectionSchema) (map[int64][]any, int) {
	reader, err := storage.NewManifestRecordReader(context.Background(), manifest, readSchema,
		storage.WithCollectionID(CollectionID),
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(cfg),
	)
	require.NoError(t, err)
	defer reader.Close()

	out := make(map[int64][]any)
	total := 0
	batches := 0
	for {
		rec, err := reader.Next()
		if err == sio.EOF {
			break
		}
		require.NoError(t, err)
		batches++
		total += rec.Len()
		for _, field := range readSchema.GetFields() {
			col := rec.Column(field.GetFieldID())
			require.NotNil(t, col, "field %d missing from record", field.GetFieldID())
			out[field.GetFieldID()] = appendArrowValues(t, out[field.GetFieldID()], col)
		}
	}
	t.Logf("readAllColumns: manifest rows=%d batches=%d", total, batches)
	return out, total
}

func appendArrowValues(t *testing.T, dst []any, col arrow.Array) []any {
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			dst = append(dst, nil)
			continue
		}
		switch arr := col.(type) {
		case *array.Int64:
			dst = append(dst, arr.Value(i))
		case *array.Int32:
			dst = append(dst, int64(arr.Value(i)))
		case *array.String:
			// String.Value aliases the reader's arrow buffer without copying;
			// clone it so the value survives the reader's Close (buffer free).
			dst = append(dst, strings.Clone(arr.Value(i)))
		case *array.Binary:
			dst = append(dst, append([]byte(nil), arr.Value(i)...))
		case *array.FixedSizeBinary:
			dst = append(dst, append([]byte(nil), arr.Value(i)...))
		case *array.Float32:
			dst = append(dst, arr.Value(i))
		case *array.Float64:
			dst = append(dst, arr.Value(i))
		case *array.Boolean:
			dst = append(dst, arr.Value(i))
		default:
			t.Fatalf("appendArrowValues: unsupported arrow type %T", col)
		}
	}
	return dst
}

// verifySegmentData asserts every column of every row (value + null bitmap +
// row order) matches wantRows for all non-system fields in readSchema.
func verifySegmentData(t *testing.T, cfg *indexpb.StorageConfig, manifest string, readSchema *schemapb.CollectionSchema, wantRows []fixtureRow) {
	got, total := readAllColumns(t, cfg, manifest, readSchema)
	require.Equal(t, len(wantRows), total, "row count mismatch")
	for _, field := range readSchema.GetFields() {
		fieldID := field.GetFieldID()
		if common.IsSystemField(fieldID) {
			continue
		}
		colValues := got[fieldID]
		require.Len(t, colValues, len(wantRows), "field %d column length", fieldID)
		for i, want := range wantRows {
			assertGoldenValue(t, fieldID, i, want.values[fieldID], colValues[i])
		}
	}
}

func assertGoldenValue(t *testing.T, fieldID int64, row int, want, got any) {
	if want == nil {
		require.Nil(t, got, "field %d row %d: expected NULL", fieldID, row)
		return
	}
	switch w := want.(type) {
	case int64:
		require.Equal(t, w, got, "field %d row %d", fieldID, row)
	case int:
		require.EqualValues(t, w, got, "field %d row %d", fieldID, row)
	case string:
		require.Equal(t, w, got, "field %d row %d", fieldID, row)
	case []byte:
		require.Equal(t, w, got, "field %d row %d", fieldID, row)
	default:
		require.Equal(t, want, got, "field %d row %d", fieldID, row)
	}
}

// verifyManifestFields asserts physical field membership of a manifest.
func verifyManifestFields(t *testing.T, cfg *indexpb.StorageConfig, manifest string, want []int64, absent []int64) {
	fields, err := packed.GetManifestFieldIDs(manifest, cfg)
	require.NoError(t, err)
	for _, id := range want {
		require.Contains(t, fields, id, "manifest must contain field %d", id)
	}
	for _, id := range absent {
		require.NotContains(t, fields, id, "manifest must not contain field %d", id)
	}
}

// verifySourceIntact asserts a failed compaction left the source segment
// untouched: same manifest pointer, source data still fully readable.
func verifySourceIntact(t *testing.T, f *bumpFixture) {
	require.Equal(t, f.sourceManifest, f.segment.GetManifest(), "source manifest pointer must not advance")
	readSchema := &schemapb.CollectionSchema{Fields: physicalReadFields(f.sourceSchema)}
	_, total := readAllColumns(t, f.cfg, f.sourceManifest, readSchema)
	require.Equal(t, len(f.rows), total, "source rows must stay readable after failure")
}

// physicalReadFields drops system fields (readers require user fields only in
// some verification paths; keeping pk+data fields is sufficient for goldens).
func physicalReadFields(schema *schemapb.CollectionSchema) []*schemapb.FieldSchema {
	fields := make([]*schemapb.FieldSchema, 0, len(schema.GetFields()))
	for _, f := range schema.GetFields() {
		if common.IsSystemField(f.GetFieldID()) {
			continue
		}
		fields = append(fields, f)
	}
	return fields
}

// timestampReadSchema reads only the system timestamp column.
func timestampReadSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
	}}
}

// runCompact runs the task and returns the single result segment.
func runCompact(t *testing.T, f *bumpFixture) *datapb.CompactionSegment {
	result, err := f.task.Compact()
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, datapb.CompactionTaskState_completed, result.GetState())
	require.Len(t, result.GetSegments(), 1)
	return result.GetSegments()[0]
}

func fieldIDsOf(schema *schemapb.CollectionSchema) []int64 {
	ids := make([]int64, 0, len(schema.GetFields()))
	for _, f := range typeutil.GetAllFieldSchemas(schema) {
		ids = append(ids, f.GetFieldID())
	}
	return ids
}

// ---------------------------------------------------------------------------
// golden transforms & case helpers
// ---------------------------------------------------------------------------

// bumpFxPad fattens varchar payloads so large fixtures span multiple reader
// batches / row groups.
var bumpFxPad = func() string {
	b := make([]byte, 1024)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}()

// goldenWithAdded returns a copy of rows with fieldID set per-row via valueAt.
func goldenWithAdded(rows []fixtureRow, fieldID int64, valueAt func(i int) any) []fixtureRow {
	out := make([]fixtureRow, len(rows))
	copy(out, rows)
	for i := range out {
		values := make(map[int64]any, len(out[i].values)+1)
		for k, v := range out[i].values {
			values[k] = v
		}
		values[fieldID] = valueAt(i)
		out[i].values = values
	}
	return out
}

// dropField returns a copy of rows with fieldID removed from every row.
func dropField(rows []fixtureRow, fieldID int64) []fixtureRow {
	out := make([]fixtureRow, len(rows))
	copy(out, rows)
	for i := range out {
		values := make(map[int64]any, len(out[i].values))
		for k, v := range out[i].values {
			if k == fieldID {
				continue
			}
			values[k] = v
		}
		out[i].values = values
	}
	return out
}

// withBM25Target adds the standard missing-BM25 target: sparse output 102
// produced from the base text input 101.
func withBM25Target() fixtureOpt {
	return func(s *fixtureSpec) {
		s.targetAdded = append(s.targetAdded, &schemapb.FieldSchema{
			FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true,
		})
		s.functions = append(s.functions, &schemapb.FunctionSchema{
			Name: "bm25", Id: 100, Type: schemapb.FunctionType_BM25,
			InputFieldNames: []string{"text"}, InputFieldIds: []int64{bumpFxTextField},
			OutputFieldNames: []string{"sparse"}, OutputFieldIds: []int64{102},
		})
	}
}

// withMultiAnalyzerBM25Target is withBM25Target plus multi_analyzer_params on
// the text input (by_field = lang, field 115).
func withMultiAnalyzerBM25Target() fixtureOpt {
	return func(s *fixtureSpec) {
		withBM25Target()(s)
		s.fillTextAnalyzer = true
	}
}

// requireSparseRows verifies the materialized sparse column. When expected is
// non-nil it byte-compares every row — the content<->row alignment oracle that
// non-emptiness cannot provide (review W1: rotation/misalignment corruption is
// invisible to NotNil and to constant NULL/default columns).
func requireSparseRows(t *testing.T, fix *bumpFixture, manifest string, fieldID int64, wantRows int, expected [][]byte) {
	got, total := readAllColumns(t, fix.cfg, manifest, &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{{FieldID: fieldID, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector}},
	})
	require.Equal(t, wantRows, total)
	for i, v := range got[fieldID] {
		require.NotNil(t, v, "sparse row %d must be materialized", i)
		if expected != nil {
			require.Equal(t, expected[i], v, "sparse content<->row alignment at %d", i)
		}
	}
}

// expectedBM25SparseRows derives the expected sparse bytes per row by running
// the REAL BM25 runner over the deterministic golden inputs. Runner mirroring
// is acceptable for this oracle: it targets row<->content alignment, not
// analyzer semantics.
func expectedBM25SparseRows(t *testing.T, schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema, inputs ...any) [][]byte {
	runner, err := function.NewFunctionRunner(schema, fn)
	require.NoError(t, err)
	defer runner.Close()
	outputs, err := runner.BatchRun(inputs...)
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	sparse, ok := outputs[0].(*schemapb.SparseFloatArray)
	require.True(t, ok, "BM25 runner must return SparseFloatArray, got %T", outputs[0])
	return sparse.GetContents()
}

// verifySystemColumns reads the row_id and timestamp system columns back from
// the written files and compares them row by row — verifySegmentData skips
// system fields, so passthrough/normalization contracts need this explicit
// oracle.
func verifySystemColumns(t *testing.T, fix *bumpFixture, manifest string, wantRowID, wantTs func(i int) int64) {
	got, total := readAllColumns(t, fix.cfg, manifest, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "ts", DataType: schemapb.DataType_Int64},
	}})
	require.Positive(t, total)
	for i := 0; i < total; i++ {
		if wantRowID != nil {
			require.Equal(t, wantRowID(i), got[common.RowIDField][i], "row_id at %d", i)
		}
		if wantTs != nil {
			require.Equal(t, wantTs(i), got[common.TimeStampField][i], "ts at %d", i)
		}
	}
}

// listSegmentDataFiles snapshots the segment's _data parquet files (relative
// path -> size:sha256): the on-disk column-group CONTENT ledger for
// exact-append/idempotency assertions — content-hashed so a same-size in-place
// tamper is visible, which set-semantics manifest checks cannot express.
func listSegmentDataFiles(t *testing.T, fix *bumpFixture) map[string]string {
	return snapshotFilesByHash(t, fix, "/_data/", "")
}

// listLobFiles snapshots every .vx LOB blob (relative path -> size:sha256).
func listLobFiles(t *testing.T, fix *bumpFixture) map[string]string {
	return snapshotFilesByHash(t, fix, "", ".vx")
}

func snapshotFilesByHash(t *testing.T, fix *bumpFixture, contains, suffix string) map[string]string {
	root := fix.cfg.GetRootPath()
	paths := make(map[string]string) // rel -> abs, collected first; reads happen after the walk
	require.NoError(t, filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if contains != "" && !strings.Contains(p, contains) {
			return nil
		}
		if suffix != "" && !strings.HasSuffix(p, suffix) {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		require.NoError(t, rerr)
		paths[rel] = p
		return nil
	}))
	files := make(map[string]string, len(paths))
	for rel, abs := range paths {
		content, err := os.ReadFile(abs)
		require.NoError(t, err)
		files[rel] = fmt.Sprintf("%d:%x", len(content), sha256.Sum256(content))
	}
	return files
}

// requireBM25StatsBlob locates the committed bm25 stats blob under the segment
// base path and asserts it deserializes into non-empty BM25 stats.
func requireBM25StatsBlob(t *testing.T, fix *bumpFixture, fieldID int64, wantRows int) *storage.BM25Stats {
	needle := fmt.Sprintf("_stats/bm25.%d/", fieldID)
	cm, err := storage.NewChunkManagerFactoryWithParam(paramtable.Get()).NewPersistentStorageChunkManager(context.Background())
	require.NoError(t, err)
	var paths []string
	require.NoError(t, cm.WalkWithPrefix(context.Background(), cm.RootPath(), true, func(obj *storage.ChunkObjectInfo) bool {
		if strings.Contains(obj.FilePath, needle) {
			paths = append(paths, obj.FilePath)
		}
		return true
	}))
	require.NotEmpty(t, paths, "bm25 stats blob must be committed (needle %s)", needle)
	blob, err := cm.Read(context.Background(), paths[0])
	require.NoError(t, err)
	stats := storage.NewBM25Stats()
	require.NoError(t, stats.Deserialize(blob))
	require.EqualValues(t, wantRows, stats.NumRow())
	return stats
}

func mustBasePath(t *testing.T, manifest string) string {
	basePath, _, err := packed.UnmarshalManifestPath(manifest)
	require.NoError(t, err)
	return basePath
}

// ============================================================================
// Data goldens: bump-only / additive / full-rewrite (B/A/F series)
// ============================================================================
// [B1][S0] bump-only: nothing absent, nothing dropped — the result must echo
// the plan segment verbatim (pure metadata bump, zero data churn).
func TestBumpUTBumpOnlyEchoesSource(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t)

	seg := runCompact(t, fix)
	require.Equal(t, fix.segment.GetSegmentID(), seg.GetSegmentID())
	require.EqualValues(t, len(fix.rows), seg.GetNumOfRows())
	require.Equal(t, fix.segment.GetManifest(), seg.GetManifest())
	require.Equal(t, fix.segment.GetManifest(), seg.GetBaseManifest())
	require.Equal(t, fix.segment.GetFieldBinlogs(), seg.GetInsertLogs())
	require.Nil(t, seg.GetStats())

	// Source data still fully readable and golden.
	verifySegmentData(t, fix.cfg, seg.GetManifest(),
		&schemapb.CollectionSchema{Fields: physicalReadFields(fix.sourceSchema)}, fix.rows)
}

// [A1][S0] additive: +nullable int64 — every historical row gets NULL, the new
// column joins the manifest, existing columns stay golden, and the result is an
// in-place increment (BaseManifest == source manifest).
func TestBumpUTAdditiveNullableColumn(t *testing.T) {
	setupBumpUTEnv(t)
	const addedID = int64(103)
	fix := buildBumpFixture(t, withTargetAddedField(&schemapb.FieldSchema{
		FieldID: addedID, Name: "added_nullable", DataType: schemapb.DataType_Int64, Nullable: true,
	}))

	seg := runCompact(t, fix)
	require.Equal(t, fix.segment.GetSegmentID(), seg.GetSegmentID(), "additive is in-place")
	require.EqualValues(t, len(fix.rows), seg.GetNumOfRows())
	// [A21] CAS adoption prerequisites.
	require.Equal(t, fix.sourceManifest, seg.GetBaseManifest())
	require.NotEqual(t, fix.sourceManifest, seg.GetManifest(), "manifest must advance")

	// Golden: old columns unchanged, new column all NULL.
	want := make([]fixtureRow, len(fix.rows))
	copy(want, fix.rows)
	for i := range want {
		values := make(map[int64]any, len(want[i].values)+1)
		for k, v := range want[i].values {
			values[k] = v
		}
		values[addedID] = nil
		want[i].values = values
	}
	readSchema := &schemapb.CollectionSchema{Fields: append(physicalReadFields(fix.sourceSchema),
		fix.targetSchema.GetFields()[len(fix.targetSchema.GetFields())-1])}
	verifySegmentData(t, fix.cfg, seg.GetManifest(), readSchema, want)
	verifyManifestFields(t, fix.cfg, seg.GetManifest(), []int64{addedID}, nil)

	// Increment stats describe only the new column group.
	require.NotNil(t, seg.GetStats())
	require.EqualValues(t, 1, seg.GetStats().GetInsertBinlogCount())
	require.EqualValues(t, len(fix.rows), seg.GetStats().GetNullCounts()[addedID])
	// The new column group's insert-log summary must carry the exact row count.
	require.Len(t, seg.GetInsertLogs(), 1)
	require.EqualValues(t, len(fix.rows), seg.GetInsertLogs()[0].GetBinlogs()[0].GetEntriesNum())

	// [TS1][S0] additive must not touch system columns: both pass through.
	verifySystemColumns(t, fix, seg.GetManifest(),
		func(i int) int64 { return int64(i) },
		func(i int) int64 { return int64(fix.rows[i].ts) })
}

// [F1][S0] full rewrite, pure drop: no filtering configured — all rows survive,
// all remaining columns stay golden, dropped column leaves the manifest.
func TestBumpUTFullRewritePureDrop(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID = int64(102)
	fix := buildBumpFixture(t,
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[droppedID] = int64(i * 10) }),
		withTargetDroppedField(droppedID),
	)

	seg := runCompact(t, fix)
	require.NotEqual(t, fix.segment.GetSegmentID(), seg.GetSegmentID(), "full rewrite is a replacement")
	require.EqualValues(t, len(fix.rows), seg.GetNumOfRows())
	require.NotEmpty(t, seg.GetManifest())

	// Golden on the target schema: dropped column absent, others intact.
	want := make([]fixtureRow, len(fix.rows))
	copy(want, fix.rows)
	for i := range want {
		values := make(map[int64]any, len(want[i].values))
		for k, v := range want[i].values {
			if k == droppedID {
				continue
			}
			values[k] = v
		}
		want[i].values = values
	}
	verifySegmentData(t, fix.cfg, seg.GetManifest(),
		&schemapb.CollectionSchema{Fields: physicalReadFields(fix.targetSchema)}, want)
	verifyManifestFields(t, fix.cfg, seg.GetManifest(), []int64{bumpFxPKField, bumpFxTextField}, []int64{droppedID})

	// [TS2/W2][S0] commitTs==0 full rewrite: BOTH system columns pass through
	// untouched (verifySegmentData skips system fields).
	verifySystemColumns(t, fix, seg.GetManifest(),
		func(i int) int64 { return int64(i) },
		func(i int) int64 { return int64(fix.rows[i].ts) })
}

// ---------------------------------------------------------------------------
// bump-only
// ---------------------------------------------------------------------------

// [B2][S0] bump-only with delta summaries: passthrough must carry Deltalogs.
func TestBumpUTBumpOnlyCarriesDeltalogs(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t, withRows(4), withDeletedPKs(map[any]uint64{int64(1): bumpFxTS(1) + 1}))

	seg := runCompact(t, fix)
	require.Equal(t, fix.segment.GetSegmentID(), seg.GetSegmentID())
	require.Equal(t, fix.segment.GetDeltalogs(), seg.GetDeltalogs())
	require.Equal(t, fix.segment.GetManifest(), seg.GetManifest())
	// bump-only must NOT apply the delete: all 4 physical rows stay readable.
	verifySegmentData(t, fix.cfg, seg.GetManifest(),
		&schemapb.CollectionSchema{Fields: physicalReadFields(fix.sourceSchema)}, fix.rows)
}

// [B3][S3] function outputs physically present: routes bump-only, no rewrite.
func TestBumpUTBumpOnlyWhenFunctionOutputsPresent(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t,
		withSourceFields(&schemapb.FieldSchema{
			FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true,
		}),
		withFillValue(func(i int, ts uint64, v map[int64]any) {
			v[102] = typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{uint32(i + 1): 1.0})
		}),
		withTargetFunctions(&schemapb.FunctionSchema{
			Name: "bm25", Id: 100, Type: schemapb.FunctionType_BM25,
			InputFieldNames: []string{"text"}, InputFieldIds: []int64{bumpFxTextField},
			OutputFieldNames: []string{"sparse"}, OutputFieldIds: []int64{102},
		}),
	)

	seg := runCompact(t, fix)
	require.Equal(t, fix.segment.GetSegmentID(), seg.GetSegmentID())
	require.Equal(t, fix.segment.GetManifest(), seg.GetManifest(), "no rewrite may happen")
}

// ---------------------------------------------------------------------------
// additive
// ---------------------------------------------------------------------------

// [A2][S0] +default column: backfilled as NULL — the reader layer null-fills
// absent fields and default materialization is deferred; the declared default
// must not leak into historical rows.
func TestBumpUTAdditiveDefaultColumn(t *testing.T) {
	setupBumpUTEnv(t)
	const addedID = int64(103)
	fix := buildBumpFixture(t, withTargetAddedField(&schemapb.FieldSchema{
		FieldID: addedID, Name: "added_default", DataType: schemapb.DataType_Int64, Nullable: true,
		DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}},
	}))

	seg := runCompact(t, fix)
	// #52781 (merged): the packed reader default-fills absent default-valued fields,
	// so the added column materializes its declared default (42), not NULL.
	want := goldenWithAdded(fix.rows, addedID, func(int) any { return int64(42) })
	readSchema := &schemapb.CollectionSchema{Fields: append(physicalReadFields(fix.sourceSchema),
		typeutil.GetField(fix.targetSchema, addedID))}
	verifySegmentData(t, fix.cfg, seg.GetManifest(), readSchema, want)
	require.EqualValues(t, 0, seg.GetStats().GetNullCounts()[addedID])
}

// [A3][S0] mixed additions: nullable + default + nullable-TEXT(binary NULL) in
// one pass; the nullable and TEXT columns land as NULL, the default column
// materializes its declared default (#52781), old columns stay golden.
func TestBumpUTAdditiveMixedColumns(t *testing.T) {
	setupBumpUTEnv(t)
	const nullID, defID, textID = int64(103), int64(104), int64(105)
	fix := buildBumpFixture(t,
		withTargetAddedField(
			&schemapb.FieldSchema{FieldID: nullID, Name: "n1", DataType: schemapb.DataType_Int64, Nullable: true},
			&schemapb.FieldSchema{
				FieldID: defID, Name: "d1", DataType: schemapb.DataType_Int64, Nullable: true,
				DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 7}},
			},
			&schemapb.FieldSchema{
				FieldID: textID, Name: "t1", DataType: schemapb.DataType_Text, Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
			},
		))

	seg := runCompact(t, fix)
	want := goldenWithAdded(fix.rows, nullID, func(int) any { return nil })
	want = goldenWithAdded(want, defID, func(int) any { return int64(7) })
	want = goldenWithAdded(want, textID, func(int) any { return nil })
	readSchema := &schemapb.CollectionSchema{Fields: append(physicalReadFields(fix.sourceSchema),
		typeutil.GetField(fix.targetSchema, nullID),
		typeutil.GetField(fix.targetSchema, defID),
		typeutil.GetField(fix.targetSchema, textID))}
	verifySegmentData(t, fix.cfg, seg.GetManifest(), readSchema, want)
	verifyManifestFields(t, fix.cfg, seg.GetManifest(), []int64{nullID, defID, textID}, nil)
	require.EqualValues(t, 3, seg.GetStats().GetInsertBinlogCount())
}

// [A4][S0] BM25 materialization: sparse output present for every row and the
// bm25 stats blob is committed and deserializable.
func TestBumpUTAdditiveBM25Materialization(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t, withBM25Target())

	seg := runCompact(t, fix)
	require.EqualValues(t, len(fix.rows), seg.GetNumOfRows())
	texts := make([]any, 0, 1)
	rows := make([]string, len(fix.rows))
	for i := range fix.rows {
		rows[i] = bumpFxVarchar(bumpFxTextField, i)
	}
	texts = append(texts, rows)
	expected := expectedBM25SparseRows(t, fix.targetSchema, fix.targetSchema.GetFunctions()[0], texts...)
	requireSparseRows(t, fix, seg.GetManifest(), 102, len(fix.rows), expected)
	// [ST1][S1] the committed stats ledger must equal the accumulation of
	// exactly the written rows — deserializable+non-empty cannot see
	// over/under-counting.
	expectedStats := storage.NewBM25Stats()
	expectedStats.AppendBytes(expected...)
	require.Equal(t, expectedStats, requireBM25StatsBlob(t, fix, 102, len(fix.rows)))
}

// expectedMinHashRows derives the expected binary-vector bytes per row by
// running the REAL MinHash runner over the deterministic golden inputs — same
// oracle rationale as expectedBM25SparseRows: row<->content alignment, not
// hash semantics.
func expectedMinHashRows(t *testing.T, schema *schemapb.CollectionSchema, fn *schemapb.FunctionSchema, texts []string) [][]byte {
	runner, err := function.NewFunctionRunner(schema, fn)
	require.NoError(t, err)
	defer runner.Close()
	outputs, err := runner.BatchRun(texts)
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	fieldData, ok := outputs[0].(*schemapb.FieldData)
	require.True(t, ok, "MinHash runner must return FieldData, got %T", outputs[0])
	flat := fieldData.GetVectors().GetBinaryVector()
	rowBytes := int(fieldData.GetVectors().GetDim()) / 8
	require.Positive(t, rowBytes)
	require.Len(t, flat, rowBytes*len(texts))
	rows := make([][]byte, len(texts))
	for i := range rows {
		rows[i] = flat[i*rowBytes : (i+1)*rowBytes]
	}
	return rows
}

// [A5][S0] MinHash materialization: binary-vector output for every row, each
// row byte-equal to the real runner's output for ITS OWN text (row<->content
// alignment — Len/NotNil alone cannot see rotation, review Round-3).
func TestBumpUTAdditiveMinHashMaterialization(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t,
		withTargetAddedField(&schemapb.FieldSchema{
			FieldID: 102, Name: "minhash", DataType: schemapb.DataType_BinaryVector, IsFunctionOutput: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "32"}},
		}),
		withTargetFunctions(&schemapb.FunctionSchema{
			Name: "minhash_func", Id: 1000, Type: schemapb.FunctionType_MinHash,
			InputFieldNames: []string{"text"}, InputFieldIds: []int64{bumpFxTextField},
			OutputFieldNames: []string{"minhash"}, OutputFieldIds: []int64{102},
		}),
	)

	seg := runCompact(t, fix)
	got, total := readAllColumns(t, fix.cfg, seg.GetManifest(), &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{typeutil.GetField(fix.targetSchema, 102)},
	})
	require.Equal(t, len(fix.rows), total)
	texts := make([]string, len(fix.rows))
	for i := range fix.rows {
		texts[i] = bumpFxVarchar(bumpFxTextField, i)
	}
	expected := expectedMinHashRows(t, fix.targetSchema, fix.targetSchema.GetFunctions()[0], texts)
	for i, v := range got[102] {
		require.NotNil(t, v, "minhash row %d", i)
		require.Len(t, v.([]byte), 32/8, "minhash row %d dim", i)
		require.Equal(t, expected[i], v.([]byte), "minhash content<->row alignment at %d", i)
	}
}

// [A6][S0] BM25 multi-analyzer by_field: materialization succeeds with the
// per-row analyzer selector physically read from the segment.
func TestBumpUTAdditiveBM25MultiAnalyzer(t *testing.T) {
	setupBumpUTEnv(t)
	const langID = int64(115)
	fix := buildBumpFixture(t,
		withSourceFields(&schemapb.FieldSchema{
			FieldID: langID, Name: "lang", DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "16"}},
		}),
		withFillValue(func(i int, ts uint64, v map[int64]any) {
			if i%2 == 0 {
				v[langID] = "default"
			} else {
				v[langID] = "english"
			}
		}),
		withMultiAnalyzerBM25Target(),
	)

	seg := runCompact(t, fix)
	requireSparseRows(t, fix, seg.GetManifest(), 102, len(fix.rows), nil) // multi-analyzer needs the lang input; alignment oracle lives in A4/F21
}

// [A7][S0] multi-batch additive: new column stays row-aligned with the anchor
// across reader batches (pk-value cross-check).
func TestBumpUTAdditiveMultiBatchAlignment(t *testing.T) {
	setupBumpUTEnv(t)
	const rows = 50_000
	const addedID = int64(103)
	fix := buildBumpFixture(t, withRows(rows),
		withFillValue(func(i int, ts uint64, v map[int64]any) {
			v[bumpFxTextField] = fmt.Sprintf("%s-%s", bumpFxVarchar(bumpFxTextField, i), bumpFxPad)
		}),
		withTargetAddedField(&schemapb.FieldSchema{
			FieldID: addedID, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true,
		}))

	seg := runCompact(t, fix)
	require.EqualValues(t, rows, seg.GetNumOfRows())

	readSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		typeutil.GetField(fix.targetSchema, bumpFxPKField),
		typeutil.GetField(fix.targetSchema, addedID),
	}}
	got, total := readAllColumns(t, fix.cfg, seg.GetManifest(), readSchema)
	require.Equal(t, rows, total)
	for i := 0; i < rows; i += 997 { // stride sampling incl. batch boundaries
		require.Equal(t, int64(i), got[bumpFxPKField][i], "pk alignment at %d", i)
		require.Nil(t, got[addedID][i], "added col at %d", i)
	}
	require.EqualValues(t, rows, seg.GetStats().GetNullCounts()[addedID])
}

// [A11][S0] #52159 shape: the BM25 input field itself was added after sealing
// (physically absent). Input is synthesized, output materialized, no crash.
func TestBumpUTAdditiveFunctionInputAddedAfterSeal(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t,
		withoutSourceText(),
		withTargetAddedField(&schemapb.FieldSchema{
			FieldID: bumpFxTextField, Name: "text", DataType: schemapb.DataType_VarChar, Nullable: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "2048"}},
		}),
		withBM25Target(),
	)

	seg := runCompact(t, fix)
	require.EqualValues(t, len(fix.rows), seg.GetNumOfRows())
	// Both the synthesized input column and the function output must land.
	// Empty synthesized text yields empty embeddings, so cells may legally read
	// back NULL — row cardinality is the invariant here.
	verifyManifestFields(t, fix.cfg, seg.GetManifest(), []int64{bumpFxTextField, 102}, nil)
	_, total := readAllColumns(t, fix.cfg, seg.GetManifest(), &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{typeutil.GetField(fix.targetSchema, 102)},
	})
	require.Equal(t, len(fix.rows), total)
}

// ---------------------------------------------------------------------------
// full rewrite
// ---------------------------------------------------------------------------

// [F2][S0] real deltalog: deleted rows are physically dropped, survivors stay
// golden in every column, NumOfRows is exact.
func TestBumpUTFullRewriteAppliesRealDelta(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID = int64(103)
	// pk 5 is an upsert-shaped boundary: delete ts == row ts. Per the delete
	// contract (entity_filter.go: "Strict < is preserved so upserts keep the
	// inserted row") it must SURVIVE — this expectation is spec-derived, and
	// guards the < vs <= boundary independently of the implementation.
	fix := buildBumpFixture(t, withRows(6),
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[droppedID] = int64(i) }),
		withTargetDroppedField(droppedID),
		withDeletedPKs(map[any]uint64{int64(1): bumpFxTS(1) + 1, int64(4): bumpFxTS(4) + 1, int64(5): bumpFxTS(5)}),
	)
	require.Len(t, fix.keptRows(), 4, "fixture golden: rows 1,4 deleted; upsert-boundary row 5 kept")

	seg := runCompact(t, fix)
	require.EqualValues(t, 4, seg.GetNumOfRows())
	verifySegmentData(t, fix.cfg, seg.GetManifest(),
		&schemapb.CollectionSchema{Fields: physicalReadFields(fix.targetSchema)}, dropField(fix.keptRows(), droppedID))
}

// [F3][S0] collection TTL: expired rows dropped, fresh rows kept and golden.
func TestBumpUTFullRewriteCollectionTTL(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID = int64(103)
	nowTS := tsoutil.ComposeTSByTime(time.Now())
	fix := buildBumpFixture(t, withRows(6),
		withRowTs(func(i int) uint64 {
			if i < 2 {
				return bumpFxTS(i) // 2021 -> expired under 1h TTL
			}
			return nowTS + uint64(i)
		}),
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[droppedID] = int64(i) }),
		withTargetDroppedField(droppedID),
		withCollectionTTL(time.Hour),
	)
	require.Len(t, fix.keptRows(), 4)

	seg := runCompact(t, fix)
	require.EqualValues(t, 4, seg.GetNumOfRows())
	verifySegmentData(t, fix.cfg, seg.GetManifest(),
		&schemapb.CollectionSchema{Fields: physicalReadFields(fix.targetSchema)}, dropField(fix.keptRows(), droppedID))
}

// [F4][S0] entity-TTL column: expired-by-value rows dropped, NULL-TTL rows kept.
func TestBumpUTFullRewriteEntityTTLColumn(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID, ttlID = int64(103), int64(104)
	past := time.Now().Add(-time.Hour).UnixMicro()
	future := time.Now().Add(24 * time.Hour).UnixMicro()
	fix := buildBumpFixture(t, withRows(6),
		withSourceFields(
			&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64},
			&schemapb.FieldSchema{FieldID: ttlID, Name: "expire_at", DataType: schemapb.DataType_Timestamptz, Nullable: true},
		),
		withFillValue(func(i int, ts uint64, v map[int64]any) {
			v[droppedID] = int64(i)
			switch i % 3 {
			case 0:
				v[ttlID] = past // expired
			case 1:
				v[ttlID] = future // kept
			default:
				v[ttlID] = nil // NULL ttl -> kept
			}
		}),
		withTargetDroppedField(droppedID),
		withTTLProperty("expire_at"),
	)
	require.Len(t, fix.keptRows(), 4)

	seg := runCompact(t, fix)
	require.EqualValues(t, 4, seg.GetNumOfRows())
	verifySegmentData(t, fix.cfg, seg.GetManifest(),
		&schemapb.CollectionSchema{Fields: physicalReadFields(fix.targetSchema)}, dropField(fix.keptRows(), droppedID))
}

// [F5][S0] delete + collection TTL + entity TTL combined, disjoint causes.
func TestBumpUTFullRewriteMixedFilters(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID, ttlID = int64(103), int64(104)
	nowTS := tsoutil.ComposeTSByTime(time.Now())
	past := time.Now().Add(-time.Hour).UnixMicro()
	future := time.Now().Add(24 * time.Hour).UnixMicro()
	fix := buildBumpFixture(t, withRows(6),
		withRowTs(func(i int) uint64 {
			if i == 1 {
				return bumpFxTS(i) // collection-TTL victim
			}
			return nowTS + uint64(i)
		}),
		withSourceFields(
			&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64},
			&schemapb.FieldSchema{FieldID: ttlID, Name: "expire_at", DataType: schemapb.DataType_Timestamptz, Nullable: true},
		),
		withFillValue(func(i int, ts uint64, v map[int64]any) {
			v[droppedID] = int64(i)
			if i == 2 {
				v[ttlID] = past // entity-TTL victim
			} else {
				v[ttlID] = future
			}
		}),
		withTargetDroppedField(droppedID),
		withTTLProperty("expire_at"),
		withCollectionTTL(time.Hour),
		withDeletedPKs(map[any]uint64{int64(0): nowTS + 100}), // delete victim
	)
	require.Len(t, fix.keptRows(), 3)

	seg := runCompact(t, fix)
	require.EqualValues(t, 3, seg.GetNumOfRows())
	verifySegmentData(t, fix.cfg, seg.GetManifest(),
		&schemapb.CollectionSchema{Fields: physicalReadFields(fix.targetSchema)}, dropField(fix.keptRows(), droppedID))
}

// [F6][S0] deletes spanning reader-batch boundaries, including a fully deleted
// prefix run — the compacted output must stay row-aligned in every column.
func TestBumpUTFullRewriteCrossBatchDeletes(t *testing.T) {
	setupBumpUTEnv(t)
	const rows = 30_000
	const droppedID = int64(103)
	deleted := map[any]uint64{}
	for i := 0; i < 512; i++ { // fully deleted prefix run
		deleted[int64(i)] = bumpFxTS(i) + 1
	}
	for i := 511; i < rows; i += 1013 { // scattered mid/batch-edge deletes
		deleted[int64(i)] = bumpFxTS(i) + 1
	}
	fix := buildBumpFixture(t, withRows(rows),
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) {
			v[droppedID] = int64(i)
			v[bumpFxTextField] = fmt.Sprintf("%s-%s", bumpFxVarchar(bumpFxTextField, i), bumpFxPad)
		}),
		withTargetDroppedField(droppedID),
		withDeletedPKs(deleted),
	)
	kept := fix.keptRows()
	require.Equal(t, rows-len(deleted), len(kept))

	seg := runCompact(t, fix)
	require.EqualValues(t, len(kept), seg.GetNumOfRows())
	got, total := readAllColumns(t, fix.cfg, seg.GetManifest(), &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{typeutil.GetField(fix.targetSchema, bumpFxPKField)},
	})
	require.Equal(t, len(kept), total)
	for i, want := range kept {
		require.Equal(t, want.pk, got[bumpFxPKField][i], "pk order at %d", i)
	}
}

// [F7][S0] filter armed but zero rows match: the zero-copy fast path must still
// produce a golden replacement.
func TestBumpUTFullRewriteFilterZeroHits(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID = int64(103)
	nowTS := tsoutil.ComposeTSByTime(time.Now())
	fix := buildBumpFixture(t, withRows(4),
		withRowTs(func(i int) uint64 { return nowTS + uint64(i) }),
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[droppedID] = int64(i) }),
		withTargetDroppedField(droppedID),
		withCollectionTTL(time.Hour),
	)
	require.Len(t, fix.keptRows(), 4)

	seg := runCompact(t, fix)
	require.EqualValues(t, 4, seg.GetNumOfRows())
	verifySegmentData(t, fix.cfg, seg.GetManifest(),
		&schemapb.CollectionSchema{Fields: physicalReadFields(fix.targetSchema)}, dropField(fix.keptRows(), droppedID))
}

// [F8][S1] everything deleted: the rewrite legally produces an empty result
// (0 rows, empty manifest) and never builds a text index.
func TestBumpUTFullRewriteAllRowsDeleted(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID = int64(103)
	nowTS := tsoutil.ComposeTSByTime(time.Now())
	fix := buildBumpFixture(t, withRows(3),
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[droppedID] = int64(i) }),
		withTargetDroppedField(droppedID),
		withDeletedPKs(map[any]uint64{int64(0): nowTS, int64(1): nowTS, int64(2): nowTS}),
	)
	require.Empty(t, fix.keptRows())

	textIndexCalls := 0
	patch := mockey.Mock(createTextIndex).To(func(_ context.Context, _ storage.ChunkManager, _ *datapb.CompactionPlan, _ compaction.Params,
		_ int64, _ int64, _ int64, _ int64, _ int64, _ *datapb.CompactionSegment,
	) (map[int64]*datapb.TextIndexStats, error) {
		textIndexCalls++
		return nil, nil
	}).Build()
	defer patch.UnPatch()

	seg := runCompact(t, fix)
	require.EqualValues(t, 0, seg.GetNumOfRows())
	require.Empty(t, seg.GetManifest())
	require.Zero(t, textIndexCalls, "text index must not run for an empty rewrite")
}

// [F9][S0] VarChar primary key with deletes.
func TestBumpUTFullRewriteVarCharPK(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID = int64(103)
	fix := buildBumpFixture(t, withRows(5), withVarCharPK(),
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[droppedID] = int64(i) }),
		withTargetDroppedField(droppedID),
		withDeletedPKs(map[any]uint64{"pk-000002": bumpFxTS(2) + 1}),
	)
	require.Len(t, fix.keptRows(), 4)

	seg := runCompact(t, fix)
	require.EqualValues(t, 4, seg.GetNumOfRows())
	verifySegmentData(t, fix.cfg, seg.GetManifest(),
		&schemapb.CollectionSchema{Fields: physicalReadFields(fix.targetSchema)}, dropField(fix.keptRows(), droppedID))
}

// [F11][S0] commit-timestamp normalization across all rows: the output ts
// column must read back as commitTs everywhere.
func TestBumpUTFullRewriteCommitTsNormalization(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID = int64(103)
	commitTs := tsoutil.ComposeTSByTime(time.Now())
	fix := buildBumpFixture(t, withRows(5),
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[droppedID] = int64(i) }),
		withTargetDroppedField(droppedID),
		withCommitTs(commitTs),
	)

	seg := runCompact(t, fix)
	require.EqualValues(t, 5, seg.GetNumOfRows())
	got, total := readAllColumns(t, fix.cfg, seg.GetManifest(), timestampReadSchema())
	require.Equal(t, 5, total)
	for i, v := range got[common.TimeStampField] {
		require.Equal(t, int64(commitTs), v, "row %d ts must be normalized to commitTs", i)
	}
	// [TS3][S0] commitTs must ONLY rewrite ts: row_id still passes through.
	verifySystemColumns(t, fix, seg.GetManifest(),
		func(i int) int64 { return int64(i) }, nil)
}

// [F12][S1] ExpirQuantiles passthrough: the result must carry exactly what the
// writer returned (writer-owned metadata, no bump-side recomputation).
func TestBumpUTFullRewriteQuantilesPassthrough(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID = int64(103)
	fix := buildBumpFixture(t,
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[droppedID] = int64(i) }),
		withTargetDroppedField(droppedID),
	)

	writer := &fakeBinlogRecordWriter{
		fieldBinlogs: map[storage.FieldID]*datapb.FieldBinlog{
			bumpFxPKField: {FieldID: bumpFxPKField, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 3}}},
		},
		manifest:       packed.MarshalManifestPath(mustBasePath(t, fix.sourceManifest)+"-out", 1),
		expirQuantiles: []int64{11, 22, 33},
		schema:         fix.targetSchema,
	}
	patch := mockey.Mock(storage.NewBinlogRecordWriter).Return(writer, nil).Build()
	defer patch.UnPatch()

	seg := runCompact(t, fix)
	require.Equal(t, []int64{11, 22, 33}, seg.GetExpirQuantiles())
}

// [F21][S0] dropped field + missing function output combined: full rewrite
// materializes the function output while dropping the column.
func TestBumpUTFullRewriteMaterializesFunctionOutput(t *testing.T) {
	setupBumpUTEnv(t)
	const droppedID = int64(103)
	fix := buildBumpFixture(t,
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[droppedID] = int64(i) }),
		withTargetDroppedField(droppedID),
		withBM25Target(),
	)

	seg := runCompact(t, fix)
	require.EqualValues(t, len(fix.rows), seg.GetNumOfRows())
	verifyManifestFields(t, fix.cfg, seg.GetManifest(), []int64{102}, []int64{droppedID})
	f21Texts := make([]string, len(fix.rows))
	for i := range fix.rows {
		f21Texts[i] = bumpFxVarchar(bumpFxTextField, i)
	}
	f21Expected := expectedBM25SparseRows(t, fix.targetSchema, fix.targetSchema.GetFunctions()[0], f21Texts)
	requireSparseRows(t, fix, seg.GetManifest(), 102, len(fix.rows), f21Expected)
}

// ---------------------------------------------------------------------------
// P1 regression: materialization slice budget (buqian review, PR #52484)
// ---------------------------------------------------------------------------

// [P1][S2][characterization] The no-fix decision for the additive batch
// amplification review (PR #52484, buqian) rests on two implicit
// storage-layer contracts, pinned here:
//  1. the V3 manifest reader delivers exactly ONE row group per Next()
//     (milvus-storage file_reader.cpp ReadNextRowGroup/SliceRowGroupFromTable;
//     buffer_size only controls prefetch), and
//  2. writer row groups cap at ~1MB uncompressed (DEFAULT_MAX_ROW_GROUP_SIZE),
//
// so a batch carries at most ~1MB/row-width rows and materialization amplifies
// a bounded row count (~1MB x materializedWidth/rowWidth per batch).
// If this test fails, the reader's batching contract changed and the
// amplification risk must be re-assessed.
func TestBumpUTReaderDeliversPerRowGroupBatches(t *testing.T) {
	setupBumpUTEnv(t)
	const rows = 30000
	fix := buildBumpFixture(t, withRows(rows))

	count := func(bufSize int64, fields []*schemapb.FieldSchema) (batches, maxBatchRows, total int) {
		opts := []storage.RwOption{
			storage.WithCollectionID(CollectionID),
			storage.WithVersion(storage.StorageV3),
			storage.WithStorageConfig(fix.cfg),
		}
		if bufSize > 0 {
			opts = append(opts, storage.WithBufferSize(bufSize))
		}
		reader, _, err := newCompactionSegmentRecordReader(context.Background(),
			fix.segment, &schemapb.CollectionSchema{Fields: fields}, fix.cfg, opts...)
		require.NoError(t, err)
		defer reader.Close()
		for {
			rec, err := reader.Next()
			if err == sio.EOF {
				break
			}
			require.NoError(t, err)
			batches++
			maxBatchRows = max(maxBatchRows, rec.Len())
			total += rec.Len()
		}
		return
	}

	full := physicalReadFields(fix.sourceSchema)
	// The production additive anchor is the RowID SYSTEM column (field 0) —
	// physicalReadFields strips system fields, so build it explicitly; the
	// narrow user column (pk) is kept as a third projection for contrast.
	rowIDOnly := []*schemapb.FieldSchema{{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64}}
	pkOnly := full[:1]

	refBatches, refMax, refTotal := count(0, full)
	require.Equal(t, rows, refTotal)
	require.Greater(t, refBatches, 1, "a multi-row-group segment must yield multiple batches")
	require.Less(t, refMax, rows, "one batch must never carry the whole segment")
	// Absolute amplitude bound: fixture rows are ~139B, so a 1MB row group
	// holds ~7.5k rows; 16384 gives ~2x headroom while still failing loudly if
	// the storage layer ever grows DEFAULT_MAX_ROW_GROUP_SIZE — which would
	// silently re-open the amplification vector this test guards against.
	require.LessOrEqual(t, refMax, 16384, "row-group quantum grew: re-assess the amplification risk")

	// Delivery granularity must be invariant to both the buffer size and the
	// read projection: always one row group per batch.
	for _, tc := range []struct {
		name    string
		bufSize int64
		fields  []*schemapb.FieldSchema
	}{
		{"full-projection small buffer", 8 * 1024, full},
		{"rowid-anchor default buffer", 0, rowIDOnly},
		{"rowid-anchor small buffer", 8 * 1024, rowIDOnly},
		{"pk-only small buffer", 8 * 1024, pkOnly},
	} {
		batches, maxRows, total := count(tc.bufSize, tc.fields)
		require.Equal(t, refBatches, batches, tc.name)
		require.Equal(t, refMax, maxRows, tc.name)
		require.Equal(t, rows, total, tc.name)
	}
}

// ============================================================================
// Fault injection: error class + cleanup + source-intact (A/F fault series)
// ============================================================================
// Every fault case follows the same triple assertion: (1) the injected failure
// surfaces as an error of the right class, (2) partial writers are cleaned up,
// (3) the source segment stays fully intact (no dirty adoption input).

var errBumpUTInjected = errors.New("bump-ut injected failure")

// faultBatchWriter is a bumpSchemaVersionBatchWriter with injectable failures.
type faultBatchWriter struct {
	writeErr error
	closeErr error
	writes   int
	closes   int
	aborts   int
}

func (w *faultBatchWriter) Write(storage.Record) error {
	w.writes++
	return w.writeErr
}
func (w *faultBatchWriter) GetWrittenUncompressed() uint64 { return 0 }
func (w *faultBatchWriter) AsNewColumnGroups()             {}
func (w *faultBatchWriter) Abort()                         { w.aborts++ }
func (w *faultBatchWriter) Close() (packed.WriterOutput, error) {
	w.closes++
	if w.closeErr != nil {
		return nil, w.closeErr
	}
	return nil, nil
}

func faultWriterResult(w *faultBatchWriter, fix *bumpFixture) *bumpSchemaVersionWriterResult {
	const addedID = int64(103)
	basePath := mustBasePath(fix.t, fix.sourceManifest)
	return &bumpSchemaVersionWriterResult{
		writer: w,
		columnGroups: []storagecommon.ColumnGroup{{
			GroupID: addedID, Columns: []int{0}, Fields: []int64{addedID},
		}},
		storageVersion: storage.StorageV3,
		basePath:       basePath,
		baseVersion:    1,
	}
}

func additiveFixture(t *testing.T) *bumpFixture {
	return buildBumpFixture(t, withTargetAddedField(&schemapb.FieldSchema{
		FieldID: 103, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true,
	}))
}

func fullRewriteFixture(t *testing.T) *bumpFixture {
	return buildBumpFixture(t,
		withSourceFields(&schemapb.FieldSchema{FieldID: 103, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[103] = int64(i) }),
		withTargetDroppedField(103),
	)
}

// [A12][S1] additive writer.Write fails mid-flight: the lease must Abort the
// writer, never Close it, and the source segment must stay untouched.
func TestBumpUTFaultAdditiveWriteError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := additiveFixture(t)
	fw := &faultBatchWriter{writeErr: errBumpUTInjected}
	patch := mockey.Mock((*bumpSchemaVersionCompactionTask).setupWriter).
		Return(faultWriterResult(fw, fix), nil).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	require.Equal(t, 1, fw.aborts)
	require.Zero(t, fw.closes)
	verifySourceIntact(t, fix)
}

// [A13][S1] additive writer.Close fails: error surfaces, the lease stays
// unconsumed, and the deferred Cleanup Aborts the writer.
func TestBumpUTFaultAdditiveCloseError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := additiveFixture(t)
	fw := &faultBatchWriter{closeErr: errBumpUTInjected}
	patch := mockey.Mock((*bumpSchemaVersionCompactionTask).setupWriter).
		Return(faultWriterResult(fw, fix), nil).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	require.Equal(t, 1, fw.closes)
	require.Equal(t, 1, fw.aborts, "Close failure keeps the lease unconsumed; Cleanup must Abort")
	verifySourceIntact(t, fix)
}

// [A14][S1] manifest commit fails: writer output is still destroyed and the
// source manifest pointer never advances.
func TestBumpUTFaultAdditiveCommitError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := additiveFixture(t)
	fw := &faultBatchWriter{}
	writerPatch := mockey.Mock((*bumpSchemaVersionCompactionTask).setupWriter).
		Return(faultWriterResult(fw, fix), nil).Build()
	defer writerPatch.UnPatch()
	commitPatch := mockey.Mock(packed.CommitManifestUpdates).
		Return("", errBumpUTInjected).Build()
	defer commitPatch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	require.Equal(t, 1, fw.closes, "writer must be closed before commit")
	verifySourceIntact(t, fix)
}

// [A15][S3] corrupt manifest string reaching setupWriter: DataIntegrity, not a
// crash. Driven through runAdditivePhysicalReconciliation directly because the
// decision step would reject the manifest earlier on the full Compact path.
func TestBumpUTFaultAdditiveBadManifest(t *testing.T) {
	setupBumpUTEnv(t)
	fix := additiveFixture(t)
	fix.segment.Manifest = "not-a-manifest"
	diff := &schemaBumpPhysicalDiff{
		existingFields: map[int64]struct{}{
			common.RowIDField: {}, common.TimeStampField: {}, bumpFxPKField: {}, bumpFxTextField: {},
		},
		absentOrdinaryFields: []*schemapb.FieldSchema{typeutil.GetField(fix.targetSchema, 103)},
	}
	readerPatch := mockey.Mock((*bumpSchemaVersionCompactionTask).openRecordReader).
		Return(&emptyRecordReader{}, diff.existingFields, nil).Build()
	defer readerPatch.UnPatch()

	_, err := fix.task.runAdditivePhysicalReconciliation(context.Background(), diff)
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "failed to parse existing manifest")
}

// [A16][S1] V3 stats blob write fails (BM25 path): error propagates, source intact.
func TestBumpUTFaultAdditiveStatsWriteError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t, withBM25Target())
	patch := mockey.Mock(packed.WriteFile).Return(errBumpUTInjected).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to write V3 stats")
	verifySourceIntact(t, fix)
}

// [A19][S1] segment reader cannot open: warn branch + propagation.
func TestBumpUTFaultAdditiveReaderOpenError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := additiveFixture(t)
	patch := mockey.Mock(storage.NewManifestRecordReader).
		Return(nil, errBumpUTInjected).Build()

	_, err := fix.task.Compact()
	patch.UnPatch() // verification below needs the real reader
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [A20][S1] plugin-context resolution fails during writer setup.
func TestBumpUTFaultAdditivePluginContextError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := additiveFixture(t)
	patch := mockey.Mock(hookutil.GetCPluginContext).
		Return((*indexcgopb.StoragePluginContext)(nil), errBumpUTInjected).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// faultRecordReader fails immediately on Next.
type faultRecordReader struct{ err error }

func (r *faultRecordReader) Next() (storage.Record, error) { return nil, r.err }
func (r *faultRecordReader) Close() error                  { return nil }

// [F14][S1] full-rewrite reader fails: error propagates, source intact.
func TestBumpUTFaultFullRewriteReaderError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	patch := mockey.Mock(newCompactionSegmentRecordReaderWithFields).
		Return(&faultRecordReader{err: errBumpUTInjected}, nil, nil).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// faultBinlogWriter is a storage.BinlogRecordWriter with injectable failures.
type faultBinlogWriter struct {
	writeErr error
	closeErr error
	statsLog *datapb.FieldBinlog
	manifest string
	schema   *schemapb.CollectionSchema
	rows     int64
}

func (w *faultBinlogWriter) Write(r storage.Record) error {
	if w.writeErr != nil {
		return w.writeErr
	}
	w.rows += int64(r.Len())
	return nil
}
func (w *faultBinlogWriter) Close() error { return w.closeErr }
func (w *faultBinlogWriter) GetLogs() (map[storage.FieldID]*datapb.FieldBinlog, *datapb.FieldBinlog, map[storage.FieldID]*datapb.FieldBinlog, string, []int64) {
	return map[storage.FieldID]*datapb.FieldBinlog{}, w.statsLog, nil, w.manifest, nil
}
func (w *faultBinlogWriter) GetRowNum() int64                   { return w.rows }
func (w *faultBinlogWriter) GetStatsBlobSize() int64            { return 0 }
func (w *faultBinlogWriter) FlushChunk() error                  { return nil }
func (w *faultBinlogWriter) GetBufferUncompressed() uint64      { return 0 }
func (w *faultBinlogWriter) GetWrittenUncompressed() uint64     { return 0 }
func (w *faultBinlogWriter) Schema() *schemapb.CollectionSchema { return w.schema }

// [F15][S2] full-rewrite writer.Write fails: clean error, no panic, no leak of
// the wrapped record path.
func TestBumpUTFaultFullRewriteWriteError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	fw := &faultBinlogWriter{writeErr: errBumpUTInjected, schema: fix.targetSchema}
	patch := mockey.Mock(storage.NewBinlogRecordWriter).Return(fw, nil).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [F16][S1] full-rewrite writer.Close fails.
func TestBumpUTFaultFullRewriteCloseError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	fw := &faultBinlogWriter{closeErr: errBumpUTInjected, schema: fix.targetSchema}
	patch := mockey.Mock(storage.NewBinlogRecordWriter).Return(fw, nil).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [F17][S1] committing writer stats to the manifest fails.
func TestBumpUTFaultFullRewriteAddStatsError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	fw := &faultBinlogWriter{
		schema:   fix.targetSchema,
		manifest: packed.MarshalManifestPath(mustBasePath(t, fix.sourceManifest)+"-out", 1),
		statsLog: &datapb.FieldBinlog{FieldID: bumpFxPKField, Binlogs: []*datapb.Binlog{{LogID: 1, MemorySize: 8}}},
	}
	writerPatch := mockey.Mock(storage.NewBinlogRecordWriter).Return(fw, nil).Build()
	defer writerPatch.UnPatch()
	statsPatch := mockey.Mock(packed.AddStatsToManifest).Return("", errBumpUTInjected).Build()
	defer statsPatch.UnPatch()

	_, err := fix.task.Compact()
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to add writer stats")
	verifySourceIntact(t, fix)
}

// [F18][S1] LOB reference merge fails (TEXT schema, REUSE_ALL context).
func TestBumpUTFaultFullRewriteLOBMergeError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t,
		withSourceFields(&schemapb.FieldSchema{FieldID: 103, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[103] = int64(i) }),
		withTargetDroppedField(103),
		withTargetAddedField(&schemapb.FieldSchema{
			FieldID: 105, Name: "note", DataType: schemapb.DataType_Text, Nullable: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
		}),
	)
	patch := mockey.Mock(compaction.ApplyLobCompactionToManifests).
		Return(map[int64]string(nil), errBumpUTInjected).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [F19][S1] text index build fails after data is written: error propagates,
// no partial adoption input is produced.
func TestBumpUTFaultFullRewriteTextIndexError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	patch := mockey.Mock(createTextIndex).To(func(_ context.Context, _ storage.ChunkManager, _ *datapb.CompactionPlan, _ compaction.Params,
		_ int64, _ int64, _ int64, _ int64, _ int64, _ *datapb.CompactionSegment,
	) (map[int64]*datapb.TextIndexStats, error) {
		return nil, errBumpUTInjected
	}).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

var _ storage.BinlogRecordWriter = (*faultBinlogWriter)(nil)

var _ = indexpb.StorageConfig{} // keep import for future fault cases

// [F22][S3] target schema without a primary key: full rewrite fails upfront.
func TestBumpUTFaultFullRewriteNoPrimaryKey(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	for _, f := range fix.targetSchema.GetFields() {
		f.IsPrimaryKey = false
	}
	_, err := fix.task.Compact()
	require.Error(t, err)
}

// [F23][S1] delta composition fails: full rewrite aborts before writing.
func TestBumpUTFaultFullRewriteComposeDeltaError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	patch := mockey.Mock(compaction.ComposeDeleteFromDeltalogs).
		Return(map[any]typeutil.Timestamp(nil), errBumpUTInjected).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [F24][S1] LOB collection from the source manifest fails during init.
func TestBumpUTFaultFullRewriteCollectLobError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t,
		withSourceFields(&schemapb.FieldSchema{FieldID: 103, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[103] = int64(i) }),
		withTargetDroppedField(103),
		withTargetAddedField(&schemapb.FieldSchema{
			FieldID: 105, Name: "note", DataType: schemapb.DataType_Text, Nullable: true,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "65535"}},
		}),
	)
	patch := mockey.Mock(compaction.CollectLobFilesFromManifests).
		Return(map[int64][]packed.LobFileInfo(nil), errBumpUTInjected).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [F25][S1] writer reports rows but no manifest: internal error, not adoption.
func TestBumpUTFaultFullRewriteEmptyManifestWithRows(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	fw := &faultBinlogWriter{schema: fix.targetSchema}
	patch := mockey.Mock(storage.NewBinlogRecordWriter).Return(fw, nil).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.Error(t, err)
	require.ErrorContains(t, err, "produced empty manifest")
}

// [F26][S1] insert-log compression fails after a successful write.
func TestBumpUTFaultFullRewriteCompressError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	patch := mockey.Mock(binlog.CompressFieldBinlogs).Return(errBumpUTInjected).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [F27][S1] committing text-index stats to the manifest fails.
func TestBumpUTFaultFullRewriteTextStatsCommitError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	textPatch := mockey.Mock(createTextIndex).To(func(_ context.Context, _ storage.ChunkManager, _ *datapb.CompactionPlan, _ compaction.Params,
		_ int64, _ int64, _ int64, _ int64, _ int64, _ *datapb.CompactionSegment,
	) (map[int64]*datapb.TextIndexStats, error) {
		return map[int64]*datapb.TextIndexStats{bumpFxTextField: {LogSize: 1}}, nil
	}).Build()
	defer textPatch.UnPatch()
	statsPatch := mockey.Mock(packed.AddStatsToManifest).Return("", errBumpUTInjected).Build()
	defer statsPatch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [F28][S2] commit-ts wrapper + write failure: the wrapped record release path
// must not panic or double-release.
func TestBumpUTFaultFullRewriteWriteErrorWithCommitTs(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t,
		withSourceFields(&schemapb.FieldSchema{FieldID: 103, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[103] = int64(i) }),
		withTargetDroppedField(103),
		withCommitTs(tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Hour))),
	)
	fw := &faultBinlogWriter{writeErr: errBumpUTInjected, schema: fix.targetSchema}
	patch := mockey.Mock(storage.NewBinlogRecordWriter).Return(fw, nil).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [F29][S1] writer construction fails.
func TestBumpUTFaultFullRewriteWriterCtorError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := fullRewriteFixture(t)
	patch := mockey.Mock(storage.NewBinlogRecordWriter).
		Return(nil, errBumpUTInjected).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [A22][S1] BM25 stats serialization fails after batches were written.
func TestBumpUTFaultAdditiveStatsSerializeError(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t, withBM25Target())
	patch := mockey.Mock((*storage.BM25Stats).Serialize).
		Return(nil, errBumpUTInjected).Build()
	defer patch.UnPatch()

	_, err := fix.task.Compact()
	require.ErrorIs(t, err, errBumpUTInjected)
	verifySourceIntact(t, fix)
}

// [A23][S1] log-ID exhaustion during V3 stats commit.
func TestBumpUTFaultAdditiveStatsAllocExhausted(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t, withBM25Target())
	fix.task.logIDAlloc = allocator.NewLocalAllocator(5, 5)

	_, err := fix.task.Compact()
	require.Error(t, err)
	verifySourceIntact(t, fix)
}

// [U9][S3] additive reconciliation on a partially materialized function: the
// materializer's own integrity guard fires (defense behind decision).
func TestBumpUTFaultAdditivePartialFunctionState(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t, withBM25Target())
	diff := &schemaBumpPhysicalDiff{
		existingFields: map[int64]struct{}{
			common.RowIDField: {}, common.TimeStampField: {}, bumpFxPKField: {}, bumpFxTextField: {},
			102: {}, // pretend one output is already physically present
		},
		missingFunctions:    fix.targetSchema.GetFunctions(),
		missingOutputFields: []*schemapb.FieldSchema{typeutil.GetField(fix.targetSchema, 102)},
	}
	// make the function multi-output so presence is partial
	fix.targetSchema.GetFunctions()[0].OutputFieldIds = []int64{102, 106}

	_, err := fix.task.runAdditivePhysicalReconciliation(context.Background(), diff)
	require.Error(t, err)
	require.ErrorContains(t, err, "partially materialized")
}

// bumpFxLobText builds a deterministic ~70KB TEXT row that exceeds the LOB
// inline threshold (65536), forcing a real .vx LOB file.
func bumpFxLobText(i int) string {
	return fmt.Sprintf("lob-%04d|", i) + strings.Repeat(fmt.Sprintf("x%03d ", i), 14000)
}

// readTextRefs reads a TEXT column back as raw LOB reference bytes, keyed by pk.
func readTextRefs(t *testing.T, fix *bumpFixture, manifest string, textID int64) map[int64][]byte {
	got, total := readAllColumns(t, fix.cfg, manifest, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		typeutil.GetField(fix.sourceSchema, bumpFxPKField),
		{FieldID: textID, Name: "big_text", DataType: schemapb.DataType_Text},
	}})
	refs := make(map[int64][]byte, total)
	for i := 0; i < total; i++ {
		pk := got[bumpFxPKField][i].(int64)
		rb, ok := got[textID][i].([]byte)
		require.True(t, ok, "text ref must read back as bytes for pk %d", pk)
		require.NotEmpty(t, rb, "text ref empty for pk %d", pk)
		refs[pk] = rb
	}
	return refs
}

// [LOB1][S0] additive keeps TEXT LOB data dereferenceable: every row's LOB
// reference bytes survive the bump unchanged AND the referenced .vx blob file
// stays byte-identical — together this proves content round-trip without
// depending on the ref encoding.
func TestBumpUTAdditiveTextLOBRoundTrip(t *testing.T) {
	setupBumpUTEnv(t)
	const textID = int64(105)
	fix := buildBumpFixture(t, withRows(6),
		withSourceFields(&schemapb.FieldSchema{FieldID: textID, Name: "big_text", DataType: schemapb.DataType_Text}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[textID] = bumpFxLobText(i) }),
		withTextLOBSource(textID),
		withTargetAddedField(&schemapb.FieldSchema{FieldID: 106, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true}),
	)
	srcRefs := readTextRefs(t, fix, fix.sourceManifest, textID)
	preLob := listLobFiles(t, fix)
	require.NotEmpty(t, preLob, "fixture must produce a real LOB file")

	seg := runCompact(t, fix)

	require.Equal(t, srcRefs, readTextRefs(t, fix, seg.GetManifest(), textID),
		"additive must not touch LOB references")
	require.Equal(t, preLob, listLobFiles(t, fix), "LOB blob files must stay byte-identical")
}

// [LOB2][S0] full rewrite with deletes: every KEPT row's LOB reference must
// survive byte-identically (a dangling reference is silent data loss under
// REUSE_ALL), and the LOB blob files stay intact.
func TestBumpUTFullRewriteTextLOBKeptRowsSurvive(t *testing.T) {
	setupBumpUTEnv(t)
	const textID = int64(105)
	const droppedID = int64(107)
	const rows = 9
	deleted := map[any]uint64{}
	for i := 0; i < rows; i += 3 {
		deleted[int64(i)] = bumpFxTS(i) + 1
	}
	fix := buildBumpFixture(t, withRows(rows),
		withSourceFields(
			&schemapb.FieldSchema{FieldID: textID, Name: "big_text", DataType: schemapb.DataType_Text},
			&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64},
		),
		withFillValue(func(i int, ts uint64, v map[int64]any) {
			v[textID] = bumpFxLobText(i)
			v[droppedID] = int64(i)
		}),
		withTextLOBSource(textID),
		withTargetDroppedField(droppedID),
		withDeletedPKs(deleted),
	)
	kept := fix.keptRows()
	require.NotEmpty(t, kept)
	srcRefs := readTextRefs(t, fix, fix.sourceManifest, textID)
	preLob := listLobFiles(t, fix)
	require.NotEmpty(t, preLob)

	seg := runCompact(t, fix)
	require.EqualValues(t, len(kept), seg.GetNumOfRows())

	postRefs := readTextRefs(t, fix, seg.GetManifest(), textID)
	require.Len(t, postRefs, len(kept))
	for _, r := range kept {
		pk := r.pk.(int64)
		require.Equal(t, srcRefs[pk], postRefs[pk], "kept row %d must keep a live LOB reference", pk)
	}
	require.Equal(t, preLob, listLobFiles(t, fix), "LOB blob files must stay byte-identical")
}

// [ST2][S1] full rewrite with deletes + BM25 materialization: the committed
// stats ledger must accumulate ONLY the surviving rows — filtered rows leaking
// into stats is invisible to every other oracle in the suite.
func TestBumpUTFullRewriteBM25StatsGoldenUnderDeletes(t *testing.T) {
	setupBumpUTEnv(t)
	const rows = 40
	const droppedID = int64(103)
	deleted := map[any]uint64{}
	for i := 0; i < rows; i += 3 {
		deleted[int64(i)] = bumpFxTS(i) + 1
	}
	fix := buildBumpFixture(t, withRows(rows),
		withSourceFields(&schemapb.FieldSchema{FieldID: droppedID, Name: "dropped", DataType: schemapb.DataType_Int64}),
		withFillValue(func(i int, ts uint64, v map[int64]any) { v[droppedID] = int64(i) }),
		withTargetDroppedField(droppedID),
		withBM25Target(),
		withDeletedPKs(deleted),
	)
	kept := fix.keptRows()
	require.NotEmpty(t, kept)

	seg := runCompact(t, fix)
	require.EqualValues(t, len(kept), seg.GetNumOfRows())

	keptTexts := make([]string, len(kept))
	for i, r := range kept {
		keptTexts[i] = r.values[bumpFxTextField].(string)
	}
	expected := expectedBM25SparseRows(t, fix.targetSchema, fix.targetSchema.GetFunctions()[0], keptTexts)
	requireSparseRows(t, fix, seg.GetManifest(), 102, len(kept), expected)
	expectedStats := storage.NewBM25Stats()
	expectedStats.AppendBytes(expected...)
	require.Equal(t, expectedStats, requireBM25StatsBlob(t, fix, 102, len(kept)))
}

// [MF1][S1] additive appends EXACTLY one data file and every pre-existing file
// stays byte-identical — the on-disk exact ledger that set-semantics manifest
// checks cannot express (duplicated column groups would be invisible there).
func TestBumpUTAdditiveDataFileLedgerExactAppend(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t, withRows(20), withTargetAddedField(&schemapb.FieldSchema{
		FieldID: 103, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true,
	}))
	pre := listSegmentDataFiles(t, fix)
	require.NotEmpty(t, pre)

	seg := runCompact(t, fix)
	require.NotEmpty(t, seg.GetManifest())

	post := listSegmentDataFiles(t, fix)
	for f, size := range pre {
		require.Contains(t, post, f, "pre-existing data file must survive: %s", f)
		require.Equal(t, size, post[f], "pre-existing data file mutated: %s", f)
	}
	require.Len(t, post, len(pre)+1, "additive must append exactly one column-group file")
}

// [MF2][S1] re-running the bump on an already-reconciled segment (the
// real-world retry vector) must route bump-only: the same manifest comes back
// and not a single data file is written — duplicate column groups from retries
// are exactly what the set-semantics manifest check cannot see.
func TestBumpUTIdempotentRerunAppendsNothing(t *testing.T) {
	setupBumpUTEnv(t)
	fix := buildBumpFixture(t, withRows(20), withTargetAddedField(&schemapb.FieldSchema{
		FieldID: 103, Name: "added", DataType: schemapb.DataType_Int64, Nullable: true,
	}))
	first := runCompact(t, fix)
	mid := listSegmentDataFiles(t, fix)

	segment2 := proto.Clone(fix.segment).(*datapb.CompactionSegmentBinlogs)
	segment2.Manifest = first.GetManifest()
	segment2.FieldBinlogs = first.GetInsertLogs()
	plan2 := proto.Clone(fix.task.plan).(*datapb.CompactionPlan)
	plan2.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{segment2}
	cmgr, err := storage.NewChunkManagerFactoryWithParam(paramtable.Get()).NewPersistentStorageChunkManager(context.Background())
	require.NoError(t, err)
	task2 := NewBumpSchemaVersionCompactionTask(context.Background(), cmgr, plan2, fix.task.compactionParams)

	result2, err := task2.Compact()
	require.NoError(t, err)
	seg2 := result2.GetSegments()[0]
	require.Equal(t, first.GetManifest(), seg2.GetManifest(), "re-run must be a pure metadata bump")
	require.Equal(t, mid, listSegmentDataFiles(t, fix), "re-run must not write any data file")
}

// ============================================================================
// Unit guards: decision/validation/writer helpers (D series)
// ============================================================================
// [D1][S3] preCompact guards: canceled context and malformed plans fail fast.
func TestBumpUTUnitPreCompactGuards(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	task := &bumpSchemaVersionCompactionTask{ctx: ctx, plan: &datapb.CompactionPlan{
		SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{StorageVersion: storage.StorageV3, Manifest: "m"}},
	}}
	require.ErrorIs(t, task.preCompact(), context.Canceled)

	task = &bumpSchemaVersionCompactionTask{ctx: context.Background(), plan: &datapb.CompactionPlan{}}
	err := task.preCompact()
	require.Error(t, err)
	require.ErrorContains(t, err, "must have exactly one segment")
}

// [D2-D5][S3] unsupported/empty function declarations are DataIntegrity errors.
func TestBumpUTUnitValidateSupportedFunctionEmptySets(t *testing.T) {
	cases := []struct {
		name string
		fn   *schemapb.FunctionSchema
		want string
	}{
		{"bm25-no-input", &schemapb.FunctionSchema{Name: "f", Type: schemapb.FunctionType_BM25, OutputFieldIds: []int64{2}}, "no input fields"},
		{"bm25-no-output", &schemapb.FunctionSchema{Name: "f", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{1}}, "no output fields"},
		{"minhash-no-input", &schemapb.FunctionSchema{Name: "f", Type: schemapb.FunctionType_MinHash, OutputFieldIds: []int64{2}}, "no input fields"},
		{"minhash-no-output", &schemapb.FunctionSchema{Name: "f", Type: schemapb.FunctionType_MinHash, InputFieldIds: []int64{1}}, "no output fields"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSupportedMissingFunctionMaterialization(tc.fn)
			require.ErrorIs(t, err, merr.ErrDataIntegrity)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// [D6][S3] minhash output must be BinaryVector.
func TestBumpUTUnitMinHashOutputTypeRejected(t *testing.T) {
	err := validateMaterializationOutputField(
		&schemapb.FunctionSchema{Name: "f", Type: schemapb.FunctionType_MinHash},
		&schemapb.FieldSchema{FieldID: 2, DataType: schemapb.DataType_FloatVector},
	)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "must be BinaryVector")
}

func bm25FieldWithAnalyzerParams(params string) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.EnableAnalyzerKey, Value: "true"},
			{Key: "multi_analyzer_params", Value: params},
		},
	}
}

// [D7-D10][S3] by_field resolution failures are DataIntegrity errors.
func TestBumpUTUnitBM25ByFieldErrors(t *testing.T) {
	fn := &schemapb.FunctionSchema{Name: "f", Type: schemapb.FunctionType_BM25}
	schemaWith := func(extra ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
		return &schemapb.CollectionSchema{Fields: append([]*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64}}, extra...)}
	}

	_, err := bm25AdditionalInputFields(schemaWith(), fn, bm25FieldWithAnalyzerParams("{bad json"))
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "failed to parse multi_analyzer_params")

	_, err = bm25AdditionalInputFields(schemaWith(), fn, bm25FieldWithAnalyzerParams(`{"analyzers":{}}`))
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "missing required 'by_field'")

	_, err = bm25AdditionalInputFields(schemaWith(), fn, bm25FieldWithAnalyzerParams(`{"by_field":"lang"}`))
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "references missing input field")

	langInt := &schemapb.FieldSchema{FieldID: 115, Name: "lang", DataType: schemapb.DataType_Int64}
	_, err = bm25AdditionalInputFields(schemaWith(langInt), fn, bm25FieldWithAnalyzerParams(`{"by_field":"lang"}`))
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "only VarChar is allowed")
}

func bumpUTStringArray(values []string) arrow.Array {
	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	for _, v := range values {
		b.Append(v)
	}
	return b.NewArray()
}

func bumpUTInt64ArrayWithNull(values []int64, nullAt map[int]bool) arrow.Array {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for i, v := range values {
		if nullAt[i] {
			b.AppendNull()
		} else {
			b.Append(v)
		}
	}
	return b.NewArray()
}

// [D11][S2] selectFullRewriteRecord type guards + VarChar PK + NULL-TTL rows.
func TestBumpUTUnitSelectFullRewriteRecord(t *testing.T) {
	intPK := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
	strPK := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true}
	filter := compaction.NewEntityFilter(nil, 0, getMilvusBirthday(), 0)

	ints := newInt64Array(t, []int64{1, 2})
	defer ints.Release()
	strs := bumpUTStringArray([]string{"a", "b"})
	defer strs.Release()

	// pk column type mismatches per PK type, plus unsupported PK type.
	rec := &materializerTestRecord{columns: map[storage.FieldID]arrow.Array{100: strs, common.TimeStampField: ints}, len: 2}
	_, err := selectFullRewriteRecord(rec, intPK, filter, -1, false)
	require.ErrorContains(t, err, "int64 primary key field not found")

	rec = &materializerTestRecord{columns: map[storage.FieldID]arrow.Array{100: ints, common.TimeStampField: ints}, len: 2}
	_, err = selectFullRewriteRecord(rec, strPK, filter, -1, false)
	require.ErrorContains(t, err, "varchar primary key field not found")

	doublePK := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Double, IsPrimaryKey: true}
	_, err = selectFullRewriteRecord(rec, doublePK, filter, -1, false)
	require.ErrorContains(t, err, "invalid primary key data type")

	// timestamp column type mismatch.
	rec = &materializerTestRecord{columns: map[storage.FieldID]arrow.Array{100: ints, common.TimeStampField: strs}, len: 2}
	_, err = selectFullRewriteRecord(rec, intPK, filter, -1, false)
	require.ErrorContains(t, err, "timestamp field not found")

	// ttl column type mismatch.
	rec = &materializerTestRecord{columns: map[storage.FieldID]arrow.Array{100: ints, common.TimeStampField: ints, 104: strs}, len: 2}
	_, err = selectFullRewriteRecord(rec, intPK, filter, 104, true)
	require.ErrorContains(t, err, "TTL field not found")

	// VarChar PK happy path with a delete; NULL-TTL rows are never
	// ttl-field-filtered (expireTs sentinel -1).
	deleteTs := tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Hour))
	strFilter := compaction.NewEntityFilter(map[any]typeutil.Timestamp{"a": deleteTs}, 0, getMilvusBirthday(), 0)
	ttl := bumpUTInt64ArrayWithNull([]int64{0, 0}, map[int]bool{0: true, 1: true})
	defer ttl.Release()
	rec = &materializerTestRecord{columns: map[storage.FieldID]arrow.Array{100: strs, common.TimeStampField: ints, 104: ttl}, len: 2}
	selection, err := selectFullRewriteRecord(rec, strPK, strFilter, 104, true)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, 1, selection.Len(), "row a deleted, row b kept; NULL ttl never filters")
}

// [D12][S1] writer lease: double Close rejected, Cleanup after Close is a no-op.
func TestBumpUTUnitWriterLeaseLifecycle(t *testing.T) {
	w := &cleanupTrackingBatchWriter{}
	lease := &bumpSchemaVersionWriterLease{writer: w}
	_, err := lease.Close()
	require.NoError(t, err)
	_, err = lease.Close()
	require.ErrorContains(t, err, "already consumed")
	lease.Cleanup()
	require.Zero(t, w.abortCount, "Cleanup after Close must not Abort")
	require.Equal(t, 1, w.closeCount)

	var nilLease *bumpSchemaVersionWriterLease
	nilLease.Cleanup() // must not panic
}

// [P3-buqian] A failed Close must NOT consume the lease, so the deferred Cleanup
// still aborts the writer — native release must not rely on the underlying Close
// self-freeing on error.
func TestBumpUTUnitWriterLeaseAbortsOnCloseError(t *testing.T) {
	w := &cleanupTrackingBatchWriter{closeErr: errBumpUTInjected}
	lease := &bumpSchemaVersionWriterLease{writer: w}
	_, err := lease.Close()
	require.ErrorIs(t, err, errBumpUTInjected)
	require.Equal(t, 1, w.closeCount)
	lease.Cleanup()
	require.Equal(t, 1, w.abortCount, "Close failure must leave the lease unconsumed so Cleanup aborts")
}

// [P3-buqian] A persisted function with no output fields is schema corruption and
// must fail loud in both the additive decision and the materializer selection,
// not be silently dropped by the all-present early return (0 == 0).
func TestBumpUTUnitZeroOutputFunctionRejected(t *testing.T) {
	zeroOut := &schemapb.FunctionSchema{Name: "f", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}}

	_, _, err := missingFunctionMaterializations(
		&schemapb.CollectionSchema{Functions: []*schemapb.FunctionSchema{zeroOut}}, map[int64]struct{}{})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "no output fields")

	_, err = functionOutputIndexesToMaterialize(zeroOut, map[int64]struct{}{})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "no output fields")
}

// [D13][S1] BM25 stats accumulation skips NULL cells.
func TestBumpUTUnitAppendBM25StatsSkipsNulls(t *testing.T) {
	row := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{1: 0.5, 7: 1.5})
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer b.Release()
	b.Append(row)
	b.AppendNull()
	b.Append(row)
	arr := b.NewArray()
	defer arr.Release()

	stats := storage.NewBM25Stats()
	size, err := appendBM25StatsFromArrowArray(stats, arr)
	require.NoError(t, err)
	require.Equal(t, 2*len(row), size, "null cell contributes no bytes")
	require.EqualValues(t, 2, stats.NumRow())
}

// [D14][S3] tiny accessors' nil/empty guards.
func TestBumpUTUnitSmallGuards(t *testing.T) {
	require.Zero(t, arrowArrayMemorySize(nil))
	task := &bumpSchemaVersionCompactionTask{plan: &datapb.CompactionPlan{}}
	require.Zero(t, task.GetCollection())
}

// [D15][S1] insert-log construction fails cleanly when log IDs are exhausted.
func TestBumpUTUnitBuildNewInsertLogsAllocExhausted(t *testing.T) {
	task := &bumpSchemaVersionCompactionTask{logIDAlloc: allocator.NewLocalAllocator(5, 5)}
	_, err := task.buildNewInsertLogsV3(&bumpSchemaVersionWriterResult{
		columnGroups: []storagecommon.ColumnGroup{{GroupID: 103, Columns: []int{0}, Fields: []int64{103}}},
	}, map[int64]int{}, 3)
	require.Error(t, err)
}

// [D16][S0] read-schema input dedup: shared and repeated input IDs collapse.
func TestBumpUTUnitMissingFunctionInputDedup(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "s1", DataType: schemapb.DataType_SparseFloatVector},
			{FieldID: 103, Name: "s2", DataType: schemapb.DataType_SparseFloatVector},
		},
	}
	task := &bumpSchemaVersionCompactionTask{plan: &datapb.CompactionPlan{Schema: schema}}
	missing := []*schemapb.FunctionSchema{
		{Name: "f1", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101, 101}, OutputFieldIds: []int64{102}},
		{Name: "f2", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{103}},
	}
	inputSchema, inputIDs, err := task.missingFunctionInputSchema(missing)
	require.NoError(t, err)
	require.Equal(t, []int64{101}, inputIDs)
	require.Len(t, inputSchema.GetFields(), 1)
}

// [D17][S3] read-schema builder's own not-found guard (defense in depth behind
// the decision-time validation).
func TestBumpUTUnitMissingFunctionInputNotFound(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	task := &bumpSchemaVersionCompactionTask{plan: &datapb.CompactionPlan{Schema: schema}}
	_, _, err := task.missingFunctionInputSchema([]*schemapb.FunctionSchema{{
		Name: "f", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{999}, OutputFieldIds: []int64{102},
	}})
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "input field 999 not found")
}

// [A9/A10][S3] additiveReadSchema anchor selection: Timestamp fallback when
// RowID is physically absent; hard error when both anchors are absent.
func TestBumpUTUnitAdditiveReadSchemaAnchors(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
		{FieldID: common.TimeStampField, Name: "ts", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	task := &bumpSchemaVersionCompactionTask{plan: &datapb.CompactionPlan{Schema: schema}}

	// RowID absent -> Timestamp anchor.
	diff := &schemaBumpPhysicalDiff{existingFields: map[int64]struct{}{common.TimeStampField: {}, 100: {}}}
	readSchema, _, err := task.additiveReadSchema(diff)
	require.NoError(t, err)
	require.Len(t, readSchema.GetFields(), 1)
	require.EqualValues(t, common.TimeStampField, readSchema.GetFields()[0].GetFieldID())

	// both anchors absent -> DataIntegrity.
	diff = &schemaBumpPhysicalDiff{existingFields: map[int64]struct{}{100: {}}}
	_, _, err = task.additiveReadSchema(diff)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	require.ErrorContains(t, err, "RowID or Timestamp anchor")
}

// [U3][S3] additive reconciliation with nothing to append is an internal error.
func TestBumpUTUnitAdditiveNoOutputFields(t *testing.T) {
	task := &bumpSchemaVersionCompactionTask{
		ctx: context.Background(),
		plan: &datapb.CompactionPlan{
			SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{SegmentID: 1}},
			Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
			}},
		},
	}
	diff := &schemaBumpPhysicalDiff{existingFields: map[int64]struct{}{common.RowIDField: {}}}
	_, err := task.runAdditivePhysicalReconciliation(context.Background(), diff)
	require.Error(t, err)
	require.ErrorContains(t, err, "no fields to append")
}

// [U5][S3] newV3WriterResult rejects non-V3 segments (defense behind preCompact).
func TestBumpUTUnitNewV3WriterRejectsNonV3(t *testing.T) {
	task := &bumpSchemaVersionCompactionTask{plan: &datapb.CompactionPlan{}}
	_, err := task.newV3WriterResult(&schemapb.CollectionSchema{}, nil,
		&datapb.CompactionSegmentBinlogs{StorageVersion: storage.StorageV2}, 1, "", 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "requires a StorageV3 segment")
}
