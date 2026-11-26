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

package storage

import (
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// toWriterRecord returns r as an arrow.Record projected onto the writer's own
// schema, plus a release func the caller must defer. The fast path (returning
// the backing arrow record untouched) is taken only when r is a
// simpleArrowRecord whose schema already equals the writer's; otherwise the
// writer's columns are selected by field ID, narrowing an over-wide record
// (e.g. the additive schema-bump reader record carrying a row-anchor column in
// addition to the appended field) to exactly the writer's column set. Passing
// such an over-wide record straight to the FFI writer misaligns column count
// against the writer's manifest and crashes loon_writer_write.
func toWriterRecord(r Record, schema *schemapb.CollectionSchema, arrowSchema *arrow.Schema) (arrow.Record, func(), error) {
	if sar, ok := r.(*simpleArrowRecord); ok && sar.r.Schema().Equal(arrowSchema) {
		return sar.r, func() {}, nil
	}
	allFields := typeutil.GetAllFieldSchemas(schema)
	arrays := make([]arrow.Array, len(allFields))
	var normalized []arrow.Array
	defer func() {
		for _, column := range normalized {
			column.Release()
		}
	}()
	for i, field := range allFields {
		column := r.Column(field.GetFieldID())
		expected := arrowSchema.Field(i).Type
		if column == nil || column.Len() < r.Len() {
			return nil, nil, merr.WrapErrServiceInternalMsg("missing or short writer column for field %s", field.GetName())
		}
		if !arrow.TypeEqual(column.DataType(), expected) {
			// Absent TEXT fields may use a different null representation than
			// the writer expects. Nulls have no LOB/string payload to convert;
			// normalize them to the writer's fixed schema on every batch.
			// Non-null TEXT must already use the representation agreed by the
			// task's reader/writer schemas; this is not a LOB decoding boundary.
			sourceType, targetType := column.DataType().ID(), expected.ID()
			if field.GetDataType() == schemapb.DataType_Text && column.NullN() == column.Len() &&
				(sourceType == arrow.STRING || sourceType == arrow.BINARY) &&
				(targetType == arrow.STRING || targetType == arrow.BINARY) {
				column = array.MakeArrayOfNull(memory.DefaultAllocator, expected, r.Len())
				normalized = append(normalized, column)
			} else {
				return nil, nil, merr.WrapErrServiceInternalMsg("writer column type mismatch for field %s: got %s, expected %s",
					field.GetName(), column.DataType(), expected)
			}
		}
		arrays[i] = column
	}
	rec := array.NewRecord(arrowSchema, arrays, int64(r.Len()))
	return rec, rec.Release, nil
}

var _ RecordWriter = (*packedRecordWriter)(nil)

type packedRecordWriter struct {
	writer                  *packed.PackedWriter
	bufferSize              int64
	columnGroups            []storagecommon.ColumnGroup
	bucketName              string
	pathsMap                map[typeutil.UniqueID]string
	schema                  *schemapb.CollectionSchema
	arrowSchema             *arrow.Schema
	rowNum                  int64
	writtenUncompressed     uint64
	columnGroupUncompressed map[typeutil.UniqueID]uint64
	columnGroupCompressed   map[typeutil.UniqueID]uint64
	outputManifest          string
	storageConfig           *indexpb.StorageConfig
}

func (pw *packedRecordWriter) Write(r Record) error {
	rec, release, err := toWriterRecord(r, pw.schema, pw.arrowSchema)
	if err != nil {
		return err
	}
	defer release()
	pw.rowNum += int64(r.Len())
	for col, arr := range rec.Columns() {
		size := calculateActualDataSize(arr)
		pw.writtenUncompressed += size
		for _, columnGroup := range pw.columnGroups {
			if lo.Contains(columnGroup.Columns, col) {
				pw.columnGroupUncompressed[columnGroup.GroupID] += size
				break
			}
		}
	}
	return pw.writer.WriteRecordBatch(rec)
}

func (pw *packedRecordWriter) GetWrittenUncompressed() uint64 {
	return pw.writtenUncompressed
}

func (pw *packedRecordWriter) GetColumnGroupWrittenUncompressed(columnGroup typeutil.UniqueID) uint64 {
	if size, ok := pw.columnGroupUncompressed[columnGroup]; ok {
		return size
	}
	return 0
}

func (pw *packedRecordWriter) GetColumnGroupWrittenCompressed(columnGroup typeutil.UniqueID) uint64 {
	if size, ok := pw.columnGroupCompressed[columnGroup]; ok {
		return size
	}
	return 0
}

func (pw *packedRecordWriter) GetWrittenPaths(columnGroup typeutil.UniqueID) string {
	if path, ok := pw.pathsMap[columnGroup]; ok {
		return path
	}
	return ""
}

func (pw *packedRecordWriter) GetWrittenManifest() string {
	return pw.outputManifest
}

func (pw *packedRecordWriter) GetWrittenRowNum() int64 {
	return pw.rowNum
}

func (pw *packedRecordWriter) Close() error {
	if pw.writer != nil {
		sizes, err := pw.writer.CloseAndTell(len(pw.columnGroups))
		if err != nil {
			return err
		}
		for i, columnGroup := range pw.columnGroups {
			pw.columnGroupCompressed[columnGroup.GroupID] = uint64(sizes[i])
		}
	}
	return nil
}

func NewPackedRecordWriter(
	bucketName string,
	paths []string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	multiPartUploadSize int64,
	columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
) (*packedRecordWriter, error) {
	return newPackedRecordWriter(bucketName, paths, schema, false, bufferSize, multiPartUploadSize, columnGroups, storageConfig, storagePluginContext)
}

// NewPackedRecordWriterWithFieldIDNames is the packed-record writer used by
// Import V3 temporary fragments. It writes arrow column names as field IDs,
// matching the field-ID-based column names that every packed reader
// (newPackedRecordReader / newFFIPackedRecordReaderFromFragments) uses to open
// the same object.
func NewPackedRecordWriterWithFieldIDNames(
	bucketName string,
	paths []string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	multiPartUploadSize int64,
	columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
) (*packedRecordWriter, error) {
	return newPackedRecordWriter(bucketName, paths, schema, true, bufferSize, multiPartUploadSize, columnGroups, storageConfig, storagePluginContext)
}

func newPackedRecordWriter(
	bucketName string,
	paths []string,
	schema *schemapb.CollectionSchema,
	useFieldID bool,
	bufferSize int64,
	multiPartUploadSize int64,
	columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
) (*packedRecordWriter, error) {
	// Validate PK field exists before proceeding
	_, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	arrowSchema, err := ConvertToArrowSchema(schema, useFieldID)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not convert collection schema %s to arrow schema: %s", schema.Name, err.Error()))
	}
	writer, err := packed.NewPackedWriter(paths, arrowSchema, bufferSize, multiPartUploadSize, columnGroups, storageConfig, storagePluginContext)
	if err != nil {
		return nil, merr.WrapErrStorage(err, "can not new packed record writer")
	}
	columnGroupUncompressed := make(map[typeutil.UniqueID]uint64)
	columnGroupCompressed := make(map[typeutil.UniqueID]uint64)
	pathsMap := make(map[typeutil.UniqueID]string)
	if len(paths) != len(columnGroups) {
		return nil, merr.WrapErrStorageMsg("paths length is not equal to column groups length for packed record writer: paths=%d columnGroups=%d", len(paths), len(columnGroups))
	}
	for i, columnGroup := range columnGroups {
		columnGroupUncompressed[columnGroup.GroupID] = 0
		columnGroupCompressed[columnGroup.GroupID] = 0
		pathsMap[columnGroup.GroupID] = paths[i]
	}
	return &packedRecordWriter{
		writer:                  writer,
		schema:                  schema,
		arrowSchema:             arrowSchema,
		bufferSize:              bufferSize,
		bucketName:              bucketName,
		pathsMap:                pathsMap,
		columnGroups:            columnGroups,
		columnGroupUncompressed: columnGroupUncompressed,
		columnGroupCompressed:   columnGroupCompressed,
		storageConfig:           storageConfig,
	}, nil
}

// v3BatchStats belongs to one writer/segment across all of its batches, column
// groups and native flushes. A failed Write (including its buffered flush) must
// not advance either the row prefix or its logical-size snapshot.
type v3BatchStats struct {
	rowNum                  int64
	writtenUncompressed     uint64
	columnGroupUncompressed map[typeutil.UniqueID]uint64
	logicalLayout           *logicalSizeLayout
	fieldLogicalSizes       *datapb.FieldLogicalSizes
}

func (s *v3BatchStats) writeBatch(rec arrow.Record, schema *schemapb.CollectionSchema, groups []storagecommon.ColumnGroup, write func(uint64) error) error {
	rows := rec.NumRows()
	if rows < 0 || rows > math.MaxInt64-s.rowNum {
		return merr.WrapErrServiceInternalMsg("V3 writer row count overflows int64")
	}
	if s.logicalLayout == nil {
		s.logicalLayout = newLogicalSizeLayout(schema, schema, nil)
	}
	batch := s.logicalLayout.newBatch(rows)
	groupBytes := make([]uint64, len(groups))
	var batchBytes uint64
	for i, a := range rec.Columns() {
		s.logicalLayout.columns[i].measure(batch, a)
		n := calculateActualDataSize(a)
		batchBytes += n
		for j, group := range groups {
			if lo.Contains(group.Columns, i) {
				groupBytes[j] += n
				break
			}
		}
	}
	if err := write(batchBytes); err != nil {
		return err
	}
	s.fieldLogicalSizes = AppendFieldLogicalSizes(s.fieldLogicalSizes, s.rowNum, batch, rows)
	s.rowNum += rows
	s.writtenUncompressed += batchBytes
	for j, group := range groups {
		s.columnGroupUncompressed[group.GroupID] += groupBytes[j]
	}
	return nil
}

func (s *v3BatchStats) GetFieldLogicalSizes() *datapb.FieldLogicalSizes {
	return cloneFieldLogicalSizes(s.fieldLogicalSizes)
}

func (s *v3BatchStats) GetWrittenRowNum() int64 { return s.rowNum }

func (s *v3BatchStats) GetWrittenUncompressed() uint64 { return s.writtenUncompressed }

func (s *v3BatchStats) GetColumnGroupWrittenUncompressed(columnGroup typeutil.UniqueID) uint64 {
	return s.columnGroupUncompressed[columnGroup]
}

type packedRecordBatchWriter struct {
	v3BatchStats
	writer                *packed.FFIPackedWriter
	bufferSize            int64
	columnGroups          []storagecommon.ColumnGroup
	bucketName            string
	pathsMap              map[typeutil.UniqueID]string
	schema                *schemapb.CollectionSchema
	arrowSchema           *arrow.Schema
	columnGroupCompressed map[typeutil.UniqueID]uint64
	storageConfig         *indexpb.StorageConfig
}

// SetFieldLogicalSizeSchema supplies parent struct identities for a partial
// materialization whose writer schema contains only flattened leaf fields.
// It never changes the physical projection or the native writer schema.
func (pw *packedRecordBatchWriter) SetFieldLogicalSizeSchema(target *schemapb.CollectionSchema) error {
	if pw.rowNum != 0 {
		return merr.WrapErrServiceInternalMsg("cannot change V3 logical-size schema after writing rows")
	}
	pw.logicalLayout = newLogicalSizeLayout(pw.schema, target, nil)
	return nil
}

func (pw *packedRecordBatchWriter) Write(r Record) error {
	rec, release, err := toWriterRecord(r, pw.schema, pw.arrowSchema)
	if err != nil {
		return err
	}
	defer release()
	return pw.writeBatch(rec, pw.schema, pw.columnGroups, func(uint64) error {
		return pw.writer.WriteRecordBatch(rec)
	})
}

func (pw *packedRecordBatchWriter) GetColumnGroupWrittenCompressed(columnGroup typeutil.UniqueID) uint64 {
	if size, ok := pw.columnGroupCompressed[columnGroup]; ok {
		return size
	}
	return 0
}

func (pw *packedRecordBatchWriter) GetWrittenPaths(columnGroup typeutil.UniqueID) string {
	if path, ok := pw.pathsMap[columnGroup]; ok {
		return path
	}
	return ""
}

// Close closes the underlying FFI writer and returns the resulting
// column-groups payload. The writer never touches the manifest — the
// caller passes the returned handle to packed.CommitManifestUpdates and
// calls Destroy on it when done.
func (pw *packedRecordBatchWriter) Close() (packed.WriterOutput, error) {
	if pw.writer == nil {
		return nil, nil
	}
	out, err := pw.writer.Close()
	if err != nil {
		pw.fieldLogicalSizes = nil
		return nil, err
	}
	pw.writer = nil
	for id := range pw.pathsMap {
		pw.columnGroupCompressed[id] = uint64(0)
	}
	return out, nil
}

// Abort releases the underlying FFI writer without producing column groups for
// CommitManifestUpdates. It prevents metadata publication only: files already
// flushed before the abort stay on storage as unreferenced objects, and GC
// skips files under a registered V3 segment prefix, so their reclamation
// depends on the manifest-aware orphan cleanup tracked by #51649.
func (pw *packedRecordBatchWriter) Abort() {
	if pw.writer == nil {
		return
	}
	pw.writer.Destroy()
	pw.writer = nil
	pw.fieldLogicalSizes = nil
}

func (pw *packedRecordBatchWriter) AsNewColumnGroups() {
	if pw.writer != nil {
		pw.writer.AsNewColumnGroups()
	}
}

func NewPackedRecordBatchWriter(
	basePath string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	multiPartUploadSize int64,
	columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	writerFormat string,
	schemaBasedFormats []string,
) (*packedRecordBatchWriter, error) {
	return newPackedRecordBatchWriter(basePath, schema, bufferSize, multiPartUploadSize, columnGroups, storageConfig, storagePluginContext, true, false, writerFormat, schemaBasedFormats)
}

func NewPartialPackedRecordBatchWriter(
	basePath string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	multiPartUploadSize int64,
	columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	writerFormat string,
	schemaBasedFormats []string,
) (*packedRecordBatchWriter, error) {
	return newPackedRecordBatchWriter(basePath, schema, bufferSize, multiPartUploadSize, columnGroups, storageConfig, storagePluginContext, false, false, writerFormat, schemaBasedFormats)
}

// NewPartialPackedRecordBatchWriterWithTextRefsAsBinary creates a partial
// column-group writer whose TEXT columns use the binary LOB-reference Arrow
// representation. Schema-bump reconciliation uses this for newly added,
// nullable TEXT columns, whose historical values are materialized as binary
// NULLs without rewriting any existing LOB data.
func NewPartialPackedRecordBatchWriterWithTextRefsAsBinary(
	basePath string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	multiPartUploadSize int64,
	columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	writerFormat string,
	schemaBasedFormats []string,
) (*packedRecordBatchWriter, error) {
	return newPackedRecordBatchWriter(basePath, schema, bufferSize, multiPartUploadSize, columnGroups, storageConfig, storagePluginContext, false, true, writerFormat, schemaBasedFormats)
}

func validatePackedRecordBatchWriterSchema(schema *schemapb.CollectionSchema) error {
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetDataType() == schemapb.DataType_Text {
			return merr.WrapErrParameterInvalidMsg(
				"TEXT field %d requires TEXT-aware writer or text refs as binary preserve-ref path",
				field.GetFieldID(),
			)
		}
	}
	return nil
}

func newPackedRecordBatchWriter(
	basePath string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	multiPartUploadSize int64,
	columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	validatePK bool,
	textRefsAsBinary bool,
	writerFormat string,
	schemaBasedFormats []string,
) (*packedRecordBatchWriter, error) {
	if validatePK {
		_, err := typeutil.GetPrimaryFieldSchema(schema)
		if err != nil {
			return nil, err
		}
	}
	if !textRefsAsBinary {
		if err := validatePackedRecordBatchWriterSchema(schema); err != nil {
			return nil, err
		}
	}

	arrowSchema, err := ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not convert collection schema %s to arrow schema: %s", schema.Name, err.Error()))
	}
	if textRefsAsBinary {
		arrowSchema = overrideTextFieldsToBinary(schema, arrowSchema)
	}

	if len(schemaBasedFormats) > 0 && len(schemaBasedFormats) != len(columnGroups) {
		return nil, merr.WrapErrParameterInvalid(len(schemaBasedFormats), len(columnGroups),
			"schema based writer formats size must match column groups size")
	}
	extraProperties := map[string]string{}
	if writerFormat != "" {
		extraProperties[packed.PropertyWriterFormat] = writerFormat
	}
	if len(schemaBasedFormats) > 0 {
		extraProperties[packed.PropertyWriterSchemaBasedFormats] = strings.Join(schemaBasedFormats, ",")
	}
	writer, err := packed.NewFFIPackedWriter(basePath, arrowSchema, columnGroups, storageConfig, storagePluginContext, extraProperties)
	if err != nil {
		return nil, merr.WrapErrStorage(err, "can not new packed record writer")
	}
	columnGroupUncompressed := make(map[typeutil.UniqueID]uint64)
	columnGroupCompressed := make(map[typeutil.UniqueID]uint64)

	// provide mock path
	pathsMap := make(map[typeutil.UniqueID]string)
	start := time.Now().UnixNano()
	for _, columnGroup := range columnGroups {
		columnGroupUncompressed[columnGroup.GroupID] = 0
		columnGroupCompressed[columnGroup.GroupID] = 0
		start++
		pathsMap[columnGroup.GroupID] = path.Join(basePath, strconv.FormatInt(columnGroup.GroupID, 10), strconv.FormatInt(start, 10))
	}

	return &packedRecordBatchWriter{
		v3BatchStats:          v3BatchStats{columnGroupUncompressed: columnGroupUncompressed},
		writer:                writer,
		schema:                schema,
		arrowSchema:           arrowSchema,
		bufferSize:            bufferSize,
		pathsMap:              pathsMap,
		columnGroups:          columnGroups,
		columnGroupCompressed: columnGroupCompressed,
		storageConfig:         storageConfig,
	}, nil
}

// Deprecated, todo remove
func NewPackedSerializeWriter(bucketName string, paths []string, schema *schemapb.CollectionSchema, bufferSize int64,
	multiPartUploadSize int64, columnGroups []storagecommon.ColumnGroup, batchSize int,
) (*SerializeWriterImpl[*Value], error) {
	packedRecordWriter, err := NewPackedRecordWriter(bucketName, paths, schema, bufferSize, multiPartUploadSize, columnGroups, nil, nil)
	if err != nil {
		return nil, merr.Wrap(err, "can not new packed record writer")
	}
	return NewSerializeRecordWriter(packedRecordWriter, func(v []*Value) (Record, error) {
		return ValueSerializer(v, schema)
	}, batchSize), nil
}

// packedTextBatchWriter wraps FFISegmentWriter for TEXT column support during compaction.
// it handles TEXT column rewriting with LOB file management in REWRITE_ALL mode.
type packedTextBatchWriter struct {
	v3BatchStats
	writer                *packed.FFISegmentWriter
	bufferSize            int64
	columnGroups          []storagecommon.ColumnGroup
	bucketName            string
	pathsMap              map[typeutil.UniqueID]string
	schema                *schemapb.CollectionSchema
	arrowSchema           *arrow.Schema
	columnGroupCompressed map[typeutil.UniqueID]uint64
	storageConfig         *indexpb.StorageConfig
	// unflushedInput is the record bytes accepted since the last flush, measured
	// here because only this writer still sees the raw TEXT payload: the writer
	// below decides its own flush from the batch it receives, where a TEXT value
	// is already a 16-24 B LOB reference.
	unflushedInput uint64
}

func (pw *packedTextBatchWriter) Write(r Record) error {
	rec, release, err := toWriterRecord(r, pw.schema, pw.arrowSchema)
	if err != nil {
		return err
	}
	defer release()
	return pw.writeBatch(rec, pw.schema, pw.columnGroups, func(bytes uint64) error {
		if err := pw.writer.Write(rec); err != nil {
			return err
		}
		// Bound unflushed input including raw TEXT, not just its encoded
		// references. Streaming passes zero because each sync closes its writer.
		pw.unflushedInput += bytes
		if pw.bufferSize > 0 && pw.unflushedInput >= uint64(pw.bufferSize) {
			return pw.SyncBuffered()
		}
		return nil
	})
}

// SyncBuffered flushes the buffered column groups to storage without closing the
// writer. That includes the TEXT LOB column writers, which also flush on their
// own threshold between calls.
func (pw *packedTextBatchWriter) SyncBuffered() error {
	if pw.writer == nil {
		return nil
	}
	if err := pw.writer.SyncBuffered(); err != nil {
		return err
	}
	pw.unflushedInput = 0
	return nil
}

func (pw *packedTextBatchWriter) GetColumnGroupWrittenCompressed(columnGroup typeutil.UniqueID) uint64 {
	if size, ok := pw.columnGroupCompressed[columnGroup]; ok {
		return size
	}
	return 0
}

func (pw *packedTextBatchWriter) GetWrittenPaths(columnGroup typeutil.UniqueID) string {
	if path, ok := pw.pathsMap[columnGroup]; ok {
		return path
	}
	return ""
}

func (pw *packedTextBatchWriter) finishTextSizes(rowsWritten int64, totals map[int64]uint64) {
	if rowsWritten != pw.rowNum {
		// The native output doesn't cover exactly the accepted prefix (e.g.
		// cleanup after a buffered flush failed). No column total is proven.
		pw.fieldLogicalSizes = nil
		pw.rowNum = rowsWritten
		return
	}
	if len(totals) == 0 || !FieldLogicalSizesCover(pw.fieldLogicalSizes, rowsWritten) {
		return
	}
	for _, col := range pw.logicalLayout.columns {
		n, ok := totals[col.id]
		if !ok || !col.nativeText {
			continue
		}
		if pw.fieldLogicalSizes.Fields == nil {
			pw.fieldLogicalSizes.Fields = make(map[int64]*datapb.FieldLogicalSize)
		}
		size := &datapb.FieldLogicalSize{PayloadBytes: n}
		if col.nullable {
			size.ValidityBits = uint64(rowsWritten)
		}
		pw.fieldLogicalSizes.Fields[col.id] = size
	}
}

// Close closes the underlying segment writer and returns the resulting
// column-groups + LOB payload. The writer never touches the manifest —
// the caller passes the returned handle to packed.CommitManifestUpdates
// and calls Destroy on it when done. FFISegmentWriter.Close releases its
// own handle and properties, so no extra cleanup is needed here.
func (pw *packedTextBatchWriter) Close() (packed.WriterOutput, error) {
	if pw.writer == nil {
		return nil, nil
	}
	out, err := pw.writer.Close()
	pw.writer = nil
	if err != nil {
		pw.fieldLogicalSizes = nil
		return nil, err
	}
	if so, ok := out.(*packed.SegmentOutput); ok {
		pw.finishTextSizes(so.RowsWritten(), so.TextLogicalBytes())
	}
	for id := range pw.pathsMap {
		pw.columnGroupCompressed[id] = uint64(0)
	}
	return out, nil
}

// NewPackedTextBatchWriter creates a new writer that uses FFISegmentWriter with TEXT column support.
// this writer is used during compaction when TEXT columns need REWRITE_ALL strategy.
// textColumnConfigs: TEXT column configurations for REWRITE_ALL fields (nil if no TEXT fields need rewriting)
func NewPackedTextBatchWriter(
	bucketName string,
	basePath string,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	multiPartUploadSize int64,
	columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	textColumnConfigs []packed.TextColumnConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	writerFormat string,
	schemaBasedFormats []string,
) (*packedTextBatchWriter, error) {
	// validate PK field exists before proceeding
	_, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	arrowSchema, err := ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not convert collection schema %s to arrow schema: %s", schema.Name, err.Error()))
	}

	// In rewrite mode, TEXT columns arrive as binary (LOB references) from the reader.
	// Override the arrow schema to match the actual input type.
	hasRewrite := false
	nativeText := typeutil.NewUniqueSet()
	for _, tc := range textColumnConfigs {
		nativeText.Insert(tc.FieldID)
		hasRewrite = hasRewrite || tc.RewriteMode
	}
	if hasRewrite {
		arrowSchema = overrideTextFieldsToBinary(schema, arrowSchema)
	}

	if len(schemaBasedFormats) > 0 && len(schemaBasedFormats) != len(columnGroups) {
		return nil, merr.WrapErrParameterInvalid(len(schemaBasedFormats), len(columnGroups),
			"schema based writer formats size must match column groups size")
	}
	// build segment writer config
	schemaBasedPattern, err := packed.SchemaBasedPattern(arrowSchema, columnGroups)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not build schema based writer pattern %s", err.Error()))
	}
	config := &packed.SegmentWriterConfig{
		SegmentPath:        basePath,
		TextColumns:        textColumnConfigs,
		ColumnGroups:       columnGroups,
		WriterFormat:       writerFormat,
		SchemaBasedPattern: schemaBasedPattern,
		SchemaBasedFormats: schemaBasedFormats,
	}

	writer, err := packed.NewFFISegmentWriter(arrowSchema, config, storageConfig, storagePluginContext)
	if err != nil {
		return nil, merr.WrapErrStorage(err, "can not new segment writer")
	}

	columnGroupUncompressed := make(map[typeutil.UniqueID]uint64)
	columnGroupCompressed := make(map[typeutil.UniqueID]uint64)

	// provide mock path
	pathsMap := make(map[typeutil.UniqueID]string)
	start := time.Now().UnixNano()
	for _, columnGroup := range columnGroups {
		columnGroupUncompressed[columnGroup.GroupID] = 0
		columnGroupCompressed[columnGroup.GroupID] = 0
		start++
		pathsMap[columnGroup.GroupID] = path.Join(basePath, strconv.FormatInt(columnGroup.GroupID, 10), strconv.FormatInt(start, 10))
	}

	return &packedTextBatchWriter{
		v3BatchStats: v3BatchStats{
			columnGroupUncompressed: columnGroupUncompressed,
			logicalLayout:           newLogicalSizeLayout(schema, schema, nativeText),
		},
		writer:                writer,
		schema:                schema,
		arrowSchema:           arrowSchema,
		bufferSize:            bufferSize,
		bucketName:            bucketName,
		pathsMap:              pathsMap,
		columnGroups:          columnGroups,
		columnGroupCompressed: columnGroupCompressed,
		storageConfig:         storageConfig,
	}, nil
}
