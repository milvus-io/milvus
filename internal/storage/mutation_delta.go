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
	"context"
	"io"
	"slices"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MutationDelta is a single schema and partition scope. Fragments retain
// their WAL identity when callers cut a group into smaller files.
type MutationDelta struct {
	Layout deltacodec.Layout
	Rows   []deltacodec.Row
}

const deltaReadWindow = 32 << 20

func WriteMutationDelta(ctx context.Context, path string, d MutationDelta, cfg *indexpb.StorageConfig, plugin *indexcgopb.StoragePluginContext) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if cfg == nil {
		return 0, merr.WrapErrServiceInternalMsg("storage config is required for a typed delta")
	}
	if len(d.Rows) == 0 {
		return 0, merr.WrapErrServiceInternalMsg("empty delta file")
	}
	if err := d.Layout.ValidateIdentity(); err != nil {
		return 0, err
	}
	used := make(map[int64]bool)
	for _, r := range d.Rows {
		for _, a := range r.Assignments {
			used[a.GetFieldId()] = true
		}
	}
	d.Layout.Fields = slices.DeleteFunc(slices.Clone(d.Layout.Fields), func(f deltacodec.Field) bool { return !used[f.ID] })
	schema, err := d.Layout.Schema()
	if err != nil {
		return 0, err
	}
	metadata := arrow.MetadataFrom(map[string]string{"delta_format_version": "1"})
	schema = arrow.NewSchema(schema.Fields(), &metadata)
	columns := make([]int, schema.NumFields())
	for i := range columns {
		columns[i] = i
	}
	w, err := packed.NewPackedWriter([]string{path}, schema, deltaReadWindow, packed.DefaultMultiPartUploadSize, []storagecommon.ColumnGroup{{Columns: columns}}, cfg, plugin)
	if err != nil {
		return 0, err
	}
	defer w.Close()
	for begin := 0; begin < len(d.Rows); {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		end := deltaBatchEnd(d.Rows, begin, len(d.Layout.Fields), deltaReadWindow/4)
		rec, err := deltacodec.Encode(d.Layout, d.Rows[begin:end])
		if err != nil {
			return 0, err
		}
		err = w.WriteRecordBatch(rec)
		rec.Release()
		if err != nil {
			return 0, err
		}
		begin = end
	}
	sizes, err := w.CloseAndTell(1)
	if err != nil {
		return 0, err
	}
	return sizes[0], nil
}

// deltaBatchEnd accounts for shared variable-length payloads plus dense Arrow
// slots for sparse fields. One oversized WAL fragment stays indivisible.
func deltaBatchEnd(rows []deltacodec.Row, begin, fieldCount, limit int) int {
	end := begin
	var size int
	for end < len(rows) && end-begin < 256 {
		r := rows[end]
		estimate := 32 + fieldCount*12 + proto.Size(r.PrimaryKeys) + 8*messageutil.PrimaryKeyCount(r.PrimaryKeys)
		for _, a := range r.Assignments {
			estimate += proto.Size(a)
		}
		if end > begin && size+estimate > limit {
			break
		}
		size += estimate
		end++
	}
	return end
}

// ReadMutationDelta opens one complete object. A nil projection requests all
// fields; an empty projection still delivers deletes and mutation identities.
// The callback owns its sparse rows and can retain them after the next read.
// The caller supplies scope, schema version and expected PK type in layout.
// Updated fields and their types are discovered from the file schema.
func ReadMutationDelta(ctx context.Context, cm ChunkManager, binlog *datapb.Binlog, version int64, layout deltacodec.Layout, nullableArrays map[int64]bool, fields []int64, cfg *indexpb.StorageConfig, plugin *indexcgopb.StoragePluginContext, visit func(deltacodec.Layout, []deltacodec.Row) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := binlog.GetLogPath()
	f, err := cm.Reader(ctx, path)
	if err != nil {
		return err
	}
	var magic [4]byte
	_, err = io.ReadFull(f, magic[:])
	f.Close()
	if err != nil {
		return merr.Wrap(err, "read delta file signature")
	}
	return readMutationDelta(ctx, string(magic[:]), cm.MultiRead, binlog, version, layout, nullableArrays, fields, cfg, plugin, visit)
}

// ReadDownloadedMutationDelta uses an already downloaded complete object. Old
// non-manifest segment logs are read through the worker's binlog I/O pool.
func ReadDownloadedMutationDelta(ctx context.Context, data []byte, binlog *datapb.Binlog, version int64, layout deltacodec.Layout, nullableArrays map[int64]bool, cfg *indexpb.StorageConfig, plugin *indexcgopb.StoragePluginContext, visit func(deltacodec.Layout, []deltacodec.Row) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(data) < 4 {
		return merr.WrapErrDataIntegrityMsg("truncated mutation delta")
	}
	return readMutationDelta(ctx, string(data[:4]), func(context.Context, []string) ([][]byte, error) { return [][]byte{data}, nil }, binlog, version, layout, nullableArrays, nil, cfg, plugin, visit)
}

func readMutationDelta(ctx context.Context, magic string, download func(context.Context, []string) ([][]byte, error), binlog *datapb.Binlog, version int64, layout deltacodec.Layout, nullableArrays map[int64]bool, fields []int64, cfg *indexpb.StorageConfig, plugin *indexcgopb.StoragePluginContext, visit func(deltacodec.Layout, []deltacodec.Row) error) error {
	if magic != "PAR1" && magic != "PARE" {
		if binlog.GetMutationCounts() != nil {
			return merr.WrapErrDataIntegrityMsg("invalid typed delta signature")
		}
		return readLegacyMutationDelta(ctx, download, binlog, StorageV1, layout, cfg, plugin, visit)
	}
	return ReadPackedMutationDelta(ctx, binlog, layout, nullableArrays, fields, cfg, plugin, visit)
}

// ReadPackedMutationDelta reads a manifest-declared Parquet delta directly
// through the configured native filesystem, including encrypted footer discovery.
func ReadPackedMutationDelta(ctx context.Context, binlog *datapb.Binlog, layout deltacodec.Layout, nullableArrays map[int64]bool, fields []int64, cfg *indexpb.StorageConfig, plugin *indexcgopb.StoragePluginContext, visit func(deltacodec.Layout, []deltacodec.Row) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := binlog.GetLogPath()
	if cfg == nil {
		return merr.WrapErrServiceInternalMsg("storage config is required for a packed delta")
	}
	actual, count, err := packed.ReadDeltaFooter(path, cfg, plugin)
	if err != nil {
		return err
	}
	format, typed := actual.Metadata().GetValue("delta_format_version")
	if !typed {
		if binlog.GetMutationCounts() != nil || !isLegacyDeltaSchema(actual) {
			return merr.WrapErrDataIntegrityMsg("missing delta format version on a non-legacy file")
		}
		return readLegacyMutationDelta(ctx, nil, binlog, StorageV2, layout, cfg, plugin, visit)
	}
	if format != "1" {
		return merr.WrapErrDataIntegrityMsg("unsupported delta format %q", format)
	}
	l, err := layout.WithSchema(actual)
	if err != nil {
		return err
	}
	for _, f := range l.Fields {
		if nullable, ok := nullableArrays[f.ID]; ok && f.Type == schemapb.DataType_Array && nullable != *f.ElementNullable {
			return merr.WrapErrDataIntegrityMsg("delta field %d element nullability differs from destination schema", f.ID)
		}
	}
	r, selected, err := openProjectedMutationFile(path, count, l, fields, cfg, plugin)
	if err != nil {
		return err
	}
	defer r.Close()
	var decoded int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, err := r.ReadNext()
		if err == io.EOF {
			if decoded != count {
				return merr.WrapErrDataIntegrityMsg("decoded %d of %d delta rows", decoded, count)
			}
			return nil
		}
		if err != nil {
			return err
		}
		// The C stream owns its current record until ReadNext or Close.
		decoded += rec.NumRows()
		rows, err := deltacodec.Decode(selected, rec)
		if err != nil {
			return err
		}
		if len(selected.Fields) == len(l.Fields) {
			for _, r := range rows {
				if r.Operation == deltacodec.Update && len(r.Assignments) == 0 {
					return merr.WrapErrDataIntegrityMsg("update without assignments in complete delta projection at %d", r.TimeTick)
				}
			}
		}
		if err := visit(selected, rows); err != nil {
			return err
		}
	}
}

func isLegacyDeltaSchema(schema *arrow.Schema) bool {
	return schema.NumFields() == 2 && schema.Metadata().FindKey("delta_format_version") < 0
}

func readLegacyMutationDelta(ctx context.Context, download func(context.Context, []string) ([][]byte, error), binlog *datapb.Binlog, version int64, layout deltacodec.Layout, cfg *indexpb.StorageConfig, plugin *indexcgopb.StoragePluginContext, visit func(deltacodec.Layout, []deltacodec.Row) error) error {
	var r RecordReader
	var err error
	if version == StorageV1 {
		r, err = NewDeltalogReaderFromBinlogs(ctx, layout.PKType, []*datapb.Binlog{binlog}, WithVersion(version), WithDownloader(download))
	} else {
		// Both StorageV2 segment deltas and V3 manifest delta objects were
		// written by the packed writer. Its field-ID mapping also works for
		// historical files whose physical field names are empty or noncanonical.
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 0, IsPrimaryKey: true, DataType: layout.PKType}, {FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64}}}
		r, err = newPackedRecordReader([]string{binlog.GetLogPath()}, schema, deltaReadWindow, cfg, plugin, packed.ExternalReaderContext{})
	}

	if err != nil {
		return err
	}
	defer r.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, err := r.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		rows := make([]deltacodec.Row, rec.Len())
		for i := range rows {
			row := &rows[i]
			row.Operation = deltacodec.Delete
			row.TimeTick = uint64(rec.Column(common.TimeStampField).(*array.Int64).Value(i))
			switch col := rec.Column(0).(type) {
			case *array.Int64:
				row.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{col.Value(i)}}}}
			case *array.String:
				row.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{strings.Clone(col.Value(i))}}}}
			default:
				return merr.WrapErrDataIntegrityMsg("invalid legacy delta key type %T", col)
			}
		}
		if err := visit(layout.Project([]int64{}), rows); err != nil {
			return err
		}
	}
}

func openProjectedMutationFile(path string, count int64, l deltacodec.Layout, fields []int64, cfg *indexpb.StorageConfig, plugin *indexcgopb.StoragePluginContext) (*packed.FFIPackedReader, deltacodec.Layout, error) {
	schema, err := l.Schema()
	if err != nil {
		return nil, l, err
	}
	all := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		all[i] = f.Name
	}
	selected := l.Project(fields)
	schema, err = selected.Schema()
	if err != nil {
		return nil, selected, err
	}
	names := make([]string, schema.NumFields())
	for i, f := range schema.Fields() {
		names[i] = f.Name
	}

	if count <= 0 {
		return nil, selected, merr.WrapErrDataIntegrityMsg("typed delta object has no rows")
	}
	r, err := packed.NewDeltaFileReader(path, count, all, schema, names, deltaReadWindow, cfg, plugin)
	return r, selected, err
}

// ApplyMutationLiterals rebuilds only the assigned column, preserving the
// physical type and original nulls (including columns with a default value).
func ApplyMutationLiterals(base arrow.Array, field *schemapb.FieldSchema, values map[int]*schemapb.UpdateLiteral) (arrow.Array, error) {
	b := array.NewBuilder(memory.DefaultAllocator, base.DataType())
	defer b.Release()
	b.Reserve(base.Len())
	for i := range base.Len() {
		v, changed := values[i]
		if !changed {
			if _, err := appendValueAt(b, base, i, field, appendValueDefault{}); err != nil {
				return nil, err
			}
			continue
		}
		if v.GetValue() == nil {
			return nil, merr.WrapErrDataIntegrityMsg("delta field %d has no literal", field.GetFieldID())
		}
		if n, null := v.GetValue().(*schemapb.UpdateLiteral_SetNull); null {
			if !n.SetNull || !field.GetNullable() {
				return nil, merr.WrapErrDataIntegrityMsg("illegal NULL assignment for delta field %d", field.GetFieldID())
			}
			b.AppendNull()
			continue
		}
		if field.GetDataType() == schemapb.DataType_Array {
			if binary, ok := b.(*array.BinaryBuilder); ok {
				encoded, err := proto.Marshal(v.GetArrayValue())
				if err != nil {
					return nil, err
				}
				binary.Append(encoded)
				continue
			}
		}
		if err := deltacodec.AppendLiteral(b, deltacodec.Field{ID: field.GetFieldID(), Type: field.GetDataType(), Element: field.GetElementType()}, v); err != nil {
			return nil, err
		}
	}
	return b.NewArray(), nil
}
