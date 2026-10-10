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
	"path"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// toWriterRecord projects a batch onto the writer schema and validates column types.
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
			// Absent TEXT fields are filled with binary nulls, including for
			// decoded UTF8 writers. Nulls have no LOB/string payload to convert;
			// normalize them to the writer's fixed schema on every batch.
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
		// size := arr.Data().SizeInBytes()
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
		err := pw.writer.Close()
		if err != nil {
			return err
		}
		for id, fpath := range pw.pathsMap {
			truePath := path.Join(pw.bucketName, fpath)
			size, err := packed.GetFileSize(truePath, pw.storageConfig)
			if err != nil {
				return err
			}
			pw.columnGroupCompressed[id] = uint64(size)
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
	// Validate PK field exists before proceeding
	_, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	arrowSchema, err := ConvertToArrowSchema(schema, false)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not convert collection schema %s to arrow schema: %s", schema.Name, err.Error()))
	}
	// if storage config is not passed, use common config
	storageType := paramtable.Get().CommonCfg.StorageType.GetValue()
	if storageConfig != nil {
		storageType = storageConfig.GetStorageType()
	}
	// compose true path before create packed writer here
	// and returned writtenPaths shall remain untouched
	truePaths := lo.Map(paths, func(p string, _ int) string {
		if storageType == "local" {
			return p
		}
		return path.Join(bucketName, p)
	})
	writer, err := packed.NewPackedWriter(truePaths, arrowSchema, bufferSize, multiPartUploadSize, columnGroups, storageConfig, storagePluginContext)
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

type packedRecordManifestWriter struct {
	writer                  *packed.FFIPackedWriter
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

func (pw *packedRecordManifestWriter) Write(r Record) error {
	rec, release, err := toWriterRecord(r, pw.schema, pw.arrowSchema)
	if err != nil {
		return err
	}
	defer release()
	pw.rowNum += int64(r.Len())
	for col, arr := range rec.Columns() {
		// size := arr.Data().SizeInBytes()
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

func (pw *packedRecordManifestWriter) GetWrittenUncompressed() uint64 {
	return pw.writtenUncompressed
}

func (pw *packedRecordManifestWriter) GetColumnGroupWrittenUncompressed(columnGroup typeutil.UniqueID) uint64 {
	if size, ok := pw.columnGroupUncompressed[columnGroup]; ok {
		return size
	}
	return 0
}

func (pw *packedRecordManifestWriter) GetColumnGroupWrittenCompressed(columnGroup typeutil.UniqueID) uint64 {
	if size, ok := pw.columnGroupCompressed[columnGroup]; ok {
		return size
	}
	return 0
}

func (pw *packedRecordManifestWriter) GetWrittenPaths(columnGroup typeutil.UniqueID) string {
	if path, ok := pw.pathsMap[columnGroup]; ok {
		return path
	}
	return ""
}

func (pw *packedRecordManifestWriter) GetWrittenManifest() string {
	return pw.outputManifest
}

func (pw *packedRecordManifestWriter) GetWrittenRowNum() int64 {
	return pw.rowNum
}

func (pw *packedRecordManifestWriter) Close() error {
	if pw.writer != nil {
		manifest, err := pw.writer.Close()
		if err != nil {
			return err
		}
		pw.outputManifest = manifest
		for id := range pw.pathsMap {
			pw.columnGroupCompressed[id] = uint64(0)
		}
	}
	return nil
}

func NewPackedRecordManifestWriter(
	bucketName string,
	basePath string,
	baseVersion int64,
	schema *schemapb.CollectionSchema,
	bufferSize int64,
	multiPartUploadSize int64,
	columnGroups []storagecommon.ColumnGroup,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
) (*packedRecordManifestWriter, error) {
	// Validate PK field exists before proceeding
	_, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}

	arrowSchema, err := ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not convert collection schema %s to arrow schema: %s", schema.Name, err.Error()))
	}

	writer, err := packed.NewFFIPackedWriter(basePath, baseVersion, arrowSchema, columnGroups, storageConfig, storagePluginContext)
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

	return &packedRecordManifestWriter{
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
