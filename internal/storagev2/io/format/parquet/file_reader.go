// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parquet

import (
	"context"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/metadata"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/bits-and-blooms/bitset"

	"github.com/milvus-io/milvus/internal/storagev2/common/constant"
	"github.com/milvus-io/milvus/internal/storagev2/filter"
	"github.com/milvus-io/milvus/internal/storagev2/io/fs"
	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
)

type FileReader struct {
	reader    *pqarrow.FileReader
	options   *options.ReadOptions
	recReader pqarrow.RecordReader
}

// When the Reader reaches the end of the underlying stream, it returns (nil, io.EOF)
func (r *FileReader) Read() (arrow.Record, error) {
	if r.recReader == nil {
		// lazy init
		if err := r.initRecReader(); err != nil {
			return nil, err
		}
	}
	rec, err := r.recReader.Read()
	if err != nil {
		return nil, err
	}

	return applyFilters(rec, r.options.Filters), nil
}

func applyFilters(rec arrow.Record, filters map[string]filter.Filter) arrow.Record {
	filterBitSet := bitset.New(uint(rec.NumRows()))
	for col, f := range filters {
		colIndices := rec.Schema().FieldIndices(col)
		if len(colIndices) == 0 {
			panic("column not found")
		}
		colIndex := colIndices[0]
		arr := rec.Column(colIndex)
		f.Apply(arr, filterBitSet)
	}

	if filterBitSet.None() {
		return rec
	}

	var cols []arrow.Array
	for i := 0; i < int(rec.NumCols()); i++ {
		col := rec.Column(i)
		switch t := col.(type) {
		case *array.Int8:
			builder := array.NewInt8Builder(memory.DefaultAllocator)
			filtered := filterRecord(t.Int8Values(), filterBitSet)
			builder.AppendValues(filtered, nil)
			cols = append(cols, builder.NewArray())
		case *array.Uint8:
			builder := array.NewUint8Builder(memory.DefaultAllocator)
			filtered := filterRecord(t.Uint8Values(), filterBitSet)
			builder.AppendValues(filtered, nil)
			cols = append(cols, builder.NewArray())
		case *array.Int16:
			builder := array.NewInt16Builder(memory.DefaultAllocator)
			filtered := filterRecord(t.Int16Values(), filterBitSet)
			builder.AppendValues(filtered, nil)
			cols = append(cols, builder.NewArray())
		case *array.Uint16:
			builder := array.NewUint16Builder(memory.DefaultAllocator)
			filtered := filterRecord(t.Uint16Values(), filterBitSet)
			builder.AppendValues(filtered, nil)
			cols = append(cols, builder.NewArray())
		case *array.Int32:
			builder := array.NewInt32Builder(memory.DefaultAllocator)
			filtered := filterRecord(t.Int32Values(), filterBitSet)
			builder.AppendValues(filtered, nil)
			cols = append(cols, builder.NewArray())
		case *array.Uint32:
			builder := array.NewUint32Builder(memory.DefaultAllocator)
			filtered := filterRecord(t.Uint32Values(), filterBitSet)
			builder.AppendValues(filtered, nil)
			cols = append(cols, builder.NewArray())
		case *array.Int64:
			builder := array.NewInt64Builder(memory.DefaultAllocator)
			filtered := filterRecord(t.Int64Values(), filterBitSet)
			builder.AppendValues(filtered, nil)
			cols = append(cols, builder.NewArray())
		case *array.Uint64:
			builder := array.NewUint64Builder(memory.DefaultAllocator)
			filtered := filterRecord(t.Uint64Values(), filterBitSet)
			builder.AppendValues(filtered, nil)
			cols = append(cols, builder.NewArray())
		default:
			panic("unsupported type")
		}
	}

	return array.NewRecord(rec.Schema(), cols, int64(cols[0].Len()))
}

type comparableColumnType interface {
	int8 | uint8 | int16 | uint16 | int32 | uint32 | int64 | uint64 | float32 | float64
}

func filterRecord[T comparableColumnType](targets []T, filterBitSet *bitset.BitSet) []T {
	var res []T
	for i := 0; i < int(filterBitSet.Len()); i++ {
		if !filterBitSet.Test(uint(i)) {
			res = append(res, targets[i])
		}
	}
	return res
}

func (r *FileReader) initRecReader() error {
	var (
		filters map[string]filter.Filter = r.options.Filters
		columns []string                 = r.options.Columns
	)

	var (
		rowGroupNum  int                    = r.reader.ParquetReader().NumRowGroups()
		fileMetaData *metadata.FileMetaData = r.reader.ParquetReader().MetaData()
	)

	var rowGroups []int
	var colIndices []int
	// filters check column statistics
x1:
	for i := 0; i < rowGroupNum; i++ {
		rowGroupMetaData := fileMetaData.RowGroup(i)
		for col, filter := range filters {
			if checkColumnStats(rowGroupMetaData, col, filter) {
				// ignore the row group
				break x1
			}
		}
		rowGroups = append(rowGroups, i)
	}

	for _, col := range columns {
		colIndex := fileMetaData.Schema.Root().FieldIndexByName(col)
		if colIndex == -1 {
			panic("column not found")
		}
		colIndices = append(colIndices, colIndex)
	}

	recReader, err := r.reader.GetRecordReader(context.TODO(), colIndices, rowGroups)
	if err != nil {
		return err
	}
	r.recReader = recReader
	return nil
}

func checkColumnStats(rowGroupMetaData *metadata.RowGroupMetaData, col string, f filter.Filter) bool {
	colIndex := rowGroupMetaData.Schema.Root().FieldIndexByName(col)
	if colIndex == -1 {
		panic("column not found")
	}
	colMetaData, err := rowGroupMetaData.ColumnChunk(colIndex)
	if err != nil {
		panic(err)
	}

	stats, err := colMetaData.Statistics()
	if err != nil || stats == nil {
		return false
	}
	return f.CheckStatistics(stats)
}

func (r *FileReader) Close() error {
	if r.recReader != nil {
		r.recReader.Release()
	}
	return nil
}

func NewFileReader(fs fs.Fs, filePath string, options *options.ReadOptions) (*FileReader, error) {
	f, err := fs.OpenFile(filePath)
	if err != nil {
		return nil, err
	}

	parquetReader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}

	reader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: constant.ReadBatchSize}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}
	return &FileReader{reader: reader, options: options}, nil
}
