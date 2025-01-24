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

package arrowutil

import (
	"context"

	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"

	"github.com/milvus-io/milvus/internal/storagev2/common/constant"
	"github.com/milvus-io/milvus/internal/storagev2/io/fs"
	"github.com/milvus-io/milvus/internal/storagev2/storage/options"
)

func MakeArrowFileReader(fs fs.Fs, filePath string) (*pqarrow.FileReader, error) {
	f, err := fs.OpenFile(filePath)
	if err != nil {
		return nil, err
	}
	parquetReader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}
	return pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: constant.ReadBatchSize}, memory.DefaultAllocator)
}

func MakeArrowRecordReader(reader *pqarrow.FileReader, opts *options.ReadOptions) (array.RecordReader, error) {
	var rowGroupsIndices []int
	var columnIndices []int
	metadata := reader.ParquetReader().MetaData()
	for _, c := range opts.Columns {
		columnIndices = append(columnIndices, metadata.Schema.ColumnIndexByName(c))
	}
	for _, f := range opts.Filters {
		columnIndices = append(columnIndices, metadata.Schema.ColumnIndexByName(f.GetColumnName()))
	}

	for i := 0; i < len(metadata.RowGroups); i++ {
		rg := metadata.RowGroup(i)
		var canIgnored bool
		for _, filter := range opts.Filters {
			columnIndex := rg.Schema.ColumnIndexByName(filter.GetColumnName())
			columnChunk, err := rg.ColumnChunk(columnIndex)
			if err != nil {
				return nil, err
			}
			columnStats, err := columnChunk.Statistics()
			if err != nil {
				return nil, err
			}
			if columnStats == nil || !columnStats.HasMinMax() {
				continue
			}
			if filter.CheckStatistics(columnStats) {
				canIgnored = true
				break
			}
		}
		if !canIgnored {
			rowGroupsIndices = append(rowGroupsIndices, i)
		}
	}

	return reader.GetRecordReader(context.TODO(), columnIndices, rowGroupsIndices)
}
