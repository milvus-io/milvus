/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tasks

import (
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	arrowMetadataFieldIDKey  = "milvus.field_id"
	arrowMetadataDataTypeKey = "milvus.data_type"
)

func dataFrameFromArrowRecordBatch(rec arrow.Record, chunkSizes []int64) (*chain.DataFrame, error) {
	if rec == nil {
		return nil, merr.WrapErrServiceInternal("nil Arrow record batch")
	}
	if len(chunkSizes) == 0 {
		return nil, merr.WrapErrServiceInternal("empty Arrow RecordBatch chunk sizes")
	}

	var totalRows int64
	for _, size := range chunkSizes {
		if size < 0 {
			return nil, merr.WrapErrServiceInternalMsg("invalid negative Arrow chunk size: %d", size)
		}
		totalRows += size
	}
	if totalRows != rec.NumRows() {
		return nil, merr.WrapErrServiceInternalMsg("Arrow record batch row count %d does not match chunk sizes total %d", rec.NumRows(), totalRows)
	}

	numCols := int(rec.NumCols())
	builder := chain.NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(chunkSizes)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		field := rec.Schema().Field(colIdx)
		colName := field.Name
		fullChunk := rec.Column(colIdx)
		chunks := make([]arrow.Array, len(chunkSizes))
		nullable := field.Nullable
		releaseChunks := func() {
			for _, chunk := range chunks {
				if chunk != nil {
					chunk.Release()
				}
			}
		}

		var offset int64
		for i, size := range chunkSizes {
			chunks[i] = array.NewSlice(fullChunk, offset, offset+size)
			if chunks[i].NullN() > 0 {
				nullable = true
			}
			offset += size
		}
		builder.SetFieldNullable(colName, nullable)
		if fieldID, ok, err := fieldMetadataInt64(field.Metadata, arrowMetadataFieldIDKey); err != nil {
			releaseChunks()
			return nil, merr.WrapErrServiceInternalMsg("invalid Arrow field metadata %s for column %s: %v", arrowMetadataFieldIDKey, colName, err)
		} else if ok {
			builder.SetFieldID(colName, fieldID)
		}
		if dataType, ok, err := fieldMetadataInt64(field.Metadata, arrowMetadataDataTypeKey); err != nil {
			releaseChunks()
			return nil, merr.WrapErrServiceInternalMsg("invalid Arrow field metadata %s for column %s: %v", arrowMetadataDataTypeKey, colName, err)
		} else if ok {
			builder.SetFieldType(colName, schemapb.DataType(dataType))
		}
		if err := builder.AddColumnFromChunks(colName, chunks); err != nil {
			return nil, err
		}
	}

	df := builder.Build()
	return df, nil
}

func fieldMetadataInt64(metadata arrow.Metadata, key string) (int64, bool, error) {
	value, ok := metadata.GetValue(key)
	if !ok {
		return 0, false, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return parsed, true, nil
}
