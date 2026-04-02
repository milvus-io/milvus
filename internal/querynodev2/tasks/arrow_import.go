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
	"fmt"
	"io"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/arrio"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	arrowMetadataFieldIDKey  = "milvus.field_id"
	arrowMetadataDataTypeKey = "milvus.data_type"
)

// dataFrameFromArrowReader reads all RecordBatches from an Arrow stream reader
// and converts them into a chain DataFrame. Each RecordBatch becomes one chunk (NQ).
func dataFrameFromArrowReader(reader arrio.Reader) (*chain.DataFrame, error) {
	if reader == nil {
		return nil, merr.WrapErrServiceInternal("nil Arrow reader")
	}
	// Release the underlying C ArrowArrayStream promptly so the C++ memory
	// backing the stream is freed as soon as we've consumed all batches,
	// rather than waiting for GC finalization. Without this, repeated
	// search→reduce cycles can leave stale C++ pointers that cause
	// type-metadata corruption after the memory is reused.
	if rc, ok := reader.(interface{ Release() }); ok {
		defer rc.Release()
	}

	var records []arrow.Record
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("failed to read Arrow batch: %v", err))
		}
		// Retain so the record stays alive after the reader advances on the next iteration.
		rec.Retain()
		records = append(records, rec)
	}

	if len(records) == 0 {
		return emptyDF(), nil
	}

	numCols := int(records[0].NumCols())
	numChunks := len(records)
	chunkSizes := make([]int64, numChunks)
	for i, rec := range records {
		chunkSizes[i] = rec.NumRows()
	}

	builder := chain.NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(chunkSizes)

	for colIdx := 0; colIdx < numCols; colIdx++ {
		field := records[0].Schema().Field(colIdx)
		colName := field.Name
		chunks := make([]arrow.Array, numChunks)
		nullable := field.Nullable
		for i, rec := range records {
			chunks[i] = rec.Column(colIdx)
			if chunks[i].NullN() > 0 {
				nullable = true
			}
			chunks[i].Retain()
		}
		builder.SetFieldNullable(colName, nullable)
		if fieldID, ok, err := fieldMetadataInt64(field.Metadata, arrowMetadataFieldIDKey); err != nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("invalid Arrow field metadata %s for column %s: %v", arrowMetadataFieldIDKey, colName, err))
		} else if ok {
			builder.SetFieldID(colName, fieldID)
		}
		if dataType, ok, err := fieldMetadataInt64(field.Metadata, arrowMetadataDataTypeKey); err != nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("invalid Arrow field metadata %s for column %s: %v", arrowMetadataDataTypeKey, colName, err))
		} else if ok {
			builder.SetFieldType(colName, schemapb.DataType(dataType))
		}
		if err := builder.AddColumnFromChunks(colName, chunks); err != nil {
			return nil, err
		}
	}

	return builder.Build(), nil
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
