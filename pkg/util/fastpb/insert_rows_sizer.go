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

package fastpb

import (
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
)

// InsertRowsSizer accumulates logical InsertRows accounting while routing source
// rows to partitions. It borrows immutable source columns, including the PK column.
// Rows must arrive in strictly increasing source order across all partitions.
// No row selections, timestamp arrays or selected RowID copies are retained.
// Discard the sizer after an error; partial accumulation is not rolled back.
type InsertRowsSizer struct {
	cursor       *InsertRequestViewCursor
	partitions   []insertRowsPartitionSize
	scanRow      int
	lastRow      int
	selectedRows int
}

type insertRowsPartitionSize struct {
	rows         int
	rowIDPayload int
	fields       []insertFieldRowDelta
}

// NewInsertRowsSizer prepares the same field plans used by the view encoder.
// Timestamps are not needed: logical InsertRows accounting excludes them.
func NewInsertRowsSizer(source *msgpb.InsertRequest) (*InsertRowsSizer, error) {
	cursor, err := NewInsertRequestViewCursor(source)
	if err != nil {
		return nil, err
	}
	cursor.sizeState.fields = make([]insertFieldSizeState, len(source.GetFieldsData()))
	for index, field := range source.GetFieldsData() {
		if err := cursor.sizeState.fields[index].reset(field); err != nil {
			return nil, err
		}
	}
	return &InsertRowsSizer{cursor: cursor, lastRow: -1}, nil
}

// AddPartition registers a partition and returns its zero-based table index.
func (s *InsertRowsSizer) AddPartition() int {
	index := len(s.partitions)
	s.partitions = append(s.partitions, insertRowsPartitionSize{fields: make([]insertFieldRowDelta, len(s.cursor.sizeState.fields))})
	return index
}

// AddRow accumulates a source row into a partition registered by AddPartition.
// Nullable-vector source positions advance once, even for interleaved partitions.
func (s *InsertRowsSizer) AddRow(row, partition int) error {
	if row < 0 {
		return insertViewInternal("row offset at selection index %d is negative: %d", s.selectedRows, row)
	}
	if row <= s.lastRow {
		return insertViewInternal("cursor row offsets must be globally increasing: rows[%d]=%d after %d", s.selectedRows, row, s.lastRow)
	}
	if row >= len(s.cursor.source.GetRowIDs()) {
		return insertViewInternal("row offset %d exceeds RowIDs length %d", row, len(s.cursor.source.GetRowIDs()))
	}
	if s.cursor.source.GetNumRows() != 0 && uint64(row) >= s.cursor.source.GetNumRows() {
		return insertViewInternal("row offset %d exceeds source NumRows %d", row, s.cursor.source.GetNumRows())
	}
	if partition < 0 || partition >= len(s.partitions) {
		return insertViewInternal("unregistered sizing partition %d", partition)
	}
	if err := s.cursor.resolveRowFieldDataIndices(s.scanRow, row); err != nil {
		return err
	}
	target := &s.partitions[partition]
	for index := range s.cursor.sizeState.fields {
		field := &s.cursor.sizeState.fields[index]
		var delta insertFieldRowDelta
		if err := field.previewRow(row, int(s.cursor.rowFieldDataIndices[index]), &delta); err != nil {
			return err
		}
		aggregate := &target.fields[index]
		var err error
		aggregate.payloadSize, err = checkedAddSize(aggregate.payloadSize, delta.payloadSize, "partition column payload")
		if err != nil {
			return err
		}
		aggregate.selectedRows += delta.selectedRows
		aggregate.sparseDim = max(aggregate.sparseDim, delta.sparseDim)
		next := int64(row + 1)
		if field.compactVector {
			next = s.cursor.rowFieldDataIndices[index]
			if field.validData[row] {
				next++
			}
		}
		s.cursor.nextFieldDataIndices[index] = next
	}
	var err error
	target.rowIDPayload, err = checkedAddSize(target.rowIDPayload, protowire.SizeVarint(uint64(s.cursor.source.RowIDs[row])), "partition RowID payload")
	if err != nil {
		return err
	}
	target.rows++
	s.scanRow, s.lastRow = row+1, row
	s.selectedRows++
	return nil
}

// Sizes finalizes nested protobuf framing without visiting source rows again.
// RowIDs use explicit logical LongArray sizes, regardless of WAL delta encoding.
// Field plans preserve nullable masks, array metadata and sparse dimensions.
func (s *InsertRowsSizer) Sizes() ([]uint64, error) {
	sizes := make([]uint64, len(s.partitions))
	for partition, target := range s.partitions {
		if target.rows == 0 {
			return nil, insertViewInternal("row selection is empty")
		}
		longSize, err := insertBytesFieldSize(1, target.rowIDPayload)
		if err != nil {
			return nil, err
		}
		size, err := insertBytesFieldSize(2, longSize)
		if err != nil {
			return nil, err
		}
		for index := range s.cursor.sizeState.fields {
			computed, err := s.cursor.sizeState.fields[index].computedSize(&target.fields[index], target.rows)
			if err != nil {
				return nil, err
			}
			columnSize, err := insertBytesFieldSize(4, computed.fieldSize)
			if err != nil {
				return nil, err
			}
			size, err = checkedAddSize(size, columnSize, "partition logical size")
			if err != nil {
				return nil, err
			}
		}
		sizes[partition] = uint64(size)
	}
	return sizes, nil
}
