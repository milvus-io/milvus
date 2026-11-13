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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// insertRowsViewEncoder encodes shared rows directly into its destination.
// It borrows source columns until the synchronous MarshalTo call completes.
type insertRowsViewEncoder struct {
	encodedNonColumnFields []byte
	columns                *InsertRequestViewEncoder
}

func newInsertRowsViewEncoder(rows *messagespb.InsertRows, source *msgpb.InsertRequest, selected []int) (*insertRowsViewEncoder, error) {
	if len(rows.GetColumns()) != 0 {
		return nil, insertViewInternal("row non-column fields must not include columns")
	}
	encoder, err := NewInsertRequestViewEncoder(&msgpb.InsertRequest{}, source, selected)
	if err != nil {
		return nil, err
	}
	encoder.size -= protowire.SizeTag(10) + protowire.SizeBytes(encoder.sizePlan[0]) + protowire.SizeTag(11) + protowire.SizeBytes(encoder.sizePlan[1]) + protowire.SizeTag(14) + protowire.SizeVarint(uint64(len(selected)))
	encoder.columnsOnly = true
	encoder.sizePlan = encoder.sizePlan[2:]
	payload, err := proto.Marshal(CompactInsertRows(rows))
	if err != nil {
		return nil, err
	}
	return &insertRowsViewEncoder{encodedNonColumnFields: payload, columns: encoder}, nil
}

func (e *insertRowsViewEncoder) EncodedSize() (int, error) {
	size, err := e.columns.EncodedSize()
	return len(e.encodedNonColumnFields) + size, err
}

func (e *insertRowsViewEncoder) MarshalTo(dst []byte) (int, error) {
	copy(dst, e.encodedNonColumnFields)
	n, err := e.columns.MarshalTo(dst[len(e.encodedNonColumnFields):])
	return len(e.encodedNonColumnFields) + n, err
}

// insertOrUpsertViewEncoder owns outer framing and shares the same nested rows codec.
type insertOrUpsertViewEncoder struct {
	rows   *insertRowsViewEncoder
	suffix []byte
}

func (e *insertOrUpsertViewEncoder) EncodedSize() (int, error) {
	size, err := e.rows.EncodedSize()
	return protowire.SizeTag(1) + protowire.SizeBytes(size) + len(e.suffix), err
}

func (e *insertOrUpsertViewEncoder) MarshalTo(dst []byte) (int, error) {
	size, err := e.EncodedSize()
	if err != nil {
		return 0, err
	}
	if len(dst) < size {
		return 0, insertViewInternal("row-write destination is too small")
	}
	rowSize, err := e.rows.EncodedSize()
	if err != nil {
		return 0, err
	}
	prefix := protowire.AppendTag(dst[:0], 1, protowire.BytesType)
	prefix = protowire.AppendVarint(prefix, uint64(rowSize))
	n, err := e.rows.MarshalTo(dst[len(prefix):])
	if err != nil {
		return 0, err
	}
	end := len(prefix) + n
	copy(dst[end:], e.suffix)
	return end + len(e.suffix), nil
}

// InsertMessageViewEncoder writes Insert V2 without copying selected columns.
type InsertMessageViewEncoder struct{ insertOrUpsertViewEncoder }

func (*InsertMessageViewEncoder) BodyType() *messagespb.InsertMessageBody { return nil }
func NewInsertMessageViewEncoder(body *messagespb.InsertMessageBody, source *msgpb.InsertRequest, selected []int) (*InsertMessageViewEncoder, error) {
	rows, err := newInsertRowsViewEncoder(body.GetRows(), source, selected)
	if err != nil {
		return nil, err
	}
	return &InsertMessageViewEncoder{insertOrUpsertViewEncoder: insertOrUpsertViewEncoder{rows: rows}}, nil
}

// UpsertViewEncoder adds explicit effects and CAS to the shared rows encoding.
type UpsertViewEncoder struct{ insertOrUpsertViewEncoder }

func (*UpsertViewEncoder) BodyType() *messagespb.UpsertMessageBody { return nil }
func NewUpsertViewEncoder(body *messagespb.UpsertMessageBody, source *msgpb.InsertRequest, selected []int) (*UpsertViewEncoder, error) {
	rows, err := newInsertRowsViewEncoder(body.GetRows(), source, selected)
	if err != nil {
		return nil, err
	}
	suffix, err := proto.Marshal(&messagespb.UpsertMessageBody{DeleteEffects: CompactDeleteEffects(body.GetRows().GetPrimaryKeys(), body.GetDeleteEffects()), PartialUpdateCas: body.GetPartialUpdateCas()})
	if err != nil {
		return nil, err
	}
	return &UpsertViewEncoder{insertOrUpsertViewEncoder: insertOrUpsertViewEncoder{rows: rows, suffix: suffix}}, nil
}

// UpsertPartitionSizes computes per-partition logical row-and-column accounting.
// RowIDs use their explicit representation, independently of wire compression.
// Scratch is shared, but each partition starts a new row selection.
// Nil selects all rows; a non-nil empty selection retains its existing error.
// The source and selections must remain unchanged during this call.
func UpsertPartitionSizes(source *msgpb.InsertRequest, selections [][]int64) ([]uint64, error) {
	cursor, err := NewInsertRequestViewCursor(source)
	if err != nil {
		return nil, err
	}
	sizes := make([]uint64, len(selections))
	capacity, explicitCapacity := 0, 0
	for _, selection := range selections {
		explicitCapacity = max(explicitCapacity, len(selection))
		count := len(selection)
		if selection == nil {
			count = len(source.RowIDs)
		}
		capacity = max(capacity, count)
	}
	rows := make([]int, 0, capacity)
	rowIDStorage := make([]int64, 0, explicitCapacity)
	for index, selection := range selections {
		rows = rows[:0]
		rowIDs := rowIDStorage[:0]
		if selection == nil {
			for row := range source.RowIDs {
				rows = append(rows, row)
			}
			rowIDs = source.RowIDs
		} else {
			for _, row := range selection {
				rows = append(rows, int(row))
				rowIDs = append(rowIDs, source.RowIDs[row])
			}
		}
		// Partition selections can interleave. Reset logical and compact positions
		// while retaining field plans and scratch owned by this sizing call.
		clear(cursor.fieldDataIndices)
		cursor.scanRow = 0
		cursor.lastSelectedRow = -1
		cursor.pendingRow = -1
		encoder, _, err := cursor.NextEncoder(&msgpb.InsertRequest{}, rows, 0)
		if err != nil {
			return nil, err
		}
		size := encoder.size - protowire.SizeTag(10) - protowire.SizeBytes(encoder.sizePlan[0]) - protowire.SizeTag(11) - protowire.SizeBytes(encoder.sizePlan[1]) - protowire.SizeTag(14) - protowire.SizeVarint(uint64(len(rows)))
		sizes[index] = uint64(size + proto.Size(&messagespb.InsertRows{RowIdsEncoding: &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: rowIDs}}}))
		// No encoder escapes this size-only operation or needs a MarshalTo call.
		cursor.activeEncoder = nil
	}
	return sizes, nil
}
