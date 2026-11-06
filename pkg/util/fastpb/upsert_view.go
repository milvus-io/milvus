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
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// UpsertViewEncoder writes borrowed columns directly after independent row
// metadata. It uses the Insert view encoder's field plans and nullable cursors.
type UpsertViewEncoder struct {
	metadata []byte
	columns  *InsertRequestViewEncoder
}

func (*UpsertViewEncoder) BodyType() *messagespb.UpsertMessageBody { return nil }
func NewUpsertViewEncoder(metadata *messagespb.UpsertMessageBody, source *msgpb.InsertRequest, rows []int) (*UpsertViewEncoder, error) {
	if len(metadata.GetColumns()) != 0 {
		return nil, insertViewInternal("Upsert view metadata must not own columns")
	}
	encoder, err := NewInsertRequestViewEncoder(&msgpb.InsertRequest{}, source, rows)
	if err != nil {
		return nil, err
	}
	// Remove Insert-only framing from the existing exact size plan.
	encoder.size -= protowire.SizeTag(10) + protowire.SizeBytes(encoder.sizePlan[0]) + protowire.SizeTag(11) + protowire.SizeBytes(encoder.sizePlan[1]) + protowire.SizeTag(14) + protowire.SizeVarint(uint64(len(rows)))
	encoder.columnsOnly = true
	// The Insert plan starts with packed timestamps and RowIDs.
	encoder.sizePlan = encoder.sizePlan[2:]
	payload, err := proto.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	return &UpsertViewEncoder{metadata: payload, columns: encoder}, nil
}

func (e *UpsertViewEncoder) EncodedSize() (int, error) {
	size, err := e.columns.EncodedSize()
	return len(e.metadata) + size, err
}

func (e *UpsertViewEncoder) MarshalTo(dst []byte) (int, error) {
	size, err := e.EncodedSize()
	if err != nil {
		return 0, err
	}
	if len(dst) < size {
		return 0, insertViewInternal("Upsert destination is too small")
	}
	copy(dst, e.metadata)
	n, err := e.columns.MarshalTo(dst[len(e.metadata):])
	return n + len(e.metadata), err
}

// UpsertViewCursor shares nullable positions and size-plan scratch across chunks.
// The source is immutable until the cursor and its synchronous encoder expire.
type UpsertViewCursor struct{ columns *InsertRequestViewCursor }

func NewUpsertViewCursor(source *msgpb.InsertRequest) (*UpsertViewCursor, error) {
	cursor, err := NewInsertRequestViewCursor(source)
	if err != nil {
		return nil, err
	}
	return &UpsertViewCursor{columns: cursor}, nil
}

// NextEncoder plans columns without encoding them. metadataPrefix contains
// cumulative row-metadata budgets. fixedBytes reserves the envelope and cipher.
// The first row is returned even when it exceeds the budget.
func (c *UpsertViewCursor) NextEncoder(rows []int, limit int, metadataPrefix []int, fixedBytes int) (*UpsertViewEncoder, int, error) {
	if len(metadataPrefix) != len(rows)+1 {
		return nil, 0, insertViewInternal("Upsert metadata budget length differs from rows")
	}
	encoder, count, err := c.columns.nextEncoder(&msgpb.InsertRequest{}, rows, 0, func(count, size int) bool {
		return limit <= 0 || size+fixedBytes+metadataPrefix[count]-metadataPrefix[0] <= limit
	})
	if err != nil {
		return nil, 0, err
	}
	encoder.size -= protowire.SizeTag(10) + protowire.SizeBytes(encoder.sizePlan[0]) + protowire.SizeTag(11) + protowire.SizeBytes(encoder.sizePlan[1]) + protowire.SizeTag(14) + protowire.SizeVarint(uint64(count))
	encoder.columnsOnly = true
	encoder.sizePlan = encoder.sizePlan[2:]
	return &UpsertViewEncoder{columns: encoder}, count, nil
}

// SetMetadata completes an accepted prefix before its single MarshalTo call.
func (e *UpsertViewEncoder) SetMetadata(metadata *messagespb.UpsertMessageBody) error {
	if len(metadata.GetColumns()) != 0 {
		return insertViewInternal("Upsert view metadata must not own columns")
	}
	payload, err := proto.Marshal(metadata)
	if err != nil {
		return err
	}
	e.metadata = payload
	return nil
}

// UpsertPartitionSizes computes the same row-and-column sizes as independent
// Upsert views. Scratch is shared, but each partition starts a new row selection.
// The source and selections must remain unchanged during this call.
func UpsertPartitionSizes(source *msgpb.InsertRequest, selections [][]int64) ([]uint64, error) {
	cursor, err := NewInsertRequestViewCursor(source)
	if err != nil {
		return nil, err
	}
	sizes := make([]uint64, len(selections))
	capacity := 0
	for _, selection := range selections {
		capacity = max(capacity, len(selection))
	}
	rows := make([]int, 0, capacity)
	rowIDs := make([]int64, 0, capacity)
	for index, selection := range selections {
		rows = rows[:0]
		rowIDs = rowIDs[:0]
		for _, row := range selection {
			rows = append(rows, int(row))
			rowIDs = append(rowIDs, source.RowIDs[row])
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
		sizes[index] = uint64(size + proto.Size(&messagespb.UpsertMessageBody{RowIds: rowIDs}))
		// No encoder escapes this size-only operation or needs a MarshalTo call.
		cursor.activeEncoder = nil
	}
	return sizes, nil
}
