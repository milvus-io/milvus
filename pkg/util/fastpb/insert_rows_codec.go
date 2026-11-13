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
	"math"
	"slices"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// CompactInsertRows chooses the smaller complete RowID field encoding. It borrows
// unchanged fields and never mutates the supplied rows or its values.
func CompactInsertRows(rows *messagespb.InsertRows) *messagespb.InsertRows {
	ids := rows.GetRowIds().GetData()
	if len(ids) == 0 {
		return rows
	}
	explicitBytes, deltaBytes := 0, 0
	for index, id := range ids {
		explicitBytes += protowire.SizeVarint(uint64(id))
		if index > 0 {
			delta := id - ids[index-1]
			if delta > 0 && id < ids[index-1] || delta < 0 && id > ids[index-1] {
				return rows
			}
			deltaBytes += protowire.SizeVarint(protowire.EncodeZigZag(delta))
		}
	}
	// Explicit field 2 contains LongArray.data; delta field 5 contains base/deltas.
	explicitSize := protowire.SizeTag(2) + protowire.SizeBytes(protowire.SizeTag(1)+protowire.SizeBytes(explicitBytes))
	deltaInner := 0
	if ids[0] != 0 {
		deltaInner = protowire.SizeTag(1) + protowire.SizeVarint(uint64(ids[0]))
	}
	if len(ids) > 1 {
		deltaInner += protowire.SizeTag(2) + protowire.SizeBytes(deltaBytes)
	}
	if protowire.SizeTag(5)+protowire.SizeBytes(deltaInner) >= explicitSize {
		return rows
	}
	deltas := make([]int64, len(ids)-1)
	for index := range deltas {
		deltas[index] = ids[index+1] - ids[index]
	}
	result := &messagespb.InsertRows{PrimaryKeys: rows.PrimaryKeys, RowIdsEncoding: &messagespb.InsertRows_DeltaRowIds{DeltaRowIds: &messagespb.DeltaEncodedRowIDs{Base: ids[0], Deltas: deltas}}, PartitionIndexes: rows.PartitionIndexes, Columns: rows.Columns, RequestRowMapping: rows.RequestRowMapping}
	result.ProtoReflect().SetUnknown(rows.ProtoReflect().GetUnknown())
	return result
}

// CompactDeleteEffects explicitly references inserted keys only when both
// ordered sequences, including duplicate occurrences, are identical.
// Delete scope and independent keys are preserved. Values remain borrowed.
func CompactDeleteEffects(keys *schemapb.IDs, effects []*messagespb.UpsertDeleteEffect) []*messagespb.UpsertDeleteEffect {
	result := effects
	copied := false
	for index, effect := range effects {
		if !sameInsertKeys(keys, effect.GetPrimaryKeys()) {
			continue
		}
		if !copied {
			result = slices.Clone(effects)
			copied = true
		}
		replacement := &messagespb.UpsertDeleteEffect{Scope: effect.Scope, KeySource: &messagespb.UpsertDeleteEffect_AllInsertedKeys{AllInsertedKeys: &messagespb.UpsertAllInsertedKeys{}}}
		replacement.ProtoReflect().SetUnknown(effect.ProtoReflect().GetUnknown())
		result[index] = replacement
	}
	return result
}

func sameInsertKeys(left, right *schemapb.IDs) bool {
	switch data := left.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		values := data.IntId.GetData()
		return len(values) > 0 && slices.Equal(values, right.GetIntId().GetData())
	case *schemapb.IDs_StrId:
		values := data.StrId.GetData()
		return len(values) > 0 && slices.Equal(values, right.GetStrId().GetData())
	default:
		return false
	}
}

// PrepareInsertOrUpsertBody chooses wire encodings without copying columns or
// mutating a caller's decoded body. Mutable writeback uses the same choices as
// initial construction, so materialization does not expand compact RowIDs/keys.
func PrepareInsertOrUpsertBody(body proto.Message) proto.Message {
	switch source := body.(type) {
	case *messagespb.InsertMessageBody:
		result := &messagespb.InsertMessageBody{Rows: CompactInsertRows(source.GetRows())}
		result.ProtoReflect().SetUnknown(source.ProtoReflect().GetUnknown())
		return result
	case *messagespb.UpsertMessageBody:
		result := &messagespb.UpsertMessageBody{Rows: CompactInsertRows(source.GetRows()), DeleteEffects: CompactDeleteEffects(source.GetRows().GetPrimaryKeys(), source.GetDeleteEffects()), PartialUpdateCas: source.GetPartialUpdateCas()}
		result.ProtoReflect().SetUnknown(source.ProtoReflect().GetUnknown())
		return result
	default:
		return body
	}
}

// NormalizeInsertOrUpsertBody exposes logical RowIDs and delete keys to existing
// execution, retry and recovery callers. It modifies only newly decoded objects.
// Referenced delete keys borrow the same decoded PK values, not a second copy.
func NormalizeInsertOrUpsertBody(body proto.Message) error {
	var rows *messagespb.InsertRows
	switch source := body.(type) {
	case *messagespb.InsertMessageBody:
		rows = source.GetRows()
	case *messagespb.UpsertMessageBody:
		rows = source.GetRows()
		for _, effect := range source.GetDeleteEffects() {
			if _, ok := effect.GetKeySource().(*messagespb.UpsertDeleteEffect_AllInsertedKeys); ok {
				effect.KeySource = &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: rows.GetPrimaryKeys()}
			}
		}
	default:
		return nil
	}
	delta := rows.GetDeltaRowIds()
	if delta == nil {
		return nil
	}
	ids := make([]int64, len(delta.Deltas)+1)
	ids[0] = delta.Base
	for index, step := range delta.Deltas {
		previous := ids[index]
		if step > 0 && previous > math.MaxInt64-step || step < 0 && previous < math.MinInt64-step {
			return insertViewInternal("delta RowID reconstruction overflows int64")
		}
		ids[index+1] = previous + step
	}
	rows.RowIdsEncoding = &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: ids}}
	return nil
}
