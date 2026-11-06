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

package message

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ValidateUpsertEnvelope checks scope markers for a persisted single-channel write.
func ValidateUpsertEnvelope(msg BasicMessage) error {
	if msg == nil || !msg.IsPersisted() || msg.IsPChannelLevel() || msg.Properties().Exist(messageBroadcastHeader) || msg.IsUnreplicable() {
		return merr.WrapErrServiceInternalMsg("Upsert requires a persisted, replicable single-VChannel envelope")
	}
	return nil
}

// UpsertRowCount returns the number of inserted logical rows.
func UpsertRowCount(header *UpsertMessageHeader) uint64 {
	var rows uint64
	for _, p := range header.GetPartitions() {
		rows += p.GetRows()
	}
	return rows
}

// UpsertPrimaryKeyCount returns the number of keys without a schema lookup.
func UpsertPrimaryKeyCount(ids *schemapb.IDs) int {
	return len(ids.GetIntId().GetData()) + len(ids.GetStrId().GetData())
}

// UpsertSelections validates placement and returns logical rows per partition.
// Each selection preserves source order and is shared by every column.
func UpsertSelections(header *UpsertMessageHeader, body *UpsertMessageBody) ([][]int64, error) {
	invalid := func() ([][]int64, error) { return nil, merr.WrapErrServiceInternalMsg("invalid Upsert row placement") }
	rows := UpsertPrimaryKeyCount(body.GetPrimaryKeys())
	partitions := header.GetPartitions()
	if rows == 0 || len(partitions) == 0 || len(body.GetRowIds()) != rows {
		return invalid()
	}
	result := make([][]int64, len(partitions))
	seen := make(map[int64]struct{}, len(partitions))
	for _, p := range partitions {
		if p.GetPartitionId() <= 0 || p.GetRows() == 0 {
			return invalid()
		}
		if _, ok := seen[p.GetPartitionId()]; ok {
			return invalid()
		}
		seen[p.GetPartitionId()] = struct{}{}
	}
	indexes := body.GetPartitionIndexes()
	if (len(partitions) == 1 && len(indexes) != 0) || (len(partitions) > 1 && len(indexes) != rows) {
		return invalid()
	}
	for row := 0; row < rows; row++ {
		index := uint32(0)
		if len(indexes) > 0 {
			index = indexes[row]
		}
		if uint64(index) >= uint64(len(partitions)) {
			return invalid()
		}
		result[index] = append(result[index], int64(row))
	}
	for i, p := range partitions {
		if uint64(len(result[i])) != p.GetRows() {
			return invalid()
		}
	}
	return result, nil
}

// ValidateUpsert checks the compound contract. Producers validate column values.
// Projections contain column descriptors without their values.
func ValidateUpsert(header *UpsertMessageHeader, body *UpsertMessageBody, primary *schemapb.FieldSchema) error {
	_, err := ValidateUpsertWithSelections(header, body, primary)
	return err
}

// ValidateUpsertWithSelections validates the compound contract and returns its
// row selections. Callers must not change row placement after validation.
func ValidateUpsertWithSelections(header *UpsertMessageHeader, body *UpsertMessageBody, primary *schemapb.FieldSchema) ([][]int64, error) {
	invalid := func(reason string) error { return merr.WrapErrServiceInternalMsg("invalid Upsert: %s", reason) }
	if header.GetCollectionId() <= 0 || header.SchemaVersion == nil || primary == nil || !primary.GetIsPrimaryKey() {
		return nil, invalid("missing collection, schema version or primary key schema")
	}
	selections, err := UpsertSelections(header, body)
	if err != nil {
		return nil, err
	}
	validKeys := func(ids *schemapb.IDs) bool {
		switch primary.GetDataType() {
		case schemapb.DataType_Int64:
			_, ok := ids.GetIdField().(*schemapb.IDs_IntId)
			return ok
		case schemapb.DataType_VarChar:
			_, ok := ids.GetIdField().(*schemapb.IDs_StrId)
			return ok
		}
		return false
	}
	if !validKeys(body.GetPrimaryKeys()) {
		return nil, invalid("primary key type differs from schema")
	}
	seen := map[int64]struct{}{}
	for _, column := range body.GetColumns() {
		id := column.GetFieldId()
		if id <= 1 || id == primary.GetFieldID() {
			return nil, invalid("columns contain a primary key or system field")
		}
		if _, ok := seen[id]; ok {
			return nil, invalid("duplicate column")
		}
		seen[id] = struct{}{}
	}
	var deleted uint64
	for _, effect := range body.GetDeleteEffects() {
		if !validKeys(effect.GetPrimaryKeys()) || UpsertPrimaryKeyCount(effect.GetPrimaryKeys()) == 0 {
			return nil, invalid("empty or mistyped delete keys")
		}
		switch scope := effect.GetScope().(type) {
		case *messagespb.UpsertDeleteEffect_AllPartitions:
			if scope.AllPartitions == nil {
				return nil, invalid("missing delete scope")
			}
		case *messagespb.UpsertDeleteEffect_PartitionId:
			if scope.PartitionId <= 0 {
				return nil, invalid("invalid delete partition")
			}
		default:
			return nil, invalid("missing delete scope")
		}
		deleted += uint64(UpsertPrimaryKeyCount(effect.GetPrimaryKeys()))
	}
	if deleted != header.GetDeleteRows() {
		return nil, invalid("delete count differs from effects")
	}
	if mapping := body.GetRequestRowMapping(); mapping != nil {
		if len(mapping.GetRowOffsets()) != UpsertPrimaryKeyCount(body.GetPrimaryKeys()) {
			return nil, invalid("request row mapping count differs")
		}
		if err := ValidateIdempotentInsertResult(&messagespb.IdempotentInsertResult{Ids: body.GetPrimaryKeys(), RowOffsets: mapping.GetRowOffsets()}); err != nil {
			return nil, err
		}
	}
	if cas := body.GetPartialUpdateCas(); cas != nil {
		if err := validatePartialUpdateCAS(cas); err != nil {
			return nil, err
		}
	}
	return selections, nil
}

// BuildUpsertResult constructs the insert result from original keys and row offsets
// without decoding columns. Append, transaction buffering, recovery, and retry share it.
func BuildUpsertResult(ctx context.Context, msg BasicMessage) (*messagespb.IdempotentInsertResult, error) {
	body, err := ReadUpsertProjection(ctx, msg)
	if err != nil {
		return nil, err
	}
	if body.GetRequestRowMapping() == nil {
		return nil, nil
	}
	result := &messagespb.IdempotentInsertResult{Ids: body.GetPrimaryKeys(), RowOffsets: body.GetRequestRowMapping().GetRowOffsets()}
	if err := ValidateIdempotentInsertResult(result); err != nil {
		return nil, err
	}
	return result, nil
}

// UpsertPrimaryKeyColumn borrows validated row keys for function and storage
// interfaces that require a FieldData column. The caller must not modify keys.
func UpsertPrimaryKeyColumn(primary *schemapb.FieldSchema, keys *schemapb.IDs) *schemapb.FieldData {
	column := &schemapb.FieldData{FieldId: primary.GetFieldID(), FieldName: primary.GetName(), Type: primary.GetDataType()}
	if primary.GetDataType() == schemapb.DataType_Int64 {
		column.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: keys.GetIntId()}}}
	} else {
		column.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: keys.GetStrId()}}}
	}
	return column
}

// ValidateUpsertSchema rejects collections whose namespace routing cannot use
// the Upsert protocol. This check also applies to replicated appends.
func ValidateUpsertSchema(schema *schemapb.CollectionSchema) error {
	if schema.GetEnableNamespace() {
		return merr.WrapErrParameterInvalidMsg("Upsert WAL does not support namespace collections")
	}
	return nil
}
