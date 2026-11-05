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
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ValidateUpsertEnvelope checks scope markers for Upsert and its transaction members.
// Legacy-only transactions do not use this contract.
func ValidateUpsertEnvelope(msg BasicMessage) error {
	if msg == nil || !msg.IsPersisted() {
		return merr.WrapErrServiceInternalMsg("Upsert protocol requires persisted messages")
	}
	if msg.IsPChannelLevel() || msg.Properties().Exist(messageBroadcastHeader) {
		return merr.WrapErrServiceInternalMsg("Upsert protocol requires a single-VChannel envelope without broadcast metadata")
	}
	if msg.IsUnreplicable() {
		return merr.WrapErrServiceInternalMsg("Upsert WAL messages must be replicable")
	}
	return nil
}

// ValidateUpsert checks one explicit delete-and-insert write unit against its primary key schema.
// Nested timestamps, shard names and segment IDs are not authoritative.
// Producers must supply valid canonical fields, as required for Insert.
// Callers must reject namespace-enabled schemas and outer partial-update CAS markers.
func ValidateUpsert(header *UpsertMessageHeader, body *UpsertMessageBody, primaryKey *schemapb.FieldSchema) error {
	invalid := func(reason string) error {
		return merr.WrapErrServiceInternalMsg("invalid Upsert WAL message: %s", reason)
	}
	if header == nil || header.GetPartition() == nil || header.SchemaVersion == nil || body.GetInsert() == nil || body.GetDelete() == nil {
		return invalid("missing header, assignment, insert or delete")
	}
	insert, deletion, partition := body.GetInsert(), body.GetDelete(), header.GetPartition()
	if header.GetCollectionId() <= 0 || insert.GetCollectionID() != header.GetCollectionId() || deletion.GetCollectionID() != header.GetCollectionId() {
		return invalid("collection identity differs")
	}
	if partition.GetPartitionId() <= 0 || insert.GetPartitionID() != partition.GetPartitionId() || deletion.GetPartitionID() != -1 {
		return invalid("invalid partition scope")
	}
	if insert.GetNumRows() == 0 || partition.GetRows() != insert.GetNumRows() || uint64(len(insert.GetRowIDs())) != insert.GetNumRows() {
		return invalid("insert row count differs")
	}
	if _, exists := insert.GetBase().GetProperties()[messagePartialUpdateCAS]; exists {
		return invalid("partial update CAS is not supported")
	}
	if insert.GetVersion() != msgpb.InsertDataVersion_ColumnBased || insert.Namespace != nil || len(insert.GetRowData()) != 0 || len(deletion.GetSerializedExprPlan()) != 0 || len(deletion.GetInt64PrimaryKeys()) != 0 {
		return invalid("unsupported namespace, row format or delete expression")
	}
	if deletion.GetNumRows() <= 0 || uint64(deletion.GetNumRows()) != header.GetDeleteRows() || primaryKey == nil || !primaryKey.GetIsPrimaryKey() {
		return invalid("invalid delete count or primary key schema")
	}
	var field *schemapb.FieldData
	for _, candidate := range insert.GetFieldsData() {
		if candidate.GetFieldId() == primaryKey.GetFieldID() {
			if field != nil {
				return invalid("duplicate primary key field")
			}
			field = candidate
		}
	}
	if field == nil || field.GetType() != primaryKey.GetDataType() {
		return invalid("missing or mistyped primary key field")
	}
	switch primaryKey.GetDataType() {
	case schemapb.DataType_Int64:
		values := field.GetScalars().GetLongData().GetData()
		keys, ok := deletion.GetPrimaryKeys().GetIdField().(*schemapb.IDs_IntId)
		if !ok || uint64(len(values)) != insert.GetNumRows() || int64(len(keys.IntId.GetData())) != deletion.GetNumRows() || !sameUpsertKeys(values, keys.IntId.GetData()) {
			return invalid("delete keys differ from distinct inserted primary keys")
		}
	case schemapb.DataType_VarChar:
		values := field.GetScalars().GetStringData().GetData()
		keys, ok := deletion.GetPrimaryKeys().GetIdField().(*schemapb.IDs_StrId)
		if !ok || uint64(len(values)) != insert.GetNumRows() || int64(len(keys.StrId.GetData())) != deletion.GetNumRows() || !sameUpsertKeys(values, keys.StrId.GetData()) {
			return invalid("delete keys differ from distinct inserted primary keys")
		}
	default:
		return invalid("unsupported primary key type")
	}
	if result := header.GetIdempotentResult(); result != nil {
		if uint64(len(result.GetRowOffsets())) != insert.GetNumRows() {
			return invalid("append result row count differs")
		}
		switch primaryKey.GetDataType() {
		case schemapb.DataType_Int64:
			keys, ok := result.GetIds().GetIdField().(*schemapb.IDs_IntId)
			if !ok || !equalUpsertKeys(field.GetScalars().GetLongData().GetData(), keys.IntId.GetData()) {
				return invalid("append result primary keys differ")
			}
		case schemapb.DataType_VarChar:
			keys, ok := result.GetIds().GetIdField().(*schemapb.IDs_StrId)
			if !ok || !equalUpsertKeys(field.GetScalars().GetStringData().GetData(), keys.StrId.GetData()) {
				return invalid("append result primary keys differ")
			}
		}
	}
	return nil
}

func sameUpsertKeys[T comparable](inserted, deleted []T) bool {
	keys := make(map[T]struct{}, len(inserted))
	for _, key := range inserted {
		keys[key] = struct{}{}
	}
	seen := make(map[T]struct{}, len(deleted))
	for _, key := range deleted {
		if _, ok := keys[key]; !ok {
			return false
		}
		seen[key] = struct{}{}
	}
	return len(keys) == len(seen)
}

func equalUpsertKeys[T comparable](left, right []T) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
