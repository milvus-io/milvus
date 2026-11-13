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
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// InsertOrUpsertHeader contains the common append and placement facts of Insert V2
// and Upsert V2. It borrows the decoded partition entries.
type InsertOrUpsertHeader struct {
	CollectionId  int64
	Partitions    []*PartitionSegmentAssignment
	SchemaVersion *int32
	DeleteRows    uint64
}

func (h *InsertOrUpsertHeader) GetCollectionId() int64                       { return h.CollectionId }
func (h *InsertOrUpsertHeader) GetPartitions() []*PartitionSegmentAssignment { return h.Partitions }
func (h *InsertOrUpsertHeader) GetSchemaVersion() int32 {
	if h.SchemaVersion != nil {
		return *h.SchemaVersion
	}
	return 0
}
func (h *InsertOrUpsertHeader) GetDeleteRows() uint64 { return h.DeleteRows }

// IsInsertOrUpsertMessage identifies the nested row format, independently of API kind.
func IsInsertOrUpsertMessage(msg BasicMessage) bool {
	return msg.Version() == VersionV2 && (msg.MessageType() == MessageTypeInsert || msg.MessageType() == MessageTypeUpsert)
}

// ReadInsertOrUpsertHeader decodes the concrete header without decoding the body.
func ReadInsertOrUpsertHeader(msg BasicMessage) (*InsertOrUpsertHeader, error) {
	if !IsInsertOrUpsertMessage(msg) {
		return nil, merr.WrapErrServiceInternalMsg("expected an Insert V2 or Upsert V2 message")
	}
	encoded, ok := msg.Properties().Get(messageHeader)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("row-write message has no header")
	}
	switch msg.MessageType() {
	case MessageTypeInsert:
		header := &InsertMessageHeaderV2{}
		if err := DecodeProto(encoded, header); err != nil {
			return nil, err
		}
		return &InsertOrUpsertHeader{CollectionId: header.CollectionId, Partitions: header.Partitions, SchemaVersion: header.SchemaVersion}, nil
	case MessageTypeUpsert:
		header := &UpsertMessageHeader{}
		if err := DecodeProto(encoded, header); err != nil {
			return nil, err
		}
		return &InsertOrUpsertHeader{CollectionId: header.CollectionId, Partitions: header.Partitions, SchemaVersion: header.SchemaVersion, DeleteRows: header.DeleteRows}, nil
	default:
		panic("unexpected row-write message type")
	}
}
