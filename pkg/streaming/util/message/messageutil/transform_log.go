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

package messageutil

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// TransformLogKind describes how a WAL message affects TransformLog.
type TransformLogKind int

const (
	TransformLogKindNone TransformLogKind = iota
	TransformLogKindDelete
	TransformLogKindBarrier
)

// ClassifyTransformLogMessage classifies a WAL message for TransformLog processing.
func ClassifyTransformLogMessage(msg message.ImmutableMessage) TransformLogKind {
	if msg == nil {
		return TransformLogKindNone
	}
	switch msg.MessageType() {
	case message.MessageTypeDelete:
		return TransformLogKindDelete
	case message.MessageTypeTxn:
		if txnContainsDelete(msg) {
			return TransformLogKindDelete
		}
		return TransformLogKindNone
	case message.MessageTypeCreateCollection,
		message.MessageTypeRecoveryBarrier,
		message.MessageTypeFlush,
		message.MessageTypeManualFlush,
		message.MessageTypeFlushAll,
		message.MessageTypeDropPartition,
		message.MessageTypeDropCollection,
		message.MessageTypeTruncateCollection,
		message.MessageTypeAlterWAL:
		return TransformLogKindBarrier
	case message.MessageTypeAlterCollection:
		alter := message.MustAsImmutableAlterCollectionMessageV2(msg)
		if IsSchemaChange(alter.Header()) {
			return TransformLogKindBarrier
		}
		return TransformLogKindNone
	default:
		return TransformLogKindNone
	}
}

func txnContainsDelete(msg message.ImmutableMessage) bool {
	txn := message.AsImmutableTxnMessage(msg)
	if txn == nil {
		return false
	}
	contains := false
	_ = txn.RangeOver(func(inner message.ImmutableMessage) error {
		if inner.MessageType() == message.MessageTypeDelete {
			contains = true
		}
		return nil
	})
	return contains
}

// TransformEntryOption carries the per-append options for building a transform
// log entry from a WAL message.
type TransformEntryOption struct {
	// DeleteFilter decides whether a delete block is accepted into the entry.
	// A nil filter accepts every delete.
	DeleteFilter func(partitionID int64, timeTick uint64) bool
}

func (o TransformEntryOption) acceptDelete(partitionID int64, timeTick uint64) bool {
	if o.DeleteFilter == nil {
		return true
	}
	return o.DeleteFilter(partitionID, timeTick)
}

// BuildTransformLogEntry converts a WAL message into its transform log entry.
// It returns nil when the message carries no transform payload, such as an
// insert, a payload-free barrier, or a delete filtered out by the option.
//
// A committed Txn containing Delete produces one entry at the outer Txn
// TimeTick, holding the Delete blocks of every Delete child.
func BuildTransformLogEntry(msg message.ImmutableMessage, opt TransformEntryOption) *streamingpb.TransformLogEntry {
	if ClassifyTransformLogMessage(msg) != TransformLogKindDelete {
		return nil
	}
	switch msg.MessageType() {
	case message.MessageTypeDelete:
		deleted := message.MustAsImmutableDeleteMessageV1(msg)
		return transformEntryFromDeletes(msg.TimeTick(), []message.ImmutableDeleteMessageV1{deleted}, opt)
	case message.MessageTypeTxn:
		txn := message.AsImmutableTxnMessage(msg)
		deletes := make([]message.ImmutableDeleteMessageV1, 0)
		_ = txn.RangeOver(func(im message.ImmutableMessage) error {
			if im.MessageType() == message.MessageTypeDelete {
				deletes = append(deletes, message.MustAsImmutableDeleteMessageV1(im))
			}
			return nil
		})
		return transformEntryFromDeletes(msg.TimeTick(), deletes, opt)
	default:
		return nil
	}
}

func transformEntryFromDeletes(timeTick uint64, deletes []message.ImmutableDeleteMessageV1, opt TransformEntryOption) *streamingpb.TransformLogEntry {
	blocks := make([]*streamingpb.TransformDeleteBlock, 0, len(deletes))
	for _, deleted := range deletes {
		request := cloneDeleteRequest(deleted.MustBody())
		if request == nil {
			continue
		}
		if !opt.acceptDelete(request.GetPartitionID(), timeTick) {
			continue
		}
		blocks = append(blocks, &streamingpb.TransformDeleteBlock{
			PartitionId: request.GetPartitionID(),
			PrimaryKeys: request.GetPrimaryKeys(),
		})
	}
	if len(blocks) == 0 {
		return nil
	}
	return &streamingpb.TransformLogEntry{
		TimeTick: timeTick,
		Entry: &streamingpb.TransformLogEntry_Delete{
			Delete: &streamingpb.TransformDeleteEntry{
				Blocks: blocks,
			},
		},
	}
}

// PrimaryKeyCount counts the primary keys of a DeleteRequest.
func PrimaryKeyCount(ids *schemapb.IDs) int {
	if ids == nil {
		return 0
	}
	switch ids.IdField.(type) {
	case *schemapb.IDs_IntId:
		return len(ids.GetIntId().GetData())
	case *schemapb.IDs_StrId:
		return len(ids.GetStrId().GetData())
	default:
		return 0
	}
}

func cloneDeleteRequest(value *msgpb.DeleteRequest) *msgpb.DeleteRequest {
	if value == nil {
		return nil
	}
	return proto.Clone(value).(*msgpb.DeleteRequest)
}
