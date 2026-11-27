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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// TransformLogKind describes how a WAL message affects TransformLog.
type TransformLogKind int

const (
	TransformLogKindNone TransformLogKind = iota
	TransformLogKindBarrier
	TransformLogKindMutation
)

func (k TransformLogKind) HasPayload() bool {
	return k == TransformLogKindMutation
}

// ClassifyTransformLogMessage classifies a WAL message for TransformLog processing.
func ClassifyTransformLogMessage(msg message.ImmutableMessage) TransformLogKind {
	if msg == nil {
		return TransformLogKindNone
	}
	switch msg.MessageType() {
	case message.MessageTypeDelete, message.MessageTypeUpdate:
		return TransformLogKindMutation
	case message.MessageTypeInsert:
		return TransformLogKindNone
	case message.MessageTypeTxn:
		return classifyTransformTxn(message.AsImmutableTxnMessage(msg))
	default:
		return TransformLogKindBarrier
	}
}

func classifyTransformTxn(txn message.ImmutableTxnMessage) TransformLogKind {
	if txn == nil || txn.Size() == 0 {
		return TransformLogKindNone
	}
	kind := TransformLogKindNone
	_ = txn.RangeOver(func(inner message.ImmutableMessage) error {
		switch inner.MessageType() {
		case message.MessageTypeDelete, message.MessageTypeUpdate:
			kind = TransformLogKindMutation
		case message.MessageTypeInsert:
		default:
			if !kind.HasPayload() {
				kind = TransformLogKindBarrier
			}
		}
		return nil
	})
	return kind
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

// BuildTransformLogEntry copies committed deletes and updates in source order.
func BuildTransformLogEntry(msg message.ImmutableMessage, opt TransformEntryOption) *streamingpb.TransformLogEntry {
	kind := ClassifyTransformLogMessage(msg)
	if kind == TransformLogKindNone {
		return nil
	}
	entry := &streamingpb.TransformLogEntry{TimeTick: msg.TimeTick()}
	if !kind.HasPayload() {
		return entry
	}
	var mutations streamingpb.TransformMutationEntry
	appendBody := func(body message.ImmutableMessage) error {
		block := &streamingpb.TransformMutationBlock{}
		switch body.MessageType() {
		case message.MessageTypeDelete:
			request := message.MustAsImmutableDeleteMessageV1(body).MustBody()
			if !opt.acceptDelete(request.GetPartitionID(), entry.TimeTick) {
				return nil
			}
			deleted := &streamingpb.TransformDeleteBlock{
				PartitionId: request.GetPartitionID(), PrimaryKeys: proto.Clone(request.GetPrimaryKeys()).(*schemapb.IDs),
			}
			block.Mutation = &streamingpb.TransformMutationBlock_Delete{Delete: deleted}
		case message.MessageTypeUpdate:
			update := message.MustAsImmutableUpdateMessageV2(body)
			block.PartitionId = update.Header().GetPartitionId()
			block.Mutation = &streamingpb.TransformMutationBlock_Update{Update: proto.Clone(update.MustBody()).(*messagespb.UpdateMessageBody)}
			if version := update.Header().SchemaVersion; version != nil {
				block.SchemaVersion = proto.Int32(*version)
			}
		default:
			return nil
		}
		mutations.Blocks = append(mutations.Blocks, block)
		return nil
	}
	if msg.MessageType() == message.MessageTypeTxn {
		_ = message.AsImmutableTxnMessage(msg).RangeOver(appendBody)
	} else {
		_ = appendBody(msg)
	}
	if len(mutations.Blocks) == 0 {
		return nil
	}
	entry.Entry = &streamingpb.TransformLogEntry_Mutations{Mutations: &mutations}
	return entry
}

// MutationsFromLegacyDeletes converts a persisted or received delete-only payload.
func MutationsFromLegacyDeletes(deletes *streamingpb.TransformDeleteEntry) *streamingpb.TransformMutationEntry {
	blocks := make([]*streamingpb.TransformMutationBlock, 0, len(deletes.GetBlocks()))
	for _, d := range deletes.GetBlocks() {
		blocks = append(blocks, &streamingpb.TransformMutationBlock{
			Mutation: &streamingpb.TransformMutationBlock_Delete{Delete: d},
		})
	}
	return &streamingpb.TransformMutationEntry{Blocks: blocks}
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
