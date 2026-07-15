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

import "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

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
