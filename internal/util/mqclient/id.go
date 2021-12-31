// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package mqclient

// MessageID is the interface that provides operations of message is
type MessageID interface {
	// Serialize the message id into a sequence of bytes that can be stored somewhere else
	Serialize() []byte

	// Get the message ledgerID
	LedgerID() int64

	// Get the message entryID
	EntryID() int64

	// Get the message batchIdx
	BatchIdx() int32

	// Get the message partitionIdx
	PartitionIdx() int32
}
