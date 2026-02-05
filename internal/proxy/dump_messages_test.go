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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestShouldDumpMessage(t *testing.T) {
	// Messages that SHOULD be dumped (replicable data)
	assert.True(t, shouldDumpMessage(message.MessageTypeInsert), "Insert should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeDelete), "Delete should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeCreateCollection), "CreateCollection should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeDropCollection), "DropCollection should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeCreatePartition), "CreatePartition should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeDropPartition), "DropPartition should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeBeginTxn), "BeginTxn should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeCommitTxn), "CommitTxn should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeTxn), "Txn should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeImport), "Import should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeManualFlush), "ManualFlush should be dumped")

	// Messages that should NOT be dumped (system messages)
	assert.False(t, shouldDumpMessage(message.MessageTypeTimeTick), "TimeTick should NOT be dumped")
	assert.False(t, shouldDumpMessage(message.MessageTypeCreateSegment), "CreateSegment should NOT be dumped")
	assert.False(t, shouldDumpMessage(message.MessageTypeFlush), "Flush should NOT be dumped")
	assert.False(t, shouldDumpMessage(message.MessageTypeRollbackTxn), "RollbackTxn should NOT be dumped")
}
