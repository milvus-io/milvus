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

package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/storage"
)

func TestStreaming_retrieve(t *testing.T) {
	streaming, err := genSimpleReplicaWithGrowingSegment()
	assert.NoError(t, err)

	collection, err := streaming.getCollectionByID(defaultCollectionID)
	assert.NoError(t, err)
	plan, err := genSimpleRetrievePlan(collection)
	assert.NoError(t, err)

	insertMsg, err := genSimpleInsertMsg(collection.schema, defaultMsgLength)
	assert.NoError(t, err)

	segment, err := streaming.getSegmentByID(defaultSegmentID, segmentTypeGrowing)
	assert.NoError(t, err)

	offset, err := segment.segmentPreInsert(len(insertMsg.RowIDs))
	assert.NoError(t, err)

	insertRecord, err := storage.TransferInsertMsgToInsertRecord(collection.schema, insertMsg)
	assert.NoError(t, err)

	err = segment.segmentInsert(offset, insertMsg.RowIDs, insertMsg.Timestamps, collection.schema.GetFields(), insertRecord)
	assert.NoError(t, err)

	t.Run("test retrieve", func(t *testing.T) {
		res, _, ids, err := retrieveStreaming(streaming, plan,
			defaultCollectionID,
			[]UniqueID{defaultPartitionID},
			defaultDMLChannel,
			nil)
		assert.NoError(t, err)
		assert.Len(t, res, 1)
		assert.Len(t, ids, 1)
	})

	t.Run("test empty partition", func(t *testing.T) {

		res, _, ids, err := retrieveStreaming(streaming, plan,
			defaultCollectionID,
			nil,
			defaultDMLChannel,
			nil)
		assert.NoError(t, err)
		assert.Len(t, res, 1)
		assert.Len(t, ids, 1)
	})

}
