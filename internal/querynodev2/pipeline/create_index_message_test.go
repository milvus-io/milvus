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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// buildCreateIndexMessageBody builds a real adaptor.CreateIndexMessageBody the way
// the WAL adaptor does, so the pipeline's filtrate/append paths see the same shape
// they see in production (header carries collectionID, body carries the FieldIndex,
// TimeTick becomes the apply barrier).
func buildCreateIndexMessageBody(t *testing.T, collectionID int64, fieldIndex *indexpb.FieldIndex, tt uint64) *adaptor.CreateIndexMessageBody {
	id := rmq.NewRmqID(1)
	mutableMsg, err := message.NewCreateIndexMessageBuilderV2().
		WithHeader(&message.CreateIndexMessageHeader{CollectionId: collectionID}).
		WithBody(&message.CreateIndexMessageBody{FieldIndex: fieldIndex}).
		WithVChannel("v1").
		BuildMutable()
	require.NoError(t, err)
	immutable := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)
	tsMsg, err := adaptor.NewCreateIndexMessageBody(immutable)
	require.NoError(t, err)
	return tsMsg.(*adaptor.CreateIndexMessageBody)
}

func TestFilterNodeCreateIndex(t *testing.T) {
	paramtable.Init()
	collectionID := int64(111)
	fieldIndex := &indexpb.FieldIndex{IndexInfo: &indexpb.IndexInfo{CollectionID: collectionID, FieldID: 101}}

	manager := &segments.Manager{
		Collection: segments.NewMockCollectionManager(t),
		Segment:    segments.NewMockSegmentManager(t),
	}
	fNode := newFilterNode(collectionID, "test-channel", manager, delegator.NewMockShardDelegator(t), 8)
	collection := segments.NewTestCollection(collectionID, querypb.LoadType_LoadCollection, nil)

	t.Run("matching collection is kept", func(t *testing.T) {
		body := buildCreateIndexMessageBody(t, collectionID, fieldIndex, uint64(100))
		require.NoError(t, fNode.filtrate(collection, body))
	})

	t.Run("mismatched collection returns error", func(t *testing.T) {
		body := buildCreateIndexMessageBody(t, collectionID+999, fieldIndex, uint64(100))
		require.Error(t, fNode.filtrate(collection, body))
	})
}

func TestInsertNodeMsgAppendCreateIndex(t *testing.T) {
	collectionID := int64(111)
	fieldIndex := &indexpb.FieldIndex{IndexInfo: &indexpb.IndexInfo{CollectionID: collectionID, FieldID: 101}}
	body := buildCreateIndexMessageBody(t, collectionID, fieldIndex, uint64(100))

	m := &insertNodeMsg{}
	require.NoError(t, m.append(body))
	require.Len(t, m.indexUpdates, 1)
	assert.EqualValues(t, 101, m.indexUpdates[0].fieldIndex.GetIndexInfo().GetFieldID())
	assert.Equal(t, uint64(100), m.indexUpdates[0].barrierTs)
}
