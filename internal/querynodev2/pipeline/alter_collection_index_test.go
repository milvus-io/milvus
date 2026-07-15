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
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// buildAlterCollectionSchemaMessageBody builds a real adaptor.AlterCollectionMessageBody
// the way the WAL adaptor delivers an add-field DDL to a single vchannel: the header's
// UpdateMask marks it a schema change and the body's updates carry both the schema and
// the index metas bound to the newly added fields (bound_field_indexes).
func buildAlterCollectionSchemaMessageBody(t *testing.T, collectionID int64, schema *schemapb.CollectionSchema, boundIndexes []*indexpb.FieldIndex, tt uint64) *adaptor.AlterCollectionMessageBody {
	id := rmq.NewRmqID(1)
	mutableMsg, err := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: collectionID,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{
				Schema:            schema,
				BoundFieldIndexes: boundIndexes,
			},
		}).
		WithVChannel("v1").
		BuildMutable()
	require.NoError(t, err)
	immutable := mutableMsg.WithTimeTick(tt).WithLastConfirmedUseMessageID().IntoImmutableMessage(id)
	tsMsg, err := adaptor.NewAlterCollectionMessageBody(immutable)
	require.NoError(t, err)
	return tsMsg.(*adaptor.AlterCollectionMessageBody)
}

// TestInsertNodeMsgAppendBoundFieldIndexes verifies the pipeline reads the index metas
// bound to an add-field DDL off the same AlterCollection message into a single indexUpdate
// carrying every bound field, with the DDL BeginTs as the shared apply barrier.
func TestInsertNodeMsgAppendBoundFieldIndexes(t *testing.T) {
	paramtable.Init()
	collectionID := int64(111)
	schema := &schemapb.CollectionSchema{Name: "coll", Version: 2}
	boundIndexes := []*indexpb.FieldIndex{
		{IndexInfo: &indexpb.IndexInfo{CollectionID: collectionID, FieldID: 101}},
		{IndexInfo: &indexpb.IndexInfo{CollectionID: collectionID, FieldID: 102}},
	}
	body := buildAlterCollectionSchemaMessageBody(t, collectionID, schema, boundIndexes, uint64(100))

	m := &insertNodeMsg{}
	require.NoError(t, m.append(body))

	// schema is applied and all bound indexes of the DDL become one indexUpdate at the barrier.
	assert.NotNil(t, m.schema)
	assert.Equal(t, uint64(100), m.schemaBarrierTs)
	require.Len(t, m.indexUpdates, 1)
	assert.Equal(t, uint64(100), m.indexUpdates[0].barrierTs)
	require.Len(t, m.indexUpdates[0].fieldIndexes, 2)
	assert.EqualValues(t, 101, m.indexUpdates[0].fieldIndexes[0].GetIndexInfo().GetFieldID())
	assert.EqualValues(t, 102, m.indexUpdates[0].fieldIndexes[1].GetIndexInfo().GetFieldID())
}

// TestInsertNodeMsgAppendNoBoundIndexes: a schema-change DDL without bound indexes (a
// plain field/property change) applies the schema but adds no index updates.
func TestInsertNodeMsgAppendNoBoundIndexes(t *testing.T) {
	paramtable.Init()
	collectionID := int64(111)
	schema := &schemapb.CollectionSchema{Name: "coll", Version: 3}
	body := buildAlterCollectionSchemaMessageBody(t, collectionID, schema, nil, uint64(200))

	m := &insertNodeMsg{}
	require.NoError(t, m.append(body))
	assert.NotNil(t, m.schema)
	assert.Empty(t, m.indexUpdates)
}
