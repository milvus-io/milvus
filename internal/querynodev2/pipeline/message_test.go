// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestInsertNodeMsgAppendCollectionIdentityUpdate(t *testing.T) {
	mutable := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: 1,
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{
				message.FieldMaskDB,
				message.FieldMaskCollectionName,
			}},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{
				DbName:         "new_db",
				CollectionName: "new_collection",
			},
		}).
		WithVChannel("v1").
		MustBuildMutable()
	immutable := mutable.WithTimeTick(100).IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	taskMsg, err := adaptor.NewAlterCollectionMessageBody(immutable)
	require.NoError(t, err)

	msg := &insertNodeMsg{}
	require.NoError(t, msg.append(taskMsg))
	require.Equal(t, uint64(100), msg.schemaBarrierTs)
	require.Equal(t, collectionIdentityUpdate{
		dbName:               "new_db",
		collectionName:       "new_collection",
		updateDBName:         true,
		updateCollectionName: true,
	}, msg.identityUpdate)
}
