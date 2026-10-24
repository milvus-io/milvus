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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRestoreCollectionKeepsWALOrigin(t *testing.T) {
	for _, creates := range []bool{false, true} {
		body := &message.RestoreSnapshotMessageBody{}
		if creates {
			body.CollectionHeader = &message.CreateCollectionMessageHeader{CollectionId: 10, PartitionIds: []int64{11, 12}}
			body.Collection = &message.CreateCollectionRequest{CollectionID: 10, CollectionSchema: &schemapb.CollectionSchema{Name: "restored"}, PartitionIDs: []int64{11, 12}}
		}
		msg := message.NewRestoreSnapshotMessageBuilderV2().WithVChannel("target_v1").
			WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 10}).WithBody(body).MustBuildMutable().
			WithTimeTick(123).WithLastConfirmed(walimplstest.NewTestMessageID(7)).IntoImmutableMessage(walimplstest.NewTestMessageID(8))
		creation := RestoreCollectionMessage(msg)
		if !creates {
			require.Nil(t, creation)
			continue
		}
		require.Equal(t, uint64(123), creation.TimeTick())
		require.Equal(t, msg.MessageID(), creation.MessageID())
		require.Equal(t, msg.LastConfirmedMessageID(), creation.LastConfirmedMessageID())
		require.Equal(t, "target_v1", creation.VChannel())
		require.Equal(t, []int64{11, 12}, creation.Header().GetPartitionIds())
		require.Equal(t, "restored", creation.MustBody().GetCollectionSchema().GetName())
	}
}
