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

package shard

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestShardRestoreSnapshotCreation(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		legacy, incomplete, appendFailure bool
	}{
		{name: "new collection"}, {name: "legacy", legacy: true}, {name: "incomplete", incomplete: true}, {name: "append failed", appendFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				var created message.ImmutableCreateCollectionMessageV1
				mockey.Mock((*mock_shards.MockShardManager).CreateCollection).To(func(_ *mock_shards.MockShardManager, msg message.ImmutableCreateCollectionMessageV1) { created = msg }).Build()
				var prepared *schemapb.CollectionSchema
				mockey.Mock((*shardInterceptor).allocFunctionRunners).To(func(_ *shardInterceptor, id int64, channel string, schema *schemapb.CollectionSchema) {
					require.EqualValues(t, 10, id)
					require.Equal(t, "target_v1", channel)
					prepared = proto.Clone(schema).(*schemapb.CollectionSchema)
				}).Build()
				impl := &shardInterceptor{shardManager: &mock_shards.MockShardManager{}}
				body := &message.RestoreSnapshotMessageBody{}
				if !tc.legacy {
					body.Collection = &message.CreateCollectionRequest{CollectionID: 10, CollectionSchema: &schemapb.CollectionSchema{Name: "restored"}}
				}
				if !tc.legacy && !tc.incomplete {
					body.CollectionHeader = &message.CreateCollectionMessageHeader{CollectionId: 10}
				}
				msg := message.NewRestoreSnapshotMessageBuilderV2().WithVChannel("target_v1").WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 10}).WithBody(body).MustBuildMutable().WithTimeTick(123).WithLastConfirmed(walimplstest.NewTestMessageID(7))
				appended := false
				_, err := impl.handleRestoreSnapshot(t.Context(), msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
					appended = true
					if tc.appendFailure {
						return nil, merr.ErrServiceUnavailable
					}
					return walimplstest.NewTestMessageID(8), nil
				})
				switch {
				case tc.incomplete:
					require.Error(t, err)
					require.False(t, appended)
				case tc.appendFailure:
					require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				default:
					require.NoError(t, err)
					require.True(t, appended)
				}
				if tc.legacy || tc.incomplete || tc.appendFailure {
					require.Nil(t, created)
					require.Nil(t, prepared)
					return
				}
				require.NotNil(t, created)
				require.EqualValues(t, 123, created.TimeTick())
				require.Equal(t, "restored", prepared.Name)
			})
		})
	}
}
