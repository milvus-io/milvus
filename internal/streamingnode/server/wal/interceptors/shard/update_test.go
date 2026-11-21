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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestPKUpdateAppendValidation(t *testing.T) {
	for _, name := range []string{"missing_version", "wrong_collection", "schema_mismatch", "missing_collection", "invalid_partition", "missing_partition", "malformed_body", "canceled", "valid"} {
		t.Run(name, func(t *testing.T) {
			manager := mock_shards.NewMockShardManager(t)
			impl := &shardInterceptor{shardManager: manager}
			impl.initOpTable()
			header := &messagespb.UpdateMessageHeader{CollectionId: 10, SchemaVersion: proto.Int32(7)}
			body := &messagespb.UpdateMessageBody{PartitionId: -1}
			switch name {
			case "missing_version":
				header.SchemaVersion = nil
			case "wrong_collection":
				header.CollectionId = 11
			case "schema_mismatch":
				manager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(8), shards.ErrCollectionSchemaVersionNotMatch)
			case "missing_collection":
				manager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(0), shards.ErrCollectionNotFound)
			default:
				manager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.MatchedBy(func(h *message.InsertMessageHeader) bool {
					return h.GetCollectionId() == 10 && h.SchemaVersion != nil && h.GetSchemaVersion() == 7
				})).Return(int32(7), nil)
				if name == "invalid_partition" {
					body.PartitionId = 0
				}
				if name == "missing_partition" {
					body.PartitionId = 20
					manager.EXPECT().CheckIfPartitionExists(shards.PartitionUniqueKey{CollectionID: 10, PartitionID: 20}).Return(shards.ErrPartitionNotFound)
				}
			}
			msg := message.NewUpdateMessageBuilderV2().WithVChannel("p_10v0").WithHeader(header).WithBody(body).MustBuildMutable()
			ctx := t.Context()
			if name == "malformed_body" {
				msg = message.NewMutableMessageBeforeAppend([]byte{0xff}, msg.IntoMessageProto().GetProperties())
			}
			if name == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			appended := false
			id, err := impl.DoAppend(ctx, msg, func(context.Context, message.MutableMessage) (message.MessageID, error) {
				appended = true
				return nil, nil
			})
			require.Equal(t, name == "valid", appended)
			if name == "valid" {
				require.NoError(t, err)
				return
			}
			require.Nil(t, id)
			require.Error(t, err)
			if name == "schema_mismatch" {
				require.True(t, status.AsStreamingError(err).IsSchemaVersionMismatch())
			}
			if name == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
			}
			if name == "malformed_body" {
				require.ErrorContains(t, err, "invalid UPDATE payload")
			}
		})
	}
}
