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

package recovery

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestReplicatedCollectionCreationPreservesSchemaAfterRecovery(t *testing.T) {
	for _, version := range []int32{0, 7} {
		for _, legacy := range []bool{false, true} {
			t.Run(strconv.Itoa(int(version))+"/legacy="+strconv.FormatBool(legacy), func(t *testing.T) {
				schema := &schemapb.CollectionSchema{Name: "wal_schema", Version: version}
				body := &message.CreateCollectionRequest{CollectionSchema: schema}
				if legacy {
					encoded, err := proto.Marshal(schema)
					require.NoError(t, err)
					body.CollectionSchema, body.Schema = nil, encoded
				}
				msg := message.NewCreateCollectionMessageBuilderV1().
					WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 100, PartitionIds: []int64{101}}).
					WithBody(body).WithVChannel("schema-dml_0_100v0").MustBuildMutable().
					WithTimeTick(100).WithLastConfirmed(rmq.NewRmqID(0)).IntoImmutableMessage(rmq.NewRmqID(1))
				replicated := message.MustNewReplicateMessage("primary", msg.IntoImmutableMessageProto())
				secondary := replicated.WithTimeTick(200).WithLastConfirmed(rmq.NewRmqID(0)).IntoImmutableMessage(rmq.NewRmqID(2))
				replayed := message.MilvusMessageToImmutableMessage(secondary.IntoImmutableMessageProto())
				info := newVChannelRecoveryInfoFromCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(replayed))
				snapshot, removed := info.ConsumeDirtyAndGetSnapshot()
				require.False(t, removed)
				encoded, err := proto.Marshal(snapshot)
				require.NoError(t, err)
				restored := &streamingpb.VChannelMeta{}
				require.NoError(t, proto.Unmarshal(encoded, restored))
				recovered := newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{restored})
				_, snSchema := recovered[replayed.VChannel()].GetSchema(replayed.TimeTick())
				require.NotNil(t, snSchema)
				require.True(t, proto.Equal(schema, snSchema), "replication and SN recovery must preserve the WAL schema used by Coord")
			})
		}
	}
}
