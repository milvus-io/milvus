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

package replicatestream

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/cdc/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestPKUpdateCDCTransaction(t *testing.T) {
	txnContext := message.TxnContext{TxnID: 1, Keepalive: time.Second}
	seal := func(m message.MutableMessage, ts uint64) message.ImmutableMessage {
		return m.WithTxnContext(txnContext).WithTimeTick(ts).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(ts)))
	}
	begin := message.MustAsImmutableBeginTxnMessageV2(seal(message.NewBeginTxnMessageBuilderV2().WithVChannel("p_10v0").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable(), 100))
	commit := message.MustAsImmutableCommitTxnMessageV2(seal(message.NewCommitTxnMessageBuilderV2().WithVChannel("p_10v0").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable(), 200))
	builder := message.NewImmutableTxnMessageBuilder(begin)
	bodies := []*messagespb.UpdateMessageBody{}
	for i := int64(0); i < 2; i++ {
		body := &messagespb.UpdateMessageBody{PartitionId: -1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{2*i + 1, 2*i + 2}}}}, Assignments: []*messagespb.UpdateAssignment{
			{FieldId: 101, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 18}}}}},
			{FieldId: 102, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_NullValue{}}},
		}}
		bodies = append(bodies, body)
		builder.Add(seal(message.NewUpdateMessageBuilderV2().WithVChannel("p_10v0").WithHeader(&messagespb.UpdateMessageHeader{CollectionId: 10, SchemaVersion: proto.Int32(7)}).WithBody(body).MustBuildMutable(), uint64(110+i)))
	}
	txn, err := builder.Build(commit)
	require.NoError(t, err)
	client := newMockReplicateStreamClient(t)
	c := &replicateStreamClient{ctx: context.Background(), clusterID: "source", client: client, channel: &meta.ReplicateChannel{Key: "test"}, metrics: NewReplicateMetrics(nil), pendingMessages: NewMsgQueue(MsgQueueOptions{Capacity: 16, MaxSize: 1024 * 1024})}
	require.NoError(t, c.Replicate(txn), "UPDATE must not be classified as unreplicable")
	require.NoError(t, c.sendTxnMessage(txn))
	require.Len(t, client.ch, 4)
	for i, wantType := range []message.MessageType{message.MessageTypeBeginTxn, message.MessageTypeUpdate, message.MessageTypeUpdate, message.MessageTypeCommitTxn} {
		req := <-client.ch
		got := message.MilvusMessageToImmutableMessage(req.GetReplicateMessage().GetMessage())
		require.Equal(t, wantType, got.MessageType())
		if i == 1 || i == 2 {
			u := message.MustAsImmutableUpdateMessageV2(got)
			require.EqualValues(t, 200, u.TimeTick(), "transaction bodies use the effective commit timestamp")
			require.EqualValues(t, 7, u.Header().GetSchemaVersion())
			require.True(t, proto.Equal(bodies[i-1], u.MustBody()))
		}
	}
}
