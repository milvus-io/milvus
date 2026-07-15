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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestClassifyTransformLogMessage(t *testing.T) {
	tests := []struct {
		name string
		msg  message.ImmutableMessage
		kind TransformLogKind
	}{
		{
			name: "delete",
			msg:  newTransformLogTestDeleteMessage(t, 10),
			kind: TransformLogKindDelete,
		},
		{
			name: "txn with delete",
			msg:  newTransformLogTestTxnMessage(t, 20, newTransformLogTxnDeleteMessage(t, 21)),
			kind: TransformLogKindDelete,
		},
		{
			name: "txn without delete",
			msg:  newTransformLogTestTxnMessage(t, 30, newTransformLogTxnInsertMessage(t, 31)),
			kind: TransformLogKindNone,
		},
		{
			name: "recovery barrier",
			msg:  newTransformLogTestRecoveryBarrierMessage(t, 40),
			kind: TransformLogKindBarrier,
		},
		{
			name: "insert",
			msg:  newTransformLogTxnInsertMessage(t, 50),
			kind: TransformLogKindNone,
		},
		{
			name: "nil",
			kind: TransformLogKindNone,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.kind, ClassifyTransformLogMessage(test.msg))
		})
	}
}

func newTransformLogTestDeleteMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  10,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTransformLogTestTxnMessage(t *testing.T, timetick uint64, bodies ...message.ImmutableMessage) message.ImmutableMessage {
	t.Helper()
	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
	beginMsg := begin.WithTxnContext(txnCtx).
		WithTimeTick(timetick - 1).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick - 1))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick - 1)))

	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(beginMsg))
	for _, body := range bodies {
		builder.Add(body)
	}

	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable()
	commitMsg := commit.WithTxnContext(txnCtx).
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))

	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(commitMsg))
	require.NoError(t, err)
	return txn
}

func newTransformLogTxnDeleteMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	return newTransformLogTestDeleteMessage(t, timetick)
}

func newTransformLogTxnInsertMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable()
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTransformLogTestRecoveryBarrierMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutable := message.NewRecoveryBarrierMessageBuilderV2().
		WithHeader(&message.RecoveryBarrierMessageHeader{}).
		WithBody(&message.RecoveryBarrierMessageBody{}).
		WithAllVChannel().
		MustBuildMutable()
	return mutable.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}
