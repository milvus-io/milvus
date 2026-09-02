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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func newTestDeleteMessage(t *testing.T, timetick uint64, partitionID int64, pks ...int64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewDeleteMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: 1,
			Rows:         1,
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: 1,
			PartitionID:  partitionID,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
			Timestamps:   []uint64{timetick},
		}).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func newTestTxnMessage(t *testing.T, timetick uint64, bodies ...message.ImmutableMessage) message.ImmutableTxnMessage {
	t.Helper()
	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}
	begin, err := message.NewBeginTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)
	imBegin := begin.WithTxnContext(txnCtx).
		WithTimeTick(1).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	beginMsg, err := message.AsImmutableBeginTxnMessageV2(imBegin)
	assert.NoError(t, err)

	commit, err := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		BuildMutable()
	assert.NoError(t, err)
	imCommit := commit.WithTxnContext(txnCtx).
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
	commitMsg, err := message.AsImmutableCommitTxnMessageV2(imCommit)
	assert.NoError(t, err)

	txnBuilder := message.NewImmutableTxnMessageBuilder(beginMsg)
	for _, body := range bodies {
		txnBuilder.Add(body)
	}
	txnMsg, err := txnBuilder.Build(commitMsg)
	assert.NoError(t, err)
	return txnMsg
}

func TestBuildTransformLogEntry_Delete(t *testing.T) {
	msg := newTestDeleteMessage(t, 100, 10, 1, 2, 3)
	entry := BuildTransformLogEntry(msg, TransformEntryOption{})
	assert.NotNil(t, entry)
	assert.Equal(t, uint64(100), entry.GetTimeTick())
	assert.Len(t, entry.GetDelete().GetBlocks(), 1)
	block := entry.GetDelete().GetBlocks()[0]
	assert.Equal(t, int64(10), block.GetPartitionId())
	assert.Equal(t, []int64{1, 2, 3}, block.GetPrimaryKeys().GetIntId().GetData())
}

func TestBuildTransformLogEntry_TxnWithDelete(t *testing.T) {
	deleteA := newTestDeleteMessage(t, 90, 10, 1)
	deleteB := newTestDeleteMessage(t, 95, 20, 2)
	txn := newTestTxnMessage(t, 100, deleteA, deleteB)
	entry := BuildTransformLogEntry(txn, TransformEntryOption{})
	assert.NotNil(t, entry)
	// one entry at the outer txn timetick with every delete block.
	assert.Equal(t, uint64(100), entry.GetTimeTick())
	assert.Len(t, entry.GetDelete().GetBlocks(), 2)
	assert.Equal(t, int64(10), entry.GetDelete().GetBlocks()[0].GetPartitionId())
	assert.Equal(t, int64(20), entry.GetDelete().GetBlocks()[1].GetPartitionId())
}

func TestBuildTransformLogEntry_NoPayload(t *testing.T) {
	// A txn without delete produces no entry.
	txn := newTestTxnMessage(t, 100, newTestInsertMessage(t, 90))
	assert.Nil(t, BuildTransformLogEntry(txn, TransformEntryOption{}))

	// A barrier message produces no entry.
	flushMsg := message.NewManualFlushMessageBuilderV2().
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithVChannel("v1").
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmed(walimplstest.NewTestMessageID(100)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(101))
	assert.Nil(t, BuildTransformLogEntry(flushMsg, TransformEntryOption{}))
	assert.Nil(t, BuildTransformLogEntry(nil, TransformEntryOption{}))
}

func TestBuildTransformLogEntry_DeleteFilter(t *testing.T) {
	msg := newTestDeleteMessage(t, 100, 10, 1, 2, 3)
	// Filter rejects the partition: no blocks, no entry.
	entry := BuildTransformLogEntry(msg, TransformEntryOption{
		DeleteFilter: func(partitionID int64, _ uint64) bool { return partitionID != 10 },
	})
	assert.Nil(t, entry)

	// Filter accepts the partition: entry kept.
	entry = BuildTransformLogEntry(msg, TransformEntryOption{
		DeleteFilter: func(partitionID int64, _ uint64) bool { return partitionID == 10 },
	})
	assert.NotNil(t, entry)
}

func newTestInsertMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	t.Helper()
	mutableMsg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{CollectionID: 1}).
		MustBuildMutable()
	return mutableMsg.WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick + 1)))
}

func TestPrimaryKeyCount(t *testing.T) {
	assert.Zero(t, PrimaryKeyCount(nil))
	assert.Equal(t, 2, PrimaryKeyCount(&schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}},
	}))
	assert.Equal(t, 2, PrimaryKeyCount(&schemapb.IDs{
		IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a", "b"}}},
	}))
}

var _ = streamingpb.TransformLogEntry{}
