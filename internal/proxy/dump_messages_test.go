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

package proxy

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	pulsar2 "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestShouldDumpMessage(t *testing.T) {
	// Messages that SHOULD be dumped (replicable data)
	assert.True(t, shouldDumpMessage(message.MessageTypeInsert), "Insert should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeDelete), "Delete should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeCreateCollection), "CreateCollection should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeDropCollection), "DropCollection should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeCreatePartition), "CreatePartition should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeDropPartition), "DropPartition should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeBeginTxn), "BeginTxn should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeCommitTxn), "CommitTxn should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeTxn), "Txn should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeImport), "Import should be dumped")
	assert.True(t, shouldDumpMessage(message.MessageTypeManualFlush), "ManualFlush should be dumped")

	// Messages that should NOT be dumped (system messages)
	assert.False(t, shouldDumpMessage(message.MessageTypeTimeTick), "TimeTick should NOT be dumped")
	assert.False(t, shouldDumpMessage(message.MessageTypeCreateSegment), "CreateSegment should NOT be dumped")
	assert.False(t, shouldDumpMessage(message.MessageTypeFlush), "Flush should NOT be dumped")
	assert.False(t, shouldDumpMessage(message.MessageTypeRollbackTxn), "RollbackTxn should NOT be dumped")
}

// mockDumpMessagesServer is a minimal implementation of milvuspb.MilvusService_DumpMessagesServer for testing.
type mockDumpMessagesServer struct {
	ctx          context.Context
	mu           sync.Mutex
	sent         []*milvuspb.DumpMessagesResponse
	sendErr      error
	sendErrAfter int
}

func (m *mockDumpMessagesServer) Send(resp *milvuspb.DumpMessagesResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sendErr != nil && (m.sendErrAfter == 0 || len(m.sent) >= m.sendErrAfter) {
		return m.sendErr
	}
	m.sent = append(m.sent, resp)
	return nil
}

func (m *mockDumpMessagesServer) getSent() []*milvuspb.DumpMessagesResponse {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sent
}

func (m *mockDumpMessagesServer) Context() context.Context     { return m.ctx }
func (m *mockDumpMessagesServer) SetHeader(metadata.MD) error  { return nil }
func (m *mockDumpMessagesServer) SendHeader(metadata.MD) error { return nil }
func (m *mockDumpMessagesServer) SetTrailer(metadata.MD)       {}
func (m *mockDumpMessagesServer) SendMsg(interface{}) error    { return nil }
func (m *mockDumpMessagesServer) RecvMsg(interface{}) error    { return nil }

// Ensure mockDumpMessagesServer satisfies the grpc.ServerStream interface.
var _ grpc.ServerStream = (*mockDumpMessagesServer)(nil)

// testStartMessageID returns a valid *commonpb.MessageID for testing.
func testStartMessageID() *commonpb.MessageID {
	return pulsar2.NewPulsarID(pulsar.EarliestMessageID()).IntoProto()
}

func testPulsarMessageID(entryID int64) message.MessageID {
	return pulsar2.NewPulsarID(pulsar.NewMessageID(1, entryID, -1, 0))
}

func assertDumpMessagesDeliverPolicy(t *testing.T, opts streaming.ReadOption, includeStartMessage bool) {
	t.Helper()
	require.NotNil(t, opts.DeliverPolicy)
	if includeStartMessage {
		_, ok := opts.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_StartFrom)
		assert.True(t, ok)
		return
	}
	_, ok := opts.DeliverPolicy.GetPolicy().(*streamingpb.DeliverPolicy_StartAfter)
	assert.True(t, ok)
}

// buildTestImmutableMessage creates an ImmutableMessage with the given timetick for testing.
func buildTestImmutableMessage(timetick uint64) message.ImmutableMessage {
	return buildTestImmutableMessageWithID(pulsar2.NewPulsarID(pulsar.EarliestMessageID()), timetick)
}

func buildTestImmutableMessageWithID(msgID message.MessageID, timetick uint64) message.ImmutableMessage {
	return message.NewCreateDatabaseMessageBuilderV2().
		WithHeader(&message.CreateDatabaseMessageHeader{}).
		WithBody(&message.CreateDatabaseMessageBody{}).
		WithVChannel("test-channel").
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(msgID).
		IntoImmutableMessage(msgID)
}

// buildRollbackTxnMessage creates a RollbackTxn ImmutableMessage (which shouldDumpMessage returns false for).
func buildRollbackTxnMessage(timetick uint64) message.ImmutableMessage {
	return buildRollbackTxnMessageWithID(pulsar2.NewPulsarID(pulsar.EarliestMessageID()), timetick)
}

func buildRollbackTxnMessageWithID(msgID message.MessageID, timetick uint64) message.ImmutableMessage {
	return message.NewRollbackTxnMessageBuilderV2().
		WithHeader(&message.RollbackTxnMessageHeader{}).
		WithBody(&message.RollbackTxnMessageBody{}).
		WithVChannel("test-channel").
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(msgID).
		IntoImmutableMessage(msgID)
}

func buildTimeTickImmutableMessage(t *testing.T, msgID message.MessageID, timetick uint64) message.ImmutableMessage {
	return message.CreateTestTimeTickSyncMessage(t, 1, timetick, msgID).IntoImmutableMessage(msgID)
}

type testTxnMessages struct {
	resumeMsgID message.MessageID
	beginMsgID  message.MessageID
	insertMsgID message.MessageID
	commitMsgID message.MessageID
	begin       message.ImmutableBeginTxnMessageV2
	insert      message.ImmutableMessage
	commit      message.ImmutableCommitTxnMessageV2
}

func buildTxnMessages(t *testing.T, timetick uint64) testTxnMessages {
	// LastConfirmedMessageID is held before the txn begin while the txn is not done.
	txnResumeMsgID := testPulsarMessageID(0)
	beginMsgID := testPulsarMessageID(1)
	insertMsgID := testPulsarMessageID(2)
	commitMsgID := testPulsarMessageID(4)
	txnCtx := message.TxnContext{
		TxnID:     1,
		Keepalive: time.Second,
	}

	begin := message.NewBeginTxnMessageBuilderV2().
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		WithVChannel("test-channel").
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(timetick).
		WithLastConfirmed(txnResumeMsgID).
		IntoImmutableMessage(beginMsgID)
	beginMsg, err := message.AsImmutableBeginTxnMessageV2(begin)
	require.NoError(t, err)

	insert := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{}).
		WithBody(&msgpb.InsertRequest{CollectionName: "test-collection"}).
		WithVChannel("test-channel").
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(timetick).
		WithLastConfirmed(txnResumeMsgID).
		IntoImmutableMessage(insertMsgID)

	commit := message.NewCommitTxnMessageBuilderV2().
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithVChannel("test-channel").
		MustBuildMutable().
		WithTxnContext(txnCtx).
		WithTimeTick(timetick).
		WithLastConfirmed(txnResumeMsgID).
		IntoImmutableMessage(commitMsgID)
	commitMsg, err := message.AsImmutableCommitTxnMessageV2(commit)
	require.NoError(t, err)

	return testTxnMessages{
		resumeMsgID: txnResumeMsgID,
		beginMsgID:  beginMsgID,
		insertMsgID: insertMsgID,
		commitMsgID: commitMsgID,
		begin:       beginMsg,
		insert:      insert,
		commit:      commitMsg,
	}
}

func buildTxnImmutableMessage(t *testing.T, timetick uint64) message.ImmutableMessage {
	txnMessages := buildTxnMessages(t, timetick)
	txnMsg, err := message.NewImmutableTxnMessageBuilder(txnMessages.begin).
		Add(txnMessages.insert).
		Build(txnMessages.commit)
	require.NoError(t, err)
	return txnMsg
}

func dumpResponseMessageID(t *testing.T, resp *milvuspb.DumpMessagesResponse) message.MessageID {
	msg := resp.GetMessage()
	require.NotNil(t, msg)
	return message.MustUnmarshalMessageID(msg.GetId())
}

func dumpResponseMessageType(t *testing.T, resp *milvuspb.DumpMessagesResponse) message.MessageType {
	msg := resp.GetMessage()
	require.NotNil(t, msg)
	immutableMsg := message.NewImmutableMesasge(
		message.MustUnmarshalMessageID(msg.GetId()),
		msg.GetPayload(),
		msg.GetProperties(),
	)
	return immutableMsg.MessageType()
}

func dumpResponseLastConfirmedMessageID(t *testing.T, resp *milvuspb.DumpMessagesResponse) message.MessageID {
	msg := resp.GetMessage()
	require.NotNil(t, msg)
	immutableMsg := message.NewImmutableMesasge(
		message.MustUnmarshalMessageID(msg.GetId()),
		msg.GetPayload(),
		msg.GetProperties(),
	)
	return immutableMsg.LastConfirmedMessageID()
}

func TestDumpMessages_NodeUnhealthy(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Abnormal)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{}, stream)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceNotReady))
}

func TestDumpMessages_MissingPChannel(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		StartMessageId: testStartMessageID(),
	}, stream)
	assert.Error(t, err)
}

func TestDumpMessages_MissingStartMessageId(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	// nil StartMessageId
	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel: "test-channel",
	}, stream)
	assert.Error(t, err)
}

func TestDumpMessages_EmptyStartMessageIdBytes(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	// StartMessageId present but with empty Id string
	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: &commonpb.MessageID{Id: ""},
	}, stream)
	assert.Error(t, err)
}

func TestDumpMessages_InvalidStartMessageId(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: &commonpb.MessageID{Id: "invalid-message-id"},
	}, stream)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.ErrorContains(t, err, "invalid start_message_id")
}

func TestDumpMessages_InvalidTimetickRange(t *testing.T) {
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
		StartTimetick:  200,
		EndTimetick:    100,
	}, stream)
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.ErrorContains(t, err, "end_timetick must be greater than or equal to start_timetick")
}

func TestDumpMessages_DefaultDeliverPolicyStartsAfter(t *testing.T) {
	scannerDone := make(chan struct{})
	close(scannerDone)

	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(scannerDone)
	mockScanner.EXPECT().Error().Return(nil)
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.MatchedBy(func(opts streaming.ReadOption) bool {
		assertDumpMessagesDeliverPolicy(t, opts, false)
		return true
	})).Return(mockScanner)
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
	}, stream)
	assert.NoError(t, err)
}

func TestDumpMessages_StartFromDeliverPolicy(t *testing.T) {
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	startMsgID := testPulsarMessageID(10)
	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			assertDumpMessagesDeliverPolicy(t, opts, true)
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			ch <- buildTestImmutableMessageWithID(startMsgID, 50)
			ch <- buildTestImmutableMessageWithID(testPulsarMessageID(11), 200)
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	req := &milvuspb.DumpMessagesRequest{
		Pchannel:            "test-channel",
		StartMessageId:      startMsgID.IntoProto(),
		IncludeStartMessage: true,
		EndTimetick:         100,
	}

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(req, stream)
	assert.NoError(t, err)
}

func TestDumpMessages_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{})) // never fires
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(mockScanner)
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	// Cancel before DumpMessages is called so ctx.Done() fires immediately in the select
	cancel()

	stream := &mockDumpMessagesServer{ctx: ctx}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
	}, stream)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestDumpMessages_ScannerError(t *testing.T) {
	scannerDone := make(chan struct{})
	close(scannerDone) // immediately done

	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(scannerDone)
	mockScanner.EXPECT().Error().Return(errors.New("scanner error"))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(mockScanner)
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
	}, stream)
	assert.EqualError(t, err, "scanner error")
}

func TestDumpMessages_ChannelClosedReturnsScannerError(t *testing.T) {
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Error().Return(errors.New("scanner error"))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			close(ch)
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
	}, stream)
	assert.EqualError(t, err, "scanner error")
}

func TestDumpMessages_ScannerDoneSuccessfully(t *testing.T) {
	scannerDone := make(chan struct{})
	close(scannerDone)

	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(scannerDone)
	mockScanner.EXPECT().Error().Return(nil)
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).Return(mockScanner)
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
	}, stream)
	assert.NoError(t, err)
	assert.Empty(t, stream.getSent())
}

func TestDumpMessages_MessageSentSuccessfully(t *testing.T) {
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			// Write a data message within the [0, 100] timetick range, then one past end to stop loop.
			ch <- buildTestImmutableMessage(50)
			ch <- buildTestImmutableMessage(200) // > endTimetick (100), triggers return nil
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
		EndTimetick:    100,
	}, stream)
	assert.NoError(t, err)
	assert.Len(t, stream.getSent(), 1)
}

func TestDumpMessages_ExpandTxnMessage(t *testing.T) {
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			ch <- buildTxnImmutableMessage(t, 50)
			ch <- buildTestImmutableMessage(200) // > endTimetick (100), triggers return nil
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
		EndTimetick:    100,
	}, stream)
	require.NoError(t, err)

	sent := stream.getSent()
	require.Len(t, sent, 3)
	assert.Equal(t, message.MessageTypeBeginTxn, dumpResponseMessageType(t, sent[0]))
	assert.Equal(t, message.MessageTypeInsert, dumpResponseMessageType(t, sent[1]))
	assert.Equal(t, message.MessageTypeCommitTxn, dumpResponseMessageType(t, sent[2]))
	txnMessages := buildTxnMessages(t, 50)
	assert.True(t, txnMessages.beginMsgID.EQ(dumpResponseMessageID(t, sent[0])))
	assert.True(t, txnMessages.insertMsgID.EQ(dumpResponseMessageID(t, sent[1])))
	assert.True(t, txnMessages.commitMsgID.EQ(dumpResponseMessageID(t, sent[2])))
	// Txn expansion rewrites begin/body LastConfirmedMessageID to the commit's
	// LastConfirmedMessageID. In production this checkpoint is held before the
	// txn begin until the txn is done, so it can replay the txn context.
	assert.True(t, txnMessages.resumeMsgID.EQ(dumpResponseLastConfirmedMessageID(t, sent[0])))
	assert.True(t, txnMessages.resumeMsgID.EQ(dumpResponseLastConfirmedMessageID(t, sent[1])))
	assert.True(t, txnMessages.resumeMsgID.EQ(dumpResponseLastConfirmedMessageID(t, sent[2])))
	assert.True(t, txnMessages.resumeMsgID.LT(txnMessages.beginMsgID))
}

func TestDumpMessages_TxnLastConfirmedCanReplayDumpedBody(t *testing.T) {
	txnMessages := buildTxnMessages(t, 50)
	txnBuffer := utility.NewTxnBuffer(
		log.With(),
		metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics(),
	)
	scannerOutput := txnBuffer.HandleImmutableMessages([]message.ImmutableMessage{
		txnMessages.begin,
		txnMessages.insert,
		txnMessages.commit,
	}, 50)
	require.Len(t, scannerOutput, 1)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	dumpedCount, err := dumpOneMessage(stream, scannerOutput[0], log.With())
	require.NoError(t, err)
	assert.Equal(t, 3, dumpedCount)

	sent := stream.getSent()
	require.Len(t, sent, 3)
	assert.Equal(t, message.MessageTypeInsert, dumpResponseMessageType(t, sent[1]))
	assert.True(t, txnMessages.insertMsgID.EQ(dumpResponseMessageID(t, sent[1])))
	assert.True(t, txnMessages.resumeMsgID.EQ(dumpResponseLastConfirmedMessageID(t, sent[1])))
}

func TestDumpMessages_IncludedStartTxnBodyMessageDoesNotForceRejectAtProxyLayer(t *testing.T) {
	startMsgID := testPulsarMessageID(2)
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			assertDumpMessagesDeliverPolicy(t, opts, true)
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			// This mock injects an already assembled transaction. The test only
			// verifies that DumpMessages does not reject at the proxy layer; a
			// real start-inside-txn read may produce no assembled transaction if
			// the scanner misses the begin message.
			ch <- buildTxnImmutableMessage(t, 50)
			ch <- buildTestImmutableMessage(200) // > endTimetick, stops loop
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:            "test-channel",
		StartMessageId:      startMsgID.IntoProto(),
		IncludeStartMessage: true,
		EndTimetick:         100,
	}, stream)
	require.NoError(t, err)
	assert.Len(t, stream.getSent(), 3)
}

func TestDumpMessages_TxnSendError(t *testing.T) {
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			ch <- buildTxnImmutableMessage(t, 50)
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	sendErr := errors.New("send txn error")
	stream := &mockDumpMessagesServer{ctx: context.Background(), sendErr: sendErr}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
	}, stream)
	assert.EqualError(t, err, "send txn error")
}

func TestDumpTxnMessageSendErrors(t *testing.T) {
	txnMsg, ok := buildTxnImmutableMessage(t, 50).(message.ImmutableTxnMessage)
	require.True(t, ok)

	testCases := []struct {
		name         string
		sendErrAfter int
		dumpedCount  int
	}{
		{
			name:         "begin",
			sendErrAfter: 0,
			dumpedCount:  0,
		},
		{
			name:         "body",
			sendErrAfter: 1,
			dumpedCount:  1,
		},
		{
			name:         "commit",
			sendErrAfter: 2,
			dumpedCount:  2,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sendErr := errors.New("send txn error")
			stream := &mockDumpMessagesServer{
				ctx:          context.Background(),
				sendErr:      sendErr,
				sendErrAfter: tc.sendErrAfter,
			}

			dumpedCount, err := dumpTxnMessage(stream, txnMsg, log.With())
			assert.EqualError(t, err, "send txn error")
			assert.Equal(t, tc.dumpedCount, dumpedCount)
		})
	}
}

func TestDumpMessages_FilterByStartTimetick(t *testing.T) {
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			ch <- buildTestImmutableMessage(50)  // < startTimetick (100), skipped
			ch <- buildTestImmutableMessage(150) // within [100, 200], sent
			ch <- buildTestImmutableMessage(250) // > endTimetick (200), stops loop
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
		StartTimetick:  100,
		EndTimetick:    200,
	}, stream)
	assert.NoError(t, err)
	assert.Len(t, stream.getSent(), 1)
}

func TestDumpMessages_StopAtEndTimetick(t *testing.T) {
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			ch <- buildTestImmutableMessage(200) // > endTimetick (100), stops immediately
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
		EndTimetick:    100,
	}, stream)
	assert.NoError(t, err)
	assert.Empty(t, stream.getSent())
}

func TestDumpMessages_FilterSystemMessages(t *testing.T) {
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			ch <- buildRollbackTxnMessage(50) // system message, should be filtered
			ch <- buildTimeTickImmutableMessage(t, testPulsarMessageID(10), 50)
			ch <- buildTestImmutableMessage(50)  // data message, should be sent
			ch <- buildTestImmutableMessage(200) // > endTimetick, stops loop
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
		EndTimetick:    100,
	}, stream)
	assert.NoError(t, err)
	// Only the data message should be sent, not RollbackTxn or TimeTick.
	assert.Len(t, stream.getSent(), 1)
}

func TestDumpMessages_IncludeStartDoesNotBypassSelfControlledFilter(t *testing.T) {
	startMsgID := testPulsarMessageID(10)
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			assertDumpMessagesDeliverPolicy(t, opts, true)
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			ch <- buildTimeTickImmutableMessage(t, startMsgID, 50)
			ch <- buildTestImmutableMessage(200) // > endTimetick, stops loop
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:            "test-channel",
		StartMessageId:      startMsgID.IntoProto(),
		IncludeStartMessage: true,
		EndTimetick:         100,
	}, stream)
	require.NoError(t, err)
	assert.Empty(t, stream.getSent())
}

func TestDumpMessages_IncludeStartDoesNotBypassRollbackTxnFilter(t *testing.T) {
	startMsgID := testPulsarMessageID(10)
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			assertDumpMessagesDeliverPolicy(t, opts, true)
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			ch <- buildRollbackTxnMessageWithID(startMsgID, 50)
			ch <- buildTestImmutableMessage(200) // > endTimetick, stops loop
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:            "test-channel",
		StartMessageId:      startMsgID.IntoProto(),
		IncludeStartMessage: true,
		EndTimetick:         100,
	}, stream)
	require.NoError(t, err)
	assert.Empty(t, stream.getSent())
}

func TestDumpMessages_IncludedStartMessageSkippedByVisibleStream(t *testing.T) {
	startMsgID := testPulsarMessageID(10)
	nextMsgID := testPulsarMessageID(11)
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			assertDumpMessagesDeliverPolicy(t, opts, true)
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			ch <- buildTestImmutableMessageWithID(nextMsgID, 50)
			ch <- buildTestImmutableMessage(200) // > endTimetick, stops loop
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:            "test-channel",
		StartMessageId:      startMsgID.IntoProto(),
		IncludeStartMessage: true,
		EndTimetick:         100,
	}, stream)
	require.NoError(t, err)
	require.Len(t, stream.getSent(), 1)
	assert.True(t, dumpResponseMessageID(t, stream.getSent()[0]).EQ(nextMsgID))
}

func TestDumpMessages_SendError(t *testing.T) {
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			ch <- buildTestImmutableMessage(50)
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	sendErr := errors.New("send error")
	stream := &mockDumpMessagesServer{ctx: context.Background(), sendErr: sendErr}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
	}, stream)
	assert.EqualError(t, err, "send error")
}

func TestDumpMessages_ChannelClosed(t *testing.T) {
	mockScanner := mock_streaming.NewMockScanner(t)
	mockScanner.EXPECT().Done().Return(make(chan struct{}))
	mockScanner.EXPECT().Error().Return(nil)
	mockScanner.EXPECT().Close()

	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, opts streaming.ReadOption) streaming.Scanner {
			ch, ok := opts.MessageHandler.(adaptor.ChanMessageHandler)
			require.True(t, ok)
			// Closing the channel triggers the !ok case in the select loop
			close(ch)
			return mockScanner
		})
	prevWAL := streaming.WAL()
	streaming.SetWALForTest(mockWAL)
	defer streaming.SetWALForTest(prevWAL)

	stream := &mockDumpMessagesServer{ctx: context.Background()}
	node := &Proxy{}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	err := node.DumpMessages(&milvuspb.DumpMessagesRequest{
		Pchannel:       "test-channel",
		StartMessageId: testStartMessageID(),
	}, stream)
	assert.NoError(t, err)
}
