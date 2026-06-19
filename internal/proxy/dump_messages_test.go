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

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	pulsar2 "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	ctx     context.Context
	mu      sync.Mutex
	sent    []*milvuspb.DumpMessagesResponse
	sendErr error
}

func (m *mockDumpMessagesServer) Send(resp *milvuspb.DumpMessagesResponse) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
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

// buildTestImmutableMessage creates an ImmutableMessage with the given timetick for testing.
func buildTestImmutableMessage(timetick uint64) message.ImmutableMessage {
	msgID := pulsar2.NewPulsarID(pulsar.EarliestMessageID())
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
	msgID := pulsar2.NewPulsarID(pulsar.EarliestMessageID())
	return message.NewRollbackTxnMessageBuilderV2().
		WithHeader(&message.RollbackTxnMessageHeader{}).
		WithBody(&message.RollbackTxnMessageBody{}).
		WithVChannel("test-channel").
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(msgID).
		IntoImmutableMessage(msgID)
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
			ch <- buildRollbackTxnMessage(50)    // system message, should be filtered
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
	// Only the data message should be sent, not the RollbackTxn
	assert.Len(t, stream.getSent(), 1)
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
