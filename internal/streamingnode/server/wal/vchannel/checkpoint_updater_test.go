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

package vchannel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

type stubCheckpointReporter struct {
	mu      sync.Mutex
	calls   int
	lastCPs []*msgpb.MsgPosition
	err     error
}

func (s *stubCheckpointReporter) UpdateChannelCheckpoint(_ context.Context, channelCPs []*msgpb.MsgPosition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.lastCPs = channelCPs
	return s.err
}

func (s *stubCheckpointReporter) snap() (int, []*msgpb.MsgPosition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.lastCPs
}

// stubStoredCheckpointReader answers what DataCoord holds per vchannel.
type stubStoredCheckpointReader struct {
	mu     sync.Mutex
	stored map[string]uint64
	err    error
	reads  []string
}

func (s *stubStoredCheckpointReader) StoredChannelCheckpoint(_ context.Context, vchannel string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads = append(s.reads, vchannel)
	return s.stored[vchannel], s.err
}

func (s *stubStoredCheckpointReader) set(vchannel string, timeTick uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stored == nil {
		s.stored = make(map[string]uint64)
	}
	s.stored[vchannel] = timeTick
	s.err = err
}

func (s *stubStoredCheckpointReader) takeReads() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	reads := s.reads
	s.reads = nil
	return reads
}

func newTestCheckpointUpdater(reporter *stubCheckpointReporter, getCheckpoint func() *utility.WALCheckpoint) *PChannelCheckpointUpdater {
	updater := newPChannelCheckpointUpdater(
		"by-dev-rootcoord-dml_0",
		func() []string { return []string{"v1", "v2"} },
		getCheckpoint,
		reporter,
		nil,
	)
	// keep the periodic loop fast for Start/Close tests
	updater.tickInterval = 10 * time.Millisecond
	return updater
}

func TestCheckpointUpdaterExecuteNoCheckpoint(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint { return nil })
	updater.execute()
	updater.execute()
	calls, _ := reporter.snap()
	assert.Zero(t, calls)
}

func TestCheckpointUpdaterExecuteNilMessageID(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{TimeTick: 100, Magic: 1}
	})
	updater.execute()
	calls, _ := reporter.snap()
	assert.Zero(t, calls)
}

func TestCheckpointUpdaterExecuteReportsPerVChannel(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	messageID := rmq.NewRmqID(42)
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{MessageID: messageID, TimeTick: 100, Magic: 1}
	})
	updater.execute()

	calls, cps := reporter.snap()
	require.Equal(t, 1, calls)
	require.Len(t, cps, 2)
	expectedMsgID := adaptor.MustGetMQWrapperIDFromMessage(messageID).Serialize()
	expectedTicks := map[string]uint64{"v1": 100, "v2": 100}
	for _, cp := range cps {
		assert.Equal(t, expectedMsgID, cp.GetMsgID())
		assert.Equal(t, expectedTicks[cp.GetChannelName()], cp.GetTimestamp())
		assert.Equal(t, message.WALNameRocksmq, message.WALName(cp.GetWALName()))
	}
}

func TestCheckpointUpdaterExecuteEmptyVChannels(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	updater := newPChannelCheckpointUpdater(
		"by-dev-rootcoord-dml_0",
		func() []string { return nil },
		func() *utility.WALCheckpoint {
			return &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 100, Magic: 1}
		},
		reporter,
		nil,
	)
	updater.execute()
	calls, _ := reporter.snap()
	assert.Zero(t, calls)
}

// TestCheckpointUpdaterExecuteUnsupportedMessageID covers a message ID type
// without an MQ wrapper counterpart (e.g. test-only IDs): the updater must
// skip the tick, never panic.
func TestCheckpointUpdaterExecuteUnsupportedMessageID(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 100, Magic: 1}
	})
	require.NotPanics(t, updater.execute)
	calls, _ := reporter.snap()
	assert.Zero(t, calls)
}

func TestCheckpointUpdaterExecuteReporterError(t *testing.T) {
	reporter := &stubCheckpointReporter{err: errors.New("coordinator down")}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 100, Magic: 1}
	})
	// must not panic; failure keeps the previous checkpoint for the next tick
	require.NotPanics(t, updater.execute)
	calls, _ := reporter.snap()
	assert.Equal(t, 1, calls)
}

func TestCheckpointUpdaterStartClose(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 100, Magic: 1}
	})

	go updater.Start()
	time.Sleep(60 * time.Millisecond)
	updater.Close()
	// Close is idempotent.
	updater.Close()

	calls, _ := reporter.snap()
	assert.GreaterOrEqual(t, calls, 1)
}

func TestCheckpointUpdaterRetryUsesCurrentPublishedPair(t *testing.T) {
	reporter := &stubCheckpointReporter{err: context.DeadlineExceeded}
	cp := &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 100}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint { return cp })
	updater.execute()
	cp = &utility.WALCheckpoint{MessageID: rmq.NewRmqID(2), TimeTick: 200}
	reporter.err = nil
	updater.execute()
	calls, positions := reporter.snap()
	require.Equal(t, 2, calls)
	for _, position := range positions {
		require.Equal(t, uint64(200), position.GetTimestamp())
		require.Equal(t, adaptor.MustGetMQWrapperIDFromMessage(cp.MessageID).Serialize(), position.GetMsgID())
	}
}

// TestCheckpointUpdaterConfirmsOnlyWhatDataCoordStored: a successful batch
// report proves nothing about a vchannel DataCoord filtered out; only a read
// back of its stored checkpoint does. Only an awaited vchannel is read back,
// only after a successful report, and Forget drops it.
func TestCheckpointUpdaterConfirmsOnlyWhatDataCoordStored(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	reader := &stubStoredCheckpointReader{}
	timeTick := uint64(100)
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: timeTick, Magic: 1}
	})
	updater.storedReader = reader
	assert.True(t, updater.confirmsStored())

	updater.execute()
	assert.Empty(t, reader.takeReads(), "nothing awaited, nothing read back")

	assert.False(t, updater.Reported("v1", 100), "nothing stored yet")
	reader.set("v1", 50, nil)
	updater.execute()
	assert.Equal(t, []string{"v1"}, reader.takeReads(), "only the awaited vchannel is read back")
	assert.False(t, updater.Reported("v1", 100), "the batch succeeded but DataCoord kept no checkpoint through 100")
	assert.True(t, updater.Reported("v1", 50), "what was read back counts")

	reader.set("v1", 100, errors.New("coordinator down"))
	updater.execute()
	assert.Equal(t, []string{"v1"}, reader.takeReads())
	assert.False(t, updater.Reported("v1", 100), "a failed read back proves nothing")

	reader.set("v1", 100, nil)
	reporter.mu.Lock()
	reporter.err = errors.New("coordinator down")
	reporter.mu.Unlock()
	updater.execute()
	assert.Empty(t, reader.takeReads(), "a failed report is not read back")
	assert.False(t, updater.Reported("v1", 100))

	reporter.mu.Lock()
	reporter.err = nil
	reporter.mu.Unlock()
	updater.execute()
	assert.Equal(t, []string{"v1"}, reader.takeReads())
	assert.True(t, updater.Reported("v1", 100))
	updater.execute()
	assert.Empty(t, reader.takeReads(), "a vchannel no longer awaited is not read back")

	updater.markStored("v1", 50)
	assert.True(t, updater.Reported("v1", 100), "an older read does not move it back")

	updater.Forget("v1")
	assert.False(t, updater.Reported("v1", 100))
	updater.Forget("v1")
	updater.execute()
	assert.Empty(t, reader.takeReads(), "a forgotten vchannel is no longer awaited")
}

// TestCheckpointUpdaterWithoutStoredReaderNeverConfirms: without a reader the
// updater cannot confirm anything, and the manager does not gate on it.
func TestCheckpointUpdaterWithoutStoredReaderNeverConfirms(t *testing.T) {
	updater := newTestCheckpointUpdater(&stubCheckpointReporter{}, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 100, Magic: 1}
	})
	assert.False(t, updater.confirmsStored())
	assert.False(t, updater.Reported("v1", 100))
	updater.execute()
	assert.False(t, updater.Reported("v1", 100))
}
