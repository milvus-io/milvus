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
	"errors"
	"sync"
	"testing"
	"time"

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

func newTestCheckpointUpdater(reporter *stubCheckpointReporter, getCheckpoint func() *utility.WALCheckpoint, getVChannelFlushTimeTick func(vchannel string) uint64) *PChannelCheckpointUpdater {
	updater := newPChannelCheckpointUpdater(
		"by-dev-rootcoord-dml_0",
		func() []string { return []string{"v1", "v2"} },
		getCheckpoint,
		getVChannelFlushTimeTick,
		reporter,
	)
	// keep the periodic loop fast for Start/Close tests
	updater.tickInterval = 10 * time.Millisecond
	return updater
}

func TestCheckpointUpdaterExecuteNoCheckpoint(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint { return nil }, nil)
	updater.execute()
	updater.execute()
	calls, _ := reporter.snap()
	assert.Zero(t, calls)
}

func TestCheckpointUpdaterExecuteNilMessageID(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{TimeTick: 100, Magic: 1}
	}, nil)
	updater.execute()
	calls, _ := reporter.snap()
	assert.Zero(t, calls)
}

func TestCheckpointUpdaterExecuteReportsPerVChannel(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	messageID := rmq.NewRmqID(42)
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{MessageID: messageID, TimeTick: 100, Magic: 1}
	}, func(vchannel string) uint64 {
		// Each vchannel reports its own flush position, not the pchannel
		// recovery checkpoint time tick.
		if vchannel == "v1" {
			return 200
		}
		return 300
	})
	updater.execute()

	calls, cps := reporter.snap()
	require.Equal(t, 1, calls)
	require.Len(t, cps, 2)
	expectedMsgID := adaptor.MustGetMQWrapperIDFromMessage(messageID).Serialize()
	expectedTicks := map[string]uint64{"v1": 200, "v2": 300}
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
		func(string) uint64 { return 100 },
		reporter,
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
	}, func(string) uint64 { return 100 })
	require.NotPanics(t, updater.execute)
	calls, _ := reporter.snap()
	assert.Zero(t, calls)
}

func TestCheckpointUpdaterExecuteReporterError(t *testing.T) {
	reporter := &stubCheckpointReporter{err: errors.New("coordinator down")}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 100, Magic: 1}
	}, func(string) uint64 { return 100 })
	// must not panic; failure keeps the previous checkpoint for the next tick
	require.NotPanics(t, updater.execute)
	calls, _ := reporter.snap()
	assert.Equal(t, 1, calls)
}

func TestCheckpointUpdaterStartClose(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 100, Magic: 1}
	}, func(string) uint64 { return 100 })

	go updater.Start()
	time.Sleep(60 * time.Millisecond)
	updater.Close()
	// Close is idempotent.
	updater.Close()

	calls, _ := reporter.snap()
	assert.GreaterOrEqual(t, calls, 1)
}
