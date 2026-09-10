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

package mlog

import (
	"io"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func TestAsyncBufferedWriteSyncer(t *testing.T) {
	blockWriter := &blockWriter{
		Writer: os.Stdout,
	}
	syncer := NewAsyncTextIOCore(
		&Config{
			AsyncWriteEnable:         true,
			AsyncWriteFlushInterval:  1 * time.Second,
			AsyncWriteDroppedTimeout: 100 * time.Millisecond,
			AsyncWriteStopTimeout:    1 * time.Second,
			AsyncWritePendingLength:  100,
			AsyncWriteBufferSize:     5,
			AsyncWriteMaxBytesPerLog: 2,
		},
		zapcore.AddSync(blockWriter),
		zap.DebugLevel,
	)
	wg := &sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			syncer.Write(zapcore.Entry{
				Level:   zap.DebugLevel,
				Message: "test",
			}, []zapcore.Field{
				zap.String("test", "test"),
				zap.Int("test", 1),
			})
			wg.Done()
		}()
	}
	syncer.Sync()
	wg.Wait()
	syncer.Stop()
}

type blockWriter struct {
	io.Writer
}

func (s *blockWriter) Write(p []byte) (n int, err error) {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	if rand.Intn(10) == 0 {
		return 0, errors.New("write error")
	}
	return s.Writer.Write(p)
}

func (s *blockWriter) Sync() error {
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	if rand.Intn(10) == 0 {
		return errors.New("sync error")
	}
	return nil
}

func TestAsyncBufferedWriteSyncerPrioritizesErrorWithoutBlocking(t *testing.T) {
	writer := newBlockingWriteSyncer()
	defer writer.unblock()
	syncer := newAsyncTextIOCoreForBlockedWriter(writer)

	require.NoError(t, syncer.Write(zapcore.Entry{Level: zap.InfoLevel, Message: "writing"}, nil))
	writer.waitUntilBlocked(t)
	require.NoError(t, syncer.Write(zapcore.Entry{Level: zap.InfoLevel, Message: "pending"}, nil))

	done := make(chan error, 1)
	go func() {
		done <- syncer.Write(zapcore.Entry{Level: zap.ErrorLevel, Message: "important"}, nil)
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "error log blocked on a full pending queue")
	}

	replacement := <-syncer.pending.entries
	assert.Equal(t, zap.ErrorLevel, replacement.level)
	syncer.pending.entries <- replacement

	writer.unblock()
	syncer.Stop()
}

func TestAsyncBufferedWriteSyncerPrioritizesErrorOverWaitingInfo(t *testing.T) {
	writer := newBlockingWriteSyncer()
	defer writer.unblock()
	syncer := newAsyncTextIOCoreForBlockedWriter(writer)

	require.NoError(t, syncer.Write(zapcore.Entry{Level: zap.InfoLevel, Message: "writing"}, nil))
	writer.waitUntilBlocked(t)
	require.NoError(t, syncer.Write(zapcore.Entry{Level: zap.InfoLevel, Message: "pending"}, nil))

	droppedBefore := testutil.ToFloat64(metrics.LoggingDroppedWriteTotal)
	pendingBefore := testutil.ToFloat64(metrics.LoggingPendingWriteTotal)
	waitingInfoDone := make(chan error, 1)
	go func() {
		waitingInfoDone <- syncer.Write(zapcore.Entry{Level: zap.InfoLevel, Message: "waiting"}, nil)
	}()
	require.Eventually(t, func() bool {
		syncer.pending.mu.Lock()
		defer syncer.pending.mu.Unlock()
		return syncer.pending.waiters == 1
	}, time.Second, time.Millisecond, "info log did not start waiting for queue space")

	require.NoError(t, syncer.Write(zapcore.Entry{Level: zap.ErrorLevel, Message: "important"}, nil))

	syncer.pending.mu.Lock()
	replacement := <-syncer.pending.entries
	assert.Equal(t, zap.ErrorLevel, replacement.level)
	syncer.pending.entries <- replacement
	syncer.pending.mu.Unlock()
	assert.Equal(t, droppedBefore+1, testutil.ToFloat64(metrics.LoggingDroppedWriteTotal))
	assert.Equal(t, pendingBefore, testutil.ToFloat64(metrics.LoggingPendingWriteTotal))

	select {
	case <-waitingInfoDone:
		require.FailNow(t, "waiting info log took the error log's queue slot")
	default:
	}

	writer.unblock()
	select {
	case err := <-waitingInfoDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.FailNow(t, "waiting info log did not resume after queue space became available")
	}
	syncer.Stop()
}

func TestAsyncBufferedWriteSyncerStopTimesOutOnBlockedWriter(t *testing.T) {
	writer := newBlockingWriteSyncer()
	defer writer.unblock()
	syncer := newAsyncTextIOCoreForBlockedWriter(writer)

	require.NoError(t, syncer.Write(zapcore.Entry{Level: zap.InfoLevel, Message: "writing"}, nil))
	writer.waitUntilBlocked(t)

	done := make(chan struct{})
	go func() {
		syncer.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "async logger stop blocked on the underlying writer")
	}

	writer.unblock()
	select {
	case <-syncer.notifier.FinishChan():
	case <-time.After(time.Second):
		require.FailNow(t, "async logger did not finish after the writer was unblocked")
	}
}

func newAsyncTextIOCoreForBlockedWriter(writer zapcore.WriteSyncer) *asyncTextIOCore {
	return NewAsyncTextIOCore(
		&Config{
			Format:                      "text",
			AsyncWriteFlushInterval:     time.Hour,
			AsyncWriteDroppedTimeout:    time.Second,
			AsyncWriteNonDroppableLevel: zap.ErrorLevel.String(),
			AsyncWriteStopTimeout:       50 * time.Millisecond,
			AsyncWritePendingLength:     1,
			AsyncWriteBufferSize:        1,
			AsyncWriteMaxBytesPerLog:    1024,
		},
		writer,
		zap.DebugLevel,
	)
}

type blockingWriteSyncer struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	releaseOnce sync.Once
}

func newBlockingWriteSyncer() *blockingWriteSyncer {
	return &blockingWriteSyncer{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingWriteSyncer) Write(p []byte) (int, error) {
	s.startedOnce.Do(func() { close(s.started) })
	<-s.release
	return len(p), nil
}

func (s *blockingWriteSyncer) Sync() error {
	return nil
}

func (s *blockingWriteSyncer) waitUntilBlocked(t *testing.T) {
	t.Helper()
	select {
	case <-s.started:
	case <-time.After(time.Second):
		require.FailNow(t, "writer was not called")
	}
}

func (s *blockingWriteSyncer) unblock() {
	s.releaseOnce.Do(func() { close(s.release) })
}
