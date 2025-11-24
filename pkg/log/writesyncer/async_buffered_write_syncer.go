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

package writesyncer

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

const (
	metricNamespace = "milvus"
	metricSubsystem = "logging"
)

// AsyncBufferedWriteSyncer is a wrapper around the zapcore.BufferedWriteSyncer.
var _ zapcore.WriteSyncer = (*AsyncBufferedWriteSyncer)(nil)

// AsyncBufferedWriteSyncerConfig is the config for the AsyncBufferedWriteSyncer.
type AsyncBufferedWriteSyncerConfig struct {
	WS                  zapcore.WriteSyncer // the underlying write syncer to write the logs
	FlushInterval       time.Duration       // the interval to flush the logs
	WriteDroppedTimeout time.Duration       // the timeout to drop the write request if the buffer is full
	StopTimeout         time.Duration       // the timeout to stop the AsyncBufferedWriteSyncer
	PendingItemSize     int                 // the size of the pending write requests
	WriteBufferSize     int                 // the size of the write buffer
}

func (config *AsyncBufferedWriteSyncerConfig) initialize() {
	if config.WriteDroppedTimeout <= 0 {
		config.WriteDroppedTimeout = 100 * time.Millisecond
	}
	if config.StopTimeout <= 0 {
		config.StopTimeout = 1 * time.Second
	}
	if config.PendingItemSize <= 0 {
		config.PendingItemSize = 128
	}
	if config.WriteBufferSize <= 0 {
		config.WriteBufferSize = 256 * 1024
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 10 * time.Second
	}
}

// NewAsyncBufferedWriteSyncer creates a new AsyncBufferedWriteSyncer.
func NewAsyncBufferedWriteSyncer(config AsyncBufferedWriteSyncerConfig) *AsyncBufferedWriteSyncer {
	config.initialize()

	s := &AsyncBufferedWriteSyncer{
		BWS: zapcore.BufferedWriteSyncer{
			WS:            config.WS,
			Size:          config.WriteBufferSize,
			FlushInterval: config.FlushInterval,
		},
		notifier:            syncutil.NewAsyncTaskNotifier[struct{}](),
		pending:             make(chan []byte, config.PendingItemSize),
		writeDroppedTimeout: config.WriteDroppedTimeout,
		stopTimeout:         config.StopTimeout,
	}
	go s.background()
	return s
}

// AsyncBufferedWriteSyncer is a wrapper around the zapcore.BufferedWriteSyncer.
// it will write the underlying log asynchronously in a background goroutine.
// so the write/sync operation is not guaranteed to be done.
// use it to promise the log written will not block the main thread processing.
type AsyncBufferedWriteSyncer struct {
	BWS zapcore.BufferedWriteSyncer

	notifier            *syncutil.AsyncTaskNotifier[struct{}]
	pending             chan []byte // the incoming new write requests
	writeDroppedTimeout time.Duration
	stopTimeout         time.Duration
	metrics             AsyncBufferedWriteSyncerMetrics
}

type AsyncBufferedWriteSyncerMetrics struct {
	DroppedWrites prometheus.Gauge
	IOFailure     prometheus.Gauge
}

// Write writes the underlying buffered write syncer and buffers the writes in a channel.
// AsyncBufferedWriteSyncer doesn't promise the write operation is done.
// the write operation will be dropped if the buffer is full or the underlying buffered write syncer is blocked.
func (s *AsyncBufferedWriteSyncer) Write(p []byte) (n int, err error) {
	length := len(p)
	select {
	case s.pending <- p:
		metrics.LoggingPendingWriteLength.Inc()
		metrics.LoggingPendingWriteBytes.Add(float64(length))
	case <-time.After(s.writeDroppedTimeout):
		metrics.LoggingDroppedWrites.Inc()
		return len(p), nil
	}
	return len(p), nil
}

// Sync waits for the pending writes to be flushed and returns the error from the underlying buffered write syncer.
// AsyncBufferedWriteSyncer just do nothing with the sync operation.
func (s *AsyncBufferedWriteSyncer) Sync() error {
	return nil
}

// background is the background goroutine to write the logs to the underlying buffered write syncer.
func (s *AsyncBufferedWriteSyncer) background() {
	defer func() {
		s.flushPendingWriteWithTimeout()
		s.notifier.Finish(struct{}{})
	}()

	for {
		select {
		case <-s.notifier.Context().Done():
			return
		case p := <-s.pending:
			length := len(p)
			metrics.LoggingPendingWriteLength.Dec()
			metrics.LoggingPendingWriteBytes.Sub(float64(length))
			if _, err := s.BWS.Write(p); err != nil {
				s.metrics.IOFailure.Inc()
			}
		}
	}
}

// flushPendingWriteWithTimeout flushes the pending write with a timeout.
func (s *AsyncBufferedWriteSyncer) flushPendingWriteWithTimeout() {
	done := make(chan struct{})
	go s.flushAllPendingWrites(done)

	select {
	case <-time.After(s.stopTimeout):
	case <-done:
	}
}

// flushAllPendingWrites flushes all the pending writes to the underlying buffered write syncer.
func (s *AsyncBufferedWriteSyncer) flushAllPendingWrites(done chan struct{}) {
	defer func() {
		if err := s.BWS.Stop(); err != nil {
			metrics.LoggingIOFailure.Inc()
		}
		close(done)
	}()

	for {
		select {
		case p := <-s.pending:
			if _, err := s.BWS.Write(p); err != nil {
				metrics.LoggingIOFailure.Inc()
			}
		default:
			return
		}
	}
}

func (s *AsyncBufferedWriteSyncer) Stop() {
	s.notifier.Cancel()
	s.notifier.BlockUntilFinish()
}
