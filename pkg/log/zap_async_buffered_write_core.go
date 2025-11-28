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

package log

import (
	"fmt"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// asyncTextIOCore is a wrapper around the textIOCore that writes the logs to the underlying buffered write syncer.
var _ zapcore.Core = (*asyncTextIOCore)(nil)

// NewAsyncTextIOCore creates a new async text IO core.
func NewAsyncTextIOCore(cfg *Config, ws zapcore.WriteSyncer, enab zapcore.LevelEnabler) *asyncTextIOCore {
	enc := newZapTextEncoder(cfg)
	bws := &zapcore.BufferedWriteSyncer{
		WS:            ws,
		Size:          cfg.AsyncWriteBufferSize,
		FlushInterval: cfg.AsyncWriteFlushInterval,
	}
	nonDroppableLevel, _ := zapcore.ParseLevel(cfg.AsyncWriteNonDroppableLevel)
	asyncTextIOCore := &asyncTextIOCore{
		LevelEnabler:        enab,
		notifier:            syncutil.NewAsyncTaskNotifier[struct{}](),
		enc:                 enc,
		bws:                 bws,
		pending:             make(chan *entryItem, cfg.AsyncWritePendingLength),
		writeDroppedTimeout: cfg.AsyncWriteDroppedTimeout,
		nonDroppableLevel:   nonDroppableLevel,
		stopTimeout:         cfg.AsyncWriteStopTimeout,
		maxBytesPerLog:      cfg.AsyncWriteMaxBytesPerLog,
	}
	go asyncTextIOCore.background()
	return asyncTextIOCore
}

// asyncTextIOCore is a wrapper around the textIOCore that writes the logs to the underlying buffered write syncer.
type asyncTextIOCore struct {
	zapcore.LevelEnabler

	notifier            *syncutil.AsyncTaskNotifier[struct{}]
	enc                 zapcore.Encoder
	bws                 *zapcore.BufferedWriteSyncer
	pending             chan *entryItem // the incoming new write requests
	writeDroppedTimeout time.Duration
	nonDroppableLevel   zapcore.Level
	stopTimeout         time.Duration
	maxBytesPerLog      int
}

// entryItem is the item to write to the underlying buffered write syncer.
type entryItem struct {
	buf   *buffer.Buffer
	level zapcore.Level
}

// With returns a copy of the Core with the given fields added.
func (s *asyncTextIOCore) With(fields []zapcore.Field) zapcore.Core {
	enc := s.enc.Clone()
	switch e := enc.(type) {
	case *textEncoder:
		e.addFields(fields)
	case zapcore.ObjectEncoder:
		for _, field := range fields {
			field.AddTo(e)
		}
	default:
		panic(fmt.Sprintf("unsupported encode type: %T for With operation", enc))
	}
	return &asyncTextIOCore{
		LevelEnabler:        s.LevelEnabler,
		notifier:            s.notifier,
		enc:                 s.enc.Clone(),
		bws:                 s.bws,
		pending:             s.pending,
		writeDroppedTimeout: s.writeDroppedTimeout,
		stopTimeout:         s.stopTimeout,
		maxBytesPerLog:      s.maxBytesPerLog,
	}
}

// Check checks if the entry is enabled by the level enabler.
func (s *asyncTextIOCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if s.Enabled(ent.Level) {
		return ce.AddCore(ent, s)
	}
	return ce
}

// Write writes the underlying buffered write syncer and buffers the writes in a channel.
// asyncTextIOCore doesn't promise the write operation is done.
// the write operation will be dropped if the buffer is full or the underlying buffered write syncer is blocked.
func (s *asyncTextIOCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	buf, err := s.enc.EncodeEntry(ent, fields)
	if err != nil {
		return err
	}
	entry := &entryItem{
		buf:   buf,
		level: ent.Level,
	}

	length := buf.Len()
	if length == 0 {
		return nil
	}
	var writeDroppedTimeout <-chan time.Time
	if ent.Level < s.nonDroppableLevel {
		writeDroppedTimeout = time.After(s.writeDroppedTimeout)
	}
	select {
	case s.pending <- entry:
		metrics.LoggingPendingWriteLength.Inc()
		metrics.LoggingPendingWriteBytes.Add(float64(length))
	case <-writeDroppedTimeout:
		metrics.LoggingDroppedWrites.Inc()
		// drop the entry if the write is dropped due to timeout
		buf.Free()
	}
	return nil
}

// Sync syncs the underlying buffered write syncer.
func (s *asyncTextIOCore) Sync() error {
	return nil
}

// background is the background goroutine to write the logs to the underlying buffered write syncer.
func (s *asyncTextIOCore) background() {
	defer func() {
		s.flushPendingWriteWithTimeout()
		s.notifier.Finish(struct{}{})
	}()

	for {
		select {
		case <-s.notifier.Context().Done():
			return
		case ent := <-s.pending:
			s.consumeEntry(ent)
		}
	}
}

// consumeEntry write the entry to the underlying buffered write syncer and free the buffer.
func (s *asyncTextIOCore) consumeEntry(ent *entryItem) {
	length := ent.buf.Len()
	metrics.LoggingPendingWriteLength.Dec()
	metrics.LoggingPendingWriteBytes.Sub(float64(length))
	writes := s.getWriteBytes(ent)
	if _, err := s.bws.Write(writes); err != nil {
		metrics.LoggingIOFailure.Inc()
	}
	ent.buf.Free()
	if ent.level > zapcore.ErrorLevel {
		s.bws.Sync()
	}
}

// getWriteBytes gets the bytes to write to the underlying buffered write syncer.
// if the length of the write exceeds the max bytes per log, it will truncate the write and return the truncated bytes.
// otherwise, it will return the original bytes.
func (s *asyncTextIOCore) getWriteBytes(ent *entryItem) []byte {
	length := ent.buf.Len()
	writes := ent.buf.Bytes()

	if length > s.maxBytesPerLog {
		// truncate the write if it exceeds the max bytes per log
		metrics.LoggingTruncatedWrites.Inc()
		metrics.LoggingTruncatedWriteBytes.Add(float64(length - s.maxBytesPerLog))

		end := writes[length-1]
		writes = writes[:s.maxBytesPerLog]
		writes[len(writes)-1] = end
	}
	return writes
}

// flushPendingWriteWithTimeout flushes the pending write with a timeout.
func (s *asyncTextIOCore) flushPendingWriteWithTimeout() {
	done := make(chan struct{})
	go s.flushAllPendingWrites(done)

	select {
	case <-time.After(s.stopTimeout):
	case <-done:
	}
}

// flushAllPendingWrites flushes all the pending writes to the underlying buffered write syncer.
func (s *asyncTextIOCore) flushAllPendingWrites(done chan struct{}) {
	defer func() {
		if err := s.bws.Stop(); err != nil {
			metrics.LoggingIOFailure.Inc()
		}
		close(done)
	}()

	for {
		select {
		case ent := <-s.pending:
			s.consumeEntry(ent)
		default:
			return
		}
	}
}

func (s *asyncTextIOCore) Stop() {
	s.notifier.Cancel()
	s.notifier.BlockUntilFinish()
}
