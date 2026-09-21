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

package l0materializer

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// WALConfig wires the temporary retained-message consumer.
type WALConfig struct {
	VChannel             string
	MaterializedTimeTick uint64
	MaterializeMaxBytes  uint64
	MaterializeMaxRows   uint64
	Runtime              moduleapi.Runtime
	Materializer         Materializer
	OnMaterialized       func(uint64)
	// Earlier L1 segments must exist in DataCoord before L0 can be compacted.
	// This waits for registration only, never for L1 flush or final commit.
	GrowingSegmentsRegistered func(uint64) bool
}

// WALMaterializer holds Delete handles until L0 output is registered with
// DataCoord. This makes the global recovery checkpoint safe for legacy queries.
// TODO: Remove after enabling queryview and reconnect the Summary materializer.
// Unlike the Summary consumer, it can write L0 while L1 segments are growing;
// DataCoord's L0 compaction policy owns the dependency on those segments.
type WALMaterializer struct {
	growingSegmentsRegistered func(uint64) bool
	mu                        sync.Mutex
	vchannel                  string
	materialized              uint64
	observed                  uint64
	pending                   []message.RetainedImmutableMessage
	pendingBytes              uint64
	pendingSince              time.Time
	flushThrough              uint64
	maxBytes                  uint64
	maxRows                   uint64
	runtime                   moduleapi.Runtime
	writer                    Materializer
	onMaterialized            func(uint64)
	task                      *walMaterializeTask
	terminalErr               error
}

func NewWALMaterializer(config WALConfig) *WALMaterializer {
	return &WALMaterializer{
		vchannel:                  config.VChannel,
		materialized:              config.MaterializedTimeTick,
		observed:                  config.MaterializedTimeTick,
		maxBytes:                  config.MaterializeMaxBytes,
		maxRows:                   config.MaterializeMaxRows,
		runtime:                   config.Runtime,
		writer:                    config.Materializer,
		onMaterialized:            config.OnMaterialized,
		growingSegmentsRegistered: config.GrowingSegmentsRegistered,
	}
}

func (m *WALMaterializer) MaterializedTimeTick() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.materialized
}

func (m *WALMaterializer) ObserveMessage(retained message.RetainedImmutableMessage) {
	msg := retained.Message()
	flush := isL0FlushMessage(msg.MessageType())
	deleted := messageutil.ClassifyTransformLogMessage(msg) == messageutil.TransformLogKindDelete
	m.mu.Lock()
	if m.terminalErr != nil {
		m.mu.Unlock()
		if deleted || flush {
			retained.IntoPoisoned()
		}
		return
	}
	if msg.TimeTick() <= m.observed {
		m.mu.Unlock()
		return
	}
	m.observed = msg.TimeTick()
	if !deleted && !flush {
		m.mu.Unlock()
		return
	}
	if len(m.pending) == 0 {
		m.pendingSince = time.Now()
	}
	m.pending = append(m.pending, retained.Clone())
	if deleted {
		// Account only Delete payloads, even for mixed Insert/Delete transactions.
		m.pendingBytes += deleteBytes(msg)
	}
	if flush {
		m.flushThrough = msg.TimeTick()
	}
	task := m.scheduleLocked()
	m.mu.Unlock()
	m.submit(task)
}

// isL0FlushMessage identifies explicit WAL boundaries for L0 materialization.
func isL0FlushMessage(t message.MessageType) bool {
	switch t {
	case message.MessageTypeManualFlush, message.MessageTypeFlushAll,
		message.MessageTypeDropCollection, message.MessageTypeDropPartition,
		message.MessageTypeTruncateCollection, message.MessageTypeAlterWAL, message.MessageTypeCreateSnapshot:
		return true
	default:
		return false
	}
}

func deleteBytes(msg message.ImmutableMessage) uint64 {
	var size uint64
	count := func(msg message.ImmutableMessage) error {
		if msg.MessageType() == message.MessageTypeDelete {
			body := message.MustAsImmutableDeleteMessageV1(msg).MustBody()
			size += uint64(proto.Size(body.GetPrimaryKeys()) + 8*messageutil.PrimaryKeyCount(body.GetPrimaryKeys()))
		}
		return nil
	}
	if msg.MessageType() == message.MessageTypeTxn {
		_ = message.AsImmutableTxnMessage(msg).RangeOver(count)
	} else {
		_ = count(msg)
	}
	return size
}

// RequestPersistThrough shares the SegmentView stall/pressure contract. A batch
// may include later pending messages but never waits for additional input.
func (m *WALMaterializer) RequestPersistThrough(through uint64) {
	m.mu.Lock()
	m.flushThrough = max(m.flushThrough, min(through, m.observed))
	task := m.scheduleLocked()
	m.mu.Unlock()
	m.submit(task)
}

// FlushStale is driven by one shared PChannel timer, including during silence.
func (m *WALMaterializer) FlushStale(now time.Time, maxAge time.Duration) {
	m.mu.Lock()
	if len(m.pending) > 0 && now.Sub(m.pendingSince) >= maxAge {
		m.flushThrough = max(m.flushThrough, m.pending[len(m.pending)-1].Message().TimeTick())
	}
	task := m.scheduleLocked()
	m.mu.Unlock()
	m.submit(task)
}

func (m *WALMaterializer) scheduleLocked() *walMaterializeTask {
	if m.terminalErr != nil || m.task != nil || len(m.pending) == 0 || m.runtime.Scheduler == nil {
		return nil
	}
	if m.flushThrough < m.pending[0].Message().TimeTick() && (m.maxBytes == 0 || m.pendingBytes < m.maxBytes) {
		return nil
	}
	// Every explicit WAL flush message ends a batch, including when it queues
	// behind an active task. Later deletes belong to the next batch.
	end := len(m.pending)
	for i, handle := range m.pending {
		if isL0FlushMessage(handle.Message().MessageType()) {
			end = i + 1
			break
		}
	}
	handles := m.pending[:end:end]
	task := &walMaterializeTask{owner: m, handles: handles, through: handles[len(handles)-1].Message().TimeTick()}
	m.pending = m.pending[end:]
	for _, handle := range handles {
		m.pendingBytes -= deleteBytes(handle.Message())
	}
	if len(m.pending) == 0 {
		m.pending = nil
		m.pendingSince = time.Time{}
	}
	m.task = task
	return task
}

func (m *WALMaterializer) submit(task *walMaterializeTask) {
	if task != nil {
		m.runtime.Scheduler.Submit(task)
	}
}

type walMaterializeTask struct {
	mu      sync.Mutex
	owner   *WALMaterializer
	handles []message.RetainedImmutableMessage
	through uint64
	done    bool
	err     error
}

func (t *walMaterializeTask) Execute(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.done {
		return t.err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m := t.owner
	if m.growingSegmentsRegistered != nil && !m.growingSegmentsRegistered(t.through) {
		return nodescheduler.ErrDelay
	}
	var entries []*streamingpb.TransformLogEntry
	positions := make(map[uint64]*msgpb.MsgPosition)
	for _, handle := range t.handles {
		entry := messageutil.BuildTransformLogEntry(handle.Message(), messageutil.TransformEntryOption{})
		if entry != nil && entry.GetDelete() != nil {
			entries = append(entries, entry)
			positions[entry.GetTimeTick()] = utility.NewMessagePosition(handle.Message(), m.vchannel)
		}
	}
	if len(entries) > 0 {
		if m.writer == nil {
			return errors.Mark(merr.WrapErrServiceInternalMsg("L0 materializer output writer is nil"), nodescheduler.ErrDelay)
		}
		if err := m.writer.Materialize(ctx, MaterializeRequest{
			VChannel: m.vchannel, TargetTimeTick: t.through,
			Entries: entries, MaxRows: m.maxRows, MaxBytes: m.maxBytes,
			StartPositions: positions,
			Checkpoint:     utility.NewMessagePosition(t.handles[len(t.handles)-1].Message(), m.vchannel),
		}); err != nil {
			if errors.Is(err, merr.ErrChannelMisrouted) {
				t.poison(ctx, err)
				return err
			}
			return errors.Mark(err, nodescheduler.ErrDelay)
		}
	}
	// Install dirty metadata outside the buffer lock, before any handle can
	// complete. The publisher saves that snapshot before the global checkpoint.
	if m.onMaterialized != nil {
		m.onMaterialized(t.through)
	}
	m.mu.Lock()
	m.materialized = t.through
	m.task = nil
	next := m.scheduleLocked()
	m.mu.Unlock()
	t.done = true
	for _, handle := range t.handles {
		handle.Release()
	}
	t.handles = nil
	m.submit(next)
	return nil
}

// poison terminates this old owner's materializer without completing its WAL
// prefix. A new owner replays those messages; poison is only local state.
func (t *walMaterializeTask) poison(ctx context.Context, err error) {
	m := t.owner
	m.mu.Lock()
	m.terminalErr = err
	pending := m.pending
	m.pending = nil
	m.pendingBytes = 0
	m.pendingSince = time.Time{}
	m.task = nil
	m.mu.Unlock()

	t.done = true
	t.err = err
	for _, handle := range t.handles {
		handle.PoisonedRelease()
	}
	t.handles = nil
	for _, handle := range pending {
		handle.PoisonedRelease()
	}
	mlog.Warn(ctx, "L0 materializer lost WAL ownership, poisoned pending messages",
		mlog.String("vchannel", m.vchannel), mlog.Err(err))
}
