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

// Package l0materializer converts the shared WALSummary's Delete records into
// L0 segments. Observation retains only boundaries; Summary owns all payloads.
package l0materializer

import (
	"context"
	"math"
	"sync"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

// Config wires one VChannel's materialization consumer.
type Config struct {
	VChannel             string
	MaterializedTimeTick uint64
	FlushThrough         uint64
	Reader               walsummary.TransformReader
	MaterializeMaxRows   uint64
	MaterializeMaxBytes  uint64
	Materializer         Materializer
	Runtime              moduleapi.Runtime
	// Invoked after successful output, outside the window lock and before the
	// next batch. The owner mirrors this frontier into its dirty VChannelMeta.
	OnMaterialized func(uint64)
}

// L0Materializer keeps a constant-sized window (M,W] subject to L1 bound L.
// Only one bounded batch executes at a time. No source message is retained.
type L0Materializer struct {
	mu                    sync.Mutex
	materializeMu         sync.Mutex
	vchannel              string
	materializedTimeTick  uint64
	requestedThrough      uint64
	flushThrough          uint64
	backlogThrough        uint64
	activeGoal            uint64
	materializeUpperBound uint64
	reader                walsummary.TransformReader
	materializeMaxRows    uint64
	materializeMaxBytes   uint64
	materializer          Materializer
	runtime               moduleapi.Runtime
	onMaterialized        func(uint64)
	task                  *materializeTask
}

func New(config Config) *L0Materializer {
	if config.MaterializeMaxRows == 0 {
		config.MaterializeMaxRows = defaultMaterializeMaxRows
	}
	if config.MaterializeMaxBytes == 0 {
		config.MaterializeMaxBytes = defaultMaterializeMaxBytes
	}
	return &L0Materializer{
		vchannel: config.VChannel, materializedTimeTick: config.MaterializedTimeTick,
		requestedThrough: config.MaterializedTimeTick, materializeUpperBound: math.MaxUint64,
		reader: config.Reader, materializer: config.Materializer,
		materializeMaxRows: config.MaterializeMaxRows, materializeMaxBytes: config.MaterializeMaxBytes,
		runtime: config.Runtime, onMaterialized: config.OnMaterialized,
		flushThrough: config.FlushThrough,
	}
}

func (m *L0Materializer) MaterializedTimeTick() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.materializedTimeTick
}

// ObserveMessage runs after Summary and Segment observation. Classification
// records only the outer TimeTick; it neither builds nor copies a DeleteEntry.
func (m *L0Materializer) ObserveMessage(msg message.ImmutableMessage) {
	if msg.VChannel() != m.vchannel && msg.VChannel() != "" && !msg.IsPChannelLevel() {
		return
	}
	if messageutil.ClassifyTransformLogMessage(msg) == messageutil.TransformLogKindNone {
		return
	}
	m.mu.Lock()
	m.requestedThrough = max(m.requestedThrough, msg.TimeTick())
	task := m.scheduleLocked()
	m.mu.Unlock()
	m.submit(task)
}

// SetMaterializeUpperBound independently wakes work after L1 final commit.
func (m *L0Materializer) SetMaterializeUpperBound(tt uint64) bool {
	m.mu.Lock()
	if tt == m.materializeUpperBound {
		m.mu.Unlock()
		return false
	}
	m.materializeUpperBound = tt
	task := m.scheduleLocked()
	m.mu.Unlock()
	m.submit(task)
	return task != nil
}

func (m *L0Materializer) HasPendingMaterializeTask() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.task != nil
}

// RequestFlushThrough records an explicit API boundary, after the owner has
// saved the intent in its recovery metadata. L >= F proves all earlier L1
// segments have completed final commit; partial L1 progress cannot force a batch.
func (m *L0Materializer) RequestFlushThrough(through uint64) {
	m.mu.Lock()
	m.flushThrough = max(m.flushThrough, through)
	task := m.scheduleLocked()
	m.mu.Unlock()
	m.submit(task)
}

// RequestBacklogThrough is driven by Summary's existing backlog governance.
// It cannot extend W: restored Summary may contain data ahead of replay.
func (m *L0Materializer) RequestBacklogThrough(through uint64) {
	m.mu.Lock()
	m.backlogThrough = max(m.backlogThrough, through)
	task := m.scheduleLocked()
	m.mu.Unlock()
	m.submit(task)
}

func (m *L0Materializer) scheduleLocked() *materializeTask {
	safe := min(m.requestedThrough, m.materializeUpperBound)
	if m.runtime.Scheduler == nil || m.task != nil || safe <= m.materializedTimeTick {
		return nil
	}
	target := safe
	switch {
	case m.activeGoal > m.materializedTimeTick:
		if m.activeGoal > safe {
			return nil
		}
		target = m.activeGoal
	case m.flushThrough > m.materializedTimeTick && m.flushThrough <= safe:
		target = m.flushThrough
		m.activeGoal = target
	case min(m.backlogThrough, safe) > m.materializedTimeTick:
		target = min(m.backlogThrough, safe)
		m.activeGoal = target
	default:
		if m.reader == nil {
			return nil
		}
		stats := m.reader.TransformStats(m.vchannel, m.materializedTimeTick, safe)
		if stats.Rows < m.materializeMaxRows && stats.Bytes < m.materializeMaxBytes {
			return nil
		}
	}
	m.task = &materializeTask{materializer: m, target: target}
	return m.task
}

func (m *L0Materializer) submit(task *materializeTask) {
	if task != nil {
		m.runtime.Scheduler.Submit(task)
	}
}

func (m *L0Materializer) materialize(ctx context.Context, target uint64) error {
	m.mu.Lock()
	after := m.materializedTimeTick
	target = min(target, m.requestedThrough, m.materializeUpperBound)
	m.mu.Unlock()
	if target <= after {
		return nil
	}
	if m.reader == nil {
		return merr.WrapErrServiceInternalMsg("L0 materializer summary reader is nil")
	}
	batch, err := m.reader.ReadTransform(ctx, m.vchannel, after, target, walsummary.ReadLimits{MaxRows: m.materializeMaxRows, MaxBytes: m.materializeMaxBytes})
	if err != nil {
		return err
	}
	if batch.CoveredThrough <= after {
		return nodescheduler.ErrDelay
	}
	if len(batch.Entries) > 0 {
		if m.materializer == nil {
			return merr.WrapErrServiceInternalMsg("L0 materializer output writer is nil")
		}
		if err := m.materializer.Materialize(ctx, MaterializeRequest{
			VChannel: m.vchannel, TargetTimeTick: batch.CoveredThrough, Entries: batch.Entries,
			MaxRows: m.materializeMaxRows, MaxBytes: m.materializeMaxBytes,
		}); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.materializedTimeTick = batch.CoveredThrough
	if m.materializedTimeTick >= m.activeGoal {
		m.activeGoal = 0
	}
	m.mu.Unlock()
	if m.onMaterialized != nil {
		m.onMaterialized(batch.CoveredThrough)
	}
	return nil
}
