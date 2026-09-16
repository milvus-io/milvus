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

func (m *L0Materializer) scheduleLocked() *materializeTask {
	if m.runtime.Scheduler == nil || m.task != nil || min(m.requestedThrough, m.materializeUpperBound) <= m.materializedTimeTick {
		return nil
	}
	m.task = &materializeTask{materializer: m}
	return m.task
}

func (m *L0Materializer) submit(task *materializeTask) {
	if task != nil {
		m.runtime.Scheduler.Submit(task)
	}
}

func (m *L0Materializer) materialize(ctx context.Context) error {
	m.mu.Lock()
	after, target := m.materializedTimeTick, min(m.requestedThrough, m.materializeUpperBound)
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
	m.mu.Unlock()
	if m.onMaterialized != nil {
		m.onMaterialized(batch.CoveredThrough)
	}
	return nil
}
