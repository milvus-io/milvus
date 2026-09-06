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

// Package transformlog implements the transform consumer of a vchannel: it
// turns the transform records of the WALSummary into L0 segments.
//
// The transform records are persisted by the pchannel-scoped WALSummary (see
// the walsummary package), which decides entirely on its own when records
// become durable; this package never reads the summary store and never reacts
// to its persistence. It observes the vchannel's messages directly and keeps
// its own in-memory materialization window, so L0 materialization is not
// ordered against summary persistence in any way. Its materialization frontier
// is carried by VChannelMeta (transform_materialized_time_tick), persisted
// together with the vchannel catalog snapshot instead of a dedicated
// transform meta.
package transformlog

import (
	"context"
	"math"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Config carries the wiring of one vchannel's transform consumer.
type Config struct {
	VChannel string
	// MaterializedTimeTick is the initial materialization frontier, restored
	// from VChannelMeta.transform_materialized_time_tick.
	MaterializedTimeTick uint64
	// PendingEntries is the initial in-memory materialization window: the
	// durable records of this vchannel after MaterializedTimeTick, loaded
	// once by recovery. Runtime observation appends to it through
	// ObserveMessage.
	PendingEntries      []*streamingpb.TransformLogEntry
	MaterializeMaxRows  uint64
	MaterializeMaxBytes uint64
	Materializer        Materializer
	Runtime             moduleapi.Runtime
	// OnMaterialized is invoked with the new frontier after a materialization
	// batch commits. It must be non-blocking and must not call back into the
	// TransformLog. The vchannel module uses it to mirror the frontier into
	// VChannelMeta and mark the vchannel snapshot dirty.
	OnMaterialized func(timeTick uint64)
}

type materializeOption struct {
	TargetTimeTick uint64
}

type materializeResult struct {
	Started                 bool
	MaterializedTimeTick    uint64
	MaterializedRows        uint64
	MaterializedBytes       uint64
	HasMaterializedSegments bool
}

// TransformLog is the transform consumer of one vchannel: it materializes the
// summary's transform records into L0 segments.
type TransformLog struct {
	materializeMu sync.Mutex
	mu            sync.Mutex
	vchannel      string

	// materializedTimeTick is the committed materialization frontier: every
	// transform record through this timetick has been emitted as L0 output.
	materializedTimeTick uint64
	// materializeUpperBound is the VChannel-wide L1 safety frontier: the
	// largest timetick materialization may currently reach.
	materializeUpperBound uint64
	// loadedThrough is the newest timetick the recovery-loaded window covers:
	// the durable frontier of this vchannel at restart. Observation skips
	// records at or below it — they were either already committed (at or
	// below materializedTimeTick) or already loaded into the window by
	// recovery. It is a constant set once at construction: live observation
	// only ever sees newer records.
	loadedThrough uint64
	// pending is the in-memory materialization window: the transform records
	// of this vchannel after the committed frontier, in ascending timetick
	// order. Recovery loads its head; observation appends to its tail;
	// committed batches trim its head.
	pending []*streamingpb.TransformLogEntry

	materializeMaxRows  uint64
	materializeMaxBytes uint64
	materializer        Materializer
	runtime             moduleapi.Runtime
	onMaterialized      func(uint64)

	materializeTasks []*transformMaterializeTask
}

// New creates the transform consumer of one vchannel.
func New(config Config) *TransformLog {
	upperBound := uint64(math.MaxUint64)
	if config.MaterializeMaxRows == 0 {
		config.MaterializeMaxRows = defaultMaterializeMaxRows
	}
	if config.MaterializeMaxBytes == 0 {
		config.MaterializeMaxBytes = defaultMaterializeMaxBytes
	}
	// The initial window loaded by recovery may carry records already
	// committed by the restored frontier; trim them defensively.
	pending := config.PendingEntries[:0]
	for _, entry := range config.PendingEntries {
		if entry != nil && entry.GetTimeTick() > config.MaterializedTimeTick {
			pending = append(pending, entry)
		}
	}
	// loadedThrough is the coverage of the recovered window: the newest
	// timetick the summary had made durable for this vchannel. Observation
	// skips records at or below it so recovery replay does not re-append
	// what the loaded window already holds. When the window is empty the
	// committed frontier is the best available bound.
	loadedThrough := config.MaterializedTimeTick
	if len(pending) > 0 {
		loadedThrough = pending[len(pending)-1].GetTimeTick()
	}
	return &TransformLog{
		vchannel:              config.VChannel,
		materializedTimeTick:  config.MaterializedTimeTick,
		materializeUpperBound: upperBound,
		loadedThrough:         loadedThrough,
		pending:               pending,
		materializer:          config.Materializer,
		materializeMaxRows:    config.MaterializeMaxRows,
		materializeMaxBytes:   config.MaterializeMaxBytes,
		runtime:               config.Runtime,
		onMaterialized:        config.OnMaterialized,
	}
}

// MaterializedTimeTick returns the committed materialization frontier.
func (t *TransformLog) MaterializedTimeTick() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.materializedTimeTick
}

// ObserveMessage observes one WAL message of this vchannel. Delete messages
// are appended to the in-memory materialization window immediately and may
// schedule a materialization task; the summary decides persistence entirely on
// its own, so materialization never waits for a flush event. Barrier messages
// (flush/manual flush/drop/truncate, see ClassifyTransformLogMessage) are
// appended as payload-free window entries so the materialization frontier can
// advance past them — a flush boundary has no delete data of its own, but the
// frontier must be able to reach it once every delete record before it has
// been emitted as L0 output.
//
// Precondition: called synchronously on the WAL observation hot path; must be
// non-blocking and must not do metadata I/O.
func (t *TransformLog) ObserveMessage(retained message.RetainedImmutableMessage) {
	if t == nil || retained == nil {
		return
	}
	msg := retained.Message()
	kind := messageutil.ClassifyTransformLogMessage(msg)
	var entry *streamingpb.TransformLogEntry
	switch kind {
	case messageutil.TransformLogKindDelete:
		entry = messageutil.BuildTransformLogEntry(msg, messageutil.TransformEntryOption{})
		if entry == nil {
			return
		}
	case messageutil.TransformLogKindBarrier:
		// A payload-free barrier only marks a data boundary: it carries no
		// delete blocks but still bounds the frontier. Only the owning
		// vchannel's barriers bound its frontier; pchannel-level broadcasts
		// carry no per-vchannel data boundary.
		if msg.VChannel() != t.vchannel {
			return
		}
		entry = &streamingpb.TransformLogEntry{TimeTick: msg.TimeTick()}
	default:
		return
	}
	timetick := entry.GetTimeTick()
	if timetick <= t.materializedTimeTick || timetick <= t.loadedThrough {
		// Already committed, or already loaded into the window by recovery
		// (replay re-observes the recovered backlog).
		return
	}
	t.mu.Lock()
	t.pending = append(t.pending, entry)
	task := t.maybeScheduleMaterializeLocked(true)
	t.mu.Unlock()
	if task != nil && t.runtime.Scheduler != nil {
		t.runtime.Scheduler.Submit(task)
	}
}

// SetMaterializeUpperBound updates the VChannel-wide L1 safety frontier and
// schedules any materialization that can now make progress. Publishing the
// same bound again is a no-op: the bound only changes on segment create /
// cleanup / final-commit transitions, and skipping unchanged publishes keeps
// the WAL observation hot path from rescheduling materialize tasks on every
// accepted insert.
func (t *TransformLog) SetMaterializeUpperBound(timetick uint64) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	if timetick == t.materializeUpperBound {
		t.mu.Unlock()
		return false
	}
	t.materializeUpperBound = timetick
	if t.runtime.Scheduler == nil {
		t.mu.Unlock()
		return false
	}
	task := t.maybeScheduleMaterializeLocked(false)
	t.mu.Unlock()
	if task == nil {
		return false
	}
	t.runtime.Scheduler.Submit(task)
	return true
}

// materializeTargetLocked returns the largest materialization frontier that is
// both covered by the in-memory window and allowed by the current L1 upper
// bound.
func (t *TransformLog) materializeTargetLocked() uint64 {
	if len(t.pending) == 0 {
		return 0
	}
	target := t.pending[len(t.pending)-1].GetTimeTick()
	if target > t.materializeUpperBound {
		target = t.materializeUpperBound
	}
	return target
}

// materialize executes one materialization batch through targetTimeTick.
func (t *TransformLog) materialize(ctx context.Context, opt materializeOption) (materializeResult, error) {
	t.materializeMu.Lock()
	defer t.materializeMu.Unlock()
	t.mu.Lock()
	targetTimeTick := opt.TargetTimeTick
	if targetTimeTick == 0 {
		targetTimeTick = t.materializeTargetLocked()
	}
	if targetTimeTick <= t.materializedTimeTick {
		t.mu.Unlock()
		return materializeResult{}, nil
	}
	maxRows := t.materializeMaxRows
	maxBytes := t.materializeMaxBytes
	t.mu.Unlock()

	work := t.prepareMaterialize(targetTimeTick, maxRows, maxBytes)

	if len(work.Entries) > 0 {
		if t.materializer == nil {
			return materializeResult{}, merr.WrapErrServiceInternalMsg("transform log materializer is nil")
		}
		if err := t.materializer.Materialize(ctx, MaterializeRequest{
			VChannel:       t.vchannel,
			TargetTimeTick: work.TargetTimeTick,
			Entries:        work.Entries,
			MaxRows:        maxRows,
			MaxBytes:       maxBytes,
		}); err != nil {
			return materializeResult{}, err
		}
	}

	t.mu.Lock()
	result := t.commitMaterializeLocked(work)
	var nextTask *transformMaterializeTask
	if t.runtime.Scheduler != nil {
		// Schedule the continuation while the window may still hold records
		// past the committed frontier. This covers both a capped batch
		// (rows/bytes limit) and a frontier the L1 upper bound retracted:
		// records newly appended after the target are materialized next. The
		// current task has not completed yet, so it becomes a predecessor of
		// the continuation and the batches stay strictly sequential.
		if target := t.materializeTargetLocked(); target > t.materializedTimeTick {
			nextTask = t.newMaterializeTaskLocked(target)
		}
	}
	t.mu.Unlock()
	if nextTask != nil {
		t.runtime.Scheduler.Submit(nextTask)
	}
	if t.onMaterialized != nil && work.TargetTimeTick > 0 {
		t.onMaterialized(work.TargetTimeTick)
	}
	return result, nil
}

// prepareMaterialize walks the in-memory window through targetTimeTick and
// caps the batch by rows and bytes. The materialization continues from the
// capped frontier in a follow-up task, so a whole backlog is never built into
// a single Materialize call. The window is fed by direct observation (and
// once, by recovery), so nothing here reads the summary store.
func (t *TransformLog) prepareMaterialize(
	targetTimeTick uint64,
	maxRows uint64,
	maxBytes uint64,
) materializeWork {
	t.mu.Lock()
	defer t.mu.Unlock()
	cursor := t.materializedTimeTick
	work := materializeWork{TargetTimeTick: cursor}
	lastIncluded := cursor
	for _, entry := range t.pending {
		tt := entry.GetTimeTick()
		if tt <= cursor {
			continue
		}
		if tt > targetTimeTick {
			break
		}
		if isTransformDeleteEntry(entry) {
			rows := transformLogEntryRows(entry)
			bytes := uint64(proto.Size(entry))
			if work.Rows > 0 && (work.Rows+rows > maxRows || work.Bytes+bytes > maxBytes) {
				work.TargetTimeTick = lastIncluded
				return work
			}
			work.Entries = append(work.Entries, entry)
			work.Rows += rows
			work.Bytes += bytes
		}
		// Payload-free barrier entries carry no delete data but still advance
		// the materialized frontier: once every delete record before the
		// barrier has been emitted as L0 output, the barrier itself has no
		// remaining work, so the frontier may reach it.
		lastIncluded = tt
	}
	work.TargetTimeTick = lastIncluded
	return work
}

func (t *TransformLog) commitMaterializeLocked(work materializeWork) materializeResult {
	if work.TargetTimeTick <= t.materializedTimeTick {
		return materializeResult{}
	}
	t.materializedTimeTick = work.TargetTimeTick
	// Trim the consumed head of the window: every record through the committed
	// frontier has been emitted as L0 output and is no longer needed.
	trim := 0
	for trim < len(t.pending) && t.pending[trim].GetTimeTick() <= t.materializedTimeTick {
		trim++
	}
	if trim > 0 {
		t.pending = append([]*streamingpb.TransformLogEntry(nil), t.pending[trim:]...)
	}
	return materializeResult{
		Started:                 true,
		MaterializedTimeTick:    work.TargetTimeTick,
		MaterializedRows:        work.Rows,
		MaterializedBytes:       work.Bytes,
		HasMaterializedSegments: len(work.Entries) > 0,
	}
}

type materializeWork struct {
	TargetTimeTick uint64
	Entries        []*streamingpb.TransformLogEntry
	Rows           uint64
	Bytes          uint64
}

// isTransformDeleteEntry reports whether an entry carries a delete payload.
func isTransformDeleteEntry(entry *streamingpb.TransformLogEntry) bool {
	if entry == nil {
		return false
	}
	return entry.GetDelete() != nil
}
