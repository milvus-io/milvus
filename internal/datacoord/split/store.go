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

// Package split is the shard split manager of DataCoord: the record of each
// split, the DataCoord half of the SplitShard ack callback, and the trigger
// that decides to split a shard.
//
// Every state of a split on a cluster is the effect of consuming the
// SplitShard message on that cluster. The trigger only issues the message; it
// writes nothing. A cluster that receives the message through replication
// never runs the trigger and ends with the same record.
package split

import (
	"context"
	"slices"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Catalog is the part of the DataCoord catalog the store persists into.
type Catalog interface {
	SaveSplitShardTask(ctx context.Context, task *datapb.SplitShardTask) error
	ListSplitShardTask(ctx context.Context) ([]*datapb.SplitShardTask, error)
	DropSplitShardTask(ctx context.Context, task *datapb.SplitShardTask) error
}

// Target is one target shard of a split as its message names it.
type Target struct {
	VChannel string
	// GenesisTimeTick is the time tick of the SplitShard copy on the target.
	GenesisTimeTick uint64
	// Residues are the residues of the primary keys the target owns, taken
	// against Record.RoutingModulus.
	Residues []uint64
}

// Record is one split as the SplitShard ack callback observes it. Every time
// tick is one of this cluster's own WAL.
type Record struct {
	TaskID       int64
	CollectionID int64
	Source       string
	Targets      []Target
	// SwitchTimeTick is T_switch: the time tick of the copy on the source.
	SwitchTimeTick uint64
	// RoutingModulus is the modulus of the routing after the split, from the
	// message. Zero when the message carries no routing.
	RoutingModulus uint64
}

// Store is the record DataCoord keeps of every shard split this cluster has
// consumed, and the only writer of it. The catalog is written before the
// cache, so a reader never sees a state that a restart would lose.
//
// A task returned by the store is shared and must not be modified.
type Store struct {
	catalog Catalog

	mu    sync.RWMutex
	tasks map[int64]*datapb.SplitShardTask
	// changed is closed and replaced by every write, to wake the waiters.
	changed chan struct{}
}

// NewStore loads the split records of the catalog.
func NewStore(ctx context.Context, catalog Catalog) (*Store, error) {
	tasks, err := catalog.ListSplitShardTask(ctx)
	if err != nil {
		return nil, merr.Wrap(err, "list the shard split tasks")
	}
	s := &Store{
		catalog: catalog,
		tasks:   make(map[int64]*datapb.SplitShardTask, len(tasks)),
		changed: make(chan struct{}),
	}
	for _, task := range tasks {
		s.tasks[task.GetTaskId()] = task
	}
	s.refreshMetricsLocked()
	if len(tasks) > 0 {
		mlog.Info(ctx, "shard split tasks recovered", mlog.Int("count", len(tasks)))
	}
	return s, nil
}

// InWindow reports whether a split in this state still has its source served
// on this cluster: from the first consumer of its message to its adoption.
func InWindow(state datapb.SplitShardTaskState) bool {
	return state < datapb.SplitShardTaskState_SplitShardTaskAdopted
}

func targetVChannels(task *datapb.SplitShardTask) []string {
	vchannels := make([]string, 0, len(task.GetTargets()))
	for _, target := range task.GetTargets() {
		vchannels = append(vchannels, target.GetVchannel())
	}
	return vchannels
}

// Get returns the record of a split, nil if this cluster has none.
func (s *Store) Get(taskID int64) *datapb.SplitShardTask {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tasks[taskID]
}

// List returns every record, ordered by task id.
func (s *Store) List() []*datapb.SplitShardTask {
	return s.filter(func(*datapb.SplitShardTask) bool { return true })
}

func (s *Store) filter(keep func(task *datapb.SplitShardTask) bool) []*datapb.SplitShardTask {
	s.mu.RLock()
	defer s.mu.RUnlock()
	tasks := make([]*datapb.SplitShardTask, 0, len(s.tasks))
	for _, task := range s.tasks {
		if keep(task) {
			tasks = append(tasks, task)
		}
	}
	slices.SortFunc(tasks, func(a, b *datapb.SplitShardTask) int { return int(a.GetTaskId() - b.GetTaskId()) })
	return tasks
}

// WindowOf returns the split of the collection that is in its window on this
// cluster, nil if there is none. A collection has at most one: no split is
// issued while another one of the collection is in its window.
func (s *Store) WindowOf(collectionID int64) *datapb.SplitShardTask {
	tasks := s.filter(func(task *datapb.SplitShardTask) bool {
		return task.GetCollectionId() == collectionID && InWindow(task.GetState())
	})
	if len(tasks) == 0 {
		return nil
	}
	return tasks[0]
}

// WindowCount returns the number of splits in their window on this cluster.
func (s *Store) WindowCount() int {
	return len(s.filter(func(task *datapb.SplitShardTask) bool { return InWindow(task.GetState()) }))
}

// IsL0CompactionSuspended reports whether L0 compaction on the vchannel is
// suspended: its source has not been adopted and taken out of service yet.
// The coordinator explicitly resumes L0 only after g2 is published and g1
// views are Down; adoption alone does not release this guard.
func (s *Store) IsL0CompactionSuspended(vchannel string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, task := range s.tasks {
		if !task.GetL0Resumed() && slices.Contains(targetVChannels(task), vchannel) {
			return true
		}
	}
	return false
}

// TransformPins returns, for each target of a split of the collection that is
// still retains its history, the time tick its Transform frontier is pinned
// at. The pin remains after adoption until accumulated L0 history is folded.
//
// The pin of a target is T_switch. The genesis of a target is above T_switch
// when the copy on the source is appended first; if a cluster ever appended it
// below, the pin is the genesis, which keeps the whole stream of the target.
func (s *Store) TransformPins(collectionID int64) map[string]uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var pins map[string]uint64
	for _, task := range s.tasks {
		if task.GetCollectionId() != collectionID || task.GetPinsReleased() ||
			task.GetState() < datapb.SplitShardTaskState_SplitShardTaskWindowOpen {
			continue
		}
		for _, target := range task.GetTargets() {
			if pins == nil {
				pins = make(map[string]uint64)
			}
			pins[target.GetVchannel()] = TransformPin(task.GetSwitchTimeTick(), target.GetGenesisTimeTick())
		}
	}
	return pins
}

// TransformPin is the time tick the Transform frontier of a target is pinned
// at, see TransformPins.
func TransformPin(switchTimeTick uint64, genesisTimeTick uint64) uint64 {
	return min(switchTimeTick, genesisTimeTick)
}

// Sources returns the source vchannels of the splits of the collection whose
// state is below the given one, ordered by task id.
func (s *Store) Sources(collectionID int64, below datapb.SplitShardTaskState) []string {
	tasks := s.filter(func(task *datapb.SplitShardTask) bool {
		return task.GetCollectionId() == collectionID && task.GetState() < below
	})
	sources := make([]string, 0, len(tasks))
	for _, task := range tasks {
		sources = append(sources, task.GetSourceVchannel())
	}
	return sources
}

// VChannels returns every vchannel a recorded split of the collection names,
// as its source or as a target.
func (s *Store) VChannels(collectionID int64) []string {
	tasks := s.filter(func(task *datapb.SplitShardTask) bool { return task.GetCollectionId() == collectionID })
	var vchannels []string
	for _, task := range tasks {
		vchannels = append(vchannels, task.GetSourceVchannel())
		vchannels = append(vchannels, targetVChannels(task)...)
	}
	return vchannels
}

// FlushableVChannels returns the vchannels of the collection whose flushes a
// split record lets into the DataView although RootCoord may not list them:
// the source of every record, and the targets of every record that pins their
// Transform frontier (state "window open" or later).
//
// A pending record names its targets but pins nothing, and the targets are
// not declared before the ack callback runs. On a secondary the source copy is
// acknowledged, and the record is pending, before the targets take their
// first replicated write; a target flush accepted then would raise the
// frontier of the target above T_switch for good. So a pending record does not
// make its targets flushable.
func (s *Store) FlushableVChannels(collectionID int64) []string {
	tasks := s.filter(func(task *datapb.SplitShardTask) bool { return task.GetCollectionId() == collectionID })
	var vchannels []string
	for _, task := range tasks {
		vchannels = append(vchannels, task.GetSourceVchannel())
		if task.GetState() >= datapb.SplitShardTaskState_SplitShardTaskWindowOpen {
			vchannels = append(vchannels, targetVChannels(task)...)
		}
	}
	return vchannels
}

// Record records a split from its ack callback and opens its window.
//
// It is idempotent: a split that is recorded keeps the record it has, time
// ticks included, so a callback entered again with the ticks of another
// attempt changes nothing. A split whose source was acknowledged before the
// callback ran is recorded with its residual already flushed.
func (s *Store) Record(ctx context.Context, record Record) (*datapb.SplitShardTask, error) {
	if record.TaskID == 0 || record.Source == "" || record.SwitchTimeTick == 0 || len(record.Targets) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("shard split task %d of collection %d is recorded without its source, its targets or T_switch",
			record.TaskID, record.CollectionID)
	}
	targets := make([]*datapb.SplitShardTaskTarget, 0, len(record.Targets))
	for _, target := range record.Targets {
		if target.VChannel == "" || target.GenesisTimeTick == 0 {
			return nil, merr.WrapErrServiceInternalMsg("target %q of shard split task %d is recorded without its genesis time tick",
				target.VChannel, record.TaskID)
		}
		targets = append(targets, &datapb.SplitShardTaskTarget{
			Vchannel:        target.VChannel,
			GenesisTimeTick: target.GenesisTimeTick,
			Residues:        slices.Clone(target.Residues),
		})
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	next := &datapb.SplitShardTask{
		TaskId:         record.TaskID,
		CollectionId:   record.CollectionID,
		SourceVchannel: record.Source,
		Targets:        targets,
		SwitchTimeTick: record.SwitchTimeTick,
		RoutingModulus: record.RoutingModulus,
		State:          datapb.SplitShardTaskState_SplitShardTaskWindowOpen,
		RecordTime:     time.Now().Unix(),
	}
	if existing := s.tasks[record.TaskID]; existing != nil {
		if err := checkSameSplit(existing, next); err != nil {
			return nil, err
		}
		if existing.GetState() >= datapb.SplitShardTaskState_SplitShardTaskWindowOpen {
			return existing, nil
		}
		next.SourceAcked = existing.GetSourceAcked()
		next.RecordTime = existing.GetRecordTime()
	}
	if next.GetSourceAcked() {
		next.State = datapb.SplitShardTaskState_SplitShardTaskResidualFlushed
		next.ResidualFlushedTime = time.Now().Unix()
	}
	if err := s.saveLocked(ctx, next); err != nil {
		return nil, err
	}
	return next, nil
}

// checkSameSplit refuses a record that names other shards than the one this
// cluster holds under the same task id.
func checkSameSplit(existing *datapb.SplitShardTask, next *datapb.SplitShardTask) error {
	if existing.GetCollectionId() != next.GetCollectionId() ||
		existing.GetSourceVchannel() != next.GetSourceVchannel() ||
		!slices.Equal(targetVChannels(existing), targetVChannels(next)) {
		return merr.WrapErrServiceInternalMsg(
			"shard split task %d is recorded for collection %d, source %s, targets %v; it cannot be recorded for collection %d, source %s, targets %v",
			existing.GetTaskId(), existing.GetCollectionId(), existing.GetSourceVchannel(), targetVChannels(existing),
			next.GetCollectionId(), next.GetSourceVchannel(), targetVChannels(next))
	}
	return nil
}

// MarkSourceAcked records that the StreamingNode of the source has
// acknowledged the SplitShard copy on the source: the residual of the source
// is flushed. A window that is open moves to the residual-flushed state. If
// the ack callback has not recorded the split yet, the fact is kept in a
// pending record that the callback completes.
func (s *Store) MarkSourceAcked(ctx context.Context, taskID int64, collectionID int64, source string, targets []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.tasks[taskID]
	var next *datapb.SplitShardTask
	if existing == nil {
		next = &datapb.SplitShardTask{
			TaskId:         taskID,
			CollectionId:   collectionID,
			SourceVchannel: source,
			State:          datapb.SplitShardTaskState_SplitShardTaskPending,
			RecordTime:     time.Now().Unix(),
		}
		for _, target := range targets {
			next.Targets = append(next.Targets, &datapb.SplitShardTaskTarget{Vchannel: target})
		}
	} else {
		if existing.GetSourceAcked() {
			return nil
		}
		next = proto.Clone(existing).(*datapb.SplitShardTask)
	}
	next.SourceAcked = true
	if next.GetState() == datapb.SplitShardTaskState_SplitShardTaskWindowOpen {
		next.State = datapb.SplitShardTaskState_SplitShardTaskResidualFlushed
		next.ResidualFlushedTime = time.Now().Unix()
		metrics.DataCoordShardSplitResidualFlushSeconds.Observe(float64(next.GetResidualFlushedTime() - next.GetRecordTime()))
	}
	return s.saveLocked(ctx, next)
}

// Advance moves a split forward to the given state. A split that is already
// there, or beyond, is left as it is. The states up to the flush of the
// residual are reached through Record and MarkSourceAcked only, and no state
// is reached before the residual has flushed.
func (s *Store) Advance(ctx context.Context, taskID int64, state datapb.SplitShardTaskState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing := s.tasks[taskID]
	if existing == nil {
		return merr.WrapErrServiceInternalMsg("shard split task %d is not recorded on this cluster", taskID)
	}
	if existing.GetState() >= state {
		return nil
	}
	if state <= datapb.SplitShardTaskState_SplitShardTaskResidualFlushed ||
		existing.GetState() < datapb.SplitShardTaskState_SplitShardTaskResidualFlushed {
		return merr.WrapErrServiceInternalMsg("shard split task %d cannot move from %s to %s",
			taskID, existing.GetState().String(), state.String())
	}
	next := proto.Clone(existing).(*datapb.SplitShardTask)
	next.State = state
	return s.saveLocked(ctx, next)
}

// WaitResidualFlushed blocks until the residual of the source of the split has
// flushed, or the context is done.
func (s *Store) WaitResidualFlushed(ctx context.Context, taskID int64) error {
	for {
		s.mu.RLock()
		task, changed := s.tasks[taskID], s.changed
		s.mu.RUnlock()
		if task == nil {
			return merr.WrapErrServiceInternalMsg("shard split task %d is not recorded on this cluster", taskID)
		}
		if task.GetState() >= datapb.SplitShardTaskState_SplitShardTaskResidualFlushed {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// DropCollection removes the records of a dropped collection.
func (s *Store) DropCollection(ctx context.Context, collectionID int64) error {
	return s.drop(ctx, func(task *datapb.SplitShardTask) bool { return task.GetCollectionId() == collectionID })
}

// DropPendingBefore removes the pending records created before the deadline
// for which gone reports that the collection no longer exists: the callback
// that would complete them ended without recording the split.
func (s *Store) DropPendingBefore(ctx context.Context, deadline time.Time, gone func(collectionID int64) bool) error {
	return s.drop(ctx, func(task *datapb.SplitShardTask) bool {
		return task.GetState() == datapb.SplitShardTaskState_SplitShardTaskPending &&
			task.GetRecordTime() < deadline.Unix() && gone(task.GetCollectionId())
	})
}

func (s *Store) drop(ctx context.Context, match func(task *datapb.SplitShardTask) bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dropped := false
	defer func() {
		if dropped {
			s.notifyLocked()
		}
	}()
	for id, task := range s.tasks {
		if !match(task) {
			continue
		}
		if err := s.catalog.DropSplitShardTask(ctx, task); err != nil {
			return merr.Wrapf(err, "drop shard split task %d", id)
		}
		delete(s.tasks, id)
		dropped = true
	}
	return nil
}

func (s *Store) saveLocked(ctx context.Context, task *datapb.SplitShardTask) error {
	if err := s.catalog.SaveSplitShardTask(ctx, task); err != nil {
		return merr.Wrapf(err, "save shard split task %d", task.GetTaskId())
	}
	s.tasks[task.GetTaskId()] = task
	s.notifyLocked()
	mlog.Info(ctx, "shard split task saved",
		mlog.Int64("splitTaskID", task.GetTaskId()),
		mlog.FieldCollectionID(task.GetCollectionId()),
		mlog.String("source", task.GetSourceVchannel()),
		mlog.Strings("targets", targetVChannels(task)),
		mlog.Uint64("switchTimeTick", task.GetSwitchTimeTick()),
		mlog.Bool("sourceAcked", task.GetSourceAcked()),
		mlog.String("state", task.GetState().String()))
	return nil
}

func (s *Store) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
	s.refreshMetricsLocked()
}

func (s *Store) refreshMetricsLocked() {
	counts := make(map[datapb.SplitShardTaskState]int)
	for _, task := range s.tasks {
		counts[task.GetState()]++
	}
	for value, name := range datapb.SplitShardTaskState_name {
		metrics.DataCoordShardSplitTasks.WithLabelValues(name).Set(float64(counts[datapb.SplitShardTaskState(value)]))
	}
}

// ResumeL0 is called only after g2 is published and all g1 views are Down.
func (s *Store) ResumeL0(ctx context.Context, taskID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task := s.tasks[taskID]
	if task == nil {
		return merr.WrapErrServiceNotReadyMsg("split task %d is not recorded", taskID)
	}
	if task.GetState() < datapb.SplitShardTaskState_SplitShardTaskAdopted {
		return merr.WrapErrServiceNotReadyMsg("split task %d has not been adopted", taskID)
	}
	if task.GetL0Resumed() {
		return nil
	}
	next := proto.Clone(task).(*datapb.SplitShardTask)
	next.L0Resumed = true
	return s.saveLocked(ctx, next)
}

// ReleasePins records that the target history has been folded into every base.
func (s *Store) ReleasePins(ctx context.Context, taskID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task := s.tasks[taskID]
	if task == nil || !task.GetL0Resumed() {
		return merr.WrapErrServiceNotReadyMsg("split task %d has not resumed L0 compaction", taskID)
	}
	if task.GetPinsReleased() {
		return nil
	}
	next := proto.Clone(task).(*datapb.SplitShardTask)
	next.PinsReleased = true
	return s.saveLocked(ctx, next)
}
