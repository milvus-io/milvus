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

package datacoord

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Task-state plumbing for the hash-routed (primary-key) shard split. The task
// mirrors SplitShardTask's lifecycle — the fence and the adoption steps are the
// same — and differs only in the middle phase, which rewrites the source data
// instead of relabeling segment metadata (hash_split_rewriter.go).

// splitSourceVChannels lists a task's source vchannel names.
func splitSourceVChannels(task *datapb.SplitShardTask) []string {
	out := make([]string, 0, len(task.GetSources()))
	for _, source := range task.GetSources() {
		out = append(out, source.GetVchannel())
	}
	return out
}

// allHashSourcesFenced reports whether every source of the task has its fence
// recorded (a non-zero T_switch).
//
// This is the precondition of the routing commit. The routing flip is global —
// a target's hash bucket draws keys from every source when the shard count
// changes to an arbitrary M — so it cannot be applied incrementally per source.
// If it went live while any source still accepted writes, one primary key would
// have two live writers on two different WALs, and with no order between those
// WALs a delete could be sequenced before the insert it must remove.
//
// Fencing the sources needs no atomicity of its own: each fence is a single
// durable WAL append, a re-fence is idempotent and returns the original
// T_switch, so a coordinator crash mid-fence converges on retry. Only the
// happens-before matters, and this predicate is what enforces it.
func allHashSourcesFenced(task *datapb.SplitShardTask) bool {
	if len(task.GetSources()) == 0 {
		return false
	}
	for _, source := range task.GetSources() {
		if source.GetSwitchTimeTick() == 0 {
			return false
		}
	}
	return true
}

// maxHashSwitchTimeTick returns the greatest T_switch across the task's sources.
//
// Every source is fenced by its own independently sequenced message, so there is
// no single collection-wide T_switch. The target vchannels' barrier must exceed
// this maximum, otherwise a target's WAL could carry a message older than a
// source's fence.
func maxHashSwitchTimeTick(task *datapb.SplitShardTask) uint64 {
	var maxTick uint64
	for _, source := range task.GetSources() {
		if tick := source.GetSwitchTimeTick(); tick > maxTick {
			maxTick = tick
		}
	}
	return maxTick
}

// hashFenceFlushed reports whether the flusher has caught up to T_switch on
// EVERY source vchannel, proving every segment the fences sealed has been
// reported to datacoord and is therefore rewritable.
//
// Same guard as the relabel path's fenceFlushed: the SplitShard fence only
// appends a message, and the streamingnode seals and reports the sealed
// segments asynchronously afterwards. Without it the rewrite could declare the
// sources drained before those segments appear, leaving them orphaned on a
// dropped shard. Each source is checked against its own T_switch, since each
// was fenced separately.
func (m *shardSplitManager) hashFenceFlushed(task *datapb.SplitShardTask) bool {
	for _, source := range task.GetSources() {
		if source.GetSwitchTimeTick() == 0 {
			continue
		}
		cp := m.meta.GetChannelCheckpoint(source.GetVchannel())
		if cp == nil || cp.GetTimestamp() < source.GetSwitchTimeTick() {
			return false
		}
	}
	return true
}

// isRehashTask reports whether a task rewrites more than one source, i.e. it is
// a change of the collection's shard count rather than one shard's doubling.
//
// The distinction matters only for admission: a rehash claims every shard of the
// collection, so it cannot share the collection with any other split task, while
// two doublings on different shards are independent.
func isRehashTask(task *datapb.SplitShardTask) bool {
	return len(task.GetSources()) > 1
}

// hasActiveRehashOnCollection reports whether an unfinished rehash owns the
// given collection.
func (m *shardSplitManager) hasActiveRehashOnCollection(collectionID int64) bool {
	found := false
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if task.GetCollectionId() == collectionID && isRehashTask(task) && isSplitShardTaskActive(task) {
			found = true
			return false
		}
		return true
	})
	return found
}

// hasAnyActiveSplitOnCollection reports whether any unfinished split task of
// either kind touches the collection.
//
// This is the admission gate of a rehash, which is the one operation that needs
// a collection-wide claim: it fences every shard, so it cannot start while some
// other task holds one of them, and no other task may start once it is running
// (enforced by hasActiveRehashOnCollection at the trigger).
func (m *shardSplitManager) hasAnyActiveSplitOnCollection(collectionID int64) bool {
	found := false
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if task.GetCollectionId() == collectionID && isSplitShardTaskActive(task) {
			found = true
			return false
		}
		return true
	})
	return found
}

// hashSplitTargetVChannels lists a task's target vchannel names.
func hashSplitTargetVChannels(targets []*datapb.SplitShardTaskTarget) []string {
	out := make([]string, 0, len(targets))
	for _, t := range targets {
		out = append(out, t.GetVchannel())
	}
	return out
}

// firstSourceVChannel is the source vchannel of a single-source split -- what
// every size-triggered split, of either kind, has. Empty for a task with no
// source, which the preparing phase refuses.
//
// A rehash has many; code that must handle all of them uses
// splitSourceVChannels instead.
func firstSourceVChannel(task *datapb.SplitShardTask) string {
	for _, source := range task.GetSources() {
		return source.GetVchannel()
	}
	return ""
}

// firstSwitchTimeTick is the fence tick of a single-source split.
func firstSwitchTimeTick(task *datapb.SplitShardTask) uint64 {
	for _, source := range task.GetSources() {
		return source.GetSwitchTimeTick()
	}
	return 0
}
