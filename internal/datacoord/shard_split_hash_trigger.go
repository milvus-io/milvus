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
	"time"

	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Trigger for hash-routed (primary-key) shard splits.
//
// It shares the namespace trigger's gates (feature switch, replication
// exclusion, concurrency cap, one task per vchannel) and differs in what it
// plans: a namespace split cuts on a namespace boundary, while a hash split
// doubles the shard's hash bucket, cutting on the next hash bit so the halves
// are balanced by construction.
//
// A doubling is always effective here, because a primary key is unique — auto
// id, or a user-supplied identifier — so hashes spread evenly and no single
// routing key can dominate a shard. (That is the difference from a namespace
// collection, whose routing key is a tenant and can legitimately hold a large
// share of one shard, which is why that design needs a boundary search.)
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §3.4, §6.1.

// detectHashSplitOnce inspects the hash-routed collections and creates a split
// task for the shards over the thresholds.
//
// Called from the same tick as the namespace detection, after it, so a cluster
// running both kinds of collections shares one concurrency budget.
func (m *shardSplitManager) detectHashSplitOnce() {
	params := &paramtable.Get().DataCoordCfg
	logger := mlog.With(mlog.FieldComponent("shard-split-manager"), mlog.String("splitKind", "hash"))
	if !params.ShardSplitEnable.GetAsBool() || !params.ShardSplitAutoTriggerEnable.GetAsBool() {
		return
	}
	if m.clusterReplicating() {
		// Same exclusion as the namespace path: a split's control messages are
		// not part of the replication stream yet, so a replica would miss the
		// topology change.
		logger.RatedWarn(m.ctx, 60, "hash shard split trigger suppressed while replication/CDC is enabled")
		return
	}
	maxConcurrent := params.ShardSplitMaxConcurrentTasks.GetAsInt()
	active := m.activeTaskCount()
	if active >= maxConcurrent {
		return
	}

	for _, collection := range m.meta.GetCollections() {
		if !isHashSplittable(collection) {
			continue
		}
		if m.hasActiveRehashOnCollection(collection.ID) {
			// A rehash owns every shard of the collection: it will fence them
			// all. Starting a doubling next to it would fence a shard the rehash
			// is about to fence too, and the second fence would come back with
			// the first task's T_switch — leaving two tasks each believing they
			// own that fence, and each waiting to retire the same source.
			logger.RatedInfo(m.ctx, 60, "skip hash split trigger, a rehash owns the collection",
				mlog.Int64("collectionID", collection.ID))
			continue
		}
		for _, vchannel := range collection.VChannelNames {
			if active >= maxConcurrent {
				return
			}
			if m.hasAnyActiveTaskOnVChannel(vchannel) {
				continue
			}
			stats := m.collectShardStats(vchannel)
			if !m.shouldSplitBySize(stats) {
				continue
			}
			if m.refuseUnrelievableDoubling(logger, collection, vchannel, stats.size) {
				continue
			}
			targets, modulus, err := m.planHashSplit(collection, vchannel)
			if err != nil {
				logger.RatedWarn(m.ctx, 60, "cannot plan a hash split for the over-threshold shard",
					mlog.String("vchannel", vchannel), mlog.Err(err))
				continue
			}
			task, err := m.createHashSplitTask(collection.ID, vchannel, targets, modulus)
			if err != nil {
				logger.Warn(m.ctx, "create hash split task failed",
					mlog.String("vchannel", vchannel), mlog.Err(err))
				continue
			}
			logger.Info(m.ctx, "hash split task created",
				mlog.Int64("taskID", task.GetTaskId()),
				mlog.Int64("collectionID", collection.ID),
				mlog.String("vchannel", vchannel),
				mlog.Int64("size", stats.size),
				mlog.Int64("rows", stats.rows))
			active++
		}
	}
}

// isHashSplittable reports whether a collection is subject to the rewrite-based
// split: an ordinary primary-key collection, routed by hash.
//
// Namespace collections are excluded — they take the metadata-only relabel path
// instead, which is cheaper and only possible because their data is aligned to
// namespace boundaries.
// isHashRouted reports whether a rehash is MEANINGFUL for a collection: its
// keys are placed by hash, so a shard count is a thing it has.
//
// Deliberately says nothing about who owns that count. Folding the mode in here
// once produced a precise contradiction: StartRehash gates on this, so a manual
// collection — the only kind a user may rehash by hand — was refused with "is
// not hash-routed", which was also a lie about it.
func isHashRouted(collection *collectionInfo) bool {
	if collection == nil || collection.Schema == nil {
		return false
	}
	// Every collection now routes the same way -- the hash of its routing key,
	// modulo the collection's modulus -- so what this really asks is whether the
	// routing key is the primary key. Only a collection whose rows are PLACED by
	// namespace routes by it (see placedByNamespace); a namespace collection
	// whose rows are placed by primary key is hash-routed like any other.
	return !placedByNamespace(collection)
}

// isHashSplittable reports whether the automatic trigger may split a
// collection: it must be hash-routed AND be the trigger's to manage.
//
// The cluster switch decides whether the trigger runs at all; this decides
// which collections it runs FOR, so one collection can be sized by hand while
// its neighbors stay managed (§15 decision 10).
func isHashSplittable(collection *collectionInfo) bool {
	if !isHashRouted(collection) {
		return false
	}
	return common.ShardSplitModeOf(collection.Properties) != common.ShardSplitModeManual
}

// shouldSplitBySize reports whether a shard is over the size or row thresholds.
// The namespace-count threshold does not apply to a hash-routed collection.
func (m *shardSplitManager) shouldSplitBySize(stats *shardStats) bool {
	params := &paramtable.Get().DataCoordCfg
	maxSize := params.ShardSplitMaxShardSize.GetAsInt64() * 1024 * 1024 * 1024
	maxRows := params.ShardSplitMaxShardRows.GetAsInt64()
	return stats.size >= maxSize || stats.rows >= maxRows
}

// planHashSplit halves the shard's key space and returns the two targets'
// residues together with the collection's modulus after the split.
//
// A shard still owning several residues is halved by dividing that set, and the
// modulus does not move. Only a shard down to its last residue has nothing left
// to divide: the modulus doubles and the residue is cut on one more hash bit.
// Either way the two halves are balanced by construction, since a sound hash
// spreads keys evenly over residues.
//
// The target vchannels are left empty here and allocated in Preparing, the
// same point at which the namespace task allocates its own: allocation has a
// cluster-wide side effect (it consumes pchannel headroom), so it belongs in
// the task's own lifecycle rather than in the detection scan.
func (m *shardSplitManager) planHashSplit(
	collection *collectionInfo,
	vchannel string,
) ([]*datapb.SplitShardTaskTarget, uint64, error) {
	residues, err := residuesOf(collection)
	if err != nil {
		return nil, 0, err
	}
	own, err := residues.of(vchannel)
	if err != nil {
		return nil, 0, err
	}
	plan, err := routing.PlanSplit(residues.modulus, own)
	if err != nil {
		return nil, 0, err
	}
	return []*datapb.SplitShardTaskTarget{
		{Buckets: plan.Left},
		{Buckets: plan.Right},
	}, plan.Modulus, nil
}

// createHashSplitTask persists a new hash split task and caches it.
func (m *shardSplitManager) createHashSplitTask(
	collectionID int64,
	vchannel string,
	targets []*datapb.SplitShardTaskTarget,
	routingModulus uint64,
) (*datapb.SplitShardTask, error) {
	taskID, err := m.allocator.AllocID(m.ctx)
	if err != nil {
		return nil, err
	}
	task := &datapb.SplitShardTask{
		TaskId:       taskID,
		CollectionId: collectionID,
		// The size-triggered split is the single-source case of the general
		// multi-source task: one shard doubling into two.
		Sources: []*datapb.SplitShardTaskSource{{Vchannel: vchannel}},
		// A hash-routed shard's segments straddle every split boundary, so the
		// data is rewritten rather than relabeled.
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		Targets:        targets,
		// Recorded rather than recomputed at commit time: a doubling raises the
		// collection's modulus, and a retry that recomputed it from the meta the
		// commit already changed would produce a different topology.
		RoutingModulus: routingModulus,
		State:          datapb.SplitShardTaskState_SplitShardTaskPreparing,
		StartTime:      uint64(time.Now().Unix()),
	}
	if err := m.catalog.SaveSplitShardTask(m.ctx, task); err != nil {
		return nil, err
	}
	m.tasks.Insert(taskID, task)
	return task, nil
}

// hasAnyActiveTaskOnVChannel reports whether a vchannel is the source or a
// target of an unfinished task of either kind, so the trigger never re-fires on
// a shard already mid-split.
func (m *shardSplitManager) hasAnyActiveTaskOnVChannel(vchannel string) bool {
	if m.hasActiveTaskOnVChannel(vchannel) {
		return true
	}
	return m.hasActiveHashTaskOnVChannel(vchannel)
}

// hasActiveHashTaskOnVChannel reports whether an unfinished hash split task
// references the vchannel as its source or as one of its targets.
func (m *shardSplitManager) hasActiveHashTaskOnVChannel(vchannel string) bool {
	found := false
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if !isSplitShardTaskActive(task) {
			return true
		}
		for _, source := range task.GetSources() {
			if source.GetVchannel() == vchannel {
				found = true
				return false
			}
		}
		for _, target := range task.GetTargets() {
			if target.GetVchannel() == vchannel {
				found = true
				return false
			}
		}
		return true
	})
	return found
}
