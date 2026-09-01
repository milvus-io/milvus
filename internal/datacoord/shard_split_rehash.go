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
	"fmt"
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Rehash: changing a hash-routed collection's shard count to an arbitrary M.
//
// Unlike the size-triggered doubling, this is never proposed automatically. It
// rewrites the whole collection and makes it briefly write-unavailable while the
// sources are fenced (design §4.4), which is not a decision to take on a size
// threshold.
//
// It reuses the doubling's task, FSM, rewriter and routing commit wholesale; a
// doubling is the N=1, M=2 case of what is built here.
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §3.5.

// reconcileDesiredShardNum runs one round of the shard-count reconciliation:
// for every collection asking for a shard count it does not have, start a
// rehash toward it.
//
// A reconciliation loop rather than a one-shot command because the request
// arrives as a declarative property (collection.shardNum) written by
// AlterCollection. That makes it survive a coordinator restart, makes a retry
// free, and makes a request that arrives while another split is running simply
// wait for its turn instead of failing.
//
// Admission failures are logged, not raised: nobody is waiting on this call.
// The checks a caller CAN act on are done synchronously in rootcoord when the
// property is set, so the ones reaching here are transient (another split
// running, replication on) and are retried on the next tick.
func (m *shardSplitManager) reconcileDesiredShardNum() {
	logger := mlog.With(mlog.FieldComponent("shard-split-manager"), mlog.String("splitKind", "rehash"))
	for _, collection := range m.meta.GetCollections() {
		if err := manualShardNumAllowed(collection); err != nil {
			// This collection is the trigger's, or the feature is off. A shardNum
			// property may still sit in its meta from before the switch, and it
			// must not be acted on now. Decided per collection: its automatic
			// neighbor must not stop it, which is the whole point of the mode
			// being a property.
			//
			// Logged, like every other skip below, and not only for symmetry: a
			// declarative intent that is silently ignored is indistinguishable
			// from one that is being worked on. That gap cost a whole end-to-end
			// run, where a manual collection sat at its old shard count with the
			// request recorded in its meta and not one line saying why.
			if desired, _ := desiredShardNum(collection); desired > 0 {
				logger.RatedInfo(m.ctx, 60, "skip the requested shard count, this collection is not sized by hand",
					mlog.Int64("collectionID", collection.ID),
					mlog.Int32("desiredShardNum", desired), mlog.Err(err))
			}
			continue
		}
		desired, err := desiredShardNum(collection)
		if err != nil {
			logger.RatedWarn(m.ctx, 300, "collection asks for an unusable shard count",
				mlog.Int64("collectionID", collection.ID), mlog.Err(err))
			continue
		}
		// The intent is declarative, so it is also withdrawable: retire a rehash
		// the collection has stopped asking for before considering a new one, so
		// changing the requested count takes effect on this same tick.
		m.cancelSupersededRehash(collection, desired)

		if desired == 0 {
			continue // no shard count requested; the common case, not worth a line
		}
		if sources := len(m.rehashSources(collection)); int(desired) == sources {
			// Reconciled. Logged at a low rate because "nothing is happening" and
			// "it is already done" look identical from outside.
			logger.RatedInfo(m.ctx, 300, "requested shard count already reached",
				mlog.Int64("collectionID", collection.ID),
				mlog.Int32("desiredShardNum", desired), mlog.Int("shards", sources))
			continue
		}
		if m.hasAnyActiveSplitOnCollection(collection.ID) {
			logger.RatedInfo(m.ctx, 60, "waiting for the collection's current split before rehashing",
				mlog.Int64("collectionID", collection.ID),
				mlog.Int32("desiredShardNum", desired))
			continue
		}
		if _, err := m.StartRehash(collection.ID, desired); err != nil {
			logger.RatedWarn(m.ctx, 60, "cannot start the requested rehash yet",
				mlog.Int64("collectionID", collection.ID),
				mlog.Int32("desiredShardNum", desired), mlog.Err(err))
		}
	}
}

// cancelSupersededRehash retires an in-flight rehash the collection is no longer
// asking for — its shardNum property was deleted, or changed to a different
// count — by driving the task to the same Aborted terminal state every other
// rejected split reaches.
//
// Cancellable means the task has not entered its fence step, the same rule
// abortTask enforces. It is a real window rather than a formality because the
// flag is persisted BEFORE the first WAL append (see fenceHashSources), so
// "not fenced" cannot mean "fenced, but the record was lost".
//
// The window is one reconciler tick wide. It is not the tick that creates the
// task: the loop runs this reconciler and then advanceTasks, so a task created
// here is prepared microseconds later in that same tick and sits in Fencing
// until the NEXT one. Anything narrower than a tick would be unreachable in
// practice.
//
// Past the fence the rehash runs to completion — the FSM is forward-only there
// (design §4.4) — and the withdrawal only stops the NEXT one from starting. The
// cancel that matters most needs no running task at all: an intent the cluster
// cannot satisfy (too few pchannels, say) is retried by the reconciler forever,
// and withdrawing it is the only way to stop that.
func (m *shardSplitManager) cancelSupersededRehash(collection *collectionInfo, desired int32) {
	m.tasks.Range(func(_ int64, task *datapb.SplitShardTask) bool {
		if task.GetCollectionId() != collection.ID || !isSplitShardTaskActive(task) {
			return true
		}
		// Only a task shaped like a rehash answers to the shard count. Testing the
		// shape rather than the mode matters: a doubling created before the cluster
		// was switched to manual control is still rewriting and still active, and
		// it is not this property's to retire.
		if !rewrites(task) || !tilesTheKeySpace(task.GetTargets()) {
			return true
		}
		if int32(len(task.GetTargets())) == desired {
			return true // still pursuing exactly what the collection asks for
		}
		if task.GetFenced() {
			m.taskLogger(task).RatedInfo(m.ctx, 300,
				"the requested shard count changed, but this rehash is past the point of no return and will finish",
				mlog.Int32("desiredShardNum", desired))
			return true
		}
		m.abortTask(task, supersededReason(desired))
		return true
	})
}

// tilesTheKeySpace reports whether the targets are the complete cover a rehash
// plans: modulus M on every one of the M targets, and remainders 0..M-1 each
// exactly once. It is what planRehash builds, and it identifies a rehash without
// having to record a second copy of the intent on the task.
//
// A doubling carves ONE bucket in two instead, so its targets carry the parent's
// doubled modulus and cover only that bucket's share of the key space.
func tilesTheKeySpace(targets []*datapb.SplitShardTaskTarget) bool {
	modulus := uint64(len(targets))
	if modulus < 2 {
		return false
	}
	seen := make([]bool, modulus)
	for _, target := range targets {
		// A rehash to M shards gives each target exactly one residue at modulus
		// M; anything else is not the plan planRehash produces.
		residues := target.GetBuckets()
		if len(residues) != 1 || residues[0] >= modulus || seen[residues[0]] {
			return false
		}
		seen[residues[0]] = true
	}
	return true
}

// supersededReason states, in the task's own FailReason, why it was retired —
// the field an operator reads when a rehash they asked for is simply gone.
func supersededReason(desired int32) string {
	if desired == 0 {
		return fmt.Sprintf("the collection withdrew its shard count request (%s deleted)",
			common.CollectionShardNum)
	}
	return fmt.Sprintf("the collection now asks for %d shards", desired)
}

// desiredShardNum reads a collection's requested shard count, 0 when it has
// never asked for one.
func desiredShardNum(collection *collectionInfo) (int32, error) {
	raw, ok := collection.Properties[common.CollectionShardNum]
	if !ok {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, merr.WrapErrParameterInvalidMsg(
			"%s is not a number: %q", common.CollectionShardNum, raw)
	}
	if value < 2 {
		return 0, merr.WrapErrParameterInvalidMsg(
			"%s must be at least 2, got %d", common.CollectionShardNum, value)
	}
	return int32(value), nil
}

// manualShardNumAllowed reports whether the cluster is in the mode where the
// user owns the shard count.
//
// The two modes are exclusive by construction, not by preference: if the size
// trigger and a user request both acted, they would fence the same shards from
// two directions — a rehash claims every shard of the collection, and a doubling
// claims one of them, and whichever fenced second would find the other's
// T_switch waiting for it.
func manualShardNumAllowed(collection *collectionInfo) error {
	params := &paramtable.Get().DataCoordCfg
	if !params.ShardSplitEnable.GetAsBool() {
		return merr.WrapErrServiceInternalMsg(
			"shard split is disabled (dataCoord.shardSplit.enable)")
	}
	if !params.ShardSplitAutoTriggerEnable.GetAsBool() {
		// The cluster-wide kill switch is off, so nothing sizes a collection
		// automatically and every one of them is the user's to set.
		return nil
	}
	// The trigger is running, so the collection's own mode decides. Reading the
	// cluster switch alone here is what let a manual collection's request be
	// accepted by rootcoord and then never acted on: the validator had moved to
	// the per-collection mode and this had not.
	if collection != nil &&
		common.ShardSplitModeOf(collection.Properties) == common.ShardSplitModeManual {
		return nil
	}
	return merr.WrapErrServiceInternalMsg(
		"the collection sizes its shards automatically; set %s=%s on it to set a "+
			"shard count by hand", common.CollectionShardSplitMode, common.ShardSplitModeManual)
}

// planRehash builds the target set of a rehash to shardNum shards: buckets
// {shardNum, 0} .. {shardNum, shardNum-1}, which tile the key space exactly.
//
// The target vchannels are left empty and allocated in Preparing, the same point
// the doubling allocates its own.
func planRehash(shardNum int32) ([]*datapb.SplitShardTaskTarget, error) {
	if shardNum < 2 {
		return nil, merr.WrapErrParameterInvalidMsg(
			"a rehash needs at least 2 shards, got %d", shardNum)
	}
	targets := make([]*datapb.SplitShardTaskTarget, 0, shardNum)
	for r := int32(0); r < shardNum; r++ {
		targets = append(targets, &datapb.SplitShardTaskTarget{
			Buckets: []uint64{uint64(r)},
		})
	}
	return targets, nil
}

// StartRehash admits and creates a rehash of the collection to shardNum shards.
//
// Everything that can fail is checked HERE, before the task exists, because the
// task becomes forward-only at its first fence: past that point a rejected
// precondition can no longer abort it, only stall it (design §4.4).
func (m *shardSplitManager) StartRehash(collectionID int64, shardNum int32) (*datapb.SplitShardTask, error) {
	logger := mlog.With(
		mlog.FieldComponent("shard-split-manager"),
		mlog.String("splitKind", "rehash"),
		mlog.Int64("collectionID", collectionID),
		mlog.Int32("shardNum", shardNum))

	if err := manualShardNumAllowed(m.meta.GetCollection(collectionID)); err != nil {
		return nil, err
	}
	if m.clusterReplicating() {
		// Same exclusion as every other split: the control messages are not part
		// of the replication stream, so a replica would miss the topology change.
		return nil, merr.WrapErrServiceInternalMsg(
			"cannot rehash while replication/CDC is enabled")
	}

	collection := m.meta.GetCollection(collectionID)
	if collection == nil {
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}
	// isHashRouted, NOT isHashSplittable: this asks whether a rehash means
	// anything for the collection, not whether the trigger owns it. Who owns the
	// count was already settled by manualShardNumAllowed above, and a manual
	// collection is precisely the one that reaches here.
	if !isHashRouted(collection) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"collection %d is not hash-routed, its shard count cannot be rehashed", collectionID)
	}

	sources := m.rehashSources(collection)
	if len(sources) == 0 {
		return nil, merr.WrapErrServiceInternalMsg(
			"collection %d has no routable shard to rehash", collectionID)
	}
	if len(sources) == int(shardNum) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"collection %d already has %d shards", collectionID, shardNum)
	}

	// A rehash claims every shard of the collection, so it cannot share the
	// collection with any other split task: the other task's fence on a shard
	// this one is about to fence would come back carrying that task's T_switch,
	// leaving both believing they own it.
	if m.hasAnyActiveSplitOnCollection(collectionID) {
		return nil, merr.WrapErrServiceInternalMsg(
			"another shard split is already running on collection %d", collectionID)
	}

	targets, err := planRehash(shardNum)
	if err != nil {
		return nil, err
	}

	// pchannel headroom, checked before the first fence rather than discovered
	// after it. Sources are not retired until their rewrite completes, so both
	// sets exist at once and the collection needs len(sources)+shardNum distinct
	// pchannels. Preparing allocates them for real; this is the early, friendly
	// failure.
	if err := m.checkRehashHeadroom(collection, len(sources), int(shardNum)); err != nil {
		return nil, err
	}

	task, err := m.createRehashTask(collectionID, sources, targets)
	if err != nil {
		return nil, err
	}
	logger.Info(m.ctx, "rehash task created",
		mlog.Int64("taskID", task.GetTaskId()),
		mlog.Int("sources", len(sources)),
		mlog.Int("targets", len(targets)))
	return task, nil
}

// rehashSources lists the collection's routable shards, which become the rehash
// sources. Shards already retired by an earlier split are excluded — they own no
// key range, so there is nothing of theirs to rewrite.
func (m *shardSplitManager) rehashSources(collection *collectionInfo) []string {
	// Reuse the routing package's own filter rather than re-deciding here which
	// states are routable: it is the same rule the write path derives its table
	// from, so the sources are exactly the shards writes can currently reach.
	// (A vchannel absent from the map reads as ShardNormal, which is how legacy
	// meta without shard info is treated everywhere.)
	infos := make([]*schemapb.CollectionShardInfo, len(collection.VChannelNames))
	for i, vchannel := range collection.VChannelNames {
		infos[i] = collection.ShardInfos[vchannel]
	}
	shards, err := routing.ShardsFromMeta(collection.VChannelNames, infos)
	if err != nil {
		return nil
	}
	sources := make([]string, 0, len(shards))
	for _, shard := range shards {
		sources = append(sources, shard.Vchannel)
	}
	return sources
}

// checkRehashHeadroom rejects a rehash the cluster cannot host.
//
// Two limits, the same pair CreateCollection enforces: the system-wide shard
// cap, and the pchannel count. Enforcing only one would let a rehash take a
// collection past a bound a fresh collection is refused.
//
// The pchannel demand is a PEAK, not a final count: a collection holds at most
// one vchannel per pchannel, and the sources are not retired until their data
// has been rewritten, so both sets exist at once.
func (m *shardSplitManager) checkRehashHeadroom(collection *collectionInfo, sources, targets int) error {
	if limit := paramtable.Get().ProxyCfg.MaxShardNum.GetAsInt(); targets > limit {
		return merr.WrapErrParameterInvalidMsg(
			"shard num (%d) exceeds system limit proxy.maxShardNum (%d)", targets, limit)
	}

	if err := m.checkRehashFootprint(collection); err != nil {
		return err
	}

	if err := m.checkProjectedShardLoad(collection, sources, targets); err != nil {
		return err
	}

	if err := m.checkRehashDiskHeadroom(collection); err != nil {
		return err
	}

	available, source := availablePChannelCount()
	// Vchannels the collection already holds, including any retired ones whose
	// pchannel is not free yet.
	held := len(collection.VChannelNames)
	needed := held + targets
	if needed > available {
		return merr.WrapErrServiceInternalMsg(
			"rehashing collection %d to %d shards needs %d pchannels (%d already held by the collection "+
				"plus %d new targets, since the %d sources are only retired once their data is rewritten), "+
				"but the cluster has %d; raise %s",
			collection.ID, targets, needed, held, targets, sources, available, source)
	}
	return nil
}

// availablePChannelCount returns how many pchannels the cluster has, and the
// configuration key that governs it.
//
// Two modes that are not interchangeable. By default the set is generated from
// rootCoord.dmlChannelNum, and raising it is applied online by
// ConfigChannelProvider's watch. With common.preCreatedTopicEnabled the set is
// the explicit common.topicNames list, whose topics must already exist in the
// WAL backend — and nothing watches that key, so growing it is NOT online. The
// governing key travels with the count so an error names the one the operator
// actually has to change.
func availablePChannelCount() (int, string) {
	if paramtable.Get().CommonCfg.PreCreatedTopicEnabled.GetAsBool() {
		return len(paramtable.Get().CommonCfg.TopicNames.GetAsStrings()), "common.topicNames"
	}
	return paramtable.Get().RootCoordCfg.DmlChannelNum.GetAsInt(), "rootCoord.dmlChannelNum"
}

// checkRehashFootprint refuses a rehash of a collection the cluster would have to
// hold twice.
//
// A rehash rewrites every shard at once, and a source's pre-split copy cannot be
// dropped while the rewrite runs: the rewritten segments land on target
// vchannels whose delegators are not adopted yet, so query nodes do not load
// them, and until adoption the source is the ONLY readable copy. Both copies are
// therefore resident for the whole rewrite -- for a rehash that is the entire
// collection, where an automatic doubling costs one shard's worth.
//
// The measure is the collection's binlog size, which is what datacoord knows;
// there is no coordinator-side view of query node memory to probe, so the
// operator sets the bar from their own cluster. Off by default, because a
// guessed default would either block legitimate rehashes or protect nobody.
func (m *shardSplitManager) checkRehashFootprint(collection *collectionInfo) error {
	limitGB := paramtable.Get().DataCoordCfg.ShardSplitRehashMaxCollectionSize.GetAsInt64()
	if limitGB <= 0 {
		return nil
	}
	size := m.collectionBinlogSize(collection.ID)
	limit := limitGB * 1024 * 1024 * 1024
	if size <= limit {
		return nil
	}
	return merr.WrapErrServiceInternalMsg(
		"collection %d holds %.1f GiB and a rehash keeps a second copy of all of it until the "+
			"rewrite is adopted, which is over the %d GiB allowed by "+
			"dataCoord.shardSplit.rehashMaxCollectionSize; raise it once the query nodes can hold "+
			"the collection twice",
		collection.ID, float64(size)/(1024*1024*1024), limitGB)
}

// checkRehashDiskHeadroom refuses a rehash the object store cannot hold twice.
//
// The rewrite's outputs are COPIES: the sources are not retired until adoption,
// so for the whole window the collection is resident twice — on disk, not only
// in memory. rehashMaxCollectionSize (§10.2) gates that on the memory side, by
// having the operator name a bar from their own cluster; it says nothing about
// whether the storage is there. This is the storage half, and unlike the memory
// one it needs no new knob: the disk quota already exists, already means "the
// bytes this cluster may hold", and import already refuses work against it the
// same way (checkDiskQuota in import_util.go). A rehash asks for exactly one
// more copy of the collection, so that is what is added to current usage.
//
// Refusing here rather than mid-rewrite matters: a rehash that runs out of
// storage half way through has already fenced its sources and is forward-only,
// so there is no cheap way back.
func (m *shardSplitManager) checkRehashDiskHeadroom(collection *collectionInfo) error {
	if !Params.QuotaConfig.DiskProtectionEnabled.GetAsBool() {
		return nil
	}
	quotaInfo := m.meta.GetQuotaInfo()
	// The second copy the window keeps alive.
	extra := quotaInfo.CollectionBinlogSize[collection.ID]
	if extra <= 0 {
		return nil
	}

	if total := Params.QuotaConfig.DiskQuota.GetAsFloat(); float64(quotaInfo.TotalBinlogSize+extra) > total {
		return merr.WrapErrServiceQuotaExceeded(fmt.Sprintf(
			"rehashing collection %d keeps a second copy of its %.1f GiB until the rewrite is adopted, "+
				"which would take the cluster past its disk quota (using %.1f GiB of %.1f GiB); "+
				"free space or raise quotaAndLimits.limitWriting.diskProtection.diskQuota",
			collection.ID, gib(extra), gib(quotaInfo.TotalBinlogSize), total/(1024*1024*1024)))
	}
	if perCollection := Params.QuotaConfig.DiskQuotaPerCollection.GetAsFloat(); float64(extra+extra) > perCollection {
		return merr.WrapErrServiceQuotaExceeded(fmt.Sprintf(
			"rehashing collection %d would hold %.1f GiB — two copies of its %.1f GiB — past its "+
				"per-collection disk quota of %.1f GiB; free space or raise "+
				"quotaAndLimits.limitWriting.diskProtection.diskQuotaPerCollection",
			collection.ID, gib(extra+extra), gib(extra), perCollection/(1024*1024*1024)))
	}
	return nil
}

func gib(bytes int64) float64 {
	return float64(bytes) / (1024 * 1024 * 1024)
}

// checkProjectedShardLoad refuses a rehash whose survivors the system would
// immediately consider over-sized.
//
// Only a SHRINK is checked. Growing the shard count divides the load further,
// so it can only move away from the thresholds; shrinking multiplies each
// survivor's share by sources/targets, and nothing else in the pipeline looks
// at where that lands.
//
// This is admission policy, not a safety invariant — an over-sized shard is not
// a corruption. But the two modes disagree with the result in different ways
// and neither is acceptable to ship: in automatic mode the size trigger would
// re-split the survivors and undo the operator's shrink, possibly repeatedly;
// in manual mode the trigger is off, so the collection simply sits holding
// shards the system's own thresholds call too big, with nothing coming to help.
//
// Uniform division is the right estimate for the reason a doubling assumes
// balanced halves: primary keys are unique and the hash is uniform (§3.4).
//
// Design: §10.5.3.
func (m *shardSplitManager) checkProjectedShardLoad(collection *collectionInfo, sources, targets int) error {
	if targets >= sources || targets <= 0 {
		return nil
	}

	rows, size := m.collectionLoad(collection.ID)
	projectedRows := rows / int64(targets)
	projectedSize := size / int64(targets)

	if maxRows := paramtable.Get().DataCoordCfg.ShardSplitMaxShardRows.GetAsInt64(); maxRows > 0 && projectedRows > maxRows {
		return merr.WrapErrParameterInvalidMsg(
			"shrinking collection %d to %d shards would leave about %d rows per shard, over the %d "+
				"allowed by dataCoord.shardSplit.maxShardRows; the cluster would consider every "+
				"surviving shard over-sized",
			collection.ID, targets, projectedRows, maxRows)
	}
	if maxSizeGB := paramtable.Get().DataCoordCfg.ShardSplitMaxShardSize.GetAsInt64(); maxSizeGB > 0 {
		maxSize := maxSizeGB * 1024 * 1024 * 1024
		if projectedSize > maxSize {
			return merr.WrapErrParameterInvalidMsg(
				"shrinking collection %d to %d shards would leave about %.1f GiB per shard, over the %d "+
					"GiB allowed by dataCoord.shardSplit.maxShardSize; the cluster would consider every "+
					"surviving shard over-sized",
				collection.ID, targets, float64(projectedSize)/(1024*1024*1024), maxSizeGB)
		}
	}
	return nil
}

// collectionLoad totals the collection's healthy segments, in rows and bytes.
//
// Same source of truth as the footprint check, so the shrink admission adds a
// rule rather than a second view of how big a collection is.
func (m *shardSplitManager) collectionLoad(collectionID int64) (rows int64, size int64) {
	for _, segment := range m.meta.GetSegmentsOfCollection(m.ctx, collectionID) {
		if !isSegmentHealthy(segment) || segment.GetIsImporting() {
			continue
		}
		rows += segment.GetNumOfRows()
		size += segment.getSegmentSize()
	}
	return rows, size
}

// collectionBinlogSize totals the collection's healthy segments, the same
// measure GetQuotaInfo reports per collection.
func (m *shardSplitManager) collectionBinlogSize(collectionID int64) int64 {
	var total int64
	for _, segment := range m.meta.GetSegmentsOfCollection(m.ctx, collectionID) {
		if !isSegmentHealthy(segment) || segment.GetIsImporting() {
			continue
		}
		total += segment.getSegmentSize()
	}
	return total
}

// createRehashTask persists a rehash task in Preparing.
func (m *shardSplitManager) createRehashTask(
	collectionID int64,
	sources []string,
	targets []*datapb.SplitShardTaskTarget,
) (*datapb.SplitShardTask, error) {
	taskID, err := m.allocator.AllocID(m.ctx)
	if err != nil {
		return nil, err
	}
	taskSources := make([]*datapb.SplitShardTaskSource, 0, len(sources))
	for _, vchannel := range sources {
		taskSources = append(taskSources, &datapb.SplitShardTaskSource{Vchannel: vchannel})
	}
	task := &datapb.SplitShardTask{
		TaskId:       taskID,
		CollectionId: collectionID,
		Sources:      taskSources,
		// A hash-routed shard's segments straddle every split boundary, so the
		// data is rewritten rather than relabeled.
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		Targets:        targets,
		State:          datapb.SplitShardTaskState_SplitShardTaskPreparing,
		StartTime:      uint64(time.Now().Unix()),
	}
	if err := m.catalog.SaveSplitShardTask(m.ctx, task); err != nil {
		return nil, err
	}
	m.tasks.Insert(taskID, task)
	return task, nil
}
