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

package rootcoord

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Changing a collection's shard count rides on AlterCollection: the user sets
// the collection.shardNum property, and the value is DECLARATIVE — it records
// the shard count the collection should have, and datacoord reconciles toward it
// by running a rehash (design §10.2). AlterCollection returns as soon as the
// desired value is persisted; DescribeCollection's shards_num reports the
// achieved count, which catches up when the rehash finishes.
//
// Declarative rather than imperative because a rehash rewrites the whole
// collection and can take hours: a request that blocked for its duration, or
// that was lost if the coordinator restarted mid-way, would be worse than a
// recorded intent that survives a restart and is retried.
//
// The trade-off is that failures found later are not in the AlterCollection
// response, so everything cheap and knowable here is checked here.
//
// Being a recorded intent also gives it a cancel: deleting the property
// withdraws the request (design §10.2). That matters most for an intent nothing
// can satisfy — too few pchannels, say — which the reconciler would otherwise
// retry for the life of the collection.

// validateShardSplitMode rejects an unrecognized mode.
//
// Checked even when no shard count is being set, because the property stands on
// its own: it decides whether the size trigger owns this collection, and a value
// nothing recognizes would read as the "auto" default — handing the trigger a
// collection the operator believes they took control of.
func validateShardSplitMode(properties []*commonpb.KeyValuePair) error {
	if _, _, err := common.ParseShardSplitMode(properties); err != nil {
		return merr.Wrap(merr.ErrParameterInvalid, err.Error())
	}
	return nil
}

// validateDesiredShardNum rejects a shard-count change rootcoord can already
// tell will not work, so the common mistakes fail in the caller's response
// rather than silently in a background task.
func validateDesiredShardNum(coll *model.Collection, properties []*commonpb.KeyValuePair, deleteKeys []string) error {
	if funcutil.SliceContain(deleteKeys, common.CollectionShardNum) {
		// Deleting the property WITHDRAWS the request, and that is how a rehash is
		// canceled: datacoord stops retrying an intent it cannot satisfy, and
		// retires the task if it has not fenced yet (a fenced one still runs to
		// completion — an unfinished split cannot be unwound, only finished).
		//
		// Withdrawing asks for no work, so none of the checks below apply, and it
		// is answered before them. AlterCollection refuses a request that carries
		// properties and delete keys at once, so a withdrawal never arrives
		// alongside a set; the early return is for clarity, not to disambiguate.
		return nil
	}

	desired, parseErr, exist := common.GetDesiredShardNum(properties)
	if !exist {
		return nil
	}
	if parseErr != nil {
		return parseErr
	}
	if coll == nil {
		return merr.WrapErrParameterInvalidMsg("cannot change the shard count of an unknown collection")
	}

	// The cluster is in automatic mode: the size trigger owns the shard count and
	// a user request must be refused, not silently recorded. Checked here so the
	// caller learns it from the AlterCollection response — datacoord's reconciler
	// would otherwise just ignore the property forever.
	if err := autoModeForbidsManualShardNum(coll, properties); err != nil {
		return err
	}

	if desired < 2 {
		return merr.WrapErrParameterInvalidMsg(
			"%s must be at least 2, got %d", common.CollectionShardNum, desired)
	}

	// A request for the count the collection already has is satisfied, not
	// invalid. The property is declarative, so re-applying the same declaration
	// has to be accepted: a client retrying after a timeout, or a config the
	// operator re-applies whole, must not get a hard error for asking for the
	// state that already holds. It is also not merely a client-side pattern --
	// the routable count reaches the desired value at the write switch, well
	// before the rehash finishes, so a legitimate retry lands in this window on
	// its own. The property write itself is then a no-op, which the caller's
	// unchanged-properties check turns into a success.
	current := routableShardCount(coll)
	if desired == current {
		return nil
	}
	// Shrinking is allowed. The split machinery was always symmetric in the
	// source and target counts; what made a shrink unshippable was that it
	// returned nothing (the emptied pchannels stayed held, so a shrink taken to
	// free capacity ended up holding MORE of the scarce resource than it started
	// with) and that nothing checked the survivors could hold the merge. Both are
	// now handled — reclamation gives the slots back, and datacoord refuses a
	// shrink whose survivors would exceed the split thresholds (§10.5.3), which
	// it can do and rootcoord cannot because the row and byte counts live there.

	// The same two caps CreateCollection enforces. Applying only one of them
	// here would let AlterCollection take a collection past a limit that
	// CreateCollection refuses, which is the kind of inconsistency nobody finds
	// until it is in production.
	if limit := paramtable.Get().ProxyCfg.MaxShardNum.GetAsInt32(); desired > limit {
		return merr.WrapErrParameterInvalidMsg(
			"shard num (%d) exceeds system limit (%d)", desired, limit)
	}

	// A rehash holds the sources and the targets at once — the sources are only
	// retired once their data has been rewritten — so the collection needs a
	// pchannel for each.
	available, source := availablePChannelCount()
	needed := len(coll.VirtualChannelNames) + int(desired)
	if needed > available {
		return merr.WrapErrParameterInvalidMsg(
			"rehashing %q to %d shards needs %d pchannels (%d already held plus %d new targets, "+
				"since the sources are only retired once their data is rewritten) but the cluster has %d; "+
				"raise %s",
			coll.Name, desired, needed, len(coll.VirtualChannelNames), desired, available, source)
	}
	return nil
}

// autoModeForbidsManualShardNum refuses a hand-set shard count on a collection
// the size trigger owns.
//
// The two ways of sizing shards are mutually exclusive by construction: the size
// trigger doubles one shard, a user request rehashes all of them, and both fence
// shards. Whichever fenced second would find the other's T_switch already
// recorded, with two tasks then believing they own the same fence.
//
// The mode is per COLLECTION (§15 decision 10): its own property decides, so a
// collection sized by hand can sit beside one that is managed. The cluster
// switch is only a kill switch over the trigger — with it off nothing can size
// a collection automatically, so every collection is effectively manual and a
// hand-set count is exactly what is left.
//
// The properties consulted are the ones the collection will have AFTER this
// request: a single AlterCollection that switches to manual and sets a count
// must be accepted, or taking manual control would always need two round trips
// and the first would be refused for the state the second fixes.
func autoModeForbidsManualShardNum(coll *model.Collection, properties []*commonpb.KeyValuePair) error {
	params := &paramtable.Get().DataCoordCfg
	if !params.ShardSplitEnable.GetAsBool() {
		return merr.WrapErrParameterInvalidMsg(
			"shard split is disabled; enable dataCoord.shardSplit.enable first")
	}
	if !params.ShardSplitAutoTriggerEnable.GetAsBool() {
		// Nothing sizes collections automatically, so the count is the user's.
		return nil
	}

	mode, requested, err := common.ParseShardSplitMode(properties)
	if err != nil {
		return merr.Wrap(merr.ErrParameterInvalid, err.Error())
	}
	if !requested {
		mode = common.ShardSplitModeOf(funcutil.KeyValuePair2Map(coll.Properties))
	}
	if mode == common.ShardSplitModeManual {
		return nil
	}
	return merr.WrapErrParameterInvalidMsg(
		"collection %q sizes its shards automatically, so %s cannot be set by hand; "+
			"set %s=%s on the collection to take manual control",
		coll.Name, common.CollectionShardNum,
		common.CollectionShardSplitMode, common.ShardSplitModeManual)
}

// availablePChannelCount returns how many pchannels the cluster has, and the
// configuration key that governs it.
//
// Two modes, and they are not interchangeable. By default the set is generated
// from rootCoord.dmlChannelNum, and raising it is picked up online by
// ConfigChannelProvider's watch. With common.preCreatedTopicEnabled the set is
// the explicit common.topicNames list, whose topics must already exist in the
// WAL backend — and nothing watches that key, so growing it is not an online
// operation. The key is returned with the count so the error names the one the
// operator actually has to change.
func availablePChannelCount() (int, string) {
	if paramtable.Get().CommonCfg.PreCreatedTopicEnabled.GetAsBool() {
		return len(paramtable.Get().CommonCfg.TopicNames.GetAsStrings()), "common.topicNames"
	}
	return paramtable.Get().RootCoordCfg.DmlChannelNum.GetAsInt(), "rootCoord.dmlChannelNum"
}

// routableShardCount counts the collection's shards a key can currently reach,
// which is what the desired count is compared against. Shards retired by an
// earlier split still appear in the vchannel list but own no key range.
//
// The rule itself lives in model.ShardInfo.IsRoutable, so this and the count a
// routing commit writes into ShardsNum cannot drift apart. A vchannel with no
// shard info is a plain, never-split shard and counts.
func routableShardCount(coll *model.Collection) int32 {
	var count int32
	for _, vchannel := range coll.VirtualChannelNames {
		info, ok := coll.ShardInfos[vchannel]
		if !ok || info.IsRoutable() {
			count++
		}
	}
	return count
}
