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

package routing

import (
	"slices"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ErrCommitAlreadyApplied reports that the collection already carries what a
// routing commit writes: a redelivery, with nothing left to do. It is an
// outcome, not a failure, and never leaves the process.
var ErrCommitAlreadyApplied = errors.New("shard split routing commit already applied")

// ErrCommitAheadOfCollection marks the refusal of a routing commit whose
// post-image differs from the collection by more than the commit's own delta,
// in the forward direction: it reflects an earlier routing commit this cluster
// has not applied yet. The code underneath is ServiceUnavailable, so the
// refusal is retriable; the commit applies once the earlier one has.
var ErrCommitAheadOfCollection = errors.New("shard split routing commit is ahead of the collection")

// CommitDelta names the shards a routing commit may change by itself. A commit
// carries a full post-image of the collection's routing, but it may move the
// collection only by its own delta:
//
//   - a SplitShard fences its source (Normal -> Splitting, residues stripped) and
//     creates its two targets (born Creating, owning their residues);
//   - an adoption retires its source (delisted from Splitting) and adopts its
//     targets (Creating -> Normal).
//
// Every other shard the post-image lists must be exactly what the collection
// holds. The delta is what keeps two routing commits of one collection in
// order on a secondary, where their ack callbacks are not: a later commit
// whose post-image already reflects an earlier, unapplied one is refused as
// ahead and retried, instead of applying the earlier commit's changes on its
// behalf -- skipping that commit's own gates, such as the adoption's drain.
type CommitDelta struct {
	// Source is the vchannel the commit fences (a split) or retires (an
	// adoption). A split reads it off its header; an adoption reads it off this
	// cluster's DataCoord record of the split task, since the adoption message
	// no longer names the shard it retires.
	Source string
	// Targets are the vchannels the commit creates (a split) or adopts (an
	// adoption), read from the same place as Source.
	Targets []string
	// Adoption says which of the two commits this is.
	Adoption bool
	// Recorded reports whether this cluster's DataCoord holds the split task's
	// record. A split applies its routing only after recording the task
	// (CommitShardSplit runs first), and a source can leave the vchannel list
	// only by an adoption, which follows the split. So a source the collection
	// does not list belongs to a split already applied and adopted here when the
	// task is recorded, and to a split this cluster has not applied yet -- the
	// source being a target of an earlier split not applied here either -- when
	// it is not. Without the record, an adoption cannot even name its delta.
	Recorded bool
}

// SplitDelta is the delta of a SplitShard commit.
func SplitDelta(source string, targets []string, recorded bool) CommitDelta {
	return CommitDelta{Source: source, Targets: targets, Recorded: recorded}
}

// AdoptionDelta is the delta of an adoption commit, named by this cluster's
// record of the split task. recorded=false stands for "no record": the judge
// answers ahead-of-collection, since the split has not been applied here.
func AdoptionDelta(source string, targets []string, recorded bool) CommitDelta {
	return CommitDelta{Source: source, Targets: targets, Adoption: true, Recorded: recorded}
}

// JudgeCommit decides what a shard split routing post-image asks of the
// collection. It is the single decision every routing apply makes -- the
// SplitShard write switch and the adoption AlterCollection alike, on every
// cluster -- and the check a split planner runs against the current meta before
// it broadcasts.
//
// It returns one of:
//
//   - nil: the post-image is the collection plus exactly this commit's delta
//     (CommitDelta), with something of that delta left to write;
//   - ErrCommitAlreadyApplied: this commit's delta is in the collection, or
//     has since been superseded by later commits -- its source delisted with
//     the split task recorded here, its targets since retired. Nothing of the
//     delta is left to write. What else the collection carries beyond the
//     delta is not judged, since it came from those later commits;
//   - an error marked ErrCommitAheadOfCollection (ServiceUnavailable): the
//     post-image is ahead of the collection by more than this commit's delta --
//     another shard has moved forward, been created or been retired, the
//     modulus has grown under an adoption, this commit's own source is not yet
//     in the state its delta starts from, or an adoption's task is not recorded
//     here. An earlier routing commit has not been applied on this cluster;
//     retrying after it has is the answer;
//   - a ServiceInternal refusal naming an incoherent post-image: arrays that
//     are not parallel or list a vchannel twice, writable shards that do not
//     tile the modulus, a shard behind the collection, a modulus shrinking or
//     revoked, residues that are not the collection's, a listed Dropped shard,
//     a shape no commit produces, a collection listing no vchannel. No retry
//     can clear it.
//
// Every refusal is a System error: the post-image is a WAL message the split
// coordinator wrote, never a user request's content.
func JudgeCommit(coll *model.Collection, updates *messagespb.AlterCollectionMessageUpdates, delta CommitDelta) error {
	vchannels := updates.GetVirtualChannelNames()
	infos := updates.GetShardInfos()
	// What the message alone can answer comes first, because it makes the
	// post-image incoherent whatever the collection holds: its shape
	// (CheckPostImageShape), a listed Dropped shard (CheckNoListedDroppedShard)
	// and, once the modulus is known to be set, whether its writable shards
	// tile the key space (CheckPostImageTiling). They are the checks
	// ValidateSplitShardMessage makes on a SplitShard before the fence; here
	// they cover the adoption too, whose post-image no builder validates before
	// this judge, and they run before an adoption's task record is asked for,
	// so an incoherent adoption never reaches the drain gate.
	if err := CheckPostImageShape(updates); err != nil {
		return merr.Wrapf(err, "commit shard split routing failed, collection %q", coll.Name)
	}
	if err := CheckNoListedDroppedShard(vchannels, infos); err != nil {
		return merr.Wrapf(err, "commit shard split routing failed, collection %q", coll.Name)
	}
	if updates.GetRoutingModulus() == 0 {
		if coll.RoutingModulus != 0 {
			// Routing is not revocable. Once a collection has been split, its
			// shards own residues and only the modulus says what those residues
			// mean; a commit that zeroes it would leave the collection reading as
			// never-split and route by position over a channel list that now
			// contains retired sources.
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, collection %q routes at modulus %d and a commit cannot take it back to none",
				coll.Name, coll.RoutingModulus)
		}
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, collection %q: the post-image sets no routing modulus", coll.Name)
	}
	if err := CheckPostImageTiling(updates); err != nil {
		return merr.Wrapf(err, "commit shard split routing failed, collection %q", coll.Name)
	}
	if delta.Adoption && updates.GetSplitTaskId() == 0 {
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, the adoption of collection %q does not name its split task, "+
				"so no datacoord can say what it retires or whether that has drained", coll.Name)
	}
	if commitAlreadyApplied(coll, updates) {
		return ErrCommitAlreadyApplied
	}
	// The namespace routing key only for a collection whose rows have always been
	// placed by it (§3.1).
	if err := CheckShardByAdmission(updates.GetShardBy(), coll.Properties); err != nil {
		return merr.Wrapf(err, "commit shard split routing failed, collection %q", coll.Name)
	}
	if delta.Adoption && !delta.Recorded {
		return errors.Mark(merr.WrapErrServiceUnavailableMsg(
			"commit shard split routing of collection %q is not applicable yet: this cluster has no record of split task %d, "+
				"so its SplitShard has not been applied here", coll.Name, updates.GetSplitTaskId()), ErrCommitAheadOfCollection)
	}

	local := localShards(coll)
	post := make(map[string]*schemapb.CollectionShardInfo, len(vchannels))
	for i, vchannel := range vchannels {
		post[vchannel] = infos[i]
	}
	baseModulus, baseResidues := localResidues(coll)
	if baseModulus == 0 {
		// A never-split collection's modulus is its vchannel count, and every
		// modulus comparison below divides by it. A collection listing no
		// vchannel is meta no create or routing commit produces (an adoption
		// always keeps its targets), so it is refused here rather than read as
		// a modulus of none.
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, collection %q lists no vchannel, so it has no routing modulus to move from", coll.Name)
	}
	postModulus := updates.GetRoutingModulus()

	// The commit's own shards first: they say whether the delta is still to be
	// written, already written, or not yet writable.
	own := ownStatus{coll: coll, baseModulus: baseModulus, postModulus: postModulus}
	if err := own.judgeSource(delta, local[delta.Source], post[delta.Source]); err != nil {
		return err
	}
	for _, target := range delta.Targets {
		if err := own.judgeTarget(delta, target, local[target], post[target]); err != nil {
			return err
		}
	}
	if !own.pending {
		if coll.ShardBy == "" && updates.GetShardBy() != "" && sameTopology(coll, updates) {
			// The one write a commit makes outside its delta: back-filling the
			// routing key onto a collection that carries exactly this topology
			// without one. Anything else the collection carries came from commits
			// that applied after this one, and is not this commit's to judge.
			return nil
		}
		// Nothing of this commit's delta is left to write: a redelivery.
		return ErrCommitAlreadyApplied
	}

	// The modulus. An adoption changes none, so a post-image at a larger one
	// reflects a split -- which doubled it -- that this cluster has not applied.
	// A split may keep the modulus or grow it to a multiple, re-expressing every
	// untouched shard's residues against the new one.
	switch {
	case postModulus < baseModulus:
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, collection %q routes at modulus %d and a commit cannot take it down to %d",
			coll.Name, baseModulus, postModulus)
	case postModulus > baseModulus && delta.Adoption:
		return errors.Mark(merr.WrapErrServiceUnavailableMsg(
			"commit shard split routing of collection %q is not applicable yet: the adoption routes at modulus %d, the collection at %d, "+
				"so a split that grew the modulus has not been applied on this cluster",
			coll.Name, postModulus, baseModulus), ErrCommitAheadOfCollection)
	case postModulus%baseModulus != 0:
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, collection %q routes at modulus %d, which does not divide the post-image's %d, "+
				"so the shards this split leaves alone cannot keep their residues",
			coll.Name, baseModulus, postModulus)
	}

	// Every other shard must be exactly what the collection holds.
	isOwn := func(vchannel string) bool {
		return vchannel == delta.Source || slices.Contains(delta.Targets, vchannel)
	}
	for _, vchannel := range coll.VirtualChannelNames {
		if isOwn(vchannel) {
			continue
		}
		state := local[vchannel]
		want, listed := post[vchannel]
		if !listed {
			// Delisted by someone else's adoption, which this cluster has not
			// applied. A shard in any state reaches the delist forward.
			return errors.Mark(merr.WrapErrServiceUnavailableMsg(
				"commit shard split routing of collection %q is not applicable yet: it retires shard %q (%s here), which is not this commit's to retire",
				coll.Name, vchannel, state.String()), ErrCommitAheadOfCollection)
		}
		if err := judgeUntouchedState(coll, vchannel, state, want.GetState()); err != nil {
			return err
		}
		if !slices.Equal(reexpressResidues(baseResidues[vchannel], baseModulus, postModulus), sortedResidues(want.GetHashRouting().GetBuckets())) {
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, shard %q of collection %q is not touched by this commit but the post-image gives it residues %v, "+
					"which are not its residues %v re-expressed at modulus %d",
				vchannel, coll.Name, want.GetHashRouting().GetBuckets(), baseResidues[vchannel], postModulus)
		}
	}
	for _, vchannel := range vchannels {
		if isOwn(vchannel) {
			continue
		}
		if _, known := local[vchannel]; !known {
			// Created by someone else's split, which this cluster has not applied.
			return errors.Mark(merr.WrapErrServiceUnavailableMsg(
				"commit shard split routing of collection %q is not applicable yet: it names shard %q (%s), which the collection does not carry "+
					"and this commit does not create, so the split that creates it has not been applied on this cluster",
				coll.Name, vchannel, post[vchannel].GetState().String()), ErrCommitAheadOfCollection)
		}
	}
	return nil
}

// ownStatus accumulates what the commit's own shards say about its delta.
type ownStatus struct {
	coll                     *model.Collection
	baseModulus, postModulus uint64
	// pending: at least one own shard is exactly one own step before its
	// post-image state.
	pending bool
	// beyond: at least one own shard has moved past its post-image state.
	beyond bool
	// sourceRetired: the own source is no longer listed and the split task is
	// recorded here, so this commit's delta is in the past: the split applied,
	// its adoption retired the source. Judged first, it tells an own target the
	// collection does not list apart: retired by a later commit (beyond),
	// rather than never created here (pending, or ahead for an adoption).
	sourceRetired bool
}

// judgeSource judges the commit's source. local is the source's state in the
// collection (absent when the collection does not list it); want is its
// post-image entry (nil when the post-image delists it).
func (s *ownStatus) judgeSource(delta CommitDelta, local schemapb.ShardState, want *schemapb.CollectionShardInfo) error {
	coll := s.coll
	_, listed := shardIndex(coll, delta.Source)
	if !delta.Adoption {
		// A split's source stays listed, fenced, until its adoption.
		if want == nil || want.GetState() != schemapb.ShardState_ShardSplitting {
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, the post-image of the split of shard %q does not list it as Splitting", delta.Source)
		}
		switch {
		case !listed && delta.Recorded:
			// Applied and adopted here already.
			s.beyond = true
			s.sourceRetired = true
		case !listed:
			return errors.Mark(merr.WrapErrServiceUnavailableMsg(
				"commit shard split routing of collection %q is not applicable yet: it splits shard %q, which the collection does not carry "+
					"and no split task is recorded for, so the split that created it has not been applied on this cluster",
				coll.Name, delta.Source), ErrCommitAheadOfCollection)
		case local == schemapb.ShardState_ShardNormal:
			s.pending = true
		case local == schemapb.ShardState_ShardSplitting:
			// Fenced here already; only this split fences this shard.
			s.beyond = true
		case local == schemapb.ShardState_ShardCreating:
			return errors.Mark(merr.WrapErrServiceUnavailableMsg(
				"commit shard split routing of collection %q is not applicable yet: it splits shard %q, which is still Creating here, "+
					"so the adoption that makes it Normal has not been applied on this cluster",
				coll.Name, delta.Source), ErrCommitAheadOfCollection)
		default:
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, shard %q cannot be split from %s", delta.Source, local.String())
		}
		return s.checkConsistent(coll)
	}

	// An adoption retires its source, or keeps it listed as Splitting when it
	// adopts the targets first.
	if want != nil && want.GetState() != schemapb.ShardState_ShardSplitting {
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, the adoption of the split of shard %q lists it as %s: a source is fenced until it is retired",
			delta.Source, want.GetState().String())
	}
	switch {
	case !listed:
		// Retired here already, whether this commit retires it or keeps it. The
		// task is recorded (an unrecorded adoption was refused above), so the
		// retirement is this task's adoption, applied here.
		s.sourceRetired = true
		if want != nil {
			s.beyond = true
		}
	case local == schemapb.ShardState_ShardSplitting:
		if want == nil {
			s.pending = true
		}
	case local == schemapb.ShardState_ShardNormal, local == schemapb.ShardState_ShardCreating:
		return errors.Mark(merr.WrapErrServiceUnavailableMsg(
			"commit shard split routing of collection %q is not applicable yet: it retires shard %q, which is %s here, not fenced, "+
				"so its SplitShard has not been applied on this cluster",
			coll.Name, delta.Source, local.String()), ErrCommitAheadOfCollection)
	default:
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, shard %q cannot be retired from %s", delta.Source, local.String())
	}
	return s.checkConsistent(coll)
}

// judgeTarget judges one of the commit's targets.
func (s *ownStatus) judgeTarget(delta CommitDelta, target string, local schemapb.ShardState, want *schemapb.CollectionShardInfo) error {
	coll := s.coll
	_, listed := shardIndex(coll, target)
	if !delta.Adoption {
		// A split creates its targets, born Creating with their residues.
		if want == nil || want.GetState() != schemapb.ShardState_ShardCreating {
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, shard %q is created by this commit but the post-image does not list it as Creating: a split target is born Creating",
				target)
		}
		if len(want.GetHashRouting().GetBuckets()) == 0 {
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, shard %q is created by this commit owning no residue", target)
		}
		switch {
		case !listed && s.sourceRetired:
			// Created here by this split and retired since, by a later split's
			// adoption: the source's retirement says this split is applied here.
			s.beyond = true
		case !listed:
			s.pending = true
		default:
			// Only this split creates this vchannel, so it is at or past Creating.
			s.beyond = true
		}
		return s.checkConsistent(coll)
	}

	// An adoption moves its targets Creating -> Normal, keeping their residues.
	switch {
	case !listed:
		if want == nil {
			return nil // retired later, on both sides
		}
		if s.sourceRetired {
			// Adopted here by this adoption and retired since, by a later
			// split's adoption: the source's retirement says this adoption is
			// applied here.
			s.beyond = true
			return s.checkConsistent(coll)
		}
		// A target this cluster does not carry while its source is still
		// listed: its split has not been applied.
		return errors.Mark(merr.WrapErrServiceUnavailableMsg(
			"commit shard split routing of collection %q is not applicable yet: it adopts shard %q, which the collection does not carry, "+
				"so its SplitShard has not been applied on this cluster",
			coll.Name, target), ErrCommitAheadOfCollection)
	case want == nil:
		// Delisted by the post-image while listed here: adopted, split again and
		// retired by later commits, which this cluster has not applied yet. Every
		// listed state reaches the delist forward.
		return errors.Mark(merr.WrapErrServiceUnavailableMsg(
			"commit shard split routing of collection %q is not applicable yet: it no longer names shard %q, retired after this commit, "+
				"while this cluster still carries it (%s)", coll.Name, target, local.String()), ErrCommitAheadOfCollection)
	}
	switch local {
	case schemapb.ShardState_ShardCreating:
		switch want.GetState() {
		case schemapb.ShardState_ShardNormal:
			s.pending = true
		case schemapb.ShardState_ShardCreating:
			// Listed unchanged: the adoption of another target of the same task,
			// or nothing to do for this one.
		case schemapb.ShardState_ShardSplitting:
			return errors.Mark(merr.WrapErrServiceUnavailableMsg(
				"commit shard split routing of collection %q is not applicable yet: it lists shard %q as Splitting, "+
					"while this cluster has not yet adopted it", coll.Name, target), ErrCommitAheadOfCollection)
		default:
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, shard %q cannot go from %s to %s", target, local.String(), want.GetState().String())
		}
	case schemapb.ShardState_ShardNormal:
		switch want.GetState() {
		case schemapb.ShardState_ShardNormal:
			// Adopted here already.
		case schemapb.ShardState_ShardSplitting:
			return errors.Mark(merr.WrapErrServiceUnavailableMsg(
				"commit shard split routing of collection %q is not applicable yet: it lists shard %q as Splitting, "+
					"so a later split of it has not been applied on this cluster", coll.Name, target), ErrCommitAheadOfCollection)
		default:
			return merr.WrapErrServiceInternalMsg(
				"commit shard split routing failed, shard %q cannot go from %s to %s", target, local.String(), want.GetState().String())
		}
	case schemapb.ShardState_ShardSplitting:
		if want.GetState() != schemapb.ShardState_ShardSplitting {
			// Split again here already: past this adoption.
			s.beyond = true
		}
	default:
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, shard %q cannot be adopted from %s", target, local.String())
	}
	// An adoption keeps its targets' residues. A post-image at a grown modulus
	// re-expresses them, and is judged by the modulus check after this; one at
	// a smaller or unrelated modulus is refused there too.
	if !s.beyond && s.postModulus%s.baseModulus == 0 &&
		!slices.Equal(reexpressResidues(localBuckets(coll, target), s.baseModulus, s.postModulus), sortedResidues(want.GetHashRouting().GetBuckets())) {
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, the adoption gives shard %q residues %v, which are not the residues %v it was created with",
			target, want.GetHashRouting().GetBuckets(), localBuckets(coll, target))
	}
	return s.checkConsistent(coll)
}

// checkConsistent refuses a delta that is partly written and partly not yet
// writable: the apply is one catalog write, so no crash leaves that behind.
func (s *ownStatus) checkConsistent(coll *model.Collection) error {
	if s.pending && s.beyond {
		return merr.WrapErrServiceInternalMsg(
			"commit shard split routing failed, collection %q carries part of this commit's delta and is past another part of it",
			coll.Name)
	}
	return nil
}

// judgeUntouchedState judges a shard the commit does not touch: it must be in
// the state the collection holds it in. Forward is ahead (a later commit moved
// it and this cluster has not applied that commit); backward is incoherent.
func judgeUntouchedState(coll *model.Collection, vchannel string, local, want schemapb.ShardState) error {
	if local == want {
		return nil
	}
	if shardStateReachableForward(local, want) {
		return errors.Mark(merr.WrapErrServiceUnavailableMsg(
			"commit shard split routing of collection %q is not applicable yet: it lists shard %q as %s, which is %s here and not this commit's to move",
			coll.Name, vchannel, want.String(), local.String()), ErrCommitAheadOfCollection)
	}
	return merr.WrapErrServiceInternalMsg(
		"commit shard split routing failed, shard %q cannot go from %s to %s", vchannel, local.String(), want.String())
}

// localShards is the state of every shard the collection lists. A shard listed
// without a shard info is a legacy never-split shard, which serves as Normal.
func localShards(coll *model.Collection) map[string]schemapb.ShardState {
	states := make(map[string]schemapb.ShardState, len(coll.VirtualChannelNames))
	for _, vchannel := range coll.VirtualChannelNames {
		state := schemapb.ShardState_ShardNormal
		if info, ok := coll.ShardInfos[vchannel]; ok {
			state = info.State
		}
		states[vchannel] = state
	}
	return states
}

// localResidues is the collection's routing modulus and every listed shard's
// residues against it. A never-split collection (modulus zero) places rows by
// vchannel position, which is shard i owning residue i at modulus
// len(vchannels) (routing.Derive's legacy rule).
func localResidues(coll *model.Collection) (uint64, map[string][]uint64) {
	residues := make(map[string][]uint64, len(coll.VirtualChannelNames))
	if coll.RoutingModulus == 0 {
		for i, vchannel := range coll.VirtualChannelNames {
			residues[vchannel] = []uint64{uint64(i)}
		}
		return uint64(len(coll.VirtualChannelNames)), residues
	}
	for _, vchannel := range coll.VirtualChannelNames {
		if info, ok := coll.ShardInfos[vchannel]; ok {
			residues[vchannel] = sortedResidues(info.Buckets)
		}
	}
	return coll.RoutingModulus, residues
}

// reexpressResidues rewrites residues taken against modulus from into their
// equivalent against modulus to, a multiple of from: r becomes r, r+from, ...
// below to. Sorted.
func reexpressResidues(residues []uint64, from, to uint64) []uint64 {
	if from == 0 || to == from {
		return sortedResidues(residues)
	}
	out := make([]uint64, 0, len(residues)*int(to/from))
	for _, r := range residues {
		for x := r; x < to; x += from {
			out = append(out, x)
		}
	}
	slices.Sort(out)
	return out
}

func sortedResidues(residues []uint64) []uint64 {
	out := slices.Clone(residues)
	slices.Sort(out)
	if out == nil {
		return []uint64{}
	}
	return out
}

// localBuckets is the residues the collection records for vchannel, nil for a
// shard listed without a shard info.
func localBuckets(coll *model.Collection, vchannel string) []uint64 {
	if info, ok := coll.ShardInfos[vchannel]; ok && info != nil {
		return info.Buckets
	}
	return nil
}

func shardIndex(coll *model.Collection, vchannel string) (int, bool) {
	i := slices.Index(coll.VirtualChannelNames, vchannel)
	return i, i >= 0
}

// commitAlreadyApplied reports whether the collection already carries exactly
// what the post-image commits: the same topology (sameTopology) and, when the
// post-image sets one, the same shard_by. A shard_by the post-image leaves
// empty is not compared, since an empty one means "nothing to back-fill" rather
// than "clear it".
func commitAlreadyApplied(coll *model.Collection, updates *messagespb.AlterCollectionMessageUpdates) bool {
	if updates.GetShardBy() != "" && coll.ShardBy != updates.GetShardBy() {
		return false
	}
	return sameTopology(coll, updates)
}

// sameTopology reports whether the collection carries exactly the topology the
// post-image commits: the same vchannels, each at the same lifecycle state and
// owning the same residues, against the same modulus.
func sameTopology(coll *model.Collection, updates *messagespb.AlterCollectionMessageUpdates) bool {
	vchannels := updates.GetVirtualChannelNames()
	if len(coll.VirtualChannelNames) != len(vchannels) || coll.RoutingModulus != updates.GetRoutingModulus() {
		return false
	}
	for i, vchannel := range vchannels {
		info, ok := coll.ShardInfos[vchannel]
		if !ok {
			return false
		}
		want := updates.GetShardInfos()[i]
		if info.State != want.GetState() || !slices.Equal(info.Buckets, want.GetHashRouting().GetBuckets()) {
			return false
		}
	}
	return true
}

// CheckPostImageShape refuses a routing post-image whose arrays do not
// describe one shard list: the vchannel, pchannel and shard-info arrays must
// be parallel and non-empty, and no vchannel may be listed twice.
//
// The three arrays are parallel by convention and are applied in lockstep
// (model.Collection.ApplyUpdates rebuilds the shard-info map from them), so a
// short pchannel list would persist a shard without a pchannel and a
// duplicated vchannel would fold two entries into one map key; either leaves
// DescribeCollection answering with mismatched lists that the proxy refuses
// every DML on. A pchannel listed twice is not refused: a split's targets
// live on their source's pchannel. Whether each shard info names the vchannel
// at its position is checked where the residues are read (ShardsFromMeta).
//
// It is the ONE shape check every routing post-image passes, whichever
// message carries it: the SplitShard builder and ack callback
// (streaming.ValidateSplitShardMessage) and every routing apply through
// JudgeCommit. System error: a post-image is planned, never typed by a user.
func CheckPostImageShape(updates *messagespb.AlterCollectionMessageUpdates) error {
	vchannels := updates.GetVirtualChannelNames()
	pchannels := updates.GetPhysicalChannelNames()
	infos := updates.GetShardInfos()
	if len(vchannels) == 0 || len(vchannels) != len(pchannels) || len(vchannels) != len(infos) {
		return merr.WrapErrServiceInternalMsg(
			"the post-image's vchannel, pchannel and shard-info arrays must be parallel and non-empty, "+
				"got %d vchannels, %d pchannels and %d shard infos", len(vchannels), len(pchannels), len(infos))
	}
	seen := make(map[string]struct{}, len(vchannels))
	for _, vchannel := range vchannels {
		if _, dup := seen[vchannel]; dup {
			return merr.WrapErrServiceInternalMsg("the post-image lists vchannel %q twice", vchannel)
		}
		seen[vchannel] = struct{}{}
	}
	return nil
}

// CheckPostImageTiling refuses a routing post-image whose writable shards do
// not tile the key space at its modulus: ShardsFromMeta keeps the shards that
// accept writes (each shard info naming the vchannel at its position), and
// Derive refuses a gap, an overlap, a writable shard owning no residue, a
// modulus over the cap, and residues on a post-image whose modulus is unset.
// A gap silently drops the writes of the residues nobody claims; an overlap
// sends one key to two shards.
//
// Like CheckPostImageShape, it is shared by the SplitShard validation and
// every routing apply. System error, for the same reason.
func CheckPostImageTiling(updates *messagespb.AlterCollectionMessageUpdates) error {
	vchannels := updates.GetVirtualChannelNames()
	writable, err := ShardsFromMeta(vchannels, updates.GetShardInfos())
	if err != nil {
		return err
	}
	_, err = Derive(updates.GetRoutingModulus(), vchannels, writable)
	return err
}

// CheckNoListedDroppedShard refuses a routing post-image that lists a shard as
// Dropped.
//
// Dropped is reachable only by delisting. An adoption retires a fenced source by
// removing it from the vchannel list, and a delist is exactly what the adoption
// callback reaches (its own replica tears the streamingnode state down) and
// gates on this cluster's drain. A post-image that keeps the source listed but
// marks it Dropped would stop routing to it without either: its unmoved data
// would be unreachable, and a later delist of a Dropped shard could only be
// refused. System error, for the same reason as every other routing refusal.
func CheckNoListedDroppedShard(vchannels []string, infos []*schemapb.CollectionShardInfo) error {
	for i, info := range infos {
		if info.GetState() != schemapb.ShardState_ShardDropped {
			continue
		}
		vchannel := ""
		if i < len(vchannels) {
			vchannel = vchannels[i]
		}
		return merr.WrapErrServiceInternalMsg(
			"shard %q is listed as Dropped: a shard reaches Dropped only by being delisted, after its drain", vchannel)
	}
	return nil
}

// shardStateReachableForward reports whether a listed shard can get from one
// lifecycle state to another by later routing commits. Staying put counts.
//
// The lifecycle only ever runs one way. A source is fenced (Normal ->
// Splitting) and later released by being delisted -- never by a listed
// Splitting -> Dropped, see CheckNoListedDroppedShard; the fence is recorded in
// the WAL and is permanent, so there is no way back to Normal. A target is
// created writable, later adopted (Creating -> Normal), and may then be split
// itself (-> Splitting).
//
// A target is NOT abandonable. It is write-routable from the moment the write
// switch publishes it, so moving one to Dropped would discard rows that were
// already accepted, and the residues it owns would have no shard at all.
func shardStateReachableForward(from, to schemapb.ShardState) bool {
	if from == to {
		return true
	}
	switch from {
	case schemapb.ShardState_ShardNormal:
		return to == schemapb.ShardState_ShardSplitting
	case schemapb.ShardState_ShardCreating:
		return to == schemapb.ShardState_ShardNormal || to == schemapb.ShardState_ShardSplitting
	default:
		// Splitting leaves only by being delisted; Dropped is never listed; and
		// any state a later version adds is one this one does not know how to
		// advance.
		return false
	}
}
