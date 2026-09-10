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
	"context"
	"slices"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitShardV2AckCallback commits a shard split's write switch once every
// replica of its broadcast has landed: the source(s) are fenced and the target
// genesis records exist, so the targets may become routable.
//
// The post-image travels in the message body; nothing here derives it from
// mutable meta, which is what makes a retry and a replay apply the same
// topology.
//
// # Order: datacoord, then meta, then the caches
//
// The two halves are independently idempotent, so a crash between them is
// repaired by the retry -- but the order still matters, because it decides what
// a reader can observe in between. DataCoord seeds each target's first channel
// checkpoint; until it has, GetChannelSeekPosition falls back to the
// collection's creation position (timestamp 0), and QueryCoord learns the target
// vchannel exists only once the routing post-image is applied. Seeding first
// therefore closes the window in which a target could be discovered with no
// checkpoint to seek from. The reverse order has no compensating benefit: the
// meta write is not what makes the split durable -- the WAL records already are.
//
// Every refusal below is a System error. The post-image was derived by the split
// coordinator, not by a user request, so a bad one is a Milvus bug; and every
// transport or store failure is transient. The broadcaster retries the callback
// with backoff until it succeeds.
func (c *DDLCallback) splitShardV2AckCallback(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
	header := result.Message.Header()
	postImage := result.Message.MustBody().GetRouting()
	logger := mlog.With(
		mlog.FieldCollectionID(header.GetCollectionId()),
		mlog.Int64("splitTaskID", header.GetSplitTaskId()))

	// allowUnavailable: a collection mid-split is available anyway, and one that
	// is dropping should be ignored below rather than errored here.
	coll, err := c.meta.GetCollectionByID(ctx, "", header.GetCollectionId(), typeutil.MaxTimestamp, true)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			// Neither half has anything left to act on: the split's collection is
			// gone, so committing its routing or its datacoord task would create
			// records for a collection that no longer exists.
			logger.Warn(ctx, "split shard committed on a collection that no longer exists, ignore it")
			return nil
		}
		return merr.Wrap(err, "load the collection for the split shard commit")
	}
	if err := validateSplitShardRoutingPostImage(header, postImage); err != nil {
		// The same derivation DataCoord ran before broadcasting, plus the
		// header-vs-body cross-check nothing else performs. Failing here is a
		// coordinator bug; retrying cannot clear it, but neither can the
		// broadcast be abandoned with a fenced source behind it -- surface it.
		logger.Error(ctx, "split shard routing post-image is malformed", mlog.Err(err))
		return err
	}

	if err := c.commitShardSplitAtDataCoord(ctx, result); err != nil {
		return err
	}

	// Whether there is anything left to write is decided inside the meta table,
	// under its lock: this callback cannot hold that lock across the datacoord
	// call above, so any answer it computed out here could be stale by the time
	// the write lands.
	switch err := c.meta.ApplyShardSplitRouting(ctx, header.GetCollectionId(), postImage, result.GetControlChannelResult().TimeTick); {
	case err == nil:
	case errors.Is(err, errShardSplitRoutingAlreadyApplied):
		logger.Info(ctx, "split shard routing already applied, only expiring caches")
	case errors.Is(err, errShardSplitRoutingSuperseded):
		// The split finished and a later commit moved the collection on while
		// this callback was still retrying. Writing the post-image now would put
		// the released source back to fenced and un-adopt the targets; failing
		// would retry that refusal forever, holding the collection's resource
		// keys and queueing every later DDL of the collection behind it.
		logger.Warn(ctx, "split shard routing post-image superseded by a later commit, skipping the meta apply")
	case errors.Is(err, errAlterCollectionNotFound):
		logger.Warn(ctx, "collection vanished while committing the split shard routing, ignore it")
		return nil
	default:
		return merr.Wrap(err, "apply the split shard routing")
	}
	if err := c.broker.BroadcastAlteredCollection(ctx, header.GetCollectionId()); err != nil {
		return merr.Wrap(err, "broadcast the altered collection after the split shard commit")
	}
	cacheExpirations, err := c.getCacheExpireForCollection(ctx, coll.DBName, coll.Name)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			// The collection entered Dropping between the lookup above and here:
			// it resolves by id but no longer by name. Its caches are about to be
			// invalidated by the drop itself, so there is nothing left to expire
			// and nothing a retry could achieve.
			logger.Warn(ctx, "collection is being dropped, skipping the cache expiry after the split shard commit")
			return nil
		}
		return merr.Wrap(err, "collect the cache expirations after the split shard commit")
	}
	return c.ExpireCaches(ctx, cacheExpirations)
}

// commitShardSplitAtDataCoord is the datacoord half: each source's T_switch as
// its fence actually landed, each target's residues, and each target's genesis
// position, all read off the broadcast result rather than off any local plan.
//
// Idempotent at datacoord by split task id, so a retry -- including one on a
// secondary that has never heard of the task -- converges on the same record.
func (c *DDLCallback) commitShardSplitAtDataCoord(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
	header := result.Message.Header()

	sources := make([]*datapb.SplitShardTaskSource, 0, len(header.GetSourceVchannels()))
	for _, vchannel := range header.GetSourceVchannels() {
		appendResult := result.Results[vchannel]
		if appendResult == nil {
			// The broadcaster acks only once every vchannel has been appended, so
			// a missing entry is an internal bug. Recording a source with tick
			// zero would make the drain wait for a fence that never happened.
			return merr.WrapErrServiceInternalMsg("split shard broadcast result is missing source vchannel %s", vchannel)
		}
		sources = append(sources, &datapb.SplitShardTaskSource{
			Vchannel:       vchannel,
			SwitchTimeTick: appendResult.TimeTick,
		})
	}

	targets := make([]*datapb.SplitShardTaskTarget, 0, len(header.GetTargets()))
	positions := make([]*msgpb.MsgPosition, 0, len(header.GetTargets()))
	for _, target := range header.GetTargets() {
		vchannel := target.GetVchannel()
		appendResult := result.Results[vchannel]
		if appendResult == nil {
			return merr.WrapErrServiceInternalMsg("split shard broadcast result is missing target vchannel %s", vchannel)
		}
		targets = append(targets, &datapb.SplitShardTaskTarget{
			Vchannel: vchannel,
			Buckets:  target.GetRouting().GetBuckets(),
		})
		positions = append(positions, streaming.SplitTargetGenesisPosition(vchannel, appendResult.MessageID, appendResult.TimeTick))
	}

	resp, err := c.mixCoord.CommitShardSplit(ctx, &datapb.CommitShardSplitRequest{
		CollectionId:         header.GetCollectionId(),
		SplitTaskId:          header.GetSplitTaskId(),
		Sources:              sources,
		Targets:              targets,
		TargetStartPositions: positions,
		RoutingModulus:       header.GetRoutingModulus(),
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return merr.Wrap(err, "commit the shard split at datacoord")
	}
	return nil
}

// validateSplitShardRoutingPostImage is the tiling check the RPC path runs on a
// request, run on the message body instead: the arrays must be parallel and the
// writable shards must tile the key space without gap or overlap. A gap silently
// drops the writes of the residues nobody claims; an overlap sends one key to
// two shards.
//
// It also cross-checks the header against the body, which nothing else does.
// The message carries the residues and the modulus TWICE -- the header's copy is
// what this callback hands datacoord, the body's copy is what it writes to the
// collection meta -- and SplitShardParam.Validate never compares the two: it
// checks each target's residues against the HEADER modulus and only that each
// target vchannel appears somewhere in the post-image. A coordinator that filled
// the two copies inconsistently would give datacoord one residue map and the
// routing table another, silently and permanently. This is the first and only
// place both are read together, so it is the only place the disagreement can be
// caught.
func validateSplitShardRoutingPostImage(header *messagespb.SplitShardMessageHeader, postImage *messagespb.AlterCollectionMessageUpdates) error {
	vchannels := postImage.GetVirtualChannelNames()
	if len(vchannels) == 0 ||
		len(vchannels) != len(postImage.GetPhysicalChannelNames()) ||
		len(vchannels) != len(postImage.GetShardInfos()) {
		return merr.WrapErrServiceInternalMsg(
			"split shard routing post-image: channel and shard-info arrays must be parallel and non-empty")
	}
	writable, err := routing.ShardsFromMeta(vchannels, postImage.GetShardInfos())
	if err != nil {
		return merr.Wrap(err, "split shard routing post-image")
	}
	if _, err := routing.Derive(postImage.GetRoutingModulus(), vchannels, writable); err != nil {
		return merr.Wrap(err, "split shard routing post-image")
	}

	if header.GetRoutingModulus() != postImage.GetRoutingModulus() {
		return merr.WrapErrServiceInternalMsg(
			"split shard routing post-image: header routes at modulus %d but the post-image at %d",
			header.GetRoutingModulus(), postImage.GetRoutingModulus())
	}
	postImageBuckets := make(map[string][]uint64, len(vchannels))
	for i, vchannel := range vchannels {
		postImageBuckets[vchannel] = postImage.GetShardInfos()[i].GetHashRouting().GetBuckets()
	}
	for _, target := range header.GetTargets() {
		want, ok := postImageBuckets[target.GetVchannel()]
		if !ok {
			return merr.WrapErrServiceInternalMsg(
				"split shard routing post-image: target %s is not named by the post-image", target.GetVchannel())
		}
		if !sameResidueSet(target.GetRouting().GetBuckets(), want) {
			return merr.WrapErrServiceInternalMsg(
				"split shard routing post-image: target %s owns residues %v in the header but %v in the post-image",
				target.GetVchannel(), target.GetRouting().GetBuckets(), want)
		}
	}
	return nil
}

// sameResidueSet compares two residue lists as sets: the order a target's
// residues are listed in is not meaningful, only which ones it owns.
func sameResidueSet(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	return slices.Equal(slices.Sorted(slices.Values(a)), slices.Sorted(slices.Values(b)))
}
