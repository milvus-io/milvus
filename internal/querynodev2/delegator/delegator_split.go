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

package delegator

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SpawnChildParams carries the source delegator's identity to the spawner so it
// can create the child delegator in the same collection/replica without the
// spawner reaching back into the source delegator's internals.
type SpawnChildParams struct {
	CollectionID   int64
	ReplicaID      int64
	Version        int64
	SourceVChannel string
	TargetVChannel string
	// Parent is the source delegator that fronts the spawned child; the spawner
	// wires it as the child's frontingParent before the child starts consuming,
	// so no delete consumed by the child escapes forwarding.
	Parent ShardDelegator
}

// ChildSpawner spawns an in-process child shard delegator for one shard-split
// target vchannel. The querynode implements it: it creates the child delegator,
// registers it in the node's delegator map, and starts its WAL-consuming
// pipeline at the target's recovery seek position (fetched from the channel
// checkpoint store via GetRecoveryInfoV2, the same seek path any delegator uses).
//
// The child is born non-serviceable — it owns no sealed segment and has no
// querycoord target version — so GetDataDistribution skips it and no proxy read
// is routed to it until querycoord adopts it. delegator0 reaches it only through
// the in-process handle returned here.
type ChildSpawner interface {
	SpawnSplitChild(ctx context.Context, params SpawnChildParams) (ShardDelegator, error)
	// AbortSplitChild tears down a child that was spawned but must not be
	// published, because the source was released while the spawn was in flight.
	AbortSplitChild(ctx context.Context, child ShardDelegator, collectionID int64, vchannel string)
}

// Search serves a search on this delegator's logical shard. During a shard
// split the source delegator fronts its in-process children: it searches its
// own view and fans the same request out to each child's own view. The segment
// sets are disjoint (a row lives either in the source's view or in a child's
// growing segment, never both), so the downstream reduce neither duplicates nor
// misses rows. A plain search concatenates the per-source partial results; an
// advanced (hybrid) search merges per sub-request (see frontAdvancedSearch).
func (sd *shardDelegator) Search(ctx context.Context, req *querypb.SearchRequest) ([]*internalpb.SearchResults, error) {
	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		return nil, merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("request channels %v", req.GetDmlChannels()))
	}
	children := sd.frontingChildren()
	scope := frontingSourceScope(children)
	results, err := sd.searchInternal(ctx, req, scope)
	if err != nil {
		return nil, err
	}
	if len(children) == 0 {
		return results, nil
	}
	// An advanced (hybrid) search returns one already-reduced element per
	// sub-request; the downstream advanced reduce stamps ReqIndex by slot, so the
	// children's per-sub-request results must be merged into the source's slots,
	// not concatenated (which would yield out-of-range ReqIndex). A plain search
	// returns per-source partial results that the downstream reduce flattens, so
	// concatenation is correct there.
	if req.GetReq().GetIsAdvanced() {
		return sd.frontAdvancedSearch(ctx, req, results, children, scope.forChild())
	}
	for _, child := range children {
		childResults, err := child.searchInternal(ctx, req, scope.forChild())
		if err != nil {
			return nil, errors.Wrapf(err, "fronting search on split child %s failed", child.vchannelName)
		}
		results = append(results, childResults...)
	}
	return results, nil
}

// frontAdvancedSearch merges each fronting child's per-sub-request results into
// the source's corresponding sub-request slot and reduces per slot, so the
// returned slice keeps exactly one element per sub-request (the contract the
// advanced reduce relies on for ReqIndex).
func (sd *shardDelegator) frontAdvancedSearch(ctx context.Context, req *querypb.SearchRequest, sourceResults []*internalpb.SearchResults, children []*shardDelegator, childScope splitReadScope) ([]*internalpb.SearchResults, error) {
	subReqs := req.GetReq().GetSubReqs()
	if len(sourceResults) != len(subReqs) {
		return nil, merr.WrapErrServiceInternalMsg("advanced search returned %d sub-results, expected %d sub-requests", len(sourceResults), len(subReqs))
	}
	perSubReq := make([][]*internalpb.SearchResults, len(subReqs))
	for i := range sourceResults {
		perSubReq[i] = []*internalpb.SearchResults{sourceResults[i]}
	}
	for _, child := range children {
		childResults, err := child.searchInternal(ctx, req, childScope)
		if err != nil {
			return nil, errors.Wrapf(err, "fronting advanced search on split child %s failed", child.vchannelName)
		}
		if len(childResults) != len(subReqs) {
			return nil, merr.WrapErrServiceInternalMsg("split child %s returned %d sub-results, expected %d", child.vchannelName, len(childResults), len(subReqs))
		}
		for i := range childResults {
			perSubReq[i] = append(perSubReq[i], childResults[i])
		}
	}
	merged := make([]*internalpb.SearchResults, len(subReqs))
	for i, subReq := range subReqs {
		reduced, err := segments.ReduceSearchOnQueryNode(ctx, perSubReq[i],
			reduce.NewReduceSearchResultInfo(subReq.GetNq(), subReq.GetTopk()).
				WithMetricType(subReq.GetMetricType()).
				WithGroupSize(subReq.GetGroupSize()).
				WithGroupByFieldIdsFromProto(subReq.GetGroupByFieldId(), req.GetReq().GetGroupByFieldIds()))
		if err != nil {
			return nil, err
		}
		merged[i] = reduced
	}
	return merged, nil
}

// Query serves a query on this delegator's logical shard, fronting its
// in-process split children the same way Search does: own view plus each child's
// view, concatenated. Disjoint segment sets keep the downstream reduce correct.
func (sd *shardDelegator) Query(ctx context.Context, req *querypb.QueryRequest) ([]*internalpb.RetrieveResults, error) {
	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		return nil, merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("request channels %v", req.GetDmlChannels()))
	}
	children := sd.frontingChildren()
	scope := frontingSourceScope(children)
	results, err := sd.queryInternal(ctx, req, scope)
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		childResults, err := child.queryInternal(ctx, req, scope.forChild())
		if err != nil {
			return nil, errors.Wrapf(err, "fronting query on split child %s failed", child.vchannelName)
		}
		results = append(results, childResults...)
	}
	return results, nil
}

// QueryStream serves a streaming query, fronting the split children: the source
// streams its own view and then each child streams its own view to the same
// stream server, so the proxy reduces the union.
func (sd *shardDelegator) QueryStream(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error {
	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		return merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("request channels %v", req.GetDmlChannels()))
	}
	children := sd.frontingChildren()
	scope := frontingSourceScope(children)
	if err := sd.queryStreamInternal(ctx, req, srv, scope); err != nil {
		return err
	}
	for _, child := range children {
		if err := child.queryStreamInternal(ctx, req, srv, scope.forChild()); err != nil {
			return errors.Wrapf(err, "fronting query stream on split child %s failed", child.vchannelName)
		}
	}
	return nil
}

// GetStatistics serves a statistics request, fronting the split children by
// concatenating the source's own statistics with each child's.
func (sd *shardDelegator) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) ([]*internalpb.GetStatisticsResponse, error) {
	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		return nil, merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("GetStatistics channels %v", req.GetDmlChannels()))
	}
	children := sd.frontingChildren()
	scope := frontingSourceScope(children)
	results, err := sd.getStatisticsInternal(ctx, req, scope)
	if err != nil {
		return nil, err
	}
	for _, child := range children {
		childResults, err := child.getStatisticsInternal(ctx, req, scope.forChild())
		if err != nil {
			return nil, errors.Wrapf(err, "fronting statistics on split child %s failed", child.vchannelName)
		}
		results = append(results, childResults...)
	}
	return results, nil
}

// splitReadScope is how one delegator takes part in a read of a shard that is
// being split: the source reads its own view, then each fronted child reads its
// own, and every segment must be counted exactly once across them.
//
// The views are disjoint while the split window is open, but not after
// adoption: a relabeled sealed segment is synced into the adopted child's view
// while the source still holds it, and the source keeps fronting the child until
// it is released. So the source records what it pinned and each child skips it.
type splitReadScope struct {
	// asChild: this delegator is a split child read in-process by its source, so
	// its pin bypasses the serviceability gate.
	asChild bool
	// pinned, when non-nil, collects the ID of every segment this read pins.
	pinned typeutil.UniqueSet
	// exclude holds the IDs another delegator of the same read already pinned.
	exclude typeutil.UniqueSet
	// family is the one snapshot of fronted children the source took for this
	// read. The read fans out to exactly these children, so its MVCC speedup and
	// tsafe wait cover exactly these too, even when a concurrent source release
	// detaches them mid-read.
	family []*shardDelegator
}

// frontingSourceScope is the source's scope for one read over the children
// snapshot it fans out to: it records its pins only when there are children to
// exclude them from.
func frontingSourceScope(children []*shardDelegator) splitReadScope {
	if len(children) == 0 {
		return splitReadScope{}
	}
	return splitReadScope{pinned: typeutil.NewUniqueSet(), family: children}
}

// readFamily returns the fronted children the read on sd must cover: the
// source's snapshot for its own read, or, for a fronted child, the child's own
// snapshot of any children it fronts in turn.
func (s splitReadScope) readFamily(sd *shardDelegator) []*shardDelegator {
	if s.asChild {
		return sd.frontingChildren()
	}
	return s.family
}

// forChild is the scope a fronted child reads under: the gate bypass, minus
// every segment the source has already pinned.
func (s splitReadScope) forChild() splitReadScope {
	return splitReadScope{asChild: true, exclude: s.pinned}
}

// pinReadableSegments selects the serviceability-gated pin for the source
// delegator's own read and the gate-bypass pin when fronting a split child, then
// applies the scope's exclusion and records the pins.
func (sd *shardDelegator) pinReadableSegments(scope splitReadScope, requiredLoadRatio float64, partitions ...int64) ([]SnapshotItem, []SegmentEntry, map[int64]int64, int64, error) {
	pin := sd.distribution.PinReadableSegments
	if scope.asChild {
		pin = sd.distribution.PinReadableSegmentsAsChild
	}
	sealed, growing, sealedRowCount, version, err := pin(requiredLoadRatio, partitions...)
	if err != nil {
		return sealed, growing, sealedRowCount, version, err
	}
	if len(scope.exclude) > 0 {
		sealed, growing = excludeSegments(sealed, growing, scope.exclude)
	}
	if scope.pinned != nil {
		for _, item := range sealed {
			for _, entry := range item.Segments {
				scope.pinned.Insert(entry.SegmentID)
			}
		}
		for _, entry := range growing {
			scope.pinned.Insert(entry.SegmentID)
		}
	}
	return sealed, growing, sealedRowCount, version, nil
}

// excludeSegments drops every sealed and growing segment whose ID is in exclude,
// and any node left with no sealed segment to read.
func excludeSegments(sealed []SnapshotItem, growing []SegmentEntry, exclude typeutil.UniqueSet) ([]SnapshotItem, []SegmentEntry) {
	keptSealed := make([]SnapshotItem, 0, len(sealed))
	for _, item := range sealed {
		segments := make([]SegmentEntry, 0, len(item.Segments))
		for _, entry := range item.Segments {
			if !exclude.Contain(entry.SegmentID) {
				segments = append(segments, entry)
			}
		}
		if len(segments) > 0 {
			keptSealed = append(keptSealed, SnapshotItem{NodeID: item.NodeID, Segments: segments})
		}
	}
	keptGrowing := make([]SegmentEntry, 0, len(growing))
	for _, entry := range growing {
		if !exclude.Contain(entry.SegmentID) {
			keptGrowing = append(keptGrowing, entry)
		}
	}
	return keptSealed, keptGrowing
}

// familyMVCCTimestamp returns the latest WAL MVCC timestamp over every vchannel
// a write of this delegator's logical shard can land on: its own vchannel and,
// while it fronts a shard split, each child in the read's snapshot children
// (recursively over each child's own fronted children, for a cascaded split).
//
// A Strong read may lower its guarantee to this value (speedupGuranteeTS) only
// because every write acknowledged before the read began is at or below the
// MVCC of the vchannel it was written to. After a fence the source vchannel
// takes no DML: the split key range is written to the target vchannels, on other
// pchannels, so the source's own MVCC can sit below an acknowledged delete.
// Taking the max over the family keeps the guarantee at or above every such
// write, and waitChildrenTSafe then holds the read until each child has consumed
// up to it.
//
// If any member's MVCC is not known locally the error is returned, and the
// caller keeps the proxy's guarantee timestamp.
func (sd *shardDelegator) familyMVCCTimestamp(ctx context.Context, children []*shardDelegator) (uint64, error) {
	mvcc, err := streaming.WAL().Local().GetLatestMVCCTimestampIfLocal(ctx, sd.vchannelName)
	if err != nil {
		return 0, err
	}
	for _, child := range children {
		childMVCC, err := child.familyMVCCTimestamp(ctx, child.frontingChildren())
		if err != nil {
			return 0, err
		}
		mvcc = max(mvcc, childMVCC)
	}
	return mvcc, nil
}

// waitChildrenTSafe waits for every fronted child's tsafe to reach ts and
// returns the minimum, so the source delegator serves the merged shard at
// min(child tsafes): it never answers at a timestamp before every child has
// consumed (and forwarded the deletes) up to it.
//
// Every child is first told that a read needs ts, before the wait on any of
// them. A child's pipeline filters empty time ticks unless a read requires them,
// and only the child's own read path would otherwise raise that requirement,
// which runs after this wait. Without it the tick that lifts a child's tsafe to
// ts can be held back for the whole filter interval, and the wait fails with a
// tsafe stall.
func (sd *shardDelegator) waitChildrenTSafe(ctx context.Context, children []*shardDelegator, ts uint64) (uint64, error) {
	for _, child := range children {
		child.updateLatestRequiredMVCCTimestamp(ts)
	}
	var minTSafe uint64
	for i, child := range children {
		childTSafe, err := child.waitTSafe(ctx, ts)
		if err != nil {
			return 0, err
		}
		if i == 0 || childTSafe < minTSafe {
			minTSafe = childTSafe
		}
	}
	return minTSafe, nil
}

// SplitChildVChannels returns the target vchannels of this source delegator's
// in-process split children, so the querynode can tear them down when the source
// channel is released (the children are registered under their own vchannels in
// the node and hold their own collection ref + pipeline).
func (sd *shardDelegator) SplitChildVChannels() []string {
	sd.childMut.Lock()
	defer sd.childMut.Unlock()
	if len(sd.children) == 0 {
		return nil
	}
	vchannels := make([]string, 0, len(sd.children))
	for vchannel := range sd.children {
		vchannels = append(vchannels, vchannel)
	}
	return vchannels
}

// frontingChildren returns a snapshot of the in-process split children the
// source must still serve on behalf of, so a fan-out can search them without
// holding the child lock across the search.
//
// The source fronts a child for as long as the child is in sd.children, i.e.
// until it is detached at source release — NOT until the child first becomes
// serviceable. A child becomes serviceable the moment querycoord syncs it the
// NEXT-target version (delegator.SyncTargetVersion), which happens strictly
// before querycoord promotes the target into the CURRENT target and the proxy
// re-routes the split key range onto it. Dropping the child from fronting at the
// earlier serviceable flip would leave the range served by neither the source
// nor the (not-yet-routed) target for that window — lost rows. Fronting until
// release closes that window; the proxy routes each key range to exactly one
// vchannel (the empty-range source is excluded from a range-tiled fan-out once
// the targets cover the space), so there is no double-serve in the overlap.
func (sd *shardDelegator) frontingChildren() []*shardDelegator {
	sd.childMut.Lock()
	defer sd.childMut.Unlock()
	if len(sd.children) == 0 {
		return nil
	}
	children := make([]*shardDelegator, 0, len(sd.children))
	for _, child := range sd.children {
		// every child is created by NewShardDelegator, so the assertion holds; a
		// non-*shardDelegator (e.g. a test mock) simply cannot be fronted.
		if concrete, ok := child.(*shardDelegator); ok {
			children = append(children, concrete)
		}
	}
	return children
}

// SetChildSpawner injects the spawner the source delegator uses to create its
// in-process children when it consumes a SplitShard fence. The querynode sets it
// right after creating the delegator.
func (sd *shardDelegator) SetChildSpawner(spawner ChildSpawner) {
	sd.childMut.Lock()
	defer sd.childMut.Unlock()
	sd.childSpawner = spawner
}

// SetFrontingParent marks this delegator as a shard-split child fronted by
// parent: every delete it consumes is forwarded to the parent. The spawner sets
// it before the child starts consuming, so no delete escapes forwarding. The
// write takes deleteMut to synchronize with ProcessDelete's read of the field.
// Passing nil detaches the child at adoption: it stops forwarding deletes and
// becomes a standalone delegator.
func (sd *shardDelegator) SetFrontingParent(parent ShardDelegator) {
	sd.deleteMut.Lock()
	defer sd.deleteMut.Unlock()
	sd.frontingParent = parent
}

// FrontingParent returns the source delegator fronting this one (nil if this is
// not a shard-split child or it has already been adopted).
func (sd *shardDelegator) FrontingParent() ShardDelegator {
	sd.deleteMut.Lock()
	defer sd.deleteMut.Unlock()
	return sd.frontingParent
}

// DetachSplitChild stops this source delegator from fronting the given target
// vchannel's child, called when the source is released after a completed split.
// The child stays registered on the node as a now-standalone delegator; the
// source no longer fans reads out to it.
func (sd *shardDelegator) DetachSplitChild(childVChannel string) {
	sd.childMut.Lock()
	defer sd.childMut.Unlock()
	delete(sd.children, childVChannel)
}

// MarkAdopted records that querycoord has adopted this split child (issued
// WatchDmChannel for its target vchannel). After this the child is reported by
// GetDataDistribution and follows the normal SyncTargetVersion path to becoming
// serviceable; the source keeps fronting it until it actually is.
func (sd *shardDelegator) MarkAdopted() {
	sd.adopted.Store(true)
}

// IsUnadoptedSplitChild reports whether this delegator is a split child still
// fronted in-process and not yet adopted by querycoord. Such a child must stay
// invisible to querycoord: GetDataDistribution skips it so querycoord neither
// routes reads to it nor tries to manage its half-built channel.
func (sd *shardDelegator) IsUnadoptedSplitChild() bool {
	return sd.FrontingParent() != nil && !sd.adopted.Load()
}

// MarkReleasing records that this source delegator's channel is being released.
// releaseSplitChildren calls it before snapshotting the children, so any child
// spawn still in flight (the spawner can block for seconds fetching recovery
// info) aborts at its publish step instead of registering an orphan fronted by
// an already-released source.
func (sd *shardDelegator) MarkReleasing() {
	sd.releasing.Store(true)
}

// ProcessSplitShard reacts to the SplitShard fence message consumed on the
// source vchannel: it spawns an in-process child delegator for every target
// vchannel so the source delegator can front the targets' growing data during
// the split window.
//
// Spawning is launched in the BACKGROUND: spawning a child fetches the target's
// recovery seek (a coordinator RPC that retries until the target materializes)
// and starts a pipeline, which can take many seconds. Doing it synchronously
// here would block the source's flow-graph goroutine (stalling shutdown) and,
// while holding childMut, every read that snapshots the children. So this only
// records intent and returns immediately; the children appear asynchronously.
//
// It is idempotent: a target whose child already exists or is already spawning
// is skipped, so a pipeline replay or recovery re-consume of the fence never
// double-spawns.
func (sd *shardDelegator) ProcessSplitShard(ctx context.Context, targets []string) error {
	sd.childMut.Lock()
	defer sd.childMut.Unlock()

	if sd.childSpawner == nil {
		return merr.WrapErrServiceInternal("shard-split child spawner is not configured on the delegator")
	}
	// A fence naming no target is a silent no-op that leaves the targets
	// unserved for the whole window, so say so rather than return nil quietly.
	if len(targets) == 0 {
		sd.getLogger(ctx).Warn(ctx, "shard-split fence names no target, nothing to front")
		return nil
	}
	if sd.spawning == nil {
		sd.spawning = make(map[string]struct{})
	}

	for _, target := range targets {
		vchannel := target
		if vchannel == "" {
			return merr.WrapErrParameterInvalidMsg("split target vchannel must not be empty")
		}
		if _, ok := sd.children[vchannel]; ok {
			continue // already spawned
		}
		if _, ok := sd.spawning[vchannel]; ok {
			continue // a background spawn is already in flight
		}
		sd.spawning[vchannel] = struct{}{}
		// Detached from the request's cancellation but not from its values: the
		// spawn must outlive the fence message that asked for it -- a child
		// canceled with the request would leave the target unfronted -- while
		// trace context should still follow it. Not the request ctx itself,
		// deliberately.
		ctx := context.WithoutCancel(ctx)
		go sd.spawnChildAsync(ctx, target) //nolint:gosec // G118: the spawn must outlive the request that carried the fence; canceling it with the request would leave the target unfronted. Values (trace) are kept, only cancellation is dropped.
	}
	return nil
}

// spawnChildAsync runs one child spawn off the flow-graph goroutine. The spawner
// itself drives its blocking recovery-info wait off the node lifetime context,
// so node shutdown unblocks it. On success the child is published into the
// fronting set; on failure it is logged and the slot is cleared (a later fence
// re-consume can retry).
func (sd *shardDelegator) spawnChildAsync(ctx context.Context, vchannel string) {
	child, err := sd.childSpawner.SpawnSplitChild(ctx, SpawnChildParams{
		CollectionID:   sd.collectionID,
		ReplicaID:      sd.replicaID,
		Version:        sd.version,
		SourceVChannel: sd.vchannelName,
		TargetVChannel: vchannel,
		Parent:         sd,
	})

	sd.childMut.Lock()
	delete(sd.spawning, vchannel)
	if err != nil {
		sd.childMut.Unlock()
		sd.getLogger(context.Background()).Warn(context.Background(), "failed to spawn split child delegator",
			mlog.String("targetVChannel", vchannel), mlog.Err(err))
		return
	}
	if sd.releasing.Load() {
		// the source was released while this spawn was in flight: do not publish
		// the child (it would be fronted by a gone source). releaseSplitChildren
		// set releasing before snapshotting children, and that snapshot is taken
		// under childMut, so it could not have seen this not-yet-published child —
		// hence we, not it, must tear the child down.
		sd.childMut.Unlock()
		sd.childSpawner.AbortSplitChild(context.Background(), child, sd.collectionID, vchannel)
		sd.getLogger(context.Background()).Info(context.Background(), "aborted a split child spawned after source release",
			mlog.String("targetVChannel", vchannel))
		return
	}
	sd.children[vchannel] = child
	sd.childMut.Unlock()
	sd.getLogger(context.Background()).Info(context.Background(), "spawned an in-process child delegator for a split target",
		mlog.String("targetVChannel", vchannel))
}
