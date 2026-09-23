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
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
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
// registers it in the node's delegator map, loads the target's recovery view
// (GetRecoveryInfoV2: its L0 and unflushed segments, as a watch would), forwards
// the deletes that view holds to the source, and starts its WAL-consuming
// pipeline at the target's checkpoint.
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
	family, err := sd.frontingFamily()
	if err != nil {
		return nil, err
	}
	scope := frontingSourceScope(family)
	results, err := sd.searchInternal(ctx, req, scope)
	if err != nil {
		return nil, err
	}
	descendants := family.descendants()
	if len(descendants) == 0 {
		return results, nil
	}
	// An advanced (hybrid) search returns one already-reduced element per
	// sub-request; the downstream advanced reduce stamps ReqIndex by slot, so the
	// children's per-sub-request results must be merged into the source's slots,
	// not concatenated (which would yield out-of-range ReqIndex). A plain search
	// returns per-source partial results that the downstream reduce flattens, so
	// concatenation is correct there.
	if req.GetReq().GetIsAdvanced() {
		return sd.frontAdvancedSearch(ctx, req, results, descendants, scope)
	}
	for _, node := range descendants {
		childResults, err := node.sd.searchInternal(ctx, req, scope.forChild(node))
		if err != nil {
			return nil, merr.Wrapf(err, "fronting search on split child %s failed", node.sd.vchannelName)
		}
		results = append(results, childResults...)
	}
	return results, nil
}

// frontAdvancedSearch merges each fronted delegator's per-sub-request results
// into the source's corresponding sub-request slot and reduces per slot, so the
// returned slice keeps exactly one element per sub-request (the contract the
// advanced reduce relies on for ReqIndex).
func (sd *shardDelegator) frontAdvancedSearch(ctx context.Context, req *querypb.SearchRequest, sourceResults []*internalpb.SearchResults, descendants []*familyNode, scope splitReadScope) ([]*internalpb.SearchResults, error) {
	subReqs := req.GetReq().GetSubReqs()
	if len(sourceResults) != len(subReqs) {
		return nil, merr.WrapErrServiceInternalMsg("advanced search returned %d sub-results, expected %d sub-requests", len(sourceResults), len(subReqs))
	}
	perSubReq := make([][]*internalpb.SearchResults, len(subReqs))
	for i := range sourceResults {
		perSubReq[i] = []*internalpb.SearchResults{sourceResults[i]}
	}
	for _, node := range descendants {
		childResults, err := node.sd.searchInternal(ctx, req, scope.forChild(node))
		if err != nil {
			return nil, merr.Wrapf(err, "fronting advanced search on split child %s failed", node.sd.vchannelName)
		}
		if len(childResults) != len(subReqs) {
			return nil, merr.WrapErrServiceInternalMsg("split child %s returned %d sub-results, expected %d", node.sd.vchannelName, len(childResults), len(subReqs))
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

// Query serves a query on this delegator's logical shard, fronting its split
// family the same way Search does: own view plus the view of every fronted
// delegator below it, concatenated. Segments are counted once across them (see
// splitReadScope), which keeps the downstream reduce correct.
func (sd *shardDelegator) Query(ctx context.Context, req *querypb.QueryRequest) ([]*internalpb.RetrieveResults, error) {
	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		return nil, merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("request channels %v", req.GetDmlChannels()))
	}
	family, err := sd.frontingFamily()
	if err != nil {
		return nil, err
	}
	scope := frontingSourceScope(family)
	results, err := sd.queryInternal(ctx, req, scope)
	if err != nil {
		return nil, err
	}
	for _, node := range family.descendants() {
		childResults, err := node.sd.queryInternal(ctx, req, scope.forChild(node))
		if err != nil {
			return nil, merr.Wrapf(err, "fronting query on split child %s failed", node.sd.vchannelName)
		}
		results = append(results, childResults...)
	}
	return results, nil
}

// QueryStream serves a streaming query, fronting the split family: the source
// streams its own view and then every fronted delegator below it streams its
// own view to the same stream server, so the proxy reduces the union.
func (sd *shardDelegator) QueryStream(ctx context.Context, req *querypb.QueryRequest, srv streamrpc.QueryStreamServer) error {
	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		return merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("request channels %v", req.GetDmlChannels()))
	}
	family, err := sd.frontingFamily()
	if err != nil {
		return err
	}
	scope := frontingSourceScope(family)
	if err := sd.queryStreamInternal(ctx, req, srv, scope); err != nil {
		return err
	}
	for _, node := range family.descendants() {
		if err := node.sd.queryStreamInternal(ctx, req, srv, scope.forChild(node)); err != nil {
			return merr.Wrapf(err, "fronting query stream on split child %s failed", node.sd.vchannelName)
		}
	}
	return nil
}

// GetStatistics serves a statistics request, fronting the split family by
// concatenating the source's own statistics with those of every fronted
// delegator below it.
func (sd *shardDelegator) GetStatistics(ctx context.Context, req *querypb.GetStatisticsRequest) ([]*internalpb.GetStatisticsResponse, error) {
	if !funcutil.SliceContain(req.GetDmlChannels(), sd.vchannelName) {
		return nil, merr.WrapErrChannelMisrouted(sd.vchannelName, fmt.Sprintf("GetStatistics channels %v", req.GetDmlChannels()))
	}
	family, err := sd.frontingFamily()
	if err != nil {
		return nil, err
	}
	scope := frontingSourceScope(family)
	results, err := sd.getStatisticsInternal(ctx, req, scope)
	if err != nil {
		return nil, err
	}
	for _, node := range family.descendants() {
		childResults, err := node.sd.getStatisticsInternal(ctx, req, scope.forChild(node))
		if err != nil {
			return nil, merr.Wrapf(err, "fronting statistics on split child %s failed", node.sd.vchannelName)
		}
		results = append(results, childResults...)
	}
	return results, nil
}

// familyNode is one delegator of a read's split family, with the children it
// fronted when the read took its snapshot. The tree is taken once per public
// read, one children snapshot per level, and every step of that read -- the
// MVCC speedup, the tsafe wait, the fan-out and the post-wait re-check -- walks
// this same tree. A child published or detached mid-read at any level therefore
// cannot make those steps disagree about which delegators the read covers.
//
// A child's children exist when the child is itself fenced by a cascaded split
// while its source still fronts it.
type familyNode struct {
	sd       *shardDelegator
	children []*familyNode
}

// fronted returns the node's fronted children, or nil for a nil node.
func (n *familyNode) fronted() []*familyNode {
	if n == nil {
		return nil
	}
	return n.children
}

// descendants lists every fronted delegator below the node, depth first.
func (n *familyNode) descendants() []*familyNode {
	var out []*familyNode
	for _, child := range n.fronted() {
		out = append(out, child)
		out = append(out, child.descendants()...)
	}
	return out
}

// splitPhase is the scope a read of a shard being split gives each delegator of
// its family. It is decided ONCE per read, from the one family-tree snapshot
// that read took, and every delegator of that read is given the same phase, so
// a delegator that querycoord syncs mid-read cannot leave half the family
// reading in one phase and half in the other.
type splitPhase int

const (
	// splitPhaseFronting: at least one delegator below the source has not taken
	// its own vchannel over yet — it is unadopted, or adopted but not yet synced
	// by querycoord and loaded against that sync. The source's own view is then
	// still the shard's complete sealed picture (the datacoord attribution loads
	// the targets' flushed segments into it), so the source reads it, and every
	// delegator below contributes its growing segments only.
	splitPhaseFronting splitPhase = iota
	// splitPhaseHandover: every delegator below the source has been adopted and
	// synced. Each one's view is then complete and, through QC2's window gating,
	// built from a target pulled after the split drained. The source's own view,
	// by contrast, is frozen at a target version from before the rewrite and
	// would serve the same rows under their pre-rewrite segment IDs — a
	// duplicate no ID exclusion can catch. So the source contributes nothing and
	// each delegator below reads its full view.
	splitPhaseHandover
)

// familyPhase decides the phase of one read from the family tree it took.
//
// The decision is over the WHOLE tree, at every depth, and it is all-or-nothing:
// handover only when every delegator below the source has been adopted and
// synced. A mixed family is read in the fronting phase, because the source's own
// view is still the only complete cover of the rows a not-yet-synced delegator
// does not hold, and reading a synced delegator's full view alongside it would
// serve the rewritten copies of rows the source is already serving.
//
// Deciding it per level would have the same flaw one level down: a synced child
// whose own child is not synced would read its full view next to the source's,
// which is exactly the duplicate the all-or-nothing rule avoids.
func familyPhase(family *familyNode) splitPhase {
	descendants := family.descendants()
	if len(descendants) == 0 {
		return splitPhaseFronting
	}
	for _, node := range descendants {
		if !node.sd.adoptedAndSynced() {
			return splitPhaseFronting
		}
	}
	return splitPhaseHandover
}

// adoptedAndSynced reports whether this fronted delegator has taken its own
// vchannel over from the source fronting it: querycoord has adopted it
// (WatchDmChannels on its target vchannel) and has since synced a target version
// into it that it is fully loaded against. Until both hold, its own view is
// incomplete and the source's is the shard's sealed picture.
func (sd *shardDelegator) adoptedAndSynced() bool {
	// Ordered so an unadopted delegator — which a test may build without a
	// distribution at all — never reaches the query view.
	return sd.adopted.Load() && sd.distribution.SyncedAndServiceable()
}

// splitReadScope is how one delegator takes part in a read of a shard that is
// being split: in the fronting phase the source reads its own view and every
// fronted delegator below it adds its growing segments; in the handover phase
// the source reads nothing and every delegator below it reads its own full view.
// Either way every segment must be counted exactly once across them.
//
// The views are disjoint while the split window is open, but not after
// adoption: a relabeled sealed segment is synced into the adopted child's view
// while the source still holds it, and the source keeps fronting the child until
// it is released. The same holds one level down for a cascaded split. So every
// delegator of the read records what it pinned and skips what those read
// before it pinned.
type splitReadScope struct {
	// asChild: this delegator is a split child read in-process by its source, so
	// its pin bypasses the serviceability gate.
	asChild bool
	// pinned, when non-nil, collects the ID of every segment this read pins.
	pinned typeutil.UniqueSet
	// exclude holds the IDs another delegator of the same read already pinned.
	exclude typeutil.UniqueSet
	// family is this delegator's node in the read's family tree: the children
	// it fronts, as the read snapshotted them. The read's MVCC speedup and tsafe
	// wait cover exactly that tree, even when a concurrent release detaches a
	// delegator from it mid-read.
	family *familyNode
	// phase is the whole read's phase, decided once from that tree and passed
	// down unchanged to every delegator of the read.
	phase splitPhase
}

// frontingSourceScope is the source's scope for one read over the family tree
// it fans out to: it decides the read's phase from that one tree and records its
// pins only when there are fronted delegators to exclude them from.
func frontingSourceScope(family *familyNode) splitReadScope {
	if len(family.fronted()) == 0 {
		return splitReadScope{family: family}
	}
	return splitReadScope{pinned: typeutil.NewUniqueSet(), family: family, phase: familyPhase(family)}
}

// forChild is the scope a fronted delegator, at any depth, reads under within
// its source's read: the gate bypass, the read's phase as the source decided it,
// its own node of the read's family tree, and the read's shared pin set, so it
// skips every segment a delegator read before it already pinned and records its
// own for those read after it.
//
// The pin set is threaded in both phases. In the handover phase the source pins
// nothing, so the delegators below it start from an empty exclusion — the "no
// exclusion" the phase calls for — while a relabeled segment held by both a
// child and its own child is still counted once.
func (s splitReadScope) forChild(node *familyNode) splitReadScope {
	return splitReadScope{asChild: true, pinned: s.pinned, exclude: s.pinned, family: node, phase: s.phase}
}

// pinReadableSegments selects what this delegator contributes to the read:
//   - the source (not asChild) pins its own view through the ordinary
//     serviceability gate, and in the handover phase contributes none of it;
//   - a fronted delegator pins through the gate bypass: its growing segments
//     only in the fronting phase, its full readable view in the handover phase.
//
// It then applies the scope's exclusion and records the pins, in the order the
// read visits the family: the source first, then each delegator below it, depth
// first.
func (sd *shardDelegator) pinReadableSegments(scope splitReadScope, requiredLoadRatio float64, partitions ...int64) ([]SnapshotItem, []SegmentEntry, map[int64]int64, int64, error) {
	pin := sd.distribution.PinReadableSegments
	switch {
	case !scope.asChild:
		// the source reads its own view through the ordinary gate.
	case scope.phase == splitPhaseHandover:
		pin = sd.distribution.PinReadableSegmentsAsChild
	default:
		pin = sd.distribution.PinGrowingSegmentsAsChild
	}
	sealed, growing, sealedRowCount, version, err := pin(requiredLoadRatio, partitions...)
	if err != nil {
		return sealed, growing, sealedRowCount, version, err
	}
	if !scope.asChild && scope.phase == splitPhaseHandover {
		// Handover: the delegators below cover the shard on their own, and this
		// one's view is a pre-rewrite snapshot of the same rows. It still took the
		// pin above, so the serviceability gate and the partition check answer
		// exactly as they always did and Unpin stays symmetric, but it contributes
		// nothing and records no ID, which is what leaves the delegators below it
		// reading with no exclusion.
		return nil, nil, nil, version, nil
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
// while it fronts a shard split, every delegator below it in the read's family
// tree (children, and their children for a cascaded split), without taking any
// new snapshot.
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
func (sd *shardDelegator) familyMVCCTimestamp(ctx context.Context, family *familyNode) (uint64, error) {
	mvcc, err := streaming.WAL().Local().GetLatestMVCCTimestampIfLocal(ctx, sd.vchannelName)
	if err != nil {
		return 0, err
	}
	for _, child := range family.fronted() {
		childMVCC, err := child.sd.familyMVCCTimestamp(ctx, child)
		if err != nil {
			return 0, err
		}
		mvcc = max(mvcc, childMVCC)
	}
	return mvcc, nil
}

// waitChildrenTSafe waits for every fronted child's tsafe to reach ts and
// returns the minimum; waitFamilyTSafe serves the merged shard at the min of it
// and the source's own tsafe, so it never answers at a timestamp before every
// child has consumed (and forwarded the deletes) up to it. A child that fronts
// children of its own is waited on through them, over its node of the read's
// family tree.
//
// Every child is first told that a read needs ts, before the wait on any of
// them. A child's pipeline filters empty time ticks unless a read requires them,
// and only the child's own read path would otherwise raise that requirement,
// which runs after this wait. Without it the tick that lifts a child's tsafe to
// ts can be held back for the whole filter interval, and the wait fails with a
// tsafe stall.
func (sd *shardDelegator) waitChildrenTSafe(ctx context.Context, children []*familyNode, ts uint64) (uint64, error) {
	for _, child := range children {
		child.sd.updateLatestRequiredMVCCTimestamp(ts)
	}
	var minTSafe uint64
	for i, child := range children {
		childTSafe, err := child.sd.waitFamilyTSafe(ctx, ts, child.fronted())
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
// before querycoord promotes the target into the CURRENT target: a target is
// one of the split's window targets for as long as its window is open, and no
// current-target snapshot taken during that window ever includes one --
// whether it is the pre-fence current target simply never advancing (a
// collection loaded before the fence: QC2 never syncs, so never promotes, a
// window target from the next target), or a fresh snapshot for a collection
// loaded mid-window, which is the window snapshot with its window targets
// held back (O1). Either way GetShardLeaders' fan-out for this shard stays
// exactly the source until every target has synced and the window ends, then
// flips to the targets in one step (design doc QC1-QC3, O1). Dropping the
// child from fronting at the earlier serviceable flip would leave the
// source's still-current reads unable to see that target's rows for the
// window between the two flips — lost rows. Fronting until release closes
// that window; because the current target is one membership list the fan-out
// reads as a whole, never source-and-targets at once, there is no double-serve
// either.
func (sd *shardDelegator) frontingChildren() []*shardDelegator {
	sd.childMut.Lock()
	defer sd.childMut.Unlock()
	return sd.frontingChildrenLocked()
}

// frontingFamily takes the family tree a public read fans out to, one children
// snapshot per level, and refuses the read while a child spawn is in flight at
// any level.
//
// A spawn is in flight from the moment a delegator consumes its fence until the
// child is published. Over that gap the split key range's new writes already
// land on the target vchannels, but no child fronts them, so the read would miss
// target inserts and target deletes of rows the family holds. The spawn usually
// finishes within milliseconds but can block for seconds on the target's
// recovery info, so the read is refused at once with a retriable System error
// for the proxy to retry, rather than held on the source.
func (sd *shardDelegator) frontingFamily() (*familyNode, error) {
	if sd.retiredWithoutFamily.Load() {
		return nil, merr.WrapErrServiceUnavailable("retired shard split source",
			fmt.Sprintf("source %s is no longer listed by its collection and fronts none of its split targets", sd.vchannelName))
	}
	if sd.splitRecoveryPending.Load() {
		return nil, merr.WrapErrServiceUnavailable("shard split recovery pending",
			fmt.Sprintf("%s does not know yet whether it fronts a shard split", sd.vchannelName))
	}
	return sd.takeFamily(true)
}

// snapshotFamily takes the family tree rooted at sd without refusing on a spawn
// in flight, for callers that only wait on the family's tsafe.
func (sd *shardDelegator) snapshotFamily() *familyNode {
	family, _ := sd.takeFamily(false) // never fails without refuse
	return family
}

// takeFamily builds the family tree rooted at sd, locking one delegator at a
// time. With refuse set it fails as soon as any level has a child spawn in
// flight.
func (sd *shardDelegator) takeFamily(refuse bool) (*familyNode, error) {
	sd.childMut.Lock()
	if refuse {
		if err := sd.refuseWhileSpawningLocked(); err != nil {
			sd.childMut.Unlock()
			return nil, err
		}
	}
	children := sd.frontingChildrenLocked()
	sd.childMut.Unlock()

	node := &familyNode{sd: sd}
	for _, child := range children {
		childNode, err := child.takeFamily(refuse)
		if err != nil {
			return nil, err
		}
		node.children = append(node.children, childNode)
	}
	return node, nil
}

// checkReadFamily re-checks, once a source read has waited for its read
// timestamp, that the family tree it took still covers that timestamp at every
// level.
//
// A read that found no spawn at entry can be overtaken by a fence. Every write
// acknowledged before the read began is at or below the read timestamp, and a
// target write is only acknowledged after its fence, so once a delegator's
// tsafe reaches the read timestamp it has consumed any fence those writes
// depend on (the filter node spawns before the delete node advances tsafe). If
// that started a spawn, or published a child the tree lacks, the read is
// refused like one that met the spawn at entry. A child detached since the
// snapshot is fine: the read still covers it. A fronted delegator's own read is
// not re-checked; its source's read covers it.
func (sd *shardDelegator) checkReadFamily(scope splitReadScope) error {
	if scope.asChild {
		return nil
	}
	return sd.checkFamilySnapshot(scope.family.fronted())
}

// checkFamilySnapshot checks sd against the children snapshot the read took of
// it, then each snapshotted child against its own.
func (sd *shardDelegator) checkFamilySnapshot(snapshot []*familyNode) error {
	if err := sd.checkChildrenSnapshot(snapshot); err != nil {
		return err
	}
	for _, child := range snapshot {
		if err := child.sd.checkFamilySnapshot(child.fronted()); err != nil {
			return err
		}
	}
	return nil
}

// checkChildrenSnapshot refuses the read if sd now has a spawn in flight or a
// child the snapshot lacks.
func (sd *shardDelegator) checkChildrenSnapshot(snapshot []*familyNode) error {
	sd.childMut.Lock()
	defer sd.childMut.Unlock()
	if err := sd.refuseWhileSpawningLocked(); err != nil {
		return err
	}
	for vchannel, child := range sd.children {
		concrete, ok := child.(*shardDelegator)
		if !ok {
			continue // not frontable, see frontingChildrenLocked
		}
		covered := false
		for _, snapshotted := range snapshot {
			if snapshotted.sd == concrete {
				covered = true
				break
			}
		}
		if !covered {
			return merr.WrapErrServiceUnavailable("shard-split child published during the read",
				fmt.Sprintf("source %s, child %s", sd.vchannelName, vchannel))
		}
	}
	return nil
}

// refuseWhileSpawningLocked returns the retriable refusal for a read through a
// source with a child spawn in flight. The caller holds childMut.
func (sd *shardDelegator) refuseWhileSpawningLocked() error {
	if len(sd.spawning) == 0 {
		return nil
	}
	return merr.WrapErrServiceUnavailable("shard-split children are still being spawned",
		fmt.Sprintf("source %s, %d target(s) not yet fronted", sd.vchannelName, len(sd.spawning)))
}

// frontingChildrenLocked is frontingChildren for a caller holding childMut.
func (sd *shardDelegator) frontingChildrenLocked() []*shardDelegator {
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

// WithChildSpawner wires the spawner a delegator uses to front its own shard
// split. The querynode passes it to every delegator it builds, whether watched
// by querycoord or spawned as a split child, so the spawner is in place before
// the delegator's pipeline can deliver a fence. A child adopted later keeps the
// spawner it was built with, so it too can front a cascaded split.
func WithChildSpawner(spawner ChildSpawner) ShardDelegatorOption {
	return func(sd *shardDelegator) {
		sd.childSpawner = spawner
	}
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

// RefuseReadsAsRetiredSource makes every public read through this delegator
// fail with a retriable error, for good. The querynode calls it on a delegator
// watched for a vchannel its collection no longer lists: a shard split source
// that an adoption retired.
//
// Such a delegator has no family. Adoption delisted the source, so there is no
// Creating target left to re-derive and front, and the targets it used to
// front are shards of their own. Its own view is the source's key range as of
// the fence, without the targets' writes since: answering from it alone would
// miss their inserts and return rows they deleted. querycoord never watches a
// delisted vchannel, so this is a safeguard; the refusal is a System error
// (nothing in the request causes it) and retriable, so the proxy retries until
// its shard leaders move to the targets at the flip.
func (sd *shardDelegator) RefuseReadsAsRetiredSource(ctx context.Context) {
	if sd.retiredWithoutFamily.CompareAndSwap(false, true) {
		sd.getLogger(ctx).Warn(ctx, "watched a shard split source its collection no longer lists; refusing every read through it")
	}
}

// ForwardKnownDeletesToParent hands the fronting parent every delete this
// delegator already holds: the records of its registered L0 segments and every
// delete in its buffer. A no-op without a fronting parent.
//
// Live forwarding (ProcessDeleteBatches) only covers the deletes a child
// consumes from its WAL after it is fronted. A child built from its target's
// recovery view -- a respawn after a QueryNode restart, or a first spawn whose
// target already synced -- starts consuming at the target's checkpoint, and
// every delete on the target in (T_switch, checkpoint] is in the target's L0
// segments instead; a delegator watched on its own and then fronted has also
// consumed deletes into its buffer that it never forwarded. Without this the
// source applies none of them to what it serves (its own sealed segments and
// the targets' flushed segments attributed to it), so those rows reappear
// through the source until the flip.
//
// Call it after SetFrontingParent: a delete processed in between is forwarded
// twice, which is harmless (a delete applies to rows older than it, however
// often), where one processed before the parent was set and missed here would
// be lost. The parent buffers what it receives like any forwarded delete, so it
// also reaches segments the parent loads later.
func (sd *shardDelegator) ForwardKnownDeletesToParent(ctx context.Context) error {
	parent := sd.FrontingParent()
	if parent == nil {
		return nil
	}
	batches, err := sd.knownDeleteBatches(ctx)
	if err != nil {
		return err
	}
	if len(batches) == 0 {
		return nil
	}
	parent.ProcessDeleteBatches(batches)
	sd.getLogger(ctx).Info(ctx, "forwarded the deletes a split child already held to its fronting parent",
		mlog.Int("batches", len(batches)))
	return nil
}

// knownDeleteBatches collects the deletes of this delegator's registered L0
// segments and of its delete buffer as delete batches, one per L0 segment and
// one per buffered item. An L0 batch is stamped with the newest timestamp in
// it, so a reader of the parent's buffer asking for deletes at or after some
// timestamp never skips one of its rows (it may get older rows of the batch
// too, which is harmless).
func (sd *shardDelegator) knownDeleteBatches(ctx context.Context) ([]DeleteBatch, error) {
	sd.deleteMut.RLock()
	items := sd.deleteBuffer.ListAfter(0)
	l0Segments := sd.deleteBuffer.ListL0()
	sd.deleteMut.RUnlock()

	batches := make([]DeleteBatch, 0, len(items)+len(l0Segments))
	l0Batches, err := sd.l0DeleteBatches(ctx, l0Segments)
	if err != nil {
		return nil, err
	}
	batches = append(batches, l0Batches...)
	for _, item := range items {
		data := make([]*DeleteData, 0, len(item.Data))
		for _, entry := range item.Data {
			data = append(data, &DeleteData{
				PartitionID: entry.PartitionID,
				PrimaryKeys: entry.DeleteData.Pks,
				Timestamps:  entry.DeleteData.Tss,
				RowCount:    entry.DeleteData.RowCount,
			})
		}
		if len(data) > 0 {
			batches = append(batches, DeleteBatch{Ts: item.Ts, Data: data})
		}
	}
	return batches, nil
}

// l0DeleteBatches reads the delete records of the given L0 segments. Under the
// RemoteLoad forward policy the delegator's L0 segments hold no records in
// memory, so they are loaded here for the forward and released afterwards.
func (sd *shardDelegator) l0DeleteBatches(ctx context.Context, l0Segments []segments.Segment) ([]DeleteBatch, error) {
	if len(l0Segments) == 0 {
		return nil, nil
	}
	if sd.l0ForwardPolicy == L0ForwardPolicyRemoteLoad {
		infos := make([]*querypb.SegmentLoadInfo, 0, len(l0Segments))
		for _, segment := range l0Segments {
			infos = append(infos, segment.LoadInfo())
		}
		loaded, err := sd.loader.Load(ctx, sd.collectionID, segments.SegmentTypeSealed, sd.version, infos...)
		if err != nil {
			return nil, merr.Wrap(err, "failed to load a split child's L0 segments to forward their deletes")
		}
		defer func() {
			for _, segment := range loaded {
				segment.Release(ctx)
			}
		}()
		l0Segments = loaded
	}
	batches := make([]DeleteBatch, 0, len(l0Segments))
	for _, segment := range l0Segments {
		l0, ok := segment.(*segments.L0Segment)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("split child L0 segment %d is a %T, not an L0 segment", segment.ID(), segment)
		}
		pks, tss := l0.DeleteRecords()
		if len(pks) == 0 {
			continue
		}
		// copied: the parent buffers them past this segment's release.
		batches = append(batches, DeleteBatch{
			Ts: lo.Max(tss),
			Data: []*DeleteData{{
				PartitionID: segment.Partition(),
				PrimaryKeys: append([]storage.PrimaryKey(nil), pks...),
				Timestamps:  append([]uint64(nil), tss...),
				RowCount:    int64(len(pks)),
			}},
		})
	}
	return batches, nil
}

// MarkSplitRecoveryPending makes every public read through this delegator fail
// with a retriable error until FinishSplitRecovery.
//
// The querynode sets it on a delegator it watches before the delegator starts,
// when the vchannel may be a split source whose fence is behind its checkpoint
// and so is never re-consumed: until the collection's shard states say whether
// it is, and which targets to re-derive, answering from its own view alone
// could miss every target's rows and deletes without an error. The refusal is a
// System error (nothing in the request causes it) and retriable.
func (sd *shardDelegator) MarkSplitRecoveryPending() {
	sd.splitRecoveryPending.Store(true)
}

// FinishSplitRecovery lifts MarkSplitRecoveryPending. The querynode calls it
// once the recovery has decided: after the targets it re-derived are pending
// spawns (which refuse reads on their own until their children publish), or
// once it found nothing to re-derive.
func (sd *shardDelegator) FinishSplitRecovery() {
	sd.splitRecoveryPending.Store(false)
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

	// A fence naming no target is a silent no-op that leaves the targets
	// unserved for the whole window, so say so rather than return nil quietly.
	if len(targets) == 0 {
		sd.getLogger(ctx).Warn(ctx, "shard-split fence names no target, nothing to front")
		return nil
	}
	for _, target := range targets {
		if target == "" {
			return merr.WrapErrParameterInvalidMsg("split target vchannel must not be empty")
		}
	}
	// Without a spawner no child can ever front a target, so nothing would ever
	// clear a pending slot: recording one would refuse every read through this
	// delegator for good. Every delegator the querynode builds gets a spawner
	// (WithChildSpawner), so this is a Milvus bug, surfaced as a System error
	// for the pipeline to log.
	if sd.childSpawner == nil {
		return merr.WrapErrServiceInternal("shard-split child spawner is not configured on the delegator")
	}
	if sd.spawning == nil {
		sd.spawning = make(map[string]struct{})
	}

	// Once the fence is consumed, each target's writes are outside this
	// delegator's view. So every target not yet fronted is recorded as pending
	// before anything else can fail, and stays pending until its child is
	// published; while any target is pending, reads through this delegator are
	// refused (frontingFamily) rather than answered without that target.
	pending := make([]string, 0, len(targets))
	for _, target := range targets {
		vchannel := target
		if _, ok := sd.children[vchannel]; ok {
			continue // already spawned
		}
		if _, ok := sd.spawning[vchannel]; ok {
			continue // a background spawn is already in flight
		}
		sd.spawning[vchannel] = struct{}{}
		pending = append(pending, target)
	}

	for _, target := range pending {
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

// splitChildSpawnBackoff is how long a failed child spawn waits before its next
// attempt: doubling from one second, capped at thirty.
func splitChildSpawnBackoff(attempt int) time.Duration {
	return min(time.Second<<min(attempt, 5), 30*time.Second)
}

// spawnChildAsync runs one child spawn off the flow-graph goroutine. The spawner
// itself drives its blocking recovery-info wait off the node lifetime context,
// so node shutdown unblocks it. On success the child is published into the
// fronting set.
//
// A failed spawn is retried with backoff, and its target stays pending
// meanwhile. Clearing the slot instead would leave reads through this delegator
// answered from its own view alone while the target takes the split key range's
// writes, a silent wrong result; stalling the pipeline instead would block the
// flow-graph goroutine behind a coordinator dependency and still not fix
// anything by itself. Pending targets make reads fail with a retriable error
// until the child is published, and the retry lets a transient failure (a
// coordinator hiccup, recovery info not yet visible) heal. Retrying stops, and
// the slot is cleared, only once the source is being released or the delegator
// has stopped, when no read comes through it any more.
func (sd *shardDelegator) spawnChildAsync(ctx context.Context, vchannel string) {
	log := sd.getLogger(ctx)
	for attempt := 0; ; attempt++ {
		child, err := sd.childSpawner.SpawnSplitChild(ctx, SpawnChildParams{
			CollectionID:   sd.collectionID,
			ReplicaID:      sd.replicaID,
			Version:        sd.version,
			SourceVChannel: sd.vchannelName,
			TargetVChannel: vchannel,
			Parent:         sd,
		})
		if err == nil {
			if err = sd.publishSpawnedChild(ctx, vchannel, child); err == nil {
				return
			}
		}
		if sd.abandonSpawn(ctx, vchannel) {
			return
		}
		// Every failure is retried, a refusal included: the spawner refuses a
		// target served by a delegator fronted by another source
		// (ErrChannelReduplicate), and a refusal given up on for good would keep
		// the target pending, refusing every read through this delegator until
		// it is released -- which during the window it is not. Retrying
		// re-evaluates the refusal, so it ends when its cause does.
		backoff := splitChildSpawnBackoff(attempt)
		log.Warn(ctx, "failed to spawn split child delegator, reads through this delegator are refused until a retry succeeds",
			mlog.String("targetVChannel", vchannel), mlog.Int("attempt", attempt+1),
			mlog.Duration("retryIn", backoff), mlog.Err(err))
		if sd.waitSpawnBackoff(ctx, vchannel, backoff) {
			return
		}
	}
}

// spawnBackoffPoll is how often a spawn waiting out its backoff checks whether
// it must give up.
const spawnBackoffPoll = 50 * time.Millisecond

// waitSpawnBackoff waits out a failed spawn's backoff, but gives the spawn up
// (see abandonSpawn) as soon as the source is being released or the delegator
// has stopped, rather than sleeping through a backoff of up to thirty seconds
// first. It reports whether the spawn was given up.
func (sd *shardDelegator) waitSpawnBackoff(ctx context.Context, vchannel string, backoff time.Duration) bool {
	deadline := time.Now().Add(backoff)
	for remaining := backoff; remaining > 0; remaining = time.Until(deadline) {
		time.Sleep(min(remaining, spawnBackoffPoll))
		if sd.abandonSpawn(ctx, vchannel) {
			return true
		}
	}
	return false
}

// abandonSpawn gives up a failed spawn, clearing its pending slot, once the
// source is being released or the delegator has stopped.
func (sd *shardDelegator) abandonSpawn(ctx context.Context, vchannel string) bool {
	if !sd.releasing.Load() && !sd.Stopped() {
		return false
	}
	sd.childMut.Lock()
	delete(sd.spawning, vchannel)
	sd.childMut.Unlock()
	sd.getLogger(ctx).Info(ctx, "gave up spawning a split child, its source is released or stopped",
		mlog.String("targetVChannel", vchannel))
	return true
}

// publishSpawnedChild clears the pending slot and adds the spawned child to the
// fronting set.
//
// Only a delegator this source fronts is published. One whose fronting parent
// is not this source forwards no delete here, so fronting it would serve rows
// deleted on the target: it is neither published nor torn down, the target
// stays pending, and the retriable error makes the spawn try again. Being
// adopted does not disqualify this source's own child, since adoption does not
// detach it and querycoord can adopt it before this runs.
//
// A child of this source is aborted instead if the source was released or has
// stopped while it spawned.
func (sd *shardDelegator) publishSpawnedChild(ctx context.Context, vchannel string, child ShardDelegator) error {
	if child.FrontingParent() != ShardDelegator(sd) {
		return merr.WrapErrServiceUnavailable("spawned split target delegator is not fronted by this source",
			fmt.Sprintf("source %s, target %s", sd.vchannelName, vchannel))
	}
	sd.childMut.Lock()
	delete(sd.spawning, vchannel)
	if sd.releasing.Load() || sd.Stopped() {
		// the source was released (or stopped) while this spawn was in flight: do
		// not publish the child (it would be fronted by a gone source).
		// releaseSplitChildren set releasing before snapshotting children, and that
		// snapshot is taken under childMut, so it could not have seen this
		// not-yet-published child — hence we, not it, must tear the child down.
		sd.childMut.Unlock()
		sd.childSpawner.AbortSplitChild(ctx, child, sd.collectionID, vchannel)
		sd.getLogger(ctx).Info(ctx, "aborted a split child spawned after source release or stop",
			mlog.String("targetVChannel", vchannel))
		return nil
	}
	sd.children[vchannel] = child
	sd.childMut.Unlock()
	sd.getLogger(ctx).Info(ctx, "spawned an in-process child delegator for a split target",
		mlog.String("targetVChannel", vchannel))
	return nil
}
