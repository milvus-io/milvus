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

package observers

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/eventlog"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type CollectionObserver struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dist                 *meta.DistributionManager
	meta                 *meta.Meta
	targetMgr            meta.TargetManagerInterface
	targetObserver       *TargetObserver
	checkerController    *checkers.CheckerController
	nodeMgr              *session.NodeManager
	partitionLoadedCount map[int64]int

	loadTasks *typeutil.ConcurrentMap[string, LoadTask]

	proxyManager proxyutil.ProxyClientManagerInterface

	startOnce sync.Once
	stopOnce  sync.Once
}

// LoadTask tracks one in-flight load. ResourceGroup optionally narrows the task
// to the replica(s) of CollectionID that live in a single resource group.
//
// An empty ResourceGroup means the task is not scoped to any resource group,
// which is the only shape upstream ever registers: every field below other than
// LoadType/CollectionID/PartitionIDs is read exclusively behind a
// `ResourceGroup != ""` guard, so a task with an empty ResourceGroup travels
// through precisely the code that existed before resource groups were plumbed
// in here.
//
// A resource-group-scoped task cannot use the shared collection/partition
// UpdatedAt to decide whether it is making progress: that timestamp belongs to
// whichever resource group loaded the collection first, so a second resource
// group's brand new task would look expired the instant it is registered and
// the replica it just spawned would be torn down. LastProgress/LastProgressAt
// give such a task its own progress watermark instead: LastProgressAt moves
// whenever this task's own per-resource-group load percentage changes -- in
// either direction, because a load that goes backwards is still moving -- and
// the task is only considered timed out after LoadTimeoutSeconds in which that
// percentage did not change at all. A percentage that could not be read is
// carried over rather than recorded, so a failed read is never mistaken for a
// stall. LastProgress is -1 until the first percentage is read.
//
// ReadySince is set the first time the readiness shield in
// observeResourceGroupTimeout finds the task's resource group serving, and is
// zero until then. A task with it set is kept alive by the shield rather than
// by a load in progress, and it no longer counts as "loading" for the purpose
// of pushing the checkers (observeLoadStatus): the group is serving, exactly
// as a Loaded collection on master is, and master pushes nothing for those.
// It is cleared when the task is re-armed to watch what a teardown left
// behind, since that group has just proven it is not serving.
//
// ReplicaNumberPending is set when a teardown removed replicas but the write
// of the collection's ReplicaNumber that must follow was refused. The two are
// separate catalog writes by separate managers, so the deletion can be
// persisted while the count is not, and a count left stale is read by
// everything downstream as replicas that still exist. A task so marked owes
// the write, and retries it at the top of each of its observations until it
// sticks (observeResourceGroupTimeout); it is what keeps a task alive for a
// group with no replica left, which would otherwise have been removed with
// the teardown and could never have retried. It is cleared once the count is
// persisted.
type LoadTask struct {
	LoadType     querypb.LoadType
	CollectionID int64
	PartitionIDs []int64

	ResourceGroup string

	LastProgress   int32
	LastProgressAt time.Time

	ReadySince time.Time

	ReplicaNumberPending bool
}

// NewCollectionObserver builds the observer. nodeMgr is read by the
// resource-group-scoped timeout alone, to ask utils.ShardLeaderReadinessByResourceGroup
// whether a group that looks stalled is in fact serving.
func NewCollectionObserver(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	targetObserver *TargetObserver,
	checherController *checkers.CheckerController,
	proxyManager proxyutil.ProxyClientManagerInterface,
	nodeMgr *session.NodeManager,
) *CollectionObserver {
	ob := &CollectionObserver{
		dist:                 dist,
		meta:                 meta,
		targetMgr:            targetMgr,
		targetObserver:       targetObserver,
		checkerController:    checherController,
		nodeMgr:              nodeMgr,
		partitionLoadedCount: make(map[int64]int),
		loadTasks:            typeutil.NewConcurrentMap[string, LoadTask](),
		proxyManager:         proxyManager,
	}

	// Add load task for collection recovery
	collections := meta.GetAllCollections(context.TODO())
	// Scoped tasks are registered by the incremental-expansion path alone,
	// which only runs when a form is installed (job.isIncrementalExpansion),
	// so on a stock binary there is nothing to rebuild -- and the rebuild is
	// the one producer of scoped tasks that would otherwise run there. Read
	// once: SetHook runs before milvus starts, so the answer cannot change
	// underneath this loop.
	formInstalled := extension.FormInstalled()
	for _, collection := range collections {
		ob.LoadCollection(context.Background(), collection.GetCollectionID(), "")
		if formInstalled && collection.GetStatus() == querypb.LoadStatus_Loaded {
			ob.recoverResourceGroupTasks(context.Background(), collection.GetCollectionID())
		}
	}

	return ob
}

// recoverResourceGroupTasks rebuilds the resource-group-scoped tasks of one
// already-loaded collection after a restart. It runs only when a form is
// installed: the path that registers scoped tasks in the first place is
// form-gated, so a stock binary has none to rebuild, and handing a stock
// binary teardown-capable tasks it never asked for would let a load timeout
// release replicas of a Loaded collection -- something master never does.
//
// Scoped tasks live only in memory: the incremental-expansion path adds a
// resource group to a collection that is already serving, deliberately leaves
// the collection meta at Loaded/100% -- overwriting it would take the resource
// groups that are serving right now down with it -- and registers a scoped task
// to watch the new group. Nothing about that task is persisted, so a
// querycoord that restarts mid-expansion would come back with the new group's
// replicas in meta, the raised replica number, a collection reporting 100%, and
// nobody watching the group that is actually still loading: it would never
// finish, never time out, and never be torn down.
//
// The state itself says which groups those are, so no new meta is needed. A
// group whose replicas already carry every target of the collection is loaded
// and needs no watcher; any other group of a loaded collection is a group that
// was still catching up when the process went away, which is exactly what a
// scoped task is for.
//
// Only loaded collections are rebuilt this way. A collection still loading is
// watched end to end by its collection-wide task, which owns both the status
// promotion and the timeout; putting scoped tasks beside it would aim a second,
// independent teardown at the same replicas.
//
// Every resource group holding a replica gets a task, without asking how loaded
// it is. Asking would be pointless here: this runs from the constructor, where
// the distribution manager is empty and the target may not be recovered, so the
// answer is 0 for everything -- a group that has been serving for weeks and a
// group that never came up are indistinguishable here. The task itself is what
// tells them apart later, on evidence, and a group that turns out to be loaded
// finishes its task on the first tick that says so.
//
// Handing a task to a serving group is safe because of two rules, not one:
//
//   - a percentage that is not evidence is unknown, and unknown pauses the
//     clock (observeResourceGroupProgress and observeResourceGroupTimeout), so
//     the timeout is only ever measured over ticks that learned something;
//   - the teardown itself may never take the last replicas of a Loaded
//     collection (releaseResourceGroupOnTimeout), so even a reading that is
//     wrong in the pessimistic direction cannot unload a serving collection;
//     and a group with a single serviceable leader is never torn down at all
//     (observeResourceGroupTimeout's readiness shield).
//
// A task whose group never becomes readable simply stays paused until the
// collection leaves meta, where observeTimeout's first check removes it.
func (ob *CollectionObserver) recoverResourceGroupTasks(ctx context.Context, collectionID int64) {
	recovered := typeutil.NewSet[string]()
	for _, replica := range ob.meta.GetByCollection(ctx, collectionID) {
		rgName := replica.GetResourceGroup()
		if rgName == "" || recovered.Contain(rgName) {
			continue
		}
		recovered.Insert(rgName)

		mlog.Info(ctx, "rebuilding the load task of a resource group after a restart",
			mlog.FieldCollectionID(collectionID),
			mlog.String("resourceGroup", rgName))
		ob.LoadCollection(ctx, collectionID, rgName)
	}
}

func (ob *CollectionObserver) Start() {
	ob.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		ob.cancel = cancel

		observePeriod := Params.QueryCoordCfg.CollectionObserverInterval.GetAsDuration(time.Millisecond)
		ob.wg.Add(1)
		go func() {
			defer ob.wg.Done()

			interval := observePeriod
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					mlog.Info(context.TODO(), "CollectionObserver stopped")
					return

				case <-ticker.C:
					ob.Observe(ctx)
					// apply dynamic update only when changed
					newInterval := Params.QueryCoordCfg.CollectionObserverInterval.GetAsDuration(time.Millisecond)
					if newInterval != interval {
						interval = newInterval
						select {
						case <-ticker.C:
						default:
						}
						ticker.Reset(interval)
					}
				}
			}
		}()
	})
}

func (ob *CollectionObserver) Stop() {
	ob.stopOnce.Do(func() {
		if ob.cancel != nil {
			ob.cancel()
		}
		ob.wg.Wait()
	})
}

// LoadCollection registers a watcher for a LoadCollection in flight.
//
// rgName scopes the watcher to the replica(s) of collectionID that live in that
// resource group, so that sibling resource groups loading the same collection
// neither hold this task open nor close it early nor expire it. Pass "" to get
// the unscoped, collection-wide watcher, which is what every caller that does
// not use resource groups wants.
func (ob *CollectionObserver) LoadCollection(ctx context.Context, collectionID int64, rgName string) {
	span := trace.SpanFromContext(ctx)

	traceID := span.SpanContext().TraceID()
	key := traceID.String()

	if !traceID.IsValid() {
		key = fmt.Sprintf("LoadCollection_%d", collectionID)
	}
	key = qualifyTaskKeyByResourceGroup(key, rgName)

	ob.loadTasks.Insert(key, LoadTask{
		LoadType:       querypb.LoadType_LoadCollection,
		CollectionID:   collectionID,
		ResourceGroup:  rgName,
		LastProgress:   -1,
		LastProgressAt: time.Now(),
	})
	ob.checkerController.Check()
}

// LoadPartitions registers a watcher for a LoadPartitions in flight. See
// LoadCollection for the meaning of rgName.
func (ob *CollectionObserver) LoadPartitions(ctx context.Context, collectionID int64, partitionIDs []int64, rgName string) {
	span := trace.SpanFromContext(ctx)

	traceID := span.SpanContext().TraceID()
	key := traceID.String()
	if !traceID.IsValid() {
		key = fmt.Sprintf("LoadPartition_%d_%v", collectionID, partitionIDs)
	}
	key = qualifyTaskKeyByResourceGroup(key, rgName)

	ob.loadTasks.Insert(key, LoadTask{
		LoadType:       querypb.LoadType_LoadPartition,
		CollectionID:   collectionID,
		PartitionIDs:   partitionIDs,
		ResourceGroup:  rgName,
		LastProgress:   -1,
		LastProgressAt: time.Now(),
	})
	ob.checkerController.Check()
}

// qualifyTaskKeyByResourceGroup keeps two loads of the same collection into two
// different resource groups on two different task entries, instead of the
// second overwriting the first. The key is returned untouched for an empty
// rgName, so unscoped tasks keep exactly the keys they have always had.
func qualifyTaskKeyByResourceGroup(key string, rgName string) string {
	if rgName == "" {
		return key
	}
	return fmt.Sprintf("%s_%s", key, rgName)
}

func (ob *CollectionObserver) Observe(ctx context.Context) {
	progress := ob.observeResourceGroupProgress(ctx)
	ob.observeTimeout(ctx, progress)
	ob.observeLoadStatus(ctx, progress)
}

// unknownLoadPercentage is what the per-tick scan publishes for a resource
// group whose load percentage could not be established this tick. It is not a
// low percentage and must never be compared as one: it says nothing was
// learned. Both consumers of the progress map read it as "no information" --
// observeResourceGroupTimeout pauses its clock, observeLoadStatus does not
// finish the task -- and it deliberately travels all the way to them unchanged,
// so that neither can be handed a figure that looks like a measurement and is
// not.
const unknownLoadPercentage int32 = -1

// resourceGroupProgress is what the per-tick scan publishes for one scoped
// task: the group's figure, and the per-replica figures it was folded from.
//
// Both come from ONE reading of the targets and the distribution. That is
// what lets the teardown be narrower than the measurement: the group is
// judged stalled on Percentage, and the replicas released for it are the ones
// whose own figure, in that same reading, sat at the stalled watermark. A
// second reading taken at teardown time could not say that -- a target
// re-pull landing in between drops every replica below 100 and would hand the
// teardown a replica that was carrying everything when the group was judged.
type resourceGroupProgress struct {
	// Percentage is the group's figure, the minimum over Replicas, or
	// unknownLoadPercentage when nothing was learned this tick.
	Percentage int32
	// Replicas holds each replica's own figure, keyed by replica ID. It is
	// nil exactly when Percentage is unknown.
	Replicas map[int64]int32
}

// unknownResourceGroupProgress is the record for a tick that learned nothing.
var unknownResourceGroupProgress = resourceGroupProgress{Percentage: unknownLoadPercentage}

// observeResourceGroupProgress is the resource-group-aware slice of the per-tick
// scan, computed exactly once per tick and consumed by both observeTimeout (as
// the progress watermark, and the per-replica figures behind it) and
// observeLoadStatus (to decide when a scoped task is finished). Keeping it in
// one place is what stops the two consumers from drifting apart, and reusing
// utils.ReplicaLoadPercentagesByResourceGroup is what stops the walk over
// channel/segment targets from being written a second time.
//
// The scan restricts itself to tasks that actually name a resource group. When
// no task does -- which is every deployment that does not use per-resource-group
// loads -- it walks the task map without reading a single target or
// distribution entry and returns a nil map, so both consumers see no progress
// entry at all and fall through to the code that existed before.
//
// A percentage is published only when the reading is EVIDENCE. Three things
// have to hold, and each of them is a way a serving resource group reads as 0
// while nothing is wrong with it:
//
//   - the collection's target must be known. It is measured against the target,
//     so with no channels in either the current or the next target every group
//     of the collection scores 0. After an ungraceful restart that is the
//     normal state for a while: the current target is persisted only on a
//     graceful stop and the next target has to be pulled from datacoord.
//   - every replica of this resource group must have a node that has reported a
//     channel of this collection. The figure is a MIN across the group's
//     replicas, so one replica whose node has not reported yet -- a pod still
//     pending, an image still pulling -- drags a fully loaded group to 0. This
//     subsumes the weaker "has anything reported this collection at all",
//     which is the state right after any coordinator restart.
//   - the read itself must succeed and answer a percentage, rather than -1 for
//     a group that holds no replica or a collection that is not registered.
//
// Anything else is unknownLoadPercentage. The predicates live here rather than
// in utils.LoadPercentageByResourceGroup because they are this observer's
// question -- "is this reading worth acting on" -- while the util answers the
// question ShowLoadCollections asks, where -1 has a narrower published meaning
// ("no replica of this collection in that group") that these situations must
// not be folded into.
func (ob *CollectionObserver) observeResourceGroupProgress(ctx context.Context) map[string]resourceGroupProgress {
	var (
		progress map[string]resourceGroupProgress
		reported map[int64][]*meta.DmChannel
	)
	ob.loadTasks.Range(func(key string, task LoadTask) bool {
		if task.ResourceGroup == "" {
			return true
		}
		if progress == nil {
			progress = make(map[string]resourceGroupProgress)
			reported = make(map[int64][]*meta.DmChannel)
		}

		if !ob.readyToObserve(ctx, task.CollectionID) {
			mlog.RatedInfo(ctx, 0.1, "collection target not known yet, resource group load percentage unknown",
				mlog.FieldCollectionID(task.CollectionID),
				mlog.String("resourceGroup", task.ResourceGroup))
			progress[key] = unknownResourceGroupProgress
			return true
		}

		// One distribution read per collection per tick, shared by every group
		// of it.
		channels, cached := reported[task.CollectionID]
		if !cached {
			channels = ob.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(task.CollectionID))
			reported[task.CollectionID] = channels
		}
		if !ob.everyReplicaHasReported(ctx, task.CollectionID, task.ResourceGroup, channels) {
			mlog.RatedInfo(ctx, 0.1, "a replica of the resource group has not reported yet, load percentage unknown",
				mlog.FieldCollectionID(task.CollectionID),
				mlog.String("resourceGroup", task.ResourceGroup))
			progress[key] = unknownResourceGroupProgress
			return true
		}

		// A task whose last figure is 100 and whose group only waits for the
		// current target to be promoted is not measured again: the walk
		// below visits every sealed-segment target of the collection per
		// replica, and this is the one state a scoped task sits in for a
		// while - every recovered task of every loaded collection, from the
		// next-target pull after a restart until the promotion lands, at
		// every 200 ms tick. The stored figure feeds both consumers exactly
		// as a fresh 100 would: the watermark refreshes, so the group is
		// never torn down, and the finish still waits for the promotion.
		//
		// What this forgoes is seeing a figure that drops back below 100
		// before the promotion - a next target re-pulled with a freshly
		// flushed segment - until the promotion tick, where the gate is off
		// and the figure is measured again. Such a group is not left alone
		// meanwhile: its task still drives the checkers, which load the
		// segment. What it cannot do is time out in that window, which a
		// group at 100 never could (the watermark refreshes at 100), and
		// which master's own task for a loaded collection never does either.
		// A re-arm resets the figure to -1 and measures afresh.
		if task.LastProgress >= 100 && !ob.targetMgr.IsCurrentTargetExist(ctx, task.CollectionID, common.AllPartitionsID) {
			progress[key] = ob.reusedFullProgress(ctx, task)
			return true
		}

		figures, err := utils.ReplicaLoadPercentagesByResourceGroup(ctx, ob.meta, ob.targetMgr, ob.dist, task.CollectionID, task.ResourceGroup)
		if err != nil {
			// Rate-limited: this runs per task per observation tick, and a
			// persistent read failure (a recorded load failure, say) would
			// otherwise print once a second for as long as it lasts.
			mlog.RatedWarn(ctx, 0.1, "failed to read resource group load percentage",
				mlog.FieldCollectionID(task.CollectionID),
				mlog.String("resourceGroup", task.ResourceGroup),
				mlog.Err(err))
		}
		percentage := utils.MinReplicaLoadPercentage(figures)
		if percentage < 0 {
			progress[key] = unknownResourceGroupProgress
			return true
		}
		progress[key] = resourceGroupProgress{Percentage: percentage, Replicas: figures}
		return true
	})
	return progress
}

// everyReplicaHasReported answers whether every replica of collectionID in
// rgName owns a node that appears in channels, the delegators the distribution
// manager currently holds for this collection.
//
// It is deliberately per replica rather than per group: the group's percentage
// is the minimum over its replicas, so a single replica that has told this
// coordinator nothing is enough to make the whole group's figure meaningless.
// A group with no replica at all answers false too -- there is nothing to have
// evidence about, and the load may still be committing its meta.
func (ob *CollectionObserver) everyReplicaHasReported(ctx context.Context, collectionID int64, rgName string, channels []*meta.DmChannel) bool {
	found := false
	for _, replica := range ob.meta.GetByCollection(ctx, collectionID) {
		if replica.GetResourceGroup() != rgName {
			continue
		}
		found = true

		reported := false
		for _, channel := range channels {
			if replica.Contains(channel.Node) {
				reported = true
				break
			}
		}
		if !reported {
			return false
		}
	}
	return found
}

// reusedFullProgress is the record for a task whose stored figure of 100 is
// reused rather than measured (see observeResourceGroupProgress): the group at
// 100, and each of its replicas at 100, so the record keeps the shape of a
// measured one - Replicas is nil exactly when Percentage is unknown, and it is
// not. The teardown, the only reader of the per-replica figures, never runs on
// a group at 100.
func (ob *CollectionObserver) reusedFullProgress(ctx context.Context, task LoadTask) resourceGroupProgress {
	figures := make(map[int64]int32)
	for _, replica := range ob.meta.GetByCollection(ctx, task.CollectionID) {
		if replica.GetResourceGroup() == task.ResourceGroup {
			figures[replica.GetID()] = 100
		}
	}
	return resourceGroupProgress{Percentage: 100, Replicas: figures}
}

func (ob *CollectionObserver) observeTimeout(ctx context.Context, progress map[string]resourceGroupProgress) {
	ob.loadTasks.Range(func(traceID string, task LoadTask) bool {
		collection := ob.meta.GetCollection(ctx, task.CollectionID)
		// collection released
		if collection == nil {
			mlog.Info(ctx, "Load Collection Task canceled, collection removed from meta", mlog.FieldCollectionID(task.CollectionID), mlog.String("traceID", traceID))
			ob.loadTasks.Remove(traceID)
			return true
		}

		// A resource-group-scoped task judges itself on its own progress
		// watermark, never on the shared collection/partition UpdatedAt, which
		// belongs to whichever resource group loaded first.
		if task.ResourceGroup != "" {
			ob.observeResourceGroupTimeout(ctx, traceID, task, progress[traceID])
			return true
		}

		switch task.LoadType {
		case querypb.LoadType_LoadCollection:
			if collection.GetStatus() == querypb.LoadStatus_Loading &&
				time.Now().After(collection.UpdatedAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))) {
				mlog.Info(ctx, "load collection timeout, cancel it",
					mlog.FieldCollectionID(collection.GetCollectionID()),
					mlog.Duration("loadTime", time.Since(collection.CreatedAt)))
				ob.meta.CollectionManager.RemoveCollection(ctx, collection.GetCollectionID())
				ob.meta.ReplicaManager.RemoveCollection(ctx, collection.GetCollectionID())
				ob.targetObserver.ReleaseCollection(collection.GetCollectionID())
				ob.loadTasks.Remove(traceID)
			}
		case querypb.LoadType_LoadPartition:
			partitionIDs := typeutil.NewSet(task.PartitionIDs...)
			partitions := ob.meta.GetPartitionsByCollection(ctx, task.CollectionID)
			partitions = lo.Filter(partitions, func(partition *meta.Partition, _ int) bool {
				return partitionIDs.Contain(partition.GetPartitionID())
			})

			// all partition released
			if len(partitions) == 0 {
				mlog.Info(ctx, "Load Partitions Task canceled, collection removed from meta",
					mlog.FieldCollectionID(task.CollectionID),
					mlog.Int64s("partitionIDs", task.PartitionIDs),
					mlog.String("traceID", traceID))
				ob.loadTasks.Remove(traceID)
				return true
			}

			working := false
			for _, partition := range partitions {
				if time.Now().Before(partition.UpdatedAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))) {
					working = true
					break
				}
			}
			// only all partitions timeout means task timeout
			if !working {
				mlog.Info(ctx, "load partitions timeout, cancel it",
					mlog.FieldCollectionID(task.CollectionID),
					mlog.Int64s("partitionIDs", task.PartitionIDs))
				for _, partition := range partitions {
					ob.meta.RemovePartition(ctx, partition.CollectionID, partition.GetPartitionID())
					ob.targetObserver.ReleasePartition(partition.GetCollectionID(), partition.GetPartitionID())
				}

				// all partition timeout, remove collection
				if len(ob.meta.GetPartitionsByCollection(ctx, task.CollectionID)) == 0 {
					mlog.Info(ctx, "collection timeout due to all partition removed", mlog.Int64("collection", task.CollectionID))

					ob.meta.CollectionManager.RemoveCollection(ctx, task.CollectionID)
					ob.meta.ReplicaManager.RemoveCollection(ctx, task.CollectionID)
					ob.targetObserver.ReleaseCollection(task.CollectionID)
				}
			}
		}
		return true
	})
}

// observeResourceGroupTimeout decides the fate of one resource-group-scoped
// task from progress: the load percentage of this task's collection restricted
// to this task's resource group, and the per-replica figures behind it, as
// computed once for this tick.
//
// The judgment reads nothing shared with sibling resource groups. Reading
// collection.UpdatedAt here would be the bug this branch exists to avoid: a
// collection loaded into one resource group hours ago carries an UpdatedAt
// hours in the past, so a second resource group's task would be declared timed
// out on its very first observer tick and the replica just spawned for it would
// be released before it could load anything.
func (ob *CollectionObserver) observeResourceGroupTimeout(ctx context.Context, key string, task LoadTask, progress resourceGroupProgress) {
	now := time.Now()
	percentage := progress.Percentage

	// A task that owes the replica-count write-back of an earlier teardown
	// settles that first, before anything is judged on the group's figure:
	// the count is what every reader of the collection divides by, and it
	// is wrong until this succeeds. This runs before the unknown/pause logic
	// below on purpose - a group whose last replica was released reports
	// nothing, and a task paused on that would never retry. Until the write
	// sticks nothing else is decided for the task; the survivors, if any,
	// are re-armed with a fresh watermark and lose nothing by the wait.
	if task.ReplicaNumberPending {
		if !ob.writeReplicaNumberBack(ctx, task) {
			return
		}
		task.ReplicaNumberPending = false
		if !ob.groupHoldsReplicas(ctx, task) {
			// Nothing left to watch and nothing left to write: the task
			// only lived to retry the count.
			ob.loadTasks.Remove(key)
			return
		}
		ob.loadTasks.Insert(key, task)
	}

	// An unknown percentage PAUSES the clock: the watermark keeps the last
	// percentage that was actually read, and LastProgressAt is moved to now so
	// that the timeout can only ever be measured over ticks that learned
	// something. Nothing is known, so nothing is decided -- and a task that has
	// never had one informative observation can never time out at all.
	//
	// Letting the clock run here instead is what would make this dangerous:
	// the figure is unknown exactly when a coordinator has just restarted and
	// no QueryNode has reported yet, so the teardown below would fire on every
	// resource group of every loaded collection at once. A task whose group
	// never becomes readable is not leaked: it is removed by observeTimeout's
	// first check as soon as the collection leaves meta (release or drop).
	if percentage < 0 {
		mlog.RatedWarn(ctx, 0.1, "resource group load percentage unknown, pausing the load timeout",
			mlog.FieldCollectionID(task.CollectionID),
			mlog.String("resourceGroup", task.ResourceGroup),
			mlog.String("traceID", key),
			mlog.Int32("lastKnownProgress", task.LastProgress))
		task.LastProgressAt = now
		ob.loadTasks.Insert(key, task)
		return
	}

	// Refresh the watermark whenever the percentage MOVED, in either
	// direction. A load can legitimately go backwards -- a delegator restarts,
	// or a freshly flushed segment enters the next target -- and then climb
	// back through values it has already visited; a watermark that only
	// ratcheted up would stop refreshing at the first regression and the load
	// timeout would tear down a resource group that was making progress the
	// whole time. Only a percentage that does not move at all is a stall.
	//
	// This is a weaker rule than the unscoped path's, which refreshes a
	// partition's UpdatedAt only on a strict rise (observePartitionLoadStatus
	// returns early when the loaded count did not grow, though it does write
	// the lower count back). The consequence is accepted deliberately: a
	// percentage that oscillates below 100 -- 50, 49, 50 -- keeps refreshing
	// and is never declared stalled, so "stalled" here means "did not move at
	// all". Tracking a loaded-count watermark of our own instead would catch
	// the oscillation, but it would also reintroduce the false regression this
	// rule exists to remove, on a figure that legitimately moves down in steady
	// state; a load that keeps moving is left alone.
	//
	// A fully loaded resource group refreshes forever: its task may
	// legitimately outlive the load timeout while waiting for the gated status
	// promotion, and a replica that is already serving must never be torn down
	// by a load timeout. A zero LastProgressAt (a task built by a caller that
	// did not seed it) starts its clock now rather than at the epoch, so it is
	// never instantly expired.
	if percentage != task.LastProgress || percentage >= 100 || task.LastProgressAt.IsZero() {
		task.LastProgress = percentage
		task.LastProgressAt = now
		ob.loadTasks.Insert(key, task)
		return
	}

	if now.Before(task.LastProgressAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))) {
		return
	}

	// The percentage has not moved for a whole load timeout. Before anything
	// is torn down, ask the question the proxy asks of this group - can its
	// replicas serve every shard of the collection right now - and if the
	// answer is yes, the group is not stalled, whatever the figure says.
	//
	// The two disagree by construction, not by accident. The percentage is
	// measured against the NEXT target and integer-truncated, so a large
	// collection under continuous flush can sit at a constant 99 for as long
	// as the ingest lasts: every tick sees a freshly flushed segment in the
	// next target that no replica carries yet. Readiness is measured against
	// the CURRENT target, and says Ready the whole time. Without this check a
	// group serving queries would be torn down for not moving, and the rule
	// that keeps a Loaded collection's LAST replicas does not reach it - a
	// collection loaded into several groups loses one that is serving.
	//
	// Ready is ANY-of over the group's replicas: a shard is served if ONE
	// query-visible replica of the group has a serviceable leader for it
	// (utils.ShardLeaderReadinessByResourceGroup), so a group with two
	// replicas serving and a third stalled at the channel answers Ready and
	// is kept WHOLE, stalled replica included. That is deliberate. Master
	// never releases a replica of a Loaded collection; its checkers keep
	// retrying the stalled one at their own cadence, and they do here too.
	// The per-replica teardown below is for a group that serves nothing.
	//
	// A Ready group refreshes its watermark rather than finishing: finishing
	// is observeLoadStatus's decision, made on the percentage reaching 100
	// with the current target promoted, and it stays that way. The task lives
	// on and is asked again a load timeout later, which is what a fully loaded
	// group does anyway. A Ready group that never samples 100 therefore keeps
	// its task indefinitely; the task goes away with the collection, when it
	// is released or dropped.
	//
	// What that costs is one readiness read per load timeout, and nothing
	// else: the task is marked ReadySince, and a task so marked no longer
	// makes the tick push the checkers (observeLoadStatus). Without the mark
	// a task kept this way would run every checker at the observer's own
	// rate for as long as the ingest lasts, for a group that is serving.
	if ob.resourceGroupIsReady(ctx, task) {
		mlog.RatedInfo(ctx, 0.1, "resource group load percentage has not moved for the load timeout, but its shard leaders are ready, keeping it",
			mlog.FieldCollectionID(task.CollectionID),
			mlog.String("resourceGroup", task.ResourceGroup),
			mlog.String("traceID", key),
			mlog.Int32("loadPercentage", percentage))
		task.LastProgressAt = now
		if task.ReadySince.IsZero() {
			task.ReadySince = now
		}
		ob.loadTasks.Insert(key, task)
		return
	}

	mlog.Info(ctx, "load timeout for resource group, cancel it",
		mlog.FieldCollectionID(task.CollectionID),
		mlog.String("resourceGroup", task.ResourceGroup),
		mlog.String("traceID", key),
		mlog.Stringer("loadType", task.LoadType),
		mlog.Int32("loadPercentage", percentage),
		mlog.Duration("stalledFor", now.Sub(task.LastProgressAt)))
	ob.releaseResourceGroupOnTimeout(ctx, key, task, progress.Replicas)
}

// resourceGroupIsReady answers whether the replicas of this task's resource
// group can serve every shard of the collection right now, which is the same
// verdict the proxy's readiness check gets. Anything short of a clear yes - an
// unready shard, a missing current target, a read the stores cannot answer -
// is a no: this is a shield against tearing down a serving group, and a
// verdict that cannot be established is no evidence the group is serving.
func (ob *CollectionObserver) resourceGroupIsReady(ctx context.Context, task LoadTask) bool {
	readiness, err := utils.ShardLeaderReadinessByResourceGroup(ctx, ob.meta, ob.targetMgr, ob.dist, ob.nodeMgr,
		task.CollectionID, task.ResourceGroup)
	if err != nil {
		mlog.RatedWarn(ctx, 0.1, "failed to read the shard leader readiness of a resource group",
			mlog.FieldCollectionID(task.CollectionID),
			mlog.String("resourceGroup", task.ResourceGroup),
			mlog.Err(err))
		return false
	}
	return readiness.Ready
}

// releaseResourceGroupOnTimeout tears down the replicas of this collection
// that stalled in the timed-out resource group, and only those. Sibling
// resource groups keep their replicas, since their loads are independent by
// construction; so do this group's own replicas that are not behind.
//
// The group is MEASURED as one figure -- the minimum over its replicas -- but
// it is torn down replica by replica. The two granularities differ on purpose:
// an expansion may put several replicas into one group at once, and if two of
// them carry everything while a third sits on a node that cannot load it, the
// group reads as the third for as long as it stalls. Releasing the group would
// take the two that are loaded with it, and retrying the expansion would do
// the same again. So figures, the per-replica reading from the tick that
// judged the group stalled, decides: a replica whose own figure sat at the
// stalled watermark is released; one above it -- at 100, or merely further
// along, which is not evidence it stalled -- is kept. A replica the reading
// did not cover (added to the group since the tick) is kept too: no figure,
// no evidence.
//
// ReplicaNumber comes down by exactly the number of replicas removed. If the
// group still holds replicas afterwards, the task lives on to watch them, with
// a fresh watermark so the survivors are judged on their own figure rather
// than on the one the released replica pinned; it finishes as any scoped task
// does once they carry everything, and a survivor that then stalls for a full
// load timeout is released the same way. The task ends with the group, when
// its last replica is gone.
//
// The collection-level meta and target go away only once the last resource
// group is gone, which is the same condition under which the unscoped path
// drops them.
//
// With one exception, which is the hard limit on what a load timeout is allowed
// to do: it may shrink an expansion that never came up, and it may abandon a
// load that never completed, but it may NEVER unload a collection that is
// serving. If releasing the stalled replicas would leave a Loaded collection
// with none, the task is dropped and nothing is released -- the collection
// keeps serving with the replicas it has. The unscoped path has always had
// this property for free, because its timeout branch only runs for a Loading
// collection; a scoped task is registered on a Loaded one, so it needs the
// rule spelled out. Any percentage that is wrong in the pessimistic direction
// -- and the ways to read a serving group as 0 are many, all of them transient
// -- stops here instead of costing the deployment its collection.
//
// The readiness shield in observeResourceGroupTimeout runs before this and is
// ANY-of over the group: a group with a single serviceable leader never gets
// here, whatever its other replicas are doing. This teardown is for a group
// that serves nothing.
func (ob *CollectionObserver) releaseResourceGroupOnTimeout(ctx context.Context, key string, task LoadTask, figures map[int64]int32) {
	replicas := ob.meta.GetByCollection(ctx, task.CollectionID)
	stalled := make([]int64, 0)
	kept := make([]int64, 0)
	for _, replica := range replicas {
		if replica.GetResourceGroup() != task.ResourceGroup {
			continue
		}
		figure, measured := figures[replica.GetID()]
		if !measured || figure > task.LastProgress {
			kept = append(kept, replica.GetID())
			continue
		}
		stalled = append(stalled, replica.GetID())
	}

	if len(stalled) == len(replicas) {
		if collection := ob.meta.GetCollection(ctx, task.CollectionID); collection != nil &&
			collection.GetStatus() == querypb.LoadStatus_Loaded {
			mlog.RatedWarn(ctx, 0.1, "load timeout for the last replicas of a loaded collection, keeping it loaded and dropping the task",
				mlog.FieldCollectionID(task.CollectionID),
				mlog.String("resourceGroup", task.ResourceGroup),
				mlog.String("traceID", key),
				mlog.Int64s("replicaIDs", stalled))
			ob.loadTasks.Remove(key)
			return
		}
	}

	if len(stalled) > 0 {
		mlog.Info(ctx, "releasing the replicas that stalled in a timed out resource group",
			mlog.FieldCollectionID(task.CollectionID),
			mlog.String("resourceGroup", task.ResourceGroup),
			mlog.String("traceID", key),
			mlog.Int64s("stalledReplicaIDs", stalled),
			mlog.Int64s("keptReplicaIDs", kept),
			mlog.Int32("loadPercentage", task.LastProgress))
		if err := ob.meta.RemoveReplicas(ctx, task.CollectionID, stalled...); err != nil {
			// Leave the task in place so the next tick retries the teardown;
			// dropping it here would leak the stalled replicas forever.
			mlog.Warn(ctx, "failed to remove replicas of timed out resource group",
				mlog.FieldCollectionID(task.CollectionID),
				mlog.String("resourceGroup", task.ResourceGroup),
				mlog.Int64s("replicaIDs", stalled),
				mlog.Err(err))
			return
		}
	}

	remaining := ob.meta.GetByCollection(ctx, task.CollectionID)
	if len(remaining) == 0 {
		ob.meta.CollectionManager.RemoveCollection(ctx, task.CollectionID)
		ob.targetObserver.ReleaseCollection(task.CollectionID)
		ob.loadTasks.Remove(key)
		return
	}
	// The incremental-expansion path raised the collection's ReplicaNumber
	// when this resource group was added; taking replicas away must write the
	// number back down, or everything that reads it - updateLoadConfig's
	// replica-changed check, ShowLoadCollections, the collection-wide
	// observer's loadPercentage denominator - keeps counting replicas that no
	// longer exist, and the load percentage can never reach 100 again.
	//
	// The replica deletion above is already persisted, so a refused write
	// here cannot be undone and must not be forgotten either: the task is
	// marked as owing it and retries at the top of its next observation,
	// whether or not the group kept any replica (see LoadTask).
	task.ReplicaNumberPending = !ob.writeReplicaNumberBack(ctx, task)

	if len(kept) > 0 {
		// The group still holds replicas: keep watching them, from a fresh
		// watermark, so they are judged on their own figure. The group has
		// just proven it is not serving, so the task is a load in progress
		// again and drives the checkers again.
		task.LastProgress = -1
		task.LastProgressAt = time.Now()
		task.ReadySince = time.Time{}
		ob.loadTasks.Insert(key, task)
		return
	}
	if task.ReplicaNumberPending {
		// Nothing left to watch, but the count is still owed: the task stays
		// for that alone, and goes once the write sticks.
		ob.loadTasks.Insert(key, task)
		return
	}
	ob.loadTasks.Remove(key)
}

// writeReplicaNumberBack makes the collection's ReplicaNumber agree with the
// replicas it actually has, and answers whether it does afterwards. A
// collection that is gone has nothing to write and answers true; a write the
// catalog refuses answers false, and the caller retries on a later tick.
func (ob *CollectionObserver) writeReplicaNumberBack(ctx context.Context, task LoadTask) bool {
	coll := ob.meta.GetCollection(ctx, task.CollectionID)
	if coll == nil {
		return true
	}
	replicas := len(ob.meta.GetByCollection(ctx, task.CollectionID))
	if int(coll.GetReplicaNumber()) == replicas {
		return true
	}
	if err := ob.meta.UpdateReplicaNumber(ctx, task.CollectionID, int32(replicas), coll.GetUserSpecifiedReplicaMode()); err != nil {
		// Rate-limited: a catalog that stays down is retried on every tick
		// of the task, and would otherwise print five times a second.
		mlog.RatedWarn(ctx, 0.1, "failed to write ReplicaNumber back down after releasing a timed-out resource group, will retry",
			mlog.FieldCollectionID(task.CollectionID),
			mlog.String("resourceGroup", task.ResourceGroup),
			mlog.Int32("staleReplicaNumber", coll.GetReplicaNumber()),
			mlog.Int("replicas", replicas),
			mlog.Err(err))
		return false
	}
	return true
}

// groupHoldsReplicas answers whether any replica of the task's collection
// still lives in the task's resource group.
func (ob *CollectionObserver) groupHoldsReplicas(ctx context.Context, task LoadTask) bool {
	for _, replica := range ob.meta.GetByCollection(ctx, task.CollectionID) {
		if replica.GetResourceGroup() == task.ResourceGroup {
			return true
		}
	}
	return false
}

func (ob *CollectionObserver) readyToObserve(ctx context.Context, collectionID int64) bool {
	metaExist := (ob.meta.GetCollection(ctx, collectionID) != nil)
	targetExist := ob.targetMgr.IsNextTargetExist(ctx, collectionID) || ob.targetMgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

func (ob *CollectionObserver) observeLoadStatus(ctx context.Context, progress map[string]resourceGroupProgress) {
	loading := false
	observeTaskNum := 0
	observeStart := time.Now()
	ob.loadTasks.Range(func(traceID string, task LoadTask) bool {
		// Every task is a load in progress, and pushes the checkers below,
		// except a scoped task the readiness shield keeps alive: its group is
		// serving, and master pushes nothing for a serving collection. An
		// unscoped task never carries the mark and behaves as it always has.
		if task.ReadySince.IsZero() {
			loading = true
		}
		observeTaskNum++

		start := time.Now()
		collection := ob.meta.GetCollection(ctx, task.CollectionID)
		if collection == nil {
			return true
		}

		var partitions []*meta.Partition
		switch task.LoadType {
		case querypb.LoadType_LoadCollection:
			partitions = ob.meta.GetPartitionsByCollection(ctx, task.CollectionID)
		case querypb.LoadType_LoadPartition:
			partitionIDs := typeutil.NewSet[int64](task.PartitionIDs...)
			partitions = ob.meta.GetPartitionsByCollection(ctx, task.CollectionID)
			partitions = lo.Filter(partitions, func(partition *meta.Partition, _ int) bool {
				return partitionIDs.Contain(partition.GetPartitionID())
			})
		}

		loaded := true
		hasUpdate := false

		channelTargetNum, subChannelCount := ob.observeChannelStatus(ctx, task.CollectionID)

		for _, partition := range partitions {
			if partition.LoadPercentage == 100 {
				continue
			}
			if ob.readyToObserve(ctx, partition.CollectionID) {
				replicaNum := ob.meta.GetReplicaNumber(ctx, partition.GetCollectionID())
				if ob.observePartitionLoadStatus(ctx, partition, replicaNum, channelTargetNum, subChannelCount) {
					hasUpdate = true
				}
			}
			partition = ob.meta.GetPartition(ctx, partition.PartitionID)
			if partition != nil && partition.LoadPercentage != 100 {
				loaded = false
			}
		}

		if hasUpdate {
			ob.observeCollectionLoadStatus(ctx, task.CollectionID)
		}

		// A resource-group-scoped task finishes on its own resource group's
		// progress, taken from the single per-tick scan. The check it replaces
		// reads partition.LoadPercentage, which sums over every replica of the
		// collection: under that check a sibling resource group still loading
		// holds this task open forever, and a sibling already finished can
		// close it while this resource group carries nothing.
		//
		// The current target is asked for directly, because the shape a scoped
		// task actually has cannot be asked anything else: such a task is
		// registered for a resource group added to a collection that is already
		// loaded, so every partition sits at 100 and the loop above skips them
		// all. It is asked at all because the per-resource-group percentage is
		// measured against the NEXT target and so reaches 100 while the
		// promotion of the current target is still pending -- and until that
		// lands the group cannot serve, since shard leader readiness is
		// measured against the current target. Finishing there would drop this
		// group's supervision, its timeout and its teardown, at the moment it
		// carries everything and answers nothing.
		//
		// And a task that still owes the replica-count write-back of a
		// teardown (ReplicaNumberPending) does not finish: its survivors may
		// reach 100 and see the promotion while the catalog is still refusing
		// the count, and finishing then would drop the only thing that
		// retries it, leaving the stale count as the end state. The retry at
		// the top of observeResourceGroupTimeout runs earlier in this same
		// tick and clears the mark once the write sticks, so the task
		// finishes on the tick the catalog is back.
		if task.ResourceGroup != "" {
			loaded = progress[traceID].Percentage >= 100 &&
				ob.targetMgr.IsCurrentTargetExist(ctx, task.CollectionID, common.AllPartitionsID) &&
				!task.ReplicaNumberPending
		}

		// all partition loaded, finish task
		if len(partitions) > 0 && loaded {
			mlog.Info(ctx, "Load task finish",
				mlog.String("traceID", traceID),
				mlog.FieldCollectionID(task.CollectionID),
				mlog.Int64s("partitionIDs", task.PartitionIDs),
				mlog.Stringer("loadType", task.LoadType))
			ob.loadTasks.Remove(traceID)
		}

		mlog.Info(ctx, "observe collection done", mlog.FieldCollectionID(task.CollectionID), mlog.Duration("dur", time.Since(start)))
		return true
	})

	if observeTaskNum > 0 {
		mlog.Info(ctx, "observe all collections done", mlog.Int("num", observeTaskNum), mlog.Duration("dur", time.Since(observeStart)))
	}

	// trigger check logic when loading collections/partitions
	if loading {
		ob.checkerController.Check()
	}
}

func (ob *CollectionObserver) observeChannelStatus(ctx context.Context, collectionID int64) (int, int) {
	channelTargets := ob.targetMgr.GetDmChannelsByCollection(ctx, collectionID, meta.NextTarget)

	channelTargetNum := len(channelTargets)
	if channelTargetNum == 0 {
		mlog.Info(ctx, "channels in target is empty, waiting for new target content")
		return 0, 0
	}

	subChannelCount := 0
	for _, channel := range channelTargets {
		delegatorList := ob.dist.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(channel.GetChannelName()))
		nodes := lo.Map(delegatorList, func(v *meta.DmChannel, _ int) int64 { return v.Node })
		group := utils.GroupNodesByReplica(ctx, ob.meta.ReplicaManager, collectionID, nodes)
		subChannelCount += len(group)
	}
	return channelTargetNum, subChannelCount
}

// observePartitionLoadStatus drives one partition's load progress and reports
// whether a load-status update was persisted this tick, so the caller knows to
// refresh the collection-level status.
func (ob *CollectionObserver) observePartitionLoadStatus(ctx context.Context, partition *meta.Partition, replicaNum int32, channelTargetNum, subChannelCount int) bool {
	segmentTargets := ob.targetMgr.GetSealedSegmentsByPartition(ctx, partition.GetCollectionID(), partition.GetPartitionID(), meta.NextTarget)

	targetNum := len(segmentTargets) + channelTargetNum
	if targetNum == 0 {
		mlog.Info(ctx, "segments and channels in target are both empty, waiting for new target content")
		return false
	}
	mlog.RatedInfo(ctx, rate.Limit(10), "partition targets",
		mlog.FieldCollectionID(partition.GetCollectionID()),
		mlog.FieldPartitionID(partition.GetPartitionID()),
		mlog.Int("segmentTargetNum", len(segmentTargets)),
		mlog.Int("channelTargetNum", channelTargetNum),
		mlog.Int("totalTargetNum", targetNum),
		mlog.Int32("replicaNum", replicaNum),
	)
	loadedCount := subChannelCount
	loadPercentage := int32(0)

	for _, segment := range segmentTargets {
		delegatorList := ob.dist.ChannelDistManager.GetByFilter(meta.WithChannelName2Channel(segment.GetInsertChannel()))
		loadedSegmentNodes := make([]int64, 0)
		for _, delegator := range delegatorList {
			if delegator.View.Segments[segment.GetID()] != nil {
				loadedSegmentNodes = append(loadedSegmentNodes, delegator.Node)
			}
		}
		group := utils.GroupNodesByReplica(ctx, ob.meta.ReplicaManager, partition.GetCollectionID(), loadedSegmentNodes)
		loadedCount += len(group)
	}
	loadPercentage = int32(loadedCount * 100 / (targetNum * int(replicaNum)))

	if loadedCount <= ob.partitionLoadedCount[partition.GetPartitionID()] && loadPercentage != 100 {
		ob.partitionLoadedCount[partition.GetPartitionID()] = loadedCount
		return false
	}

	ob.partitionLoadedCount[partition.GetPartitionID()] = loadedCount
	if loadPercentage == 100 {
		if !ob.targetObserver.Check(ctx, partition.GetCollectionID(), partition.PartitionID) {
			mlog.Warn(ctx, "failed to manual check current target, skip update load status",
				mlog.FieldCollectionID(partition.GetCollectionID()),
				mlog.FieldPartitionID(partition.GetPartitionID()))
			return false
		}
		delete(ob.partitionLoadedCount, partition.GetPartitionID())
	}
	err := ob.meta.UpdatePartitionLoadPercent(ctx, partition.PartitionID, loadPercentage)
	if err != nil {
		mlog.Warn(ctx, "failed to update partition load percentage",
			mlog.FieldCollectionID(partition.GetCollectionID()),
			mlog.FieldPartitionID(partition.GetPartitionID()))
	}
	mlog.Info(ctx, "partition load status updated",
		mlog.FieldCollectionID(partition.GetCollectionID()),
		mlog.FieldPartitionID(partition.GetPartitionID()),
		mlog.Int32("partitionLoadPercentage", loadPercentage),
		mlog.Int("subChannelCount", subChannelCount),
		mlog.Int("loadSegmentCount", loadedCount-subChannelCount),
	)
	eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("partition %d load percentage update: %d", partition.PartitionID, loadPercentage)))
	return true
}

func (ob *CollectionObserver) observeCollectionLoadStatus(ctx context.Context, collectionID int64) {
	collectionPercentage, err := ob.meta.UpdateCollectionLoadPercent(ctx, collectionID)
	if err != nil {
		mlog.Warn(ctx, "failed to update collection load percentage", mlog.FieldCollectionID(collectionID))
	}
	mlog.Info(ctx, "collection load status updated",
		mlog.FieldCollectionID(collectionID),
		mlog.Int32("collectionLoadPercentage", collectionPercentage),
	)
	if collectionPercentage == 100 {
		ob.invalidateCache(ctx, collectionID)
	}
	eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("collection %d load percentage update: %d", collectionID, collectionPercentage)))
}

func (ob *CollectionObserver) invalidateCache(ctx context.Context, collectionID int64) {
	ctx, cancel := context.WithTimeout(ctx, paramtable.Get().QueryCoordCfg.BrokerTimeout.GetAsDuration(time.Second))
	defer cancel()
	err := ob.proxyManager.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
		CollectionID: collectionID,
	}, proxyutil.SetMsgType(commonpb.MsgType_LoadCollection))
	if err != nil {
		mlog.Warn(ctx, "failed to invalidate proxy's shard leader cache", mlog.Err(err))
		return
	}
}
