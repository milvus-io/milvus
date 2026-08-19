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
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/eventlog"
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
// whenever this task's own per-resource-group load percentage advances, and the
// task is only considered timed out after LoadTimeoutSeconds without an
// advance.
type LoadTask struct {
	LoadType     querypb.LoadType
	CollectionID int64
	PartitionIDs []int64

	ResourceGroup string

	LastProgress   int32
	LastProgressAt time.Time
}

func NewCollectionObserver(
	dist *meta.DistributionManager,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	targetObserver *TargetObserver,
	checherController *checkers.CheckerController,
	proxyManager proxyutil.ProxyClientManagerInterface,
) *CollectionObserver {
	ob := &CollectionObserver{
		dist:                 dist,
		meta:                 meta,
		targetMgr:            targetMgr,
		targetObserver:       targetObserver,
		checkerController:    checherController,
		partitionLoadedCount: make(map[int64]int),
		loadTasks:            typeutil.NewConcurrentMap[string, LoadTask](),
		proxyManager:         proxyManager,
	}

	// Add load task for collection recovery
	collections := meta.GetAllCollections(context.TODO())
	for _, collection := range collections {
		ob.LoadCollection(context.Background(), collection.GetCollectionID(), "")
	}

	return ob
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

// observeResourceGroupProgress is the resource-group-aware slice of the per-tick
// scan, computed exactly once per tick and consumed by both observeTimeout (as
// the progress watermark) and observeLoadStatus (to decide when a scoped task is
// finished). Keeping it in one place is what stops the two consumers from
// drifting apart, and reusing utils.LoadPercentageByResourceGroup is what stops
// the walk over channel/segment targets from being written a second time.
//
// The scan restricts itself to tasks that actually name a resource group. When
// no task does -- which is every deployment that does not use per-resource-group
// loads -- it walks the task map without reading a single target or
// distribution entry and returns a nil map, so both consumers see no progress
// entry at all and fall through to the code that existed before.
func (ob *CollectionObserver) observeResourceGroupProgress(ctx context.Context) map[string]int32 {
	var progress map[string]int32
	ob.loadTasks.Range(func(key string, task LoadTask) bool {
		if task.ResourceGroup == "" {
			return true
		}
		percentage, err := utils.LoadPercentageByResourceGroup(ctx, ob.meta, ob.targetMgr, ob.dist, task.CollectionID, task.ResourceGroup)
		if err != nil {
			// Rate-limited: this runs per task per observation tick, and a
			// persistent read failure (a recorded load failure, say) would
			// otherwise print once a second until the task times out.
			mlog.RatedWarn(ctx, 0.1, "failed to read resource group load percentage",
				mlog.FieldCollectionID(task.CollectionID),
				mlog.String("resourceGroup", task.ResourceGroup),
				mlog.Err(err))
		}
		if progress == nil {
			progress = make(map[string]int32)
		}
		progress[key] = percentage
		return true
	})
	return progress
}

func (ob *CollectionObserver) observeTimeout(ctx context.Context, progress map[string]int32) {
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
// task from percentage, the load percentage of this task's collection
// restricted to this task's resource group, as computed once for this tick.
//
// The judgment reads nothing shared with sibling resource groups. Reading
// collection.UpdatedAt here would be the bug this branch exists to avoid: a
// collection loaded into one resource group hours ago carries an UpdatedAt
// hours in the past, so a second resource group's task would be declared timed
// out on its very first observer tick and the replica just spawned for it would
// be released before it could load anything.
func (ob *CollectionObserver) observeResourceGroupTimeout(ctx context.Context, key string, task LoadTask, percentage int32) {
	now := time.Now()

	// Refresh the watermark when progress moved. A fully loaded resource group
	// refreshes forever: its task may legitimately outlive the load timeout
	// while waiting for the gated status promotion, and a replica that is
	// already serving must never be torn down by a load timeout. A zero
	// LastProgressAt (a task built by a caller that did not seed it) starts its
	// clock now rather than at the epoch, so it is never instantly expired.
	if percentage > task.LastProgress || percentage >= 100 || task.LastProgressAt.IsZero() {
		task.LastProgress = percentage
		task.LastProgressAt = now
		ob.loadTasks.Insert(key, task)
		return
	}

	if now.Before(task.LastProgressAt.Add(Params.QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))) {
		return
	}

	mlog.Info(ctx, "load timeout for resource group, cancel it",
		mlog.FieldCollectionID(task.CollectionID),
		mlog.String("resourceGroup", task.ResourceGroup),
		mlog.String("traceID", key),
		mlog.Stringer("loadType", task.LoadType),
		mlog.Int32("loadPercentage", percentage),
		mlog.Duration("stalledFor", now.Sub(task.LastProgressAt)))
	ob.releaseResourceGroupOnTimeout(ctx, key, task)
}

// releaseResourceGroupOnTimeout tears down exactly the replicas of this
// collection that live in the timed-out resource group. Sibling resource groups
// keep their replicas, since their loads are independent by construction. The
// collection-level meta and target go away only once the last resource group is
// gone, which is the same condition under which the unscoped path drops them.
func (ob *CollectionObserver) releaseResourceGroupOnTimeout(ctx context.Context, key string, task LoadTask) {
	replicaIDs := make([]int64, 0)
	for _, replica := range ob.meta.GetByCollection(ctx, task.CollectionID) {
		if replica.GetResourceGroup() == task.ResourceGroup {
			replicaIDs = append(replicaIDs, replica.GetID())
		}
	}

	if len(replicaIDs) > 0 {
		if err := ob.meta.RemoveReplicas(ctx, task.CollectionID, replicaIDs...); err != nil {
			// Leave the task in place so the next tick retries the teardown;
			// dropping it here would leak the stalled replicas forever.
			mlog.Warn(ctx, "failed to remove replicas of timed out resource group",
				mlog.FieldCollectionID(task.CollectionID),
				mlog.String("resourceGroup", task.ResourceGroup),
				mlog.Int64s("replicaIDs", replicaIDs),
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
	// when this resource group was added; taking its replicas away must write
	// the number back down, or everything that reads it - updateLoadConfig's
	// replica-changed check, ShowLoadCollections, the collection-wide
	// observer's loadPercentage denominator - keeps counting replicas that no
	// longer exist, and the load percentage can never reach 100 again.
	if coll := ob.meta.GetCollection(ctx, task.CollectionID); coll != nil &&
		int(coll.GetReplicaNumber()) != len(remaining) {
		if err := ob.meta.UpdateReplicaNumber(ctx, task.CollectionID,
			int32(len(remaining)), coll.GetUserSpecifiedReplicaMode()); err != nil {
			mlog.Warn(ctx, "failed to write ReplicaNumber back down after releasing a timed-out resource group",
				mlog.FieldCollectionID(task.CollectionID),
				mlog.String("resourceGroup", task.ResourceGroup),
				mlog.Err(err))
		}
	}
	ob.loadTasks.Remove(key)
}

func (ob *CollectionObserver) readyToObserve(ctx context.Context, collectionID int64) bool {
	metaExist := (ob.meta.GetCollection(ctx, collectionID) != nil)
	targetExist := ob.targetMgr.IsNextTargetExist(ctx, collectionID) || ob.targetMgr.IsCurrentTargetExist(ctx, collectionID, common.AllPartitionsID)

	return metaExist && targetExist
}

func (ob *CollectionObserver) observeLoadStatus(ctx context.Context, progress map[string]int32) {
	loading := false
	observeTaskNum := 0
	observeStart := time.Now()
	ob.loadTasks.Range(func(traceID string, task LoadTask) bool {
		loading = true
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
		targetNotReady := false

		channelTargetNum, subChannelCount := ob.observeChannelStatus(ctx, task.CollectionID)

		for _, partition := range partitions {
			if partition.LoadPercentage == 100 {
				continue
			}
			if ob.readyToObserve(ctx, partition.CollectionID) {
				replicaNum := ob.meta.GetReplicaNumber(ctx, partition.GetCollectionID())
				has, gated := ob.observePartitionLoadStatus(ctx, partition, replicaNum, channelTargetNum, subChannelCount)
				if has {
					hasUpdate = true
				}
				if gated {
					targetNotReady = true
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
		// targetNotReady keeps the one guarantee the replaced check gave for
		// free: partition.LoadPercentage only reaches 100 after
		// targetObserver.Check has advanced the current target, whereas the
		// per-resource-group percentage is measured against the next target and
		// reaches 100 before that promotion. Finishing the task while the
		// promotion is still pending would leave the collection below Loaded
		// with nothing left to drive a retry.
		if task.ResourceGroup != "" {
			loaded = progress[traceID] >= 100 && !targetNotReady
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

// observePartitionLoadStatus drives one partition's load progress. It returns:
//
//   - loadUpdated: a load-status update was persisted this tick, so the caller
//     should refresh the collection-level status.
//   - targetNotReady: the gated promotion of this partition's status was skipped
//     this tick, because the next target is not populated yet or because
//     targetObserver.Check refused to advance the current target. Only
//     resource-group-scoped tasks read this; it tells them not to finish yet.
func (ob *CollectionObserver) observePartitionLoadStatus(ctx context.Context, partition *meta.Partition, replicaNum int32, channelTargetNum, subChannelCount int) (loadUpdated bool, targetNotReady bool) {
	segmentTargets := ob.targetMgr.GetSealedSegmentsByPartition(ctx, partition.GetCollectionID(), partition.GetPartitionID(), meta.NextTarget)

	targetNum := len(segmentTargets) + channelTargetNum
	if targetNum == 0 {
		mlog.Info(ctx, "segments and channels in target are both empty, waiting for new target content")
		return false, true
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
		return false, false
	}

	ob.partitionLoadedCount[partition.GetPartitionID()] = loadedCount
	if loadPercentage == 100 {
		if !ob.targetObserver.Check(ctx, partition.GetCollectionID(), partition.PartitionID) {
			mlog.Warn(ctx, "failed to manual check current target, skip update load status",
				mlog.FieldCollectionID(partition.GetCollectionID()),
				mlog.FieldPartitionID(partition.GetPartitionID()))
			return false, true
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
	return true, false
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
