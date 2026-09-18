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

package meta

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type TargetScope = int32

const (
	CurrentTarget TargetScope = iota + 1
	NextTarget
	CurrentTargetFirst
	NextTargetFirst
)

type TargetManagerInterface interface {
	UpdateCollectionCurrentTarget(ctx context.Context, collectionID int64) bool
	UpdateCollectionNextTarget(ctx context.Context, collectionID int64) error
	RemoveCollection(ctx context.Context, collectionID int64)
	RemovePartition(ctx context.Context, collectionID int64, partitionIDs ...int64)
	RemovePartitionFromNextTarget(ctx context.Context, collectionID int64, partitionIDs ...int64)
	GetGrowingSegmentsByCollection(ctx context.Context, collectionID int64, scope TargetScope) typeutil.UniqueSet
	GetGrowingSegmentsByChannel(ctx context.Context, collectionID int64, channelName string, scope TargetScope) typeutil.UniqueSet
	GetSealedSegmentsByCollection(ctx context.Context, collectionID int64, scope TargetScope) map[int64]*datapb.SegmentInfo
	GetSealedSegmentsByChannel(ctx context.Context, collectionID int64, channelName string, scope TargetScope) map[int64]*datapb.SegmentInfo
	GetDroppedSegmentsByChannel(ctx context.Context, collectionID int64, channelName string, scope TargetScope) []int64
	GetSealedSegmentsByPartition(ctx context.Context, collectionID int64, partitionID int64, scope TargetScope) map[int64]*datapb.SegmentInfo
	GetDmChannelsByCollection(ctx context.Context, collectionID int64, scope TargetScope) map[string]*DmChannel
	GetDmChannel(ctx context.Context, collectionID int64, channel string, scope TargetScope) *DmChannel
	GetSealedSegment(ctx context.Context, collectionID int64, id int64, scope TargetScope) *datapb.SegmentInfo
	GetCollectionTargetVersion(ctx context.Context, collectionID int64, scope TargetScope) int64
	IsCurrentTargetExist(ctx context.Context, collectionID int64, partitionID int64) bool
	IsNextTargetExist(ctx context.Context, collectionID int64) bool
	SaveCurrentTarget(ctx context.Context, catalog metastore.QueryCoordCatalog)
	Recover(ctx context.Context, catalog metastore.QueryCoordCatalog) error
	CanSegmentBeMoved(ctx context.Context, collectionID, segmentID int64) bool
	GetTargetJSON(ctx context.Context, scope TargetScope, collectionID int64) string
	GetPartitions(ctx context.Context, collectionID int64, scope TargetScope) ([]int64, error)
	IsCurrentTargetReady(ctx context.Context, collectionID int64) bool
	GetCollectionRowCount(ctx context.Context, collectionID int64, scope TargetScope) int64
	GetSplitWindowTargets(ctx context.Context, collectionID int64, scope TargetScope) typeutil.Set[string]
}

type TargetManager struct {
	broker Broker
	meta   *Meta
	// splitStates reads the collection's shard states right before each
	// next-target pull, to mark the split targets that pull was taken inside
	// the split window of. nil (only through NewTargetManager) disables the mark.
	splitStates *ShardSplitStateCache

	// all read segment/channel operation happens on current -> only current target are visible to outer
	// all add segment/channel operation happens on next -> changes can only happen on next target
	// all remove segment/channel operation happens on Both current and next -> delete status should be consistent
	current *target
	next    *target
}

func NewTargetManager(broker Broker, meta *Meta) *TargetManager {
	return newTargetManager(broker, meta, nil)
}

// NewTargetManagerWithSplitState is NewTargetManager with the shard-split state
// cache the next-target pull marks its split window targets through. The
// querycoord server passes the same cache its checkers use.
//
// It panics on a nil cache: a caller that means to split must not silently lose
// the window marks. A target manager without one is built by NewTargetManager.
func NewTargetManagerWithSplitState(broker Broker, meta *Meta, splitStates *ShardSplitStateCache) *TargetManager {
	if splitStates == nil {
		panic(merr.WrapErrServiceInternal("NewTargetManagerWithSplitState requires a shard split state cache",
			"build a target manager without one with NewTargetManager"))
	}
	return newTargetManager(broker, meta, splitStates)
}

func newTargetManager(broker Broker, meta *Meta, splitStates *ShardSplitStateCache) *TargetManager {
	return &TargetManager{
		broker:      broker,
		meta:        meta,
		splitStates: splitStates,
		current:     newTarget(),
		next:        newTarget(),
	}
}

// UpdateCollectionCurrentTarget updates the current target to next target,
// WARN: DO NOT call this method for an existing collection as target observer running, or it will lead to a double-update,
// which may make the current target not available
func (mgr *TargetManager) UpdateCollectionCurrentTarget(ctx context.Context, collectionID int64) bool {
	log := mlog.With(mlog.FieldCollectionID(collectionID))

	newTarget := mgr.next.getCollectionTarget(collectionID)
	if newTarget == nil || newTarget.IsEmpty() {
		log.Info(ctx, "next target does not exist, skip it")
		return false
	}
	mgr.current.updateCollectionTarget(collectionID, newTarget)
	mgr.next.removeCollectionTarget(collectionID)

	partStatsVersionInfo := "partitionStats:"
	for channelName, dmlChannel := range newTarget.dmChannels {
		ts, _ := tsoutil.ParseTS(dmlChannel.GetSeekPosition().GetTimestamp())
		metrics.QueryCoordCurrentTargetCheckpointUnixSeconds.WithLabelValues(
			paramtable.GetStringNodeID(),
			channelName,
		).Set(float64(ts.Unix()))
		partStatsVersionInfo += fmt.Sprintf("%s:[", channelName)
		partStatsVersion := dmlChannel.PartitionStatsVersions
		for partID, statVersion := range partStatsVersion {
			partStatsVersionInfo += fmt.Sprintf("%d:%d,", partID, statVersion)
		}
		partStatsVersionInfo += "],"
	}
	log.Debug(ctx, "finish to update current target for collection",
		mlog.Int64s("segments", newTarget.GetAllSegmentIDs()),
		mlog.Strings("channels", newTarget.GetAllDmChannelNames()),
		mlog.Int64("version", newTarget.GetTargetVersion()),
		mlog.String("partStatsVersion", partStatsVersionInfo),
	)
	return true
}

// UpdateCollectionNextTarget updates the next target with new target pulled from DataCoord,
// WARN: DO NOT call this method for an existing collection as target observer running, or it will lead to a double-update,
// which may make the current target not available
func (mgr *TargetManager) UpdateCollectionNextTarget(ctx context.Context, collectionID int64) error {
	var vChannelInfos []*datapb.VchannelInfo
	var segmentInfos []*datapb.SegmentInfo
	var shardStates *ShardStateSnapshot
	err := retry.Handle(ctx, func() (bool, error) {
		var err error
		// Read the shard states BEFORE the pull: the mark is the complement of
		// what this read saw settled (ShardStateSnapshot.SplitWindowTargets),
		// which never misses a target the later pull still has in the window.
		// A failed fresh read falls back to the last cached one, which is just
		// as sound; only with nothing cached is the read retried with the pull.
		if mgr.splitStates != nil {
			shardStates, err = mgr.readShardStates(ctx, collectionID)
			if err != nil {
				return true, err
			}
		}
		vChannelInfos, segmentInfos, err = mgr.broker.GetRecoveryInfoV2(ctx, collectionID)
		if err != nil {
			return true, err
		}
		return false, nil
	}, retry.Attempts(10))
	if err != nil {
		mlog.Warn(ctx, "failed to get next targets for collection", mlog.FieldCollectionID(collectionID), mlog.Err(err))
		return err
	}

	// A dropped checkpoint sentinel is not a valid seek position; do not
	// build a next target that could dispatch WatchDmChannels with it.
	for _, channelInfo := range vChannelInfos {
		if funcutil.IsDroppedChannelCheckpoint(channelInfo.GetSeekPosition()) {
			mlog.Warn(ctx, "refuse to build next target: channel checkpoint is a dropped sentinel; sticky until collection meta is fully dropped",
				mlog.FieldCollectionID(collectionID),
				mlog.String("channel", channelInfo.GetChannelName()),
				mlog.Uint64("seekTs", channelInfo.GetSeekPosition().GetTimestamp()),
			)
			return merr.WrapErrChannelDroppedSentinel(
				channelInfo.GetChannelName(),
				"refuse to build next target",
			)
		}
	}

	partitionIDs := mgr.meta.GetPartitionIDsByCollection(ctx, collectionID)
	segments := make(map[int64]*datapb.SegmentInfo, len(segmentInfos))
	partitionSet := make(map[int64]struct{}, len(partitionIDs))
	for _, partitionID := range partitionIDs {
		partitionSet[partitionID] = struct{}{}
	}
	for _, segmentInfo := range segmentInfos {
		if _, ok := partitionSet[segmentInfo.GetPartitionID()]; ok || segmentInfo.GetPartitionID() == common.AllPartitionsID {
			segments[segmentInfo.GetID()] = segmentInfo
		}
	}

	dmChannels := make(map[string]*DmChannel)
	for _, channelInfo := range vChannelInfos {
		dmChannels[channelInfo.ChannelName] = DmChannelFromVChannel(channelInfo)
	}

	if len(segments) == 0 && len(dmChannels) == 0 {
		mlog.Debug(ctx, "skip empty next targets for collection", mlog.FieldCollectionID(collectionID), mlog.Int64s("PartitionIDs", partitionIDs))
		return nil
	}

	var windowTargets []string
	if shardStates != nil {
		windowTargets = shardStates.SplitWindowTargets(lo.Map(vChannelInfos, func(info *datapb.VchannelInfo, _ int) string {
			return info.GetChannelName()
		}))
	}
	allocatedTarget := NewCollectionTarget(segments, dmChannels, partitionIDs)
	allocatedTarget.windowTargets = typeutil.NewSet(windowTargets...)

	mgr.next.updateCollectionTarget(collectionID, allocatedTarget)

	if len(windowTargets) > 0 {
		mlog.Info(ctx, "next target pulled inside a shard split window; its split targets are held back from sync and promotion",
			mlog.FieldCollectionID(collectionID),
			mlog.Int64("version", allocatedTarget.GetTargetVersion()),
			mlog.Strings("windowTargets", windowTargets))
	}
	mlog.Debug(ctx, "finish to update next targets for collection",
		mlog.FieldCollectionID(collectionID),
		mlog.Int64s("PartitionIDs", partitionIDs))

	return nil
}

// readShardStates reads the collection's shard states for the next-target pull
// about to be taken. It needs the split state cache.
func (mgr *TargetManager) readShardStates(ctx context.Context, collectionID int64) (*ShardStateSnapshot, error) {
	states, err := mgr.splitStates.ReadShardStates(ctx, collectionID)
	if err != nil {
		mlog.Warn(ctx, "failed to read shard split states before pulling the next target",
			mlog.FieldCollectionID(collectionID), mlog.Err(err))
		return nil, merr.Wrap(err, "read shard split states before pulling the next target")
	}
	return states, nil
}

func mergeDmChannelInfo(infos []*datapb.VchannelInfo) *DmChannel {
	var dmChannel *DmChannel

	for _, info := range infos {
		if dmChannel == nil {
			dmChannel = DmChannelFromVChannel(info)
			continue
		}

		if info.SeekPosition.GetTimestamp() < dmChannel.SeekPosition.GetTimestamp() {
			dmChannel.SeekPosition = info.SeekPosition
		}
		dmChannel.DroppedSegmentIds = append(dmChannel.DroppedSegmentIds, info.DroppedSegmentIds...)
		dmChannel.UnflushedSegmentIds = append(dmChannel.UnflushedSegmentIds, info.UnflushedSegmentIds...)
		dmChannel.FlushedSegmentIds = append(dmChannel.FlushedSegmentIds, info.FlushedSegmentIds...)
	}

	return dmChannel
}

// RemoveCollection removes all channels and segments in the given collection
func (mgr *TargetManager) RemoveCollection(ctx context.Context, collectionID int64) {
	mlog.Info(ctx, "remove collection from targets",
		mlog.FieldCollectionID(collectionID))

	current := mgr.current.getCollectionTarget(collectionID)
	if current != nil {
		for channelName := range current.GetAllDmChannels() {
			metrics.QueryCoordCurrentTargetCheckpointUnixSeconds.DeleteLabelValues(
				paramtable.GetStringNodeID(),
				channelName,
			)
			metrics.QueryCoordCurrentTargetAllReplicasCheckpointUnixSeconds.DeleteLabelValues(
				paramtable.GetStringNodeID(),
				channelName,
			)
		}
	}

	mgr.current.removeCollectionTarget(collectionID)
	mgr.next.removeCollectionTarget(collectionID)
}

// RemovePartition removes all segment in the given partition,
// NOTE: this doesn't remove any channel even the given one is the only partition
// Deprecated: use RemovePartitionFromNextTarget instead @weiliu1031
func (mgr *TargetManager) RemovePartition(ctx context.Context, collectionID int64, partitionIDs ...int64) {
	log := mlog.With(mlog.FieldCollectionID(collectionID),
		mlog.Int64s("PartitionIDs", partitionIDs))

	log.Info(ctx, "remove partition from targets")

	partitionSet := typeutil.NewUniqueSet(partitionIDs...)

	oldCurrentTarget := mgr.current.getCollectionTarget(collectionID)
	if oldCurrentTarget != nil {
		newTarget := mgr.removePartitionFromCollectionTarget(oldCurrentTarget, partitionSet)
		if newTarget != nil {
			mgr.current.updateCollectionTarget(collectionID, newTarget)
			log.Info(ctx, "finish to remove partition from current target for collection",
				mlog.Int64s("segments", newTarget.GetAllSegmentIDs()),
				mlog.Strings("channels", newTarget.GetAllDmChannelNames()))
		} else {
			log.Info(ctx, "all partitions have been released, release the collection next target now")
			mgr.current.removeCollectionTarget(collectionID)
		}
	}

	oleNextTarget := mgr.next.getCollectionTarget(collectionID)
	if oleNextTarget != nil {
		newTarget := mgr.removePartitionFromCollectionTarget(oleNextTarget, partitionSet)
		if newTarget != nil {
			mgr.next.updateCollectionTarget(collectionID, newTarget)
			log.Info(ctx, "finish to remove partition from next target for collection",
				mlog.Int64s("segments", newTarget.GetAllSegmentIDs()),
				mlog.Strings("channels", newTarget.GetAllDmChannelNames()))
		} else {
			log.Info(ctx, "all partitions have been released, release the collection current target now")
			mgr.next.removeCollectionTarget(collectionID)
		}
	}
}

// remove partition from next target
// NOTE: don't edit current target directly, it will be updated by target observer, which push the new next target as current target
// need the full progress to update next target to current target, so the query view on delegator could be updated when current target is updated
func (mgr *TargetManager) RemovePartitionFromNextTarget(ctx context.Context, collectionID int64, partitionIDs ...int64) {
	log := mlog.With(mlog.FieldCollectionID(collectionID),
		mlog.Int64s("PartitionIDs", partitionIDs))

	partitionSet := typeutil.NewUniqueSet(partitionIDs...)

	log.Info(ctx, "remove partition from next target")
	oleNextTarget := mgr.next.getCollectionTarget(collectionID)
	if oleNextTarget != nil {
		newTarget := mgr.removePartitionFromCollectionTarget(oleNextTarget, partitionSet)
		if newTarget != nil {
			mgr.next.updateCollectionTarget(collectionID, newTarget)
			log.Info(ctx, "finish to remove partition from next target for collection",
				mlog.Int64s("segments", newTarget.GetAllSegmentIDs()),
				mlog.Strings("channels", newTarget.GetAllDmChannelNames()))
		} else {
			log.Info(ctx, "all partitions have been released, release the collection current target now")
			mgr.current.removeCollectionTarget(collectionID)
			mgr.next.removeCollectionTarget(collectionID)
		}
	}
}

func (mgr *TargetManager) removePartitionFromCollectionTarget(oldTarget *CollectionTarget, partitionSet typeutil.UniqueSet) *CollectionTarget {
	segments := make(map[int64]*datapb.SegmentInfo)
	for _, segment := range oldTarget.GetAllSegments() {
		if !partitionSet.Contain(segment.GetPartitionID()) {
			segments[segment.GetID()] = segment
		}
	}

	// clear partition streaming segment
	channels := make(map[string]*DmChannel)
	for _, channel := range oldTarget.GetAllDmChannels() {
		channels[channel.GetChannelName()] = channel
	}
	partitions := lo.Filter(oldTarget.partitions.Collect(), func(partitionID int64, _ int) bool {
		return !partitionSet.Contain(partitionID)
	})

	newTarget := NewCollectionTarget(segments, channels, partitions)
	// trimming partitions does not change when the snapshot was pulled.
	newTarget.windowTargets = oldTarget.windowTargets
	return newTarget
}

func (mgr *TargetManager) getCollectionTarget(scope TargetScope, collectionID int64) []*CollectionTarget {
	switch scope {
	case CurrentTarget:

		ret := make([]*CollectionTarget, 0, 1)
		current := mgr.current.getCollectionTarget(collectionID)
		if current != nil {
			ret = append(ret, current)
		}
		return ret
	case NextTarget:
		ret := make([]*CollectionTarget, 0, 1)
		next := mgr.next.getCollectionTarget(collectionID)
		if next != nil {
			ret = append(ret, next)
		}
		return ret
	case CurrentTargetFirst:
		ret := make([]*CollectionTarget, 0, 2)
		current := mgr.current.getCollectionTarget(collectionID)
		if current != nil {
			ret = append(ret, current)
		}

		next := mgr.next.getCollectionTarget(collectionID)
		if next != nil {
			ret = append(ret, next)
		}

		return ret
	case NextTargetFirst:
		ret := make([]*CollectionTarget, 0, 2)
		next := mgr.next.getCollectionTarget(collectionID)
		if next != nil {
			ret = append(ret, next)
		}

		current := mgr.current.getCollectionTarget(collectionID)
		if current != nil {
			ret = append(ret, current)
		}

		return ret
	}
	return nil
}

func (mgr *TargetManager) GetGrowingSegmentsByCollection(ctx context.Context, collectionID int64,
	scope TargetScope,
) typeutil.UniqueSet {
	targets := mgr.getCollectionTarget(scope, collectionID)

	for _, t := range targets {
		segments := typeutil.NewUniqueSet()
		for _, channel := range t.GetAllDmChannels() {
			segments.Insert(channel.GetUnflushedSegmentIds()...)
		}

		if len(segments) > 0 {
			return segments
		}
	}

	return nil
}

func (mgr *TargetManager) GetGrowingSegmentsByChannel(ctx context.Context, collectionID int64,
	channelName string,
	scope TargetScope,
) typeutil.UniqueSet {
	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		segments := typeutil.NewUniqueSet()
		for _, channel := range t.GetAllDmChannels() {
			if channel.ChannelName == channelName {
				segments.Insert(channel.GetUnflushedSegmentIds()...)
			}
		}

		if len(segments) > 0 {
			return segments
		}
	}

	return nil
}

func (mgr *TargetManager) GetSealedSegmentsByCollection(ctx context.Context, collectionID int64,
	scope TargetScope,
) map[int64]*datapb.SegmentInfo {
	targets := mgr.getCollectionTarget(scope, collectionID)

	for _, t := range targets {
		return t.GetAllSegments()
	}

	return nil
}

func (mgr *TargetManager) GetSealedSegmentsByChannel(ctx context.Context, collectionID int64,
	channelName string,
	scope TargetScope,
) map[int64]*datapb.SegmentInfo {
	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		ret := lo.KeyBy(t.GetChannelSegments(channelName), func(s *datapb.SegmentInfo) int64 {
			return s.GetID()
		})

		if len(ret) > 0 {
			return ret
		}
	}

	return nil
}

func (mgr *TargetManager) GetDroppedSegmentsByChannel(ctx context.Context, collectionID int64,
	channelName string,
	scope TargetScope,
) []int64 {
	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		if channel, ok := t.dmChannels[channelName]; ok {
			return channel.GetDroppedSegmentIds()
		}
	}

	return nil
}

func (mgr *TargetManager) GetSealedSegmentsByPartition(ctx context.Context, collectionID int64,
	partitionID int64,
	scope TargetScope,
) map[int64]*datapb.SegmentInfo {
	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		segments := make(map[int64]*datapb.SegmentInfo)
		for _, s := range t.GetPartitionSegments(partitionID) {
			segments[s.GetID()] = s
		}

		if len(segments) > 0 {
			return segments
		}
	}

	return nil
}

func (mgr *TargetManager) GetDmChannelsByCollection(ctx context.Context, collectionID int64, scope TargetScope) map[string]*DmChannel {
	targets := mgr.getCollectionTarget(scope, collectionID)

	for _, t := range targets {
		return t.GetAllDmChannels()
	}

	return nil
}

func (mgr *TargetManager) GetDmChannel(ctx context.Context, collectionID int64, channel string, scope TargetScope) *DmChannel {
	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		if ch, ok := t.GetAllDmChannels()[channel]; ok {
			return ch
		}
	}
	return nil
}

func (mgr *TargetManager) GetSealedSegment(ctx context.Context, collectionID int64, id int64, scope TargetScope) *datapb.SegmentInfo {
	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		if s, ok := t.GetAllSegments()[id]; ok {
			return s
		}
	}

	return nil
}

func (mgr *TargetManager) GetCollectionTargetVersion(ctx context.Context, collectionID int64, scope TargetScope) int64 {
	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		if t.GetTargetVersion() > 0 {
			return t.GetTargetVersion()
		}
	}

	return 0
}

func (mgr *TargetManager) IsCurrentTargetExist(ctx context.Context, collectionID int64, partitionID int64) bool {
	targets := mgr.getCollectionTarget(CurrentTarget, collectionID)

	return len(targets) > 0 && (targets[0].partitions.Contain(partitionID) || partitionID == common.AllPartitionsID) && len(targets[0].dmChannels) > 0
}

func (mgr *TargetManager) IsNextTargetExist(ctx context.Context, collectionID int64) bool {
	newChannels := mgr.GetDmChannelsByCollection(ctx, collectionID, NextTarget)

	return len(newChannels) > 0
}

func (mgr *TargetManager) SaveCurrentTarget(ctx context.Context, catalog metastore.QueryCoordCatalog) {
	if mgr.current != nil {
		// use pool here to control maximal writer used by save target
		pool := conc.NewPool[any](runtime.GOMAXPROCS(0) * 2)
		defer pool.Release()
		// use batch write in case of the number of collections is large
		batchSize := 16
		var wg sync.WaitGroup
		submit := func(tasks []typeutil.Pair[int64, *querypb.CollectionTarget]) {
			wg.Add(1)
			pool.Submit(func() (any, error) {
				defer wg.Done()
				ids := lo.Map(tasks, func(p typeutil.Pair[int64, *querypb.CollectionTarget], _ int) int64 { return p.A })
				if err := catalog.SaveCollectionTargets(ctx, lo.Map(tasks, func(p typeutil.Pair[int64, *querypb.CollectionTarget], _ int) *querypb.CollectionTarget {
					return p.B
				})...); err != nil {
					mlog.Warn(ctx, "failed to save current target for collection", mlog.Int64s("collectionIDs", ids), mlog.Err(err))
				} else {
					mlog.Info(ctx, "succeed to save current target for collection", mlog.Int64s("collectionIDs", ids))
				}
				return nil, nil
			})
		}
		tasks := make([]typeutil.Pair[int64, *querypb.CollectionTarget], 0, batchSize)
		mgr.current.collectionTargetMap.Range(func(id int64, target *CollectionTarget) bool {
			tasks = append(tasks, typeutil.NewPair(id, target.toPbMsg()))
			if len(tasks) >= batchSize {
				submit(tasks)
				tasks = make([]typeutil.Pair[int64, *querypb.CollectionTarget], 0, batchSize)
			}
			return true
		})
		if len(tasks) > 0 {
			submit(tasks)
		}
		wg.Wait()
	}
}

func (mgr *TargetManager) Recover(ctx context.Context, catalog metastore.QueryCoordCatalog) error {
	targets, err := catalog.GetCollectionTargets(ctx)
	if err != nil {
		mlog.Warn(ctx, "failed to recover collection target from etcd", mlog.Err(err))
		return err
	}

	for _, t := range targets {
		newTarget := FromPbCollectionTarget(t)
		mgr.current.updateCollectionTarget(t.GetCollectionID(), newTarget)
		mlog.Info(ctx, "recover current target for collection",
			mlog.FieldCollectionID(t.GetCollectionID()),
			mlog.Strings("channels", newTarget.GetAllDmChannelNames()),
			mlog.Int("segmentNum", len(newTarget.GetAllSegmentIDs())),
			mlog.Int64("version", newTarget.GetTargetVersion()),
		)
	}

	// Remove all target keys from etcd after in-memory recovery is done.
	// Uses RemoveWithPrefix which is a single etcd call.
	if len(targets) > 0 {
		if err := catalog.RemoveCollectionTargets(ctx); err != nil {
			mlog.Warn(ctx, "failed to remove collection targets from etcd", mlog.Err(err))
		}
	}

	return nil
}

// if segment isn't l0 segment, and exist in current/next target, then it can be moved
func (mgr *TargetManager) CanSegmentBeMoved(ctx context.Context, collectionID, segmentID int64) bool {
	current := mgr.current.getCollectionTarget(collectionID)
	if current != nil && current.segments[segmentID] != nil {
		return true
	}

	next := mgr.next.getCollectionTarget(collectionID)
	if next != nil && next.segments[segmentID] != nil {
		return true
	}

	return false
}

func (mgr *TargetManager) GetTargetJSON(ctx context.Context, scope TargetScope, collectionID int64) string {
	ret := mgr.getTarget(scope)
	if ret == nil {
		return ""
	}

	v, err := json.Marshal(ret.toQueryCoordCollectionTargets(collectionID))
	if err != nil {
		mlog.Warn(ctx, "failed to marshal target", mlog.Err(err))
		return ""
	}
	return string(v)
}

func (mgr *TargetManager) GetPartitions(ctx context.Context, collectionID int64, scope TargetScope) ([]int64, error) {
	ret := mgr.getCollectionTarget(scope, collectionID)
	if len(ret) == 0 {
		return nil, merr.WrapErrCollectionNotLoaded(collectionID)
	}

	return ret[0].partitions.Collect(), nil
}

func (mgr *TargetManager) getTarget(scope TargetScope) *target {
	if scope == CurrentTarget {
		return mgr.current
	}

	return mgr.next
}

func (mgr *TargetManager) IsCurrentTargetReady(ctx context.Context, collectionID int64) bool {
	target, ok := mgr.current.collectionTargetMap.Get(collectionID)
	if !ok {
		return false
	}

	return target.Ready()
}

// GetSplitWindowTargets returns the channels the collection's target in scope
// marked as split window targets when it was pulled (see
// ShardStateSnapshot.SplitWindowTargets); empty when that target was not pulled
// inside a split window, or does not exist.
func (mgr *TargetManager) GetSplitWindowTargets(ctx context.Context, collectionID int64, scope TargetScope) typeutil.Set[string] {
	targets := mgr.getCollectionTarget(scope, collectionID)
	if len(targets) == 0 {
		return nil
	}
	return targets[0].SplitWindowTargets()
}

func (mgr *TargetManager) GetCollectionRowCount(ctx context.Context, collectionID int64, scope TargetScope) int64 {
	target := mgr.getCollectionTarget(scope, collectionID)
	if len(target) == 0 {
		return 0
	}
	return target[0].GetRowCount()
}
