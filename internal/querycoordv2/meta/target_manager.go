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
	"time"

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

// liveVersionGCGrace is how long a retained version is kept after it stops being referenced by any
// delegator's reported view, to bridge the window between a delegator being synced to a version and
// its heartbeat reporting it back into dist. Overridable in tests.
var liveVersionGCGrace = 30 * time.Second

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
	GetChannelTargetVersion(ctx context.Context, collectionID int64, channel string, scope TargetScope) int64
	UpdateChannelCurrentTarget(ctx context.Context, collectionID int64, channel string) bool
	IsSegmentInLiveVersion(collectionID int64, channel string, version int64, segmentID int64) (bool, bool)
	GCLiveVersions(collectionID int64, channel string, readableVersions typeutil.UniqueSet)
	IsCurrentTargetExist(ctx context.Context, collectionID int64, partitionID int64) bool
	IsNextTargetExist(ctx context.Context, collectionID int64) bool
	SaveCurrentTarget(ctx context.Context, catalog metastore.QueryCoordCatalog)
	Recover(ctx context.Context, catalog metastore.QueryCoordCatalog) error
	CanSegmentBeMoved(ctx context.Context, collectionID, segmentID int64) bool
	GetTargetJSON(ctx context.Context, scope TargetScope, collectionID int64) string
	GetPartitions(ctx context.Context, collectionID int64, scope TargetScope) ([]int64, error)
	IsCurrentTargetReady(ctx context.Context, collectionID int64) bool
	GetCollectionRowCount(ctx context.Context, collectionID int64, scope TargetScope) int64
}

type TargetManager struct {
	broker Broker
	meta   *Meta

	// all read segment/channel operation happens on current -> only current target are visible to outer
	// all add segment/channel operation happens on next -> changes can only happen on next target
	// all remove segment/channel operation happens on Both current and next -> delete status should be consistent
	current *target
	next    *target

	// Segment sets of target versions that some delegator's readable view still points at, on top
	// of current and next. A delegator keeps serving the version it was last synced to until it is
	// synced again, and QueryCoord must be able to tell what that version contained -- otherwise it
	// cannot know whether a redundant segment is still being read. Versions are dropped once no
	// delegator references them (see GCLiveVersions).
	//
	// Runtime only: after a restart the map is empty, and the release path falls back to
	// "cannot prove it is unused, keep it" until the delegators are re-synced.
	liveVersionsMut sync.RWMutex
	liveVersions    map[int64]map[string]map[int64]typeutil.UniqueSet // collection -> channel -> version -> segment ids
}

func NewTargetManager(broker Broker, meta *Meta) *TargetManager {
	return &TargetManager{
		broker:       broker,
		meta:         meta,
		current:      newTarget(),
		next:         newTarget(),
		liveVersions: make(map[int64]map[string]map[int64]typeutil.UniqueSet),
	}
}

// UpdateChannelCurrentTarget promotes one channel: the channel's segments, dm channel and version
// are taken from the next target into the current one, leaving every other channel untouched.
// Channels advance independently, so a channel whose delegators cannot become ready no longer holds
// the rest of the collection back.
//
// The version being replaced is kept alive (see rememberLiveVersion): a delegator that is still
// synced to it keeps serving it, and coord must be able to say what it contained.
func (mgr *TargetManager) UpdateChannelCurrentTarget(ctx context.Context, collectionID int64, channel string) bool {
	log := mlog.With(mlog.FieldCollectionID(collectionID), mlog.String("channel", channel))

	next := mgr.next.getCollectionTarget(collectionID)
	if next == nil || next.IsEmpty() {
		log.Info(ctx, "next target does not exist, skip promoting channel")
		return false
	}
	if _, ok := next.dmChannels[channel]; !ok {
		log.Info(ctx, "channel is not in the next target, skip promoting it")
		return false
	}

	current := mgr.current.getCollectionTarget(collectionID)
	base := current
	if base == nil || base.IsEmpty() {
		// First promote: publish only this channel, not the whole next -- a not-yet-ready channel's
		// segments must not appear in current. (IsCurrentTargetExist(-1) still turns true here, so
		// the partial-search sync for the other channels is skipped; making that per-channel is a
		// follow-up.)
		base = NewCollectionTarget(map[int64]*datapb.SegmentInfo{}, map[string]*DmChannel{}, nil)
	} else {
		mgr.rememberLiveVersion(collectionID, channel, current)
	}
	promoted := base.WithChannelFrom(next, channel)
	mgr.current.replaceCollectionTarget(collectionID, promoted)

	// per-channel checkpoint-lag metric: the collection-level UpdateCollectionCurrentTarget no longer
	// runs in production, so this is the only place it is emitted now.
	if dmChannel, ok := promoted.dmChannels[channel]; ok {
		ts, _ := tsoutil.ParseTS(dmChannel.GetSeekPosition().GetTimestamp())
		metrics.QueryCoordCurrentTargetCheckpointUnixSeconds.WithLabelValues(
			paramtable.GetStringNodeID(),
			channel,
		).Set(float64(ts.Unix()))
	}

	// Drop next once every one of its channels has been promoted into current, so the next tick
	// rebuilds it via !IsNextTargetExist instead of waiting out NextTargetSurviveTime. Without this
	// next lives forever and compaction/flush/delete absorption is delayed up to the survive time.
	if mgr.allChannelsPromoted(promoted, next) {
		mgr.next.removeCollectionTarget(collectionID)
	}

	log.Debug(ctx, "promoted channel to the next target version",
		mlog.Int64("version", promoted.GetChannelTargetVersion(channel)))
	return true
}

// allChannelsPromoted reports whether current has caught up to next on every one of next's channels,
// i.e. next has been fully absorbed and can be dropped.
func (mgr *TargetManager) allChannelsPromoted(current, next *CollectionTarget) bool {
	for channel := range next.GetAllDmChannels() {
		if current.GetChannelTargetVersion(channel) != next.GetChannelTargetVersion(channel) {
			return false
		}
	}
	return true
}

// replaceNextTarget swaps the next target, retaining the segment sets of the version it replaces:
// a delegator that was already synced to the outgoing version keeps serving it until it is synced
// again, and the release path must be able to resolve what that version contained. Without this the
// outgoing next is simply forgotten -- the orphan that the whole design exists to remove.
func (mgr *TargetManager) replaceNextTarget(collectionID int64, newTarget *CollectionTarget) {
	if old := mgr.next.getCollectionTarget(collectionID); old != nil && !old.IsEmpty() {
		for channel := range old.GetAllDmChannels() {
			mgr.rememberLiveVersion(collectionID, channel, old)
		}
	}
	mgr.next.updateCollectionTarget(collectionID, newTarget)
}

// rememberLiveVersion records the segment set a channel's version had, so that a delegator still
// synced to it can be reasoned about after the version stops being current or next.
func (mgr *TargetManager) rememberLiveVersion(collectionID int64, channel string, target *CollectionTarget) {
	version := target.GetChannelTargetVersion(channel)
	if version == 0 {
		return
	}
	segments := typeutil.NewUniqueSet()
	for _, segment := range target.GetChannelSegments(channel) {
		segments.Insert(segment.GetID())
	}

	mgr.liveVersionsMut.Lock()
	defer mgr.liveVersionsMut.Unlock()
	channels, ok := mgr.liveVersions[collectionID]
	if !ok {
		channels = make(map[string]map[int64]typeutil.UniqueSet)
		mgr.liveVersions[collectionID] = channels
	}
	versions, ok := channels[channel]
	if !ok {
		versions = make(map[int64]typeutil.UniqueSet)
		channels[channel] = versions
	}
	versions[version] = segments
}

// IsSegmentInLiveVersion reports whether a segment belongs to a retained (no longer current or next)
// version of the channel that some delegator may still be reading, and whether coord holds that
// version at all. A caller that gets (false, false) cannot prove anything about that version and
// must keep the segment.
func (mgr *TargetManager) IsSegmentInLiveVersion(collectionID int64, channel string, version int64, segmentID int64) (inVersion bool, known bool) {
	mgr.liveVersionsMut.RLock()
	defer mgr.liveVersionsMut.RUnlock()

	versions, ok := mgr.liveVersions[collectionID][channel]
	if !ok {
		return false, false
	}
	segments, ok := versions[version]
	if !ok {
		return false, false
	}
	return segments.Contain(segmentID), true
}

// GCLiveVersions drops the retained versions of a channel that no delegator points at any more.
// readableVersions are the target versions the channel's delegators currently serve.
func (mgr *TargetManager) GCLiveVersions(collectionID int64, channel string, readableVersions typeutil.UniqueSet) {
	mgr.liveVersionsMut.Lock()
	defer mgr.liveVersionsMut.Unlock()

	versions, ok := mgr.liveVersions[collectionID][channel]
	if !ok {
		return
	}
	// A version is a UnixNano timestamp of when it was built. A delegator that was just synced to a
	// version has not necessarily reported it in dist yet, so it would be absent from readableVersions
	// for one round; keep versions younger than the grace window regardless, to avoid dropping one
	// that is about to be reported (dropping it does not lose safety -- servesSegment then keeps the
	// segment -- but it stalls reclamation until the next sync).
	graceThreshold := time.Now().Add(-liveVersionGCGrace).UnixNano()
	for version := range versions {
		if !readableVersions.Contain(version) && version < graceThreshold {
			delete(versions, version)
		}
	}
	if len(versions) == 0 {
		delete(mgr.liveVersions[collectionID], channel)
	}
	if len(mgr.liveVersions[collectionID]) == 0 {
		delete(mgr.liveVersions, collectionID)
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
	err := retry.Handle(ctx, func() (bool, error) {
		var err error
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

	allocatedTarget := NewCollectionTarget(segments, dmChannels, partitionIDs)

	mgr.replaceNextTarget(collectionID, allocatedTarget)

	mlog.Debug(ctx, "finish to update next targets for collection",
		mlog.FieldCollectionID(collectionID),
		mlog.Int64s("PartitionIDs", partitionIDs))

	return nil
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

	// GC is driven by the channel's dist heartbeat, which stops arriving once the collection is
	// released, so the retained versions would otherwise be pinned for the coordinator's lifetime.
	mgr.liveVersionsMut.Lock()
	delete(mgr.liveVersions, collectionID)
	mgr.liveVersionsMut.Unlock()
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
			mgr.replaceNextTarget(collectionID, newTarget)
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
			mgr.replaceNextTarget(collectionID, newTarget)
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

	return NewCollectionTarget(segments, channels, partitions)
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

// GetChannelTargetVersion returns the target version of one channel. Channels advance
// independently, so this -- not the collection version -- is what a delegator's readable view for
// that channel is compared against.
func (mgr *TargetManager) GetChannelTargetVersion(ctx context.Context, collectionID int64, channel string, scope TargetScope) int64 {
	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		if version := t.GetChannelTargetVersion(channel); version > 0 {
			return version
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

func (mgr *TargetManager) GetCollectionRowCount(ctx context.Context, collectionID int64, scope TargetScope) int64 {
	target := mgr.getCollectionTarget(scope, collectionID)
	if len(target) == 0 {
		return 0
	}
	return target[0].GetRowCount()
}
