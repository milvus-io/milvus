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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type TargetScope = int32

const (
	CurrentTarget TargetScope = iota + 1
	NextTarget
	CurrentTargetFirst
	NextTargetFirst
)

type TargetManagerInterface interface {
	UpdateCollectionCurrentTarget(collectionID int64) bool
	UpdateCollectionNextTarget(collectionID int64) error
	PullNextTargetV1(broker Broker, collectionID int64, chosenPartitionIDs ...int64) (map[int64]*datapb.SegmentInfo, map[string]*DmChannel, error)
	PullNextTargetV2(broker Broker, collectionID int64, chosenPartitionIDs ...int64) (map[int64]*datapb.SegmentInfo, map[string]*DmChannel, error)
	RemoveCollection(collectionID int64)
	RemovePartition(collectionID int64, partitionIDs ...int64)
	GetGrowingSegmentsByCollection(collectionID int64, scope TargetScope) typeutil.UniqueSet
	GetGrowingSegmentsByChannel(collectionID int64, channelName string, scope TargetScope) typeutil.UniqueSet
	GetSealedSegmentsByCollection(collectionID int64, scope TargetScope) map[int64]*datapb.SegmentInfo
	GetSealedSegmentsByChannel(collectionID int64, channelName string, scope TargetScope) map[int64]*datapb.SegmentInfo
	GetDroppedSegmentsByChannel(collectionID int64, channelName string, scope TargetScope) []int64
	GetSealedSegmentsByPartition(collectionID int64, partitionID int64, scope TargetScope) map[int64]*datapb.SegmentInfo
	GetDmChannelsByCollection(collectionID int64, scope TargetScope) map[string]*DmChannel
	GetDmChannel(collectionID int64, channel string, scope TargetScope) *DmChannel
	GetSealedSegment(collectionID int64, id int64, scope TargetScope) *datapb.SegmentInfo
	GetCollectionTargetVersion(collectionID int64, scope TargetScope) int64
	IsCurrentTargetExist(collectionID int64, partitionID int64) bool
	IsNextTargetExist(collectionID int64) bool
	SaveCurrentTarget(catalog metastore.QueryCoordCatalog)
	Recover(catalog metastore.QueryCoordCatalog) error
	CanSegmentBeMoved(collectionID, segmentID int64) bool
}

type TargetManager struct {
	rwMutex sync.RWMutex
	broker  Broker
	meta    *Meta

	// all read segment/channel operation happens on current -> only current target are visible to outer
	// all add segment/channel operation happens on next -> changes can only happen on next target
	// all remove segment/channel operation happens on Both current and next -> delete status should be consistent
	current *target
	next    *target
}

func NewTargetManager(broker Broker, meta *Meta) *TargetManager {
	return &TargetManager{
		broker:  broker,
		meta:    meta,
		current: newTarget(),
		next:    newTarget(),
	}
}

// UpdateCollectionCurrentTarget updates the current target to next target,
// WARN: DO NOT call this method for an existing collection as target observer running, or it will lead to a double-update,
// which may make the current target not available
func (mgr *TargetManager) UpdateCollectionCurrentTarget(collectionID int64) bool {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	log := log.With(zap.Int64("collectionID", collectionID))

	log.Debug("start to update current target for collection")

	newTarget := mgr.next.getCollectionTarget(collectionID)
	if newTarget == nil || newTarget.IsEmpty() {
		log.Info("next target does not exist, skip it")
		return false
	}
	mgr.current.updateCollectionTarget(collectionID, newTarget)
	mgr.next.removeCollectionTarget(collectionID)

	partStatsVersionInfo := "partitionStats:"
	for channelName, dmlChannel := range newTarget.dmChannels {
		ts, _ := tsoutil.ParseTS(dmlChannel.GetSeekPosition().GetTimestamp())
		metrics.QueryCoordCurrentTargetCheckpointUnixSeconds.WithLabelValues(
			fmt.Sprint(paramtable.GetNodeID()),
			channelName,
		).Set(float64(ts.Unix()))
		partStatsVersionInfo += fmt.Sprintf("%s:[", channelName)
		partStatsVersion := dmlChannel.PartitionStatsVersions
		for partID, statVersion := range partStatsVersion {
			partStatsVersionInfo += fmt.Sprintf("%d:%d,", partID, statVersion)
		}
		partStatsVersionInfo += "],"
	}
	log.Debug("finish to update current target for collection",
		zap.Int64s("segments", newTarget.GetAllSegmentIDs()),
		zap.Strings("channels", newTarget.GetAllDmChannelNames()),
		zap.Int64("version", newTarget.GetTargetVersion()),
		zap.String("partStatsVersion", partStatsVersionInfo),
	)
	return true
}

// UpdateCollectionNextTarget updates the next target with new target pulled from DataCoord,
// WARN: DO NOT call this method for an existing collection as target observer running, or it will lead to a double-update,
// which may make the current target not available
func (mgr *TargetManager) UpdateCollectionNextTarget(collectionID int64) error {
	mgr.rwMutex.Lock()
	partitions := mgr.meta.GetPartitionsByCollection(collectionID)
	partitionIDs := lo.Map(partitions, func(partition *Partition, i int) int64 {
		return partition.PartitionID
	})
	allocatedTarget := NewCollectionTarget(nil, nil, partitionIDs)
	mgr.rwMutex.Unlock()

	log := log.With(zap.Int64("collectionID", collectionID),
		zap.Int64s("PartitionIDs", partitionIDs))
	segments, channels, err := mgr.PullNextTargetV2(mgr.broker, collectionID, partitionIDs...)
	if err != nil {
		log.Warn("failed to get next targets for collection", zap.Error(err))
		return err
	}

	if len(segments) == 0 && len(channels) == 0 {
		log.Debug("skip empty next targets for collection")
		return nil
	}
	allocatedTarget.segments = segments
	allocatedTarget.dmChannels = channels

	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	mgr.next.updateCollectionTarget(collectionID, allocatedTarget)
	log.Debug("finish to update next targets for collection",
		zap.Int64s("segments", allocatedTarget.GetAllSegmentIDs()),
		zap.Strings("channels", allocatedTarget.GetAllDmChannelNames()))

	return nil
}

func (mgr *TargetManager) PullNextTargetV1(broker Broker, collectionID int64, chosenPartitionIDs ...int64) (map[int64]*datapb.SegmentInfo, map[string]*DmChannel, error) {
	if len(chosenPartitionIDs) == 0 {
		return nil, nil, nil
	}
	channelInfos := make(map[string][]*datapb.VchannelInfo)
	segments := make(map[int64]*datapb.SegmentInfo, 0)
	dmChannels := make(map[string]*DmChannel)

	fullPartitions, err := broker.GetPartitions(context.Background(), collectionID)
	if err != nil {
		return nil, nil, err
	}

	// we should pull `channel targets` from all partitions because QueryNodes need to load
	// the complete growing segments. And we should pull `segments targets` only from the chosen partitions.
	for _, partitionID := range fullPartitions {
		log.Debug("get recovery info...",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID))
		vChannelInfos, binlogs, err := broker.GetRecoveryInfo(context.TODO(), collectionID, partitionID)
		if err != nil {
			return nil, nil, err
		}
		for _, info := range vChannelInfos {
			channelInfos[info.GetChannelName()] = append(channelInfos[info.GetChannelName()], info)
		}
		if !lo.Contains(chosenPartitionIDs, partitionID) {
			continue
		}
		for _, binlog := range binlogs {
			segments[binlog.GetSegmentID()] = &datapb.SegmentInfo{
				ID:            binlog.GetSegmentID(),
				CollectionID:  collectionID,
				PartitionID:   partitionID,
				InsertChannel: binlog.GetInsertChannel(),
				NumOfRows:     binlog.GetNumOfRows(),
				Binlogs:       binlog.GetFieldBinlogs(),
				Statslogs:     binlog.GetStatslogs(),
				Deltalogs:     binlog.GetDeltalogs(),
			}
		}
	}

	for _, infos := range channelInfos {
		merged := mgr.mergeDmChannelInfo(infos)
		dmChannels[merged.GetChannelName()] = merged
	}

	return segments, dmChannels, nil
}

func (mgr *TargetManager) PullNextTargetV2(broker Broker, collectionID int64, chosenPartitionIDs ...int64) (map[int64]*datapb.SegmentInfo, map[string]*DmChannel, error) {
	log.Debug("start to pull next targets for collection",
		zap.Int64("collectionID", collectionID),
		zap.Int64s("chosenPartitionIDs", chosenPartitionIDs))

	if len(chosenPartitionIDs) == 0 {
		return nil, nil, nil
	}

	channelInfos := make(map[string][]*datapb.VchannelInfo)
	segments := make(map[int64]*datapb.SegmentInfo, 0)
	dmChannels := make(map[string]*DmChannel)

	getRecoveryInfo := func() error {
		var err error

		vChannelInfos, segmentInfos, err := broker.GetRecoveryInfoV2(context.TODO(), collectionID)
		if err != nil {
			// if meet rpc error, for compatibility with previous versions, try pull next target v1
			if errors.Is(err, merr.ErrServiceUnimplemented) {
				segments, dmChannels, err = mgr.PullNextTargetV1(broker, collectionID, chosenPartitionIDs...)
				return err
			}

			return err
		}

		for _, info := range vChannelInfos {
			channelInfos[info.GetChannelName()] = append(channelInfos[info.GetChannelName()], info)
			for _, segmentID := range info.GetLevelZeroSegmentIds() {
				segments[segmentID] = &datapb.SegmentInfo{
					ID:            segmentID,
					CollectionID:  collectionID,
					InsertChannel: info.GetChannelName(),
					State:         commonpb.SegmentState_Flushed,
					Level:         datapb.SegmentLevel_L0,
				}
			}
		}

		partitionSet := typeutil.NewUniqueSet(chosenPartitionIDs...)
		for _, segmentInfo := range segmentInfos {
			if partitionSet.Contain(segmentInfo.GetPartitionID()) || segmentInfo.GetPartitionID() == common.AllPartitionsID {
				segments[segmentInfo.GetID()] = segmentInfo
			}
		}

		for _, infos := range channelInfos {
			merged := mgr.mergeDmChannelInfo(infos)
			dmChannels[merged.GetChannelName()] = merged
		}
		return nil
	}

	err := retry.Do(context.TODO(), getRecoveryInfo, retry.Attempts(10))
	if err != nil {
		return nil, nil, err
	}

	return segments, dmChannels, nil
}

func (mgr *TargetManager) mergeDmChannelInfo(infos []*datapb.VchannelInfo) *DmChannel {
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
func (mgr *TargetManager) RemoveCollection(collectionID int64) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	log.Info("remove collection from targets",
		zap.Int64("collectionID", collectionID))

	current := mgr.current.getCollectionTarget(collectionID)
	if current != nil {
		for channelName := range current.GetAllDmChannels() {
			metrics.QueryCoordCurrentTargetCheckpointUnixSeconds.DeleteLabelValues(
				fmt.Sprint(paramtable.GetNodeID()),
				channelName,
			)
		}
	}

	mgr.current.removeCollectionTarget(collectionID)
	mgr.next.removeCollectionTarget(collectionID)
}

// RemovePartition removes all segment in the given partition,
// NOTE: this doesn't remove any channel even the given one is the only partition
func (mgr *TargetManager) RemovePartition(collectionID int64, partitionIDs ...int64) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	log := log.With(zap.Int64("collectionID", collectionID),
		zap.Int64s("PartitionIDs", partitionIDs))

	log.Info("remove partition from targets")

	partitionSet := typeutil.NewUniqueSet(partitionIDs...)

	oldCurrentTarget := mgr.current.getCollectionTarget(collectionID)
	if oldCurrentTarget != nil {
		newTarget := mgr.removePartitionFromCollectionTarget(oldCurrentTarget, partitionSet)
		if newTarget != nil {
			mgr.current.updateCollectionTarget(collectionID, newTarget)
			log.Info("finish to remove partition from current target for collection",
				zap.Int64s("segments", newTarget.GetAllSegmentIDs()),
				zap.Strings("channels", newTarget.GetAllDmChannelNames()))
		} else {
			log.Info("all partitions have been released, release the collection next target now")
			mgr.current.removeCollectionTarget(collectionID)
		}
	}

	oleNextTarget := mgr.next.getCollectionTarget(collectionID)
	if oleNextTarget != nil {
		newTarget := mgr.removePartitionFromCollectionTarget(oleNextTarget, partitionSet)
		if newTarget != nil {
			mgr.next.updateCollectionTarget(collectionID, newTarget)
			log.Info("finish to remove partition from next target for collection",
				zap.Int64s("segments", newTarget.GetAllSegmentIDs()),
				zap.Strings("channels", newTarget.GetAllDmChannelNames()))
		} else {
			log.Info("all partitions have been released, release the collection current target now")
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

func (mgr *TargetManager) GetGrowingSegmentsByCollection(collectionID int64,
	scope TargetScope,
) typeutil.UniqueSet {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

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

func (mgr *TargetManager) GetGrowingSegmentsByChannel(collectionID int64,
	channelName string,
	scope TargetScope,
) typeutil.UniqueSet {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

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

func (mgr *TargetManager) GetSealedSegmentsByCollection(collectionID int64,
	scope TargetScope,
) map[int64]*datapb.SegmentInfo {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targets := mgr.getCollectionTarget(scope, collectionID)

	for _, t := range targets {
		return t.GetAllSegments()
	}

	return nil
}

func (mgr *TargetManager) GetSealedSegmentsByChannel(collectionID int64,
	channelName string,
	scope TargetScope,
) map[int64]*datapb.SegmentInfo {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		ret := make(map[int64]*datapb.SegmentInfo)
		for k, v := range t.GetAllSegments() {
			if v.GetInsertChannel() == channelName {
				ret[k] = v
			}
		}

		if len(ret) > 0 {
			return ret
		}
	}

	return nil
}

func (mgr *TargetManager) GetDroppedSegmentsByChannel(collectionID int64,
	channelName string,
	scope TargetScope,
) []int64 {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		if channel, ok := t.dmChannels[channelName]; ok {
			return channel.GetDroppedSegmentIds()
		}
	}

	return nil
}

func (mgr *TargetManager) GetSealedSegmentsByPartition(collectionID int64,
	partitionID int64,
	scope TargetScope,
) map[int64]*datapb.SegmentInfo {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		segments := make(map[int64]*datapb.SegmentInfo)
		for _, s := range t.GetAllSegments() {
			if s.GetPartitionID() == partitionID {
				segments[s.GetID()] = s
			}
		}

		if len(segments) > 0 {
			return segments
		}
	}

	return nil
}

func (mgr *TargetManager) GetDmChannelsByCollection(collectionID int64, scope TargetScope) map[string]*DmChannel {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targets := mgr.getCollectionTarget(scope, collectionID)

	for _, t := range targets {
		return t.GetAllDmChannels()
	}

	return nil
}

func (mgr *TargetManager) GetDmChannel(collectionID int64, channel string, scope TargetScope) *DmChannel {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		if ch, ok := t.GetAllDmChannels()[channel]; ok {
			return ch
		}
	}
	return nil
}

func (mgr *TargetManager) GetSealedSegment(collectionID int64, id int64, scope TargetScope) *datapb.SegmentInfo {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		if s, ok := t.GetAllSegments()[id]; ok {
			return s
		}
	}

	return nil
}

func (mgr *TargetManager) GetCollectionTargetVersion(collectionID int64, scope TargetScope) int64 {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targets := mgr.getCollectionTarget(scope, collectionID)
	for _, t := range targets {
		if t.GetTargetVersion() > 0 {
			return t.GetTargetVersion()
		}
	}

	return 0
}

func (mgr *TargetManager) IsCurrentTargetExist(collectionID int64, partitionID int64) bool {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()

	targets := mgr.getCollectionTarget(CurrentTarget, collectionID)

	return len(targets) > 0 && (targets[0].partitions.Contain(partitionID) || partitionID == common.AllPartitionsID) && len(targets[0].dmChannels) > 0
}

func (mgr *TargetManager) IsNextTargetExist(collectionID int64) bool {
	newChannels := mgr.GetDmChannelsByCollection(collectionID, NextTarget)

	return len(newChannels) > 0
}

func (mgr *TargetManager) SaveCurrentTarget(catalog metastore.QueryCoordCatalog) {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
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
				if err := catalog.SaveCollectionTargets(lo.Map(tasks, func(p typeutil.Pair[int64, *querypb.CollectionTarget], _ int) *querypb.CollectionTarget {
					return p.B
				})...); err != nil {
					log.Warn("failed to save current target for collection", zap.Int64s("collectionIDs", ids), zap.Error(err))
				} else {
					log.Info("succeed to save current target for collection", zap.Int64s("collectionIDs", ids))
				}
				return nil, nil
			})
		}
		tasks := make([]typeutil.Pair[int64, *querypb.CollectionTarget], 0, batchSize)
		for id, target := range mgr.current.collectionTargetMap {
			tasks = append(tasks, typeutil.NewPair(id, target.toPbMsg()))
			if len(tasks) >= batchSize {
				submit(tasks)
				tasks = make([]typeutil.Pair[int64, *querypb.CollectionTarget], 0, batchSize)
			}
		}
		if len(tasks) > 0 {
			submit(tasks)
		}
		wg.Wait()
	}
}

func (mgr *TargetManager) Recover(catalog metastore.QueryCoordCatalog) error {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()

	targets, err := catalog.GetCollectionTargets()
	if err != nil {
		log.Warn("failed to recover collection target from etcd", zap.Error(err))
		return err
	}

	for _, t := range targets {
		newTarget := FromPbCollectionTarget(t)
		mgr.current.updateCollectionTarget(t.GetCollectionID(), newTarget)
		log.Info("recover current target for collection",
			zap.Int64("collectionID", t.GetCollectionID()),
			zap.Strings("channels", newTarget.GetAllDmChannelNames()),
			zap.Int("segmentNum", len(newTarget.GetAllSegmentIDs())),
		)

		// clear target info in meta store
		err := catalog.RemoveCollectionTarget(t.GetCollectionID())
		if err != nil {
			log.Warn("failed to clear collection target from etcd", zap.Error(err))
		}
	}

	return nil
}

// if segment isn't l0 segment, and exist in current/next target, then it can be moved
func (mgr *TargetManager) CanSegmentBeMoved(collectionID, segmentID int64) bool {
	mgr.rwMutex.Lock()
	defer mgr.rwMutex.Unlock()
	current := mgr.current.getCollectionTarget(collectionID)
	if current != nil && current.segments[segmentID] != nil && current.segments[segmentID].GetLevel() != datapb.SegmentLevel_L0 {
		return true
	}

	next := mgr.next.getCollectionTarget(collectionID)
	if next != nil && next.segments[segmentID] != nil && next.segments[segmentID].GetLevel() != datapb.SegmentLevel_L0 {
		return true
	}

	return false
}
