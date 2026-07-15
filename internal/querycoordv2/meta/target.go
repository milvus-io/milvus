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
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CollectionTarget collection target is immutable,
type CollectionTarget struct {
	segments           map[int64]*datapb.SegmentInfo
	channel2Segments   map[string][]*datapb.SegmentInfo
	partition2Segments map[int64][]*datapb.SegmentInfo
	dmChannels         map[string]*DmChannel
	partitions         typeutil.Set[int64] // stores target partitions info

	// Channels advance independently: each one carries its own target version. The
	// collection-level version is kept as the oldest of them, so collection-scoped callers keep
	// seeing a version that no channel has moved past yet.
	channelVersions map[string]int64
	version         int64

	// record target status, if target has been save before milvus v2.4.19, then the target will lack of segment info.
	lackSegmentInfo bool

	// cache collection total row count
	totalRowCount int64
}

func NewCollectionTarget(segments map[int64]*datapb.SegmentInfo, dmChannels map[string]*DmChannel, partitionIDs []int64) *CollectionTarget {
	channel2Segments := make(map[string][]*datapb.SegmentInfo, len(dmChannels))
	partition2Segments := make(map[int64][]*datapb.SegmentInfo, len(partitionIDs))
	totalRowCount := int64(0)
	for _, segment := range segments {
		channel := segment.GetInsertChannel()
		if _, ok := channel2Segments[channel]; !ok {
			channel2Segments[channel] = make([]*datapb.SegmentInfo, 0)
		}
		channel2Segments[channel] = append(channel2Segments[channel], segment)
		partitionID := segment.GetPartitionID()
		if _, ok := partition2Segments[partitionID]; !ok {
			partition2Segments[partitionID] = make([]*datapb.SegmentInfo, 0)
		}
		partition2Segments[partitionID] = append(partition2Segments[partitionID], segment)
		totalRowCount += segment.GetNumOfRows()
	}
	version := time.Now().UnixNano()
	channelVersions := make(map[string]int64, len(dmChannels))
	for channel := range dmChannels {
		channelVersions[channel] = version
	}
	return &CollectionTarget{
		segments:           segments,
		channel2Segments:   channel2Segments,
		partition2Segments: partition2Segments,
		dmChannels:         dmChannels,
		partitions:         typeutil.NewSet(partitionIDs...),
		channelVersions:    channelVersions,
		version:            version,
		totalRowCount:      totalRowCount,
	}
}

func FromPbCollectionTarget(target *querypb.CollectionTarget) *CollectionTarget {
	segments := make(map[int64]*datapb.SegmentInfo)
	dmChannels := make(map[string]*DmChannel)
	channel2Segments := make(map[string][]*datapb.SegmentInfo)
	partition2Segments := make(map[int64][]*datapb.SegmentInfo)
	var partitions []int64

	lackSegmentInfo := false
	totalRowCount := int64(0)
	channelVersions := make(map[string]int64, len(target.GetChannelTargets()))
	for _, t := range target.GetChannelTargets() {
		if _, ok := channel2Segments[t.GetChannelName()]; !ok {
			channel2Segments[t.GetChannelName()] = make([]*datapb.SegmentInfo, 0)
		}
		for _, partition := range t.GetPartitionTargets() {
			if _, ok := partition2Segments[partition.GetPartitionID()]; !ok {
				partition2Segments[partition.GetPartitionID()] = make([]*datapb.SegmentInfo, 0, len(partition.GetSegments()))
			}
			for _, segment := range partition.GetSegments() {
				if segment.GetNumOfRows() <= 0 {
					lackSegmentInfo = true
				}
				info := &datapb.SegmentInfo{
					ID:            segment.GetID(),
					Level:         segment.GetLevel(),
					CollectionID:  target.GetCollectionID(),
					PartitionID:   partition.GetPartitionID(),
					InsertChannel: t.GetChannelName(),
					NumOfRows:     segment.GetNumOfRows(),
				}
				segments[segment.GetID()] = info
				channel2Segments[t.GetChannelName()] = append(channel2Segments[t.GetChannelName()], info)
				partition2Segments[partition.GetPartitionID()] = append(partition2Segments[partition.GetPartitionID()], info)
				totalRowCount += segment.GetNumOfRows()
			}
			partitions = append(partitions, partition.GetPartitionID())
		}
		dmChannels[t.GetChannelName()] = &DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID:        target.GetCollectionID(),
				ChannelName:         t.GetChannelName(),
				SeekPosition:        t.GetSeekPosition(),
				UnflushedSegmentIds: t.GetGrowingSegmentIDs(),
				FlushedSegmentIds:   lo.Keys(segments),
				DroppedSegmentIds:   t.GetDroppedSegmentIDs(),
				DeleteCheckpoint:    t.GetDeleteCheckpoint(),
			},
		}

		// Targets persisted before channel-level versions carry none: they were promoted as a
		// whole, so the collection version is every channel's version.
		channelVersion := t.GetVersion()
		if channelVersion == 0 {
			channelVersion = target.GetVersion()
		}
		channelVersions[t.GetChannelName()] = channelVersion
	}

	if lackSegmentInfo {
		mlog.Info(context.TODO(), "target has lack of segment info", mlog.FieldCollectionID(target.GetCollectionID()))
	}

	return &CollectionTarget{
		segments:           segments,
		channel2Segments:   channel2Segments,
		partition2Segments: partition2Segments,
		dmChannels:         dmChannels,
		partitions:         typeutil.NewSet(partitions...),
		channelVersions:    channelVersions,
		version:            target.GetVersion(),
		lackSegmentInfo:    lackSegmentInfo,
		totalRowCount:      totalRowCount,
	}
}

func (p *CollectionTarget) toPbMsg() *querypb.CollectionTarget {
	if len(p.dmChannels) == 0 {
		return &querypb.CollectionTarget{}
	}

	channelSegments := make(map[string][]*datapb.SegmentInfo)
	for _, s := range p.segments {
		if _, ok := channelSegments[s.GetInsertChannel()]; !ok {
			channelSegments[s.GetInsertChannel()] = make([]*datapb.SegmentInfo, 0)
		}
		channelSegments[s.GetInsertChannel()] = append(channelSegments[s.GetInsertChannel()], s)
	}

	collectionID := int64(-1)
	channelTargets := make(map[string]*querypb.ChannelTarget, 0)
	for _, channel := range p.dmChannels {
		collectionID = channel.GetCollectionID()
		partitionTargets := make(map[int64]*querypb.PartitionTarget)
		if infos, ok := channelSegments[channel.GetChannelName()]; ok {
			for _, info := range infos {
				partitionTarget, ok := partitionTargets[info.GetPartitionID()]
				if !ok {
					partitionTarget = &querypb.PartitionTarget{
						PartitionID: info.PartitionID,
						Segments:    make([]*querypb.SegmentTarget, 0),
					}
					partitionTargets[info.GetPartitionID()] = partitionTarget
				}

				partitionTarget.Segments = append(partitionTarget.Segments, &querypb.SegmentTarget{
					ID:        info.GetID(),
					Level:     info.GetLevel(),
					NumOfRows: info.GetNumOfRows(),
				})
			}
		}

		channelTargets[channel.GetChannelName()] = &querypb.ChannelTarget{
			ChannelName:       channel.GetChannelName(),
			SeekPosition:      channel.GetSeekPosition(),
			GrowingSegmentIDs: channel.GetUnflushedSegmentIds(),
			DroppedSegmentIDs: channel.GetDroppedSegmentIds(),
			PartitionTargets:  lo.Values(partitionTargets),
			DeleteCheckpoint:  channel.GetDeleteCheckpoint(),
			Version:           p.channelVersions[channel.GetChannelName()],
		}
	}

	return &querypb.CollectionTarget{
		CollectionID:   collectionID,
		ChannelTargets: lo.Values(channelTargets),
		Version:        p.version,
	}
}

func (p *CollectionTarget) GetAllSegments() map[int64]*datapb.SegmentInfo {
	return p.segments
}

func (p *CollectionTarget) GetChannelSegments(channel string) []*datapb.SegmentInfo {
	return p.channel2Segments[channel]
}

func (p *CollectionTarget) GetPartitionSegments(partitionID int64) []*datapb.SegmentInfo {
	return p.partition2Segments[partitionID]
}

// GetChannelTargetVersion returns the target version of one channel. Channels advance
// independently, so this -- not the collection version -- is what a delegator's readable view is
// compared against.
func (p *CollectionTarget) GetChannelTargetVersion(channel string) int64 {
	return p.channelVersions[channel]
}

func (p *CollectionTarget) GetChannelVersions() map[string]int64 {
	versions := make(map[string]int64, len(p.channelVersions))
	for channel, version := range p.channelVersions {
		versions[channel] = version
	}
	return versions
}

// WithChannelFrom returns a copy of this target with one channel replaced by that channel's content
// (segments, dm channel and version) taken from src. This is how a single channel is promoted:
// current is rebuilt with the promoted channel taken from next, leaving the other channels alone.
func (p *CollectionTarget) WithChannelFrom(src *CollectionTarget, channel string) *CollectionTarget {
	segments := make(map[int64]*datapb.SegmentInfo, len(p.segments))
	for id, segment := range p.segments {
		if segment.GetInsertChannel() != channel {
			segments[id] = segment
		}
	}
	for _, segment := range src.GetChannelSegments(channel) {
		segments[segment.GetID()] = segment
	}

	dmChannels := make(map[string]*DmChannel, len(p.dmChannels))
	for name, dmChannel := range p.dmChannels {
		dmChannels[name] = dmChannel
	}
	if srcChannel, ok := src.dmChannels[channel]; ok {
		dmChannels[channel] = srcChannel
	} else {
		delete(dmChannels, channel)
	}

	// the promoted channel may carry partitions the old current never had (LoadPartitions on an
	// already-loaded collection lands them in next first), so union both sides -- dropping src's
	// would leave IsCurrentTargetExist(newPartition) permanently false and stall the load.
	partitionSet := typeutil.NewSet(p.partitions.Collect()...)
	partitionSet.Insert(src.partitions.Collect()...)
	promoted := NewCollectionTarget(segments, dmChannels, partitionSet.Collect())

	// keep the untouched channels on the versions they already had; the promoted one takes src's
	for name, version := range p.channelVersions {
		if _, ok := promoted.channelVersions[name]; ok {
			promoted.channelVersions[name] = version
		}
	}
	promoted.channelVersions[channel] = src.GetChannelTargetVersion(channel)
	promoted.version = promoted.oldestChannelVersion()
	return promoted
}

// oldestChannelVersion is what collection-scoped callers see: the version no channel has moved past.
func (p *CollectionTarget) oldestChannelVersion() int64 {
	oldest := int64(0)
	for _, version := range p.channelVersions {
		if oldest == 0 || version < oldest {
			oldest = version
		}
	}
	if oldest == 0 {
		return p.version
	}
	return oldest
}

func (p *CollectionTarget) GetTargetVersion() int64 {
	return p.version
}

func (p *CollectionTarget) GetAllDmChannels() map[string]*DmChannel {
	return p.dmChannels
}

func (p *CollectionTarget) GetAllSegmentIDs() []int64 {
	return lo.Keys(p.segments)
}

func (p *CollectionTarget) GetAllDmChannelNames() []string {
	return lo.Keys(p.dmChannels)
}

func (p *CollectionTarget) IsEmpty() bool {
	return len(p.dmChannels)+len(p.segments) == 0
}

// if target is ready, it should have all segment info
func (p *CollectionTarget) Ready() bool {
	return !p.lackSegmentInfo
}

func (p *CollectionTarget) GetRowCount() int64 {
	return p.totalRowCount
}

type target struct {
	keyLock *lock.KeyLock[int64] // guards updateCollectionTarget
	// just maintain target at collection level
	collectionTargetMap *typeutil.ConcurrentMap[int64, *CollectionTarget]
}

func newTarget() *target {
	return &target{
		keyLock:             lock.NewKeyLock[int64](),
		collectionTargetMap: typeutil.NewConcurrentMap[int64, *CollectionTarget](),
	}
}

func (t *target) updateCollectionTarget(collectionID int64, target *CollectionTarget) {
	t.keyLock.Lock(collectionID)
	defer t.keyLock.Unlock(collectionID)
	if old, ok := t.collectionTargetMap.Get(collectionID); ok && old != nil && target.GetTargetVersion() <= old.GetTargetVersion() {
		return
	}

	t.collectionTargetMap.Insert(collectionID, target)
}

// replaceCollectionTarget stores a target unconditionally. Promoting a single channel leaves the
// other channels on their versions, so the collection-level version (the oldest of them) does not
// have to move -- the monotonicity that updateCollectionTarget enforces now holds per channel, not
// per collection.
func (t *target) replaceCollectionTarget(collectionID int64, target *CollectionTarget) {
	t.keyLock.Lock(collectionID)
	defer t.keyLock.Unlock(collectionID)
	t.collectionTargetMap.Insert(collectionID, target)
}

func (t *target) removeCollectionTarget(collectionID int64) {
	t.collectionTargetMap.Remove(collectionID)
}

func (t *target) getCollectionTarget(collectionID int64) *CollectionTarget {
	ret, _ := t.collectionTargetMap.Get(collectionID)
	return ret
}

func (t *target) toQueryCoordCollectionTargets(collectionID int64) []*metricsinfo.QueryCoordTarget {
	targets := make([]*metricsinfo.QueryCoordTarget, 0, t.collectionTargetMap.Len())
	t.collectionTargetMap.Range(func(k int64, v *CollectionTarget) bool {
		if collectionID > 0 && collectionID != k {
			return true
		}
		segments := lo.MapToSlice(v.GetAllSegments(), func(k int64, s *datapb.SegmentInfo) *metricsinfo.Segment {
			return metrics.NewSegmentFrom(s)
		})

		dmChannels := lo.MapToSlice(v.GetAllDmChannels(), func(k string, ch *DmChannel) *metricsinfo.DmChannel {
			return metrics.NewDMChannelFrom(ch.VchannelInfo)
		})

		qct := &metricsinfo.QueryCoordTarget{
			CollectionID: k,
			Segments:     segments,
			DMChannels:   dmChannels,
		}
		targets = append(targets, qct)
		return true
	})
	return targets
}
