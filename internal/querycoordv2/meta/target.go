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
	version            int64

	// record target status, if target has been save before milvus v2.4.19, then the target will lack of segment info.
	lackSegmentInfo bool

	// cache collection total row count
	totalRowCount int64

	// windowTargets are the channels this target's pull listed that may be
	// shard-split targets not yet adopted at pull time: every listed channel the
	// shard-state read taken just before the pull did not see Normal, Splitting
	// or Dropped (see ShardStateSnapshot.SplitWindowTargets). It never misses a
	// target still Creating in the pull, and may over-mark one adopted between
	// the read and the pull, which the window-end refresh re-pulls. Such a pull is
	// a window snapshot: datacoord attributes the targets' flushed data to their
	// source while the source is listed, so these channels carry no sealed
	// segment of their own here and look trivially data-ready. A delegator of a
	// window target must never be synced to, nor promoted through, this target.
	//
	// It is set by the next-target pull only, and is not persisted, because a
	// current target never carries a live mark: such a snapshot is promoted only
	// narrowed to the channels outside the mark (withoutChannels, under
	// TargetManager.GetSplitWindowExclusions), which leaves the promoted copy
	// with none, and is otherwise not promoted at all. A recovered next target is
	// re-pulled and marked afresh.
	windowTargets typeutil.Set[string]
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
	return &CollectionTarget{
		segments:           segments,
		channel2Segments:   channel2Segments,
		partition2Segments: partition2Segments,
		dmChannels:         dmChannels,
		partitions:         typeutil.NewSet(partitionIDs...),
		version:            time.Now().UnixNano(),
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
				// the split signal DataCoord reported with this seek position:
				// without it the QueryNode would watch a split source from past
				// its fence without recovering the split's children.
				SplitTargetChannels: t.GetSplitTargetChannels(),
			},
		}
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
			// saved with the seek position it was pulled with, see FromPbCollectionTarget.
			SplitTargetChannels: channel.GetSplitTargetChannels(),
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

// SplitWindowTargets returns the channels marked as split window targets when
// this target was pulled; see windowTargets.
func (p *CollectionTarget) SplitWindowTargets() typeutil.Set[string] {
	return p.windowTargets
}

// withoutChannels returns a copy of the target with the given channels -- and
// every sealed segment attributed to one of them -- removed. The copy keeps the
// original's version, partitions and lackSegmentInfo flag: it is the same pull,
// narrowed to the channels a reader is actually served from.
//
// It is how a next target pulled inside a shard split window becomes a current
// target without its window targets. Those channels carry no sealed segment of
// their own in such a pull (datacoord attributes a target's flushed data to its
// still-listed source), so dropping segments by attribution normally drops
// none; it is done anyway so the copy can never describe data on a channel it
// does not list. The partition set is deliberately NOT narrowed: a partition
// whose segments all sit on excluded channels is still a partition of this
// collection, and IsCurrentTargetExist must keep answering for it.
//
// windowTargets on the copy is what remains of the mark after the exclusion --
// empty whenever the caller excludes the whole mark, which is the only
// promotion GetSplitWindowExclusions allows.
func (p *CollectionTarget) withoutChannels(exclude typeutil.Set[string]) *CollectionTarget {
	if len(exclude) == 0 {
		return p
	}
	dmChannels := make(map[string]*DmChannel, len(p.dmChannels))
	for name, channel := range p.dmChannels {
		if exclude.Contain(name) {
			continue
		}
		dmChannels[name] = channel
	}
	segments := make(map[int64]*datapb.SegmentInfo, len(p.segments))
	for id, segment := range p.segments {
		if exclude.Contain(segment.GetInsertChannel()) {
			continue
		}
		segments[id] = segment
	}
	kept := NewCollectionTarget(segments, dmChannels, p.partitions.Collect())
	kept.version = p.version
	kept.lackSegmentInfo = p.lackSegmentInfo
	remainingMark := make([]string, 0, len(p.windowTargets))
	for channel := range p.windowTargets {
		if !exclude.Contain(channel) {
			remainingMark = append(remainingMark, channel)
		}
	}
	if len(remainingMark) > 0 {
		kept.windowTargets = typeutil.NewSet(remainingMark...)
	}
	return kept
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
