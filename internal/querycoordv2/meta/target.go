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
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
}

func NewCollectionTarget(segments map[int64]*datapb.SegmentInfo, dmChannels map[string]*DmChannel, partitionIDs []int64) *CollectionTarget {
	channel2Segments := make(map[string][]*datapb.SegmentInfo, len(dmChannels))
	partition2Segments := make(map[int64][]*datapb.SegmentInfo, len(partitionIDs))
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
	}
	return &CollectionTarget{
		segments:           segments,
		channel2Segments:   channel2Segments,
		partition2Segments: partition2Segments,
		dmChannels:         dmChannels,
		partitions:         typeutil.NewSet(partitionIDs...),
		version:            time.Now().UnixNano(),
	}
}

func FromPbCollectionTarget(target *querypb.CollectionTarget) *CollectionTarget {
	segments := make(map[int64]*datapb.SegmentInfo)
	dmChannels := make(map[string]*DmChannel)
	channel2Segments := make(map[string][]*datapb.SegmentInfo)
	partition2Segments := make(map[int64][]*datapb.SegmentInfo)
	var partitions []int64

	lackSegmentInfo := false
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
			},
		}
	}

	if lackSegmentInfo {
		log.Info("target has lack of segment info", zap.Int64("collectionID", target.GetCollectionID()))
	}

	return &CollectionTarget{
		segments:           segments,
		channel2Segments:   channel2Segments,
		partition2Segments: partition2Segments,
		dmChannels:         dmChannels,
		partitions:         typeutil.NewSet(partitions...),
		version:            target.GetVersion(),
		lackSegmentInfo:    lackSegmentInfo,
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

func (t *target) toQueryCoordCollectionTargets() []*metricsinfo.QueryCoordTarget {
	targets := make([]*metricsinfo.QueryCoordTarget, 0, t.collectionTargetMap.Len())
	t.collectionTargetMap.Range(func(k int64, v *CollectionTarget) bool {
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
