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

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/metrics"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// CollectionTarget collection target is immutable,
type CollectionTarget struct {
	segments   map[int64]*datapb.SegmentInfo
	dmChannels map[string]*DmChannel
	partitions typeutil.Set[int64] // stores target partitions info
	version    int64
}

func NewCollectionTarget(segments map[int64]*datapb.SegmentInfo, dmChannels map[string]*DmChannel, partitionIDs []int64) *CollectionTarget {
	return &CollectionTarget{
		segments:   segments,
		dmChannels: dmChannels,
		partitions: typeutil.NewSet(partitionIDs...),
		version:    time.Now().UnixNano(),
	}
}

func FromPbCollectionTarget(target *querypb.CollectionTarget) *CollectionTarget {
	segments := make(map[int64]*datapb.SegmentInfo)
	dmChannels := make(map[string]*DmChannel)
	var partitions []int64

	for _, t := range target.GetChannelTargets() {
		for _, partition := range t.GetPartitionTargets() {
			for _, segment := range partition.GetSegments() {
				segments[segment.GetID()] = &datapb.SegmentInfo{
					ID:            segment.GetID(),
					Level:         segment.GetLevel(),
					CollectionID:  target.GetCollectionID(),
					PartitionID:   partition.GetPartitionID(),
					InsertChannel: t.GetChannelName(),
					NumOfRows:     segment.GetNumOfRows(),
				}
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

	return &CollectionTarget{
		segments:   segments,
		dmChannels: dmChannels,
		partitions: typeutil.NewSet(partitions...),
		version:    target.GetVersion(),
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

type target struct {
	// just maintain target at collection level
	collectionTargetMap map[int64]*CollectionTarget
}

func newTarget() *target {
	return &target{
		collectionTargetMap: make(map[int64]*CollectionTarget),
	}
}

func (t *target) updateCollectionTarget(collectionID int64, target *CollectionTarget) {
	if t.collectionTargetMap[collectionID] != nil && target.GetTargetVersion() <= t.collectionTargetMap[collectionID].GetTargetVersion() {
		return
	}

	t.collectionTargetMap[collectionID] = target
}

func (t *target) removeCollectionTarget(collectionID int64) {
	delete(t.collectionTargetMap, collectionID)
}

func (t *target) getCollectionTarget(collectionID int64) *CollectionTarget {
	return t.collectionTargetMap[collectionID]
}

func (t *target) toQueryCoordCollectionTargets() []*metricsinfo.QueryCoordTarget {
	return lo.MapToSlice(t.collectionTargetMap, func(k int64, v *CollectionTarget) *metricsinfo.QueryCoordTarget {
		segments := lo.MapToSlice(v.GetAllSegments(), func(k int64, s *datapb.SegmentInfo) *metricsinfo.Segment {
			return metrics.NewSegmentFrom(s)
		})

		dmChannels := lo.MapToSlice(v.GetAllDmChannels(), func(k string, ch *DmChannel) *metricsinfo.DmChannel {
			return metrics.NewDMChannelFrom(ch.VchannelInfo)
		})

		return &metricsinfo.QueryCoordTarget{
			CollectionID: k,
			Segments:     segments,
			DMChannels:   dmChannels,
		}
	})
}
