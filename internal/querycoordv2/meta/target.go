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

	"github.com/milvus-io/milvus/internal/metacache"
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
	segmentIDs           map[int64]struct{}
	channel2SegmentIDs   map[string]map[int64]struct{}
	partition2SegmentIDs map[int64]map[int64]struct{}
	dmChannels           map[string]*DmChannel
	partitions           typeutil.Set[int64] // stores target partitions info
	version              int64

	// record target status, if target has been save before milvus v2.4.19, then the target will lack of segment info.
	lackSegmentInfo bool

	// cache collection total row count
	totalRowCount int64

	// metaView resolves segment details on demand from the shared metacache
	// store instead of keeping full proto copies.
	metaView metacache.MetaView
}

// NewCollectionTarget builds a CollectionTarget backed by a shared
// metacache.MetaView: it only keeps segment IDs plus their channel/partition
// grouping, resolving full segment protos from the store on demand.
func NewCollectionTarget(segIDs map[int64]struct{}, dmChannels map[string]*DmChannel, partitionIDs []int64, metaView metacache.MetaView) *CollectionTarget {
	channel2SegmentIDs := make(map[string]map[int64]struct{}, len(dmChannels))
	partition2SegmentIDs := make(map[int64]map[int64]struct{}, len(partitionIDs))
	totalRowCount := int64(0)
	lackSegmentInfo := false

	segs := metaView.GetSegmentsByIDs(lo.Keys(segIDs))
	for id := range segIDs {
		seg, ok := segs[id]
		if !ok {
			// segment ID is present in the target's ID set but could not be
			// resolved from the shared store; mark the target as lacking
			// segment info so Ready() doesn't report it as complete.
			lackSegmentInfo = true
			continue
		}

		channel := seg.GetInsertChannel()
		if channel2SegmentIDs[channel] == nil {
			channel2SegmentIDs[channel] = make(map[int64]struct{})
		}
		channel2SegmentIDs[channel][id] = struct{}{}

		partitionID := seg.GetPartitionID()
		if partition2SegmentIDs[partitionID] == nil {
			partition2SegmentIDs[partitionID] = make(map[int64]struct{})
		}
		partition2SegmentIDs[partitionID][id] = struct{}{}

		totalRowCount += seg.GetNumOfRows()
	}

	if lackSegmentInfo {
		mlog.Info(context.TODO(), "target has lack of segment info")
	}

	return &CollectionTarget{
		segmentIDs:           segIDs,
		channel2SegmentIDs:   channel2SegmentIDs,
		partition2SegmentIDs: partition2SegmentIDs,
		dmChannels:           dmChannels,
		partitions:           typeutil.NewSet(partitionIDs...),
		version:              time.Now().UnixNano(),
		lackSegmentInfo:      lackSegmentInfo,
		totalRowCount:        totalRowCount,
		metaView:             metaView,
	}
}

// FromPbCollectionTarget rebuilds a CollectionTarget from its persisted
// proto form, resolving segment details from the shared metacache.MetaView
// rather than reconstructing full segment protos from the saved target.
func FromPbCollectionTarget(target *querypb.CollectionTarget, metaView metacache.MetaView) *CollectionTarget {
	segmentIDs := make(map[int64]struct{})
	dmChannels := make(map[string]*DmChannel)
	var partitions []int64

	for _, t := range target.GetChannelTargets() {
		for _, partition := range t.GetPartitionTargets() {
			for _, segment := range partition.GetSegments() {
				segmentIDs[segment.GetID()] = struct{}{}
			}
			partitions = append(partitions, partition.GetPartitionID())
		}
		dmChannels[t.GetChannelName()] = &DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID:        target.GetCollectionID(),
				ChannelName:         t.GetChannelName(),
				SeekPosition:        t.GetSeekPosition(),
				UnflushedSegmentIds: t.GetGrowingSegmentIDs(),
				FlushedSegmentIds:   lo.Keys(segmentIDs),
				DroppedSegmentIds:   t.GetDroppedSegmentIDs(),
				DeleteCheckpoint:    t.GetDeleteCheckpoint(),
			},
		}
	}

	ct := NewCollectionTarget(segmentIDs, dmChannels, partitions, metaView)
	// preserve the persisted version instead of the freshly generated one.
	ct.version = target.GetVersion()
	return ct
}

// resolveSegments resolves a set of segment IDs into their full protos from
// the shared metacache store.
func (p *CollectionTarget) resolveSegments(ids map[int64]struct{}) []*datapb.SegmentInfo {
	if len(ids) == 0 {
		return nil
	}
	segs := p.metaView.GetSegmentsByIDs(lo.Keys(ids))
	return lo.Values(segs)
}

func (p *CollectionTarget) toPbMsg() *querypb.CollectionTarget {
	if len(p.dmChannels) == 0 {
		return &querypb.CollectionTarget{}
	}

	collectionID := int64(-1)
	channelTargets := make(map[string]*querypb.ChannelTarget, 0)
	for _, channel := range p.dmChannels {
		collectionID = channel.GetCollectionID()
		partitionTargets := make(map[int64]*querypb.PartitionTarget)
		for _, info := range p.resolveSegments(p.channel2SegmentIDs[channel.GetChannelName()]) {
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

		channelTargets[channel.GetChannelName()] = &querypb.ChannelTarget{
			ChannelName:       channel.GetChannelName(),
			SeekPosition:      channel.GetSeekPosition(),
			GrowingSegmentIDs: channel.GetUnflushedSegmentIds(),
			DroppedSegmentIDs: channel.GetDroppedSegmentIds(),
			PartitionTargets:  lo.Values(partitionTargets),
			DeleteCheckpoint:  channel.GetDeleteCheckpoint(),
		}
	}

	return &querypb.CollectionTarget{
		CollectionID:   collectionID,
		ChannelTargets: lo.Values(channelTargets),
		Version:        p.version,
	}
}

func (p *CollectionTarget) GetAllSegments() map[int64]*datapb.SegmentInfo {
	return p.metaView.GetSegmentsByIDs(lo.Keys(p.segmentIDs))
}

// GetSegment resolves a single segment by ID from the shared metacache
// store, without materializing the full segment map.
func (p *CollectionTarget) GetSegment(id int64) (*datapb.SegmentInfo, bool) {
	if _, ok := p.segmentIDs[id]; !ok {
		return nil, false
	}
	return p.metaView.GetSegment(id)
}

func (p *CollectionTarget) GetChannelSegments(channel string) []*datapb.SegmentInfo {
	return p.resolveSegments(p.channel2SegmentIDs[channel])
}

func (p *CollectionTarget) GetPartitionSegments(partitionID int64) []*datapb.SegmentInfo {
	return p.resolveSegments(p.partition2SegmentIDs[partitionID])
}

func (p *CollectionTarget) GetTargetVersion() int64 {
	return p.version
}

func (p *CollectionTarget) GetAllDmChannels() map[string]*DmChannel {
	return p.dmChannels
}

func (p *CollectionTarget) GetAllSegmentIDs() []int64 {
	return lo.Keys(p.segmentIDs)
}

func (p *CollectionTarget) GetAllDmChannelNames() []string {
	return lo.Keys(p.dmChannels)
}

func (p *CollectionTarget) IsEmpty() bool {
	return len(p.dmChannels)+len(p.segmentIDs) == 0
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
