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

	// metaView resolves segment details on demand from the shared metacache
	// store instead of keeping full proto copies.
	metaView metacache.MetaView
}

// NewCollectionTarget builds a CollectionTarget from the segment infos that
// define it — the broker's recovery info, or a persisted target. The ID set and
// the channel/partition grouping are derived from those infos and never change
// for the life of the target; the full protos are resolved from the shared
// metacache.MetaView on demand, since a segment's contents (manifest, data
// version) legitimately move on while the target stands.
func NewCollectionTarget(segments map[int64]*datapb.SegmentInfo, dmChannels map[string]*DmChannel, partitionIDs []int64, metaView metacache.MetaView) *CollectionTarget {
	segmentIDs := make(map[int64]struct{}, len(segments))
	channel2SegmentIDs := make(map[string]map[int64]struct{}, len(dmChannels))
	partition2SegmentIDs := make(map[int64]map[int64]struct{}, len(partitionIDs))

	for id, seg := range segments {
		segmentIDs[id] = struct{}{}

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
	}

	return &CollectionTarget{
		segmentIDs:           segmentIDs,
		channel2SegmentIDs:   channel2SegmentIDs,
		partition2SegmentIDs: partition2SegmentIDs,
		dmChannels:           dmChannels,
		partitions:           typeutil.NewSet(partitionIDs...),
		version:              time.Now().UnixNano(),
		metaView:             metaView,
	}
}

// newCollectionTargetFromGrouping rebuilds a target from an existing grouping,
// used where the segment set is derived from another target rather than from a
// fresh list of segment infos.
func newCollectionTargetFromGrouping(
	segmentIDs map[int64]struct{},
	channel2SegmentIDs map[string]map[int64]struct{},
	partition2SegmentIDs map[int64]map[int64]struct{},
	dmChannels map[string]*DmChannel,
	partitionIDs []int64,
	lackSegmentInfo bool,
	metaView metacache.MetaView,
) *CollectionTarget {
	return &CollectionTarget{
		segmentIDs:           segmentIDs,
		channel2SegmentIDs:   channel2SegmentIDs,
		partition2SegmentIDs: partition2SegmentIDs,
		dmChannels:           dmChannels,
		partitions:           typeutil.NewSet(partitionIDs...),
		version:              time.Now().UnixNano(),
		lackSegmentInfo:      lackSegmentInfo,
		metaView:             metaView,
	}
}

// FromPbCollectionTarget rebuilds a CollectionTarget from its persisted proto
// form. The persisted target is what defines the ID set and its grouping, so
// segment details that the target itself decides on (partition, channel, row
// count) are read back from it rather than from the shared store.
func FromPbCollectionTarget(target *querypb.CollectionTarget, metaView metacache.MetaView) *CollectionTarget {
	segments := make(map[int64]*datapb.SegmentInfo)
	dmChannels := make(map[string]*DmChannel)
	var partitions []int64

	for _, t := range target.GetChannelTargets() {
		channelSegmentIDs := make([]int64, 0)
		for _, partition := range t.GetPartitionTargets() {
			for _, segment := range partition.GetSegments() {
				segments[segment.GetID()] = &datapb.SegmentInfo{
					ID:            segment.GetID(),
					CollectionID:  target.GetCollectionID(),
					PartitionID:   partition.GetPartitionID(),
					InsertChannel: t.GetChannelName(),
					NumOfRows:     segment.GetNumOfRows(),
					Level:         segment.GetLevel(),
				}
				channelSegmentIDs = append(channelSegmentIDs, segment.GetID())
			}
			partitions = append(partitions, partition.GetPartitionID())
		}
		dmChannels[t.GetChannelName()] = &DmChannel{
			VchannelInfo: &datapb.VchannelInfo{
				CollectionID:        target.GetCollectionID(),
				ChannelName:         t.GetChannelName(),
				SeekPosition:        t.GetSeekPosition(),
				UnflushedSegmentIds: t.GetGrowingSegmentIDs(),
				FlushedSegmentIds:   channelSegmentIDs,
				DroppedSegmentIds:   t.GetDroppedSegmentIDs(),
				DeleteCheckpoint:    t.GetDeleteCheckpoint(),
			},
		}
	}

	ct := NewCollectionTarget(segments, dmChannels, partitions, metaView)
	// preserve the persisted version instead of the freshly generated one.
	ct.version = target.GetVersion()

	// A target persisted before milvus v2.4.19 carries no row count. A segment
	// saved without one is only really unknown when the shared store cannot
	// resolve it either — otherwise the live value stands, which also keeps a
	// target saved while a segment was unresolvable from disabling balance for
	// the whole collection.
	for id, seg := range segments {
		if seg.GetNumOfRows() > 0 {
			continue
		}
		if _, ok := metaView.GetSegment(id); !ok {
			ct.lackSegmentInfo = true
			mlog.Info(context.TODO(), "target has lack of segment info",
				mlog.Int64("collectionID", target.GetCollectionID()), mlog.Int64("segmentID", id))
			break
		}
	}
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

// segmentPartitions inverts the partition grouping once, so persisting a
// target costs one pass over its segments rather than a partition scan each.
func (p *CollectionTarget) segmentPartitions() map[int64]int64 {
	id2Partition := make(map[int64]int64, len(p.segmentIDs))
	for partitionID, ids := range p.partition2SegmentIDs {
		for id := range ids {
			id2Partition[id] = partitionID
		}
	}
	return id2Partition
}

func (p *CollectionTarget) toPbMsg() *querypb.CollectionTarget {
	if len(p.dmChannels) == 0 {
		return &querypb.CollectionTarget{}
	}

	collectionID := int64(-1)
	id2Partition := p.segmentPartitions()
	channelTargets := make(map[string]*querypb.ChannelTarget, 0)
	for _, channel := range p.dmChannels {
		collectionID = channel.GetCollectionID()
		partitionTargets := make(map[int64]*querypb.PartitionTarget)
		// Persist every segment this target owns, not only the ones the shared
		// store can resolve right now: an ID dropped here would be lost from
		// the recovered target for good.
		segs := p.metaView.GetSegmentsByIDs(lo.Keys(p.channel2SegmentIDs[channel.GetChannelName()]))
		for id := range p.channel2SegmentIDs[channel.GetChannelName()] {
			partitionID, ok := id2Partition[id]
			if !ok {
				continue
			}
			partitionTarget, ok := partitionTargets[partitionID]
			if !ok {
				partitionTarget = &querypb.PartitionTarget{
					PartitionID: partitionID,
					Segments:    make([]*querypb.SegmentTarget, 0),
				}
				partitionTargets[partitionID] = partitionTarget
			}

			info := segs[id]
			partitionTarget.Segments = append(partitionTarget.Segments, &querypb.SegmentTarget{
				ID:        id,
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

// GetChannelSegmentIDs returns the IDs of the channel's segments. Callers that
// test membership or count segments must use this rather than the resolved
// protos: a segment DataCoord has since removed still belongs to this target.
func (p *CollectionTarget) GetChannelSegmentIDs(channel string) map[int64]struct{} {
	return p.channel2SegmentIDs[channel]
}

// GetPartitionSegmentIDs returns the IDs of the partition's segments.
func (p *CollectionTarget) GetPartitionSegmentIDs(partitionID int64) map[int64]struct{} {
	return p.partition2SegmentIDs[partitionID]
}

// ContainSegment reports whether the segment belongs to this target, whether or
// not it currently resolves from the shared store.
func (p *CollectionTarget) ContainSegment(id int64) bool {
	_, ok := p.segmentIDs[id]
	return ok
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

// GetRowCount sums the target's segments as they stand now, so the balancer
// scores against current row counts rather than a build-time snapshot.
//
// NumOfRows is authoritative for every storage version: DataCoord reconciles a
// V1/V2 segment against its insert binlogs before the segment enters the shared
// store, and a V3 count is reported by the writer (datapb.Statistics carries no
// insert row count on purpose).
func (p *CollectionTarget) GetRowCount() int64 {
	total := int64(0)
	for _, seg := range p.metaView.GetSegmentsByIDs(lo.Keys(p.segmentIDs)) {
		total += seg.GetNumOfRows()
	}
	return total
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
