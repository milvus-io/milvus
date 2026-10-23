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
	"google.golang.org/protobuf/proto"

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
	// segment2Channel is the inverse of channel2SegmentIDs: the channel this
	// target attributes each of its segments to. It is what the pull said, and
	// it is what a resolved proto reports as its InsertChannel (see attributed).
	segment2Channel map[int64]string

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
	segment2Channel := make(map[int64]string, len(segments))

	for id, seg := range segments {
		segmentIDs[id] = struct{}{}

		channel := seg.GetInsertChannel()
		if channel2SegmentIDs[channel] == nil {
			channel2SegmentIDs[channel] = make(map[int64]struct{})
		}
		channel2SegmentIDs[channel][id] = struct{}{}
		segment2Channel[id] = channel

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
		segment2Channel:      segment2Channel,
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
	segment2Channel := make(map[int64]string, len(segmentIDs))
	for channel, ids := range channel2SegmentIDs {
		for id := range ids {
			segment2Channel[id] = channel
		}
	}
	return &CollectionTarget{
		segmentIDs:           segmentIDs,
		channel2SegmentIDs:   channel2SegmentIDs,
		partition2SegmentIDs: partition2SegmentIDs,
		dmChannels:           dmChannels,
		partitions:           typeutil.NewSet(partitionIDs...),
		version:              time.Now().UnixNano(),
		lackSegmentInfo:      lackSegmentInfo,
		metaView:             metaView,
		segment2Channel:      segment2Channel,
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
				// the split signal DataCoord reported with this seek position:
				// without it the QueryNode would watch a split source from past
				// its fence without recovering the split's children.
				SplitTargetChannels: t.GetSplitTargetChannels(),
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

// attributed returns the segment under the channel this target attributes it
// to. The shared store resolves the channel datacoord's segment meta records,
// which the recovery info a target is pulled from overrides inside a shard
// split's lineage window: a split target's flushed segment is attributed to
// the split source while the source is listed, so that the source delegator
// loads and serves it (datacoord's splitAttributedInsertChannel). Every
// consumer that routes by a target segment's InsertChannel -- the segment
// checker's load plans, the leader checker's grow actions, the distribution
// handler's routing entry, the readiness and load-progress exclusions --
// relied on the pulled proto carrying that attribution; a proto resolved live
// would carry the recorded channel instead and route a window's segment to a
// delegator that does not exist yet. The attribution is restored on a copy,
// since the store's proto is shared. Outside a lineage window the recorded
// channel and the attribution agree, and the proto is returned as is.
func (p *CollectionTarget) attributed(info *datapb.SegmentInfo) *datapb.SegmentInfo {
	channel, ok := p.segment2Channel[info.GetID()]
	if !ok || channel == info.GetInsertChannel() {
		return info
	}
	attributed := proto.Clone(info).(*datapb.SegmentInfo)
	attributed.InsertChannel = channel
	return attributed
}

// resolveSegments resolves a set of segment IDs into their full protos from
// the shared metacache store, each under the channel this target attributes
// it to.
func (p *CollectionTarget) resolveSegments(ids map[int64]struct{}) []*datapb.SegmentInfo {
	if len(ids) == 0 {
		return nil
	}
	segs := p.metaView.GetSegmentsByIDs(lo.Keys(ids))
	return lo.MapToSlice(segs, func(_ int64, seg *datapb.SegmentInfo) *datapb.SegmentInfo {
		return p.attributed(seg)
	})
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
		// A target owns IDs and nothing else. An ID the shared store can no
		// longer resolve has been garbage collected, and a GC'd segment must
		// not come back through recovery -- so it is dropped here rather than
		// persisted with placeholder zeros, which would otherwise surface on
		// the next boot as lackSegmentInfo and disable balancing for the whole
		// collection.
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

			info, resolved := segs[id]
			if !resolved {
				// Garbage collected: drop it from the persisted form.
				continue
			}
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
	segs := p.metaView.GetSegmentsByIDs(lo.Keys(p.segmentIDs))
	for id, seg := range segs {
		segs[id] = p.attributed(seg)
	}
	return segs
}

// GetSegment resolves a single segment by ID from the shared metacache
// store, without materializing the full segment map.
func (p *CollectionTarget) GetSegment(id int64) (*datapb.SegmentInfo, bool) {
	if _, ok := p.segmentIDs[id]; !ok {
		return nil, false
	}
	seg, ok := p.metaView.GetSegment(id)
	if !ok {
		return nil, false
	}
	return p.attributed(seg), true
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
	// Attribution is the target's own channel grouping, so the narrowing
	// needs no store lookup and keeps a segment the store cannot resolve.
	segmentIDs := make(map[int64]struct{}, len(p.segmentIDs))
	channel2SegmentIDs := make(map[string]map[int64]struct{}, len(p.channel2SegmentIDs))
	for channel, ids := range p.channel2SegmentIDs {
		if exclude.Contain(channel) {
			continue
		}
		kept := make(map[int64]struct{}, len(ids))
		for id := range ids {
			kept[id] = struct{}{}
			segmentIDs[id] = struct{}{}
		}
		channel2SegmentIDs[channel] = kept
	}
	partition2SegmentIDs := make(map[int64]map[int64]struct{}, len(p.partition2SegmentIDs))
	for partitionID, ids := range p.partition2SegmentIDs {
		kept := make(map[int64]struct{}, len(ids))
		for id := range ids {
			if _, ok := segmentIDs[id]; ok {
				kept[id] = struct{}{}
			}
		}
		partition2SegmentIDs[partitionID] = kept
	}
	kept := newCollectionTargetFromGrouping(segmentIDs, channel2SegmentIDs, partition2SegmentIDs,
		dmChannels, p.partitions.Collect(), p.lackSegmentInfo, p.metaView)
	kept.version = p.version
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
