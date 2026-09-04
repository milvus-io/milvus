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

package datacoord

import (
	"context"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Handler handles some channel method for ChannelManager
type Handler interface {
	// GetQueryVChanPositions gets the information recovery needed of a channel for QueryCoord
	GetQueryVChanPositions(ch RWChannel, partitionIDs ...UniqueID) *datapb.VchannelInfo
	// GetDataVChanPositions gets the information recovery needed of a channel for DataNode
	GetDataVChanPositions(ch RWChannel, partitionID UniqueID) *datapb.VchannelInfo
	CheckShouldDropChannel(ch string) bool
	FinishDropChannel(ch string, collectionID int64) error
	GetCollection(ctx context.Context, collectionID UniqueID) (*collectionInfo, error)
	GetCurrentSegmentsView(ctx context.Context, channel RWChannel, partitionIDs ...UniqueID) *SegmentsView
	ListLoadedSegments(ctx context.Context) ([]int64, error)
	GenSnapshot(ctx context.Context, collectionID UniqueID) (*snapshotstorage.SnapshotData, error)
	GetDeltaLogFromCompactTo(ctx context.Context, segmentID UniqueID) ([]*datapb.FieldBinlog, error)
}

type SegmentsView struct {
	FlushedSegmentIDs   []int64
	GrowingSegmentIDs   []int64
	DroppedSegmentIDs   []int64
	L0SegmentIDs        []int64
	ImportingSegmentIDs []int64
}

// ServerHandler is a helper of Server
type ServerHandler struct {
	s *Server
}

// newServerHandler creates a new ServerHandler
func newServerHandler(s *Server) *ServerHandler {
	return &ServerHandler{s: s}
}

// GetDataVChanPositions gets vchannel latest positions with provided dml channel names for DataNode.
// unflushend segmentIDs ---> L1, growing segments
// flushend segmentIDs   ---> L1&L2, flushed segments
// dropped segmentIDs    ---> dropped segments
// level zero segmentIDs ---> L0 segments
func (h *ServerHandler) GetDataVChanPositions(channel RWChannel, partitionID UniqueID) *datapb.VchannelInfo {
	segments := h.s.meta.GetRealSegmentsForChannel(channel.GetName())
	mlog.Info(context.TODO(), "GetDataVChanPositions",
		mlog.FieldCollectionID(channel.GetCollectionID()),
		mlog.String("channel", channel.GetName()),
		mlog.Int("numOfSegments", len(segments)),
	)
	var (
		levelZeroIDs = make(typeutil.UniqueSet)
		flushedIDs   = make(typeutil.UniqueSet)
		unflushedIDs = make(typeutil.UniqueSet)
		droppedIDs   = make(typeutil.UniqueSet)
	)
	for _, s := range segments {
		if (partitionID > allPartitionID && s.PartitionID != partitionID) ||
			((s.GetState() != commonpb.SegmentState_Growing && s.GetState() != commonpb.SegmentState_Sealed) && s.GetStartPosition() == nil && s.GetDmlPosition() == nil) {
			// empty growing and sealed segment don't have dml position and start position
			// and it should be recovered for streamingnode, so we add the state-filter here.
			continue
		}
		if s.GetIsImporting() {
			// Skip bulk insert segments.
			continue
		}

		switch {
		case s.GetState() == commonpb.SegmentState_Dropped:
			droppedIDs.Insert(s.GetID())
		case s.GetLevel() == datapb.SegmentLevel_L0:
			levelZeroIDs.Insert(s.GetID())
		case isFlushState(s.GetState()):
			flushedIDs.Insert(s.GetID())
		default:
			unflushedIDs.Insert(s.GetID())
		}
	}

	return &datapb.VchannelInfo{
		CollectionID:        channel.GetCollectionID(),
		ChannelName:         channel.GetName(),
		SeekPosition:        h.GetChannelSeekPosition(channel, partitionID),
		FlushedSegmentIds:   flushedIDs.Collect(),
		UnflushedSegmentIds: unflushedIDs.Collect(),
		DroppedSegmentIds:   droppedIDs.Collect(),
		LevelZeroSegmentIds: levelZeroIDs.Collect(),
	}
}

// GetQueryVChanPositions gets vchannel latest positions with provided dml channel names for QueryCoord.
// unflushend segmentIDs ---> L1, growing segments
// flushend segmentIDs   ---> L1&L2, flushed segments, including indexed or unindexed
// dropped segmentIDs    ---> dropped segments
// level zero segmentIDs ---> L0 segments
func (h *ServerHandler) GetQueryVChanPositions(channel RWChannel, partitionIDs ...UniqueID) *datapb.VchannelInfo {
	validPartitions := lo.Filter(partitionIDs, func(partitionID int64, _ int) bool { return partitionID > allPartitionID })
	filterWithPartition := len(validPartitions) > 0
	validPartitionsMap := make(map[int64]bool)
	partStatsVersions := h.s.meta.partitionStatsMeta.GetChannelPartitionsStatsVersion(channel.GetCollectionID(), channel.GetName())
	partStatsVersionsMap := make(map[int64]int64)
	if filterWithPartition {
		for _, partitionID := range validPartitions {
			partStatsVersionsMap[partitionID] = partStatsVersions[partitionID]
			validPartitionsMap[partitionID] = true
		}
		validPartitionsMap[common.AllPartitionsID] = true
	} else {
		partStatsVersionsMap = partStatsVersions
	}

	var (
		flushedIDs       = make(typeutil.UniqueSet)
		droppedIDs       = make(typeutil.UniqueSet)
		growingIDs       = make(typeutil.UniqueSet)
		levelZeroIDs     = make(typeutil.UniqueSet)
		deleteCheckPoint *msgpb.MsgPosition
	)

	// cannot use GetSegmentsByChannel since dropped segments are needed here
	segments := h.s.meta.GetRealSegmentsForChannel(channel.GetName())

	validSegmentInfos := make(map[int64]*SegmentInfo)
	indexedSegments := FilterInIndexedSegments(context.Background(), h, h.s.meta, false, segments...)
	indexed := typeutil.NewUniqueSet(lo.Map(indexedSegments, func(segment *SegmentInfo, _ int) int64 { return segment.GetID() })...)
	for _, s := range segments {
		if filterWithPartition && !validPartitionsMap[s.GetPartitionID()] {
			continue
		}
		if s.GetStartPosition() == nil && s.GetDmlPosition() == nil && len(s.GetBinlogs()) == 0 {
			continue
		}
		if s.GetIsImporting() {
			// Skip bulk insert segments.
			continue
		}
		validSegmentInfos[s.GetID()] = s

		if s.GetIsInvisible() && s.GetCreatedByCompaction() {
			// skip invisible compaction segments
			continue
		}

		switch {
		case s.GetState() == commonpb.SegmentState_Dropped:
			droppedIDs.Insert(s.GetID())
		case !isFlushState(s.GetState()) || s.GetIsInvisible():
			growingIDs.Insert(s.GetID())
		case s.GetLevel() == datapb.SegmentLevel_L0:
			levelZeroIDs.Insert(s.GetID())
			// use smallest start position of l0 segments as deleteCheckPoint, so query coord will only maintain stream delete record  after this ts
			if deleteCheckPoint == nil || s.GetStartPosition().GetTimestamp() < deleteCheckPoint.GetTimestamp() {
				deleteCheckPoint = s.GetStartPosition()
			}
		default:
			flushedIDs.Insert(s.GetID())
		}
	}

	// ================================================
	// Segments blood relationship:
	//          a   b
	//           \ /
	//            c   d
	//             \ /
	//             / \
	//            e   f
	//
	// GC:        a, b
	// Indexed:   c, d, e, f
	//              ||
	//              || (Index dropped and creating new index and not finished)
	//              \/
	// UnIndexed: c, d, e
	//
	// Retrieve unIndexed expected result:
	// unIndexed: c, d
	// ================================================

	segmentIndexed := func(segID UniqueID) bool {
		return indexed.Contain(segID) || ((validSegmentInfos[segID].GetIsSorted() || validSegmentInfos[segID].GetIsSortedByNamespace()) && validSegmentInfos[segID].GetNumOfRows() < Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex.GetAsInt64())
	}

	fallbackParentReady := func(segID UniqueID) bool { return indexed.Contain(segID) }
	flushedIDs, droppedIDs = retrieveSegment(validSegmentInfos, flushedIDs, droppedIDs, segmentIndexed, fallbackParentReady)

	seekPosition := h.GetChannelSeekPosition(channel, partitionIDs...)
	// if no l0 segment exist, use checkpoint as delete checkpoint
	if len(levelZeroIDs) == 0 {
		deleteCheckPoint = seekPosition
	}

	return &datapb.VchannelInfo{
		CollectionID:           channel.GetCollectionID(),
		ChannelName:            channel.GetName(),
		SeekPosition:           seekPosition,
		FlushedSegmentIds:      flushedIDs.Collect(),
		UnflushedSegmentIds:    growingIDs.Collect(),
		DroppedSegmentIds:      droppedIDs.Collect(),
		LevelZeroSegmentIds:    levelZeroIDs.Collect(),
		PartitionStatsVersions: partStatsVersionsMap,
		DeleteCheckpoint:       deleteCheckPoint,
	}
}

func retrieveSegment(validSegmentInfos map[int64]*SegmentInfo,
	flushedIDs, droppedIDs typeutil.UniqueSet,
	segmentIndexed, fallbackParentReady func(segID UniqueID) bool,
) (typeutil.UniqueSet, typeutil.UniqueSet) {
	// A recovered view may contain both compactTo segments and still-live
	// compactFrom segments when an ordered compaction metadata update is
	// interrupted. CompactTo segments are published before any compactFrom
	// segment is retired, so normalize the immutable input frontier as follows:
	//   - all direct parents present: retirement has not started and the output
	//     set may be incomplete; keep the parents and remove the child;
	//   - only some direct parents present: retirement has started, which proves
	//     the output set was fully published; keep the child and remove the
	//     remaining parents.
	// Each child can be evaluated independently. M:N outputs share the same
	// CompactionFrom set, so they make the same decision against this snapshot.
	initialFlushedIDs := typeutil.NewUniqueSet(flushedIDs.Collect()...)
	removeFromView := make(typeutil.UniqueSet)
	for id := range initialFlushedIDs {
		segment := validSegmentInfos[id]
		if segment == nil || len(segment.GetCompactionFrom()) == 0 {
			continue
		}

		presentParents := make(typeutil.UniqueSet)
		for _, parentID := range segment.GetCompactionFrom() {
			if initialFlushedIDs.Contain(parentID) {
				presentParents.Insert(parentID)
			}
		}

		switch len(presentParents) {
		case len(segment.GetCompactionFrom()):
			removeFromView.Insert(id)
		case 0:
		default:
			removeFromView.Insert(presentParents.Collect()...)
		}
	}
	initialFlushedIDs.Remove(removeFromView.Collect()...)
	flushedIDs = initialFlushedIDs

	allParentsReady := func(ids ...UniqueID) bool {
		for _, id := range ids {
			seg, ok := validSegmentInfos[id]
			if !ok || seg == nil || seg.GetIsInvisible() || !fallbackParentReady(id) {
				return false
			}
		}
		return true
	}

	// Select the initial fallbacks in one pass. Every expansion, including the
	// ones added later to resolve overlap, must use a complete ready parent set.
	expanded := make(typeutil.UniqueSet)
	for id := range flushedIDs {
		segment := validSegmentInfos[id]
		compactionFrom := segment.GetCompactionFrom()
		if len(compactionFrom) == 0 || segmentIndexed(id) || !allParentsReady(compactionFrom...) {
			continue
		}
		expanded.Insert(id)
	}

	buildCandidates := func() (typeutil.UniqueSet, map[UniqueID]typeutil.UniqueSet) {
		candidates := make(typeutil.UniqueSet)
		fallbackOwners := make(map[UniqueID]typeutil.UniqueSet)
		type candidateVisit struct {
			id     UniqueID
			rootID UniqueID
		}
		var visited map[candidateVisit]struct{}
		var emit func(id, rootID UniqueID, fromFallback bool)
		emit = func(id, rootID UniqueID, fromFallback bool) {
			segment, ok := validSegmentInfos[id]
			if ok && segment != nil && expanded.Contain(id) {
				if visited == nil {
					visited = make(map[candidateVisit]struct{})
				}
				visit := candidateVisit{id: id, rootID: rootID}
				if _, ok := visited[visit]; ok {
					return
				}
				visited[visit] = struct{}{}

				for _, parentID := range segment.GetCompactionFrom() {
					emit(parentID, rootID, true)
				}
				return
			}

			candidates.Insert(id)
			if fromFallback {
				if fallbackOwners[id] == nil {
					fallbackOwners[id] = make(typeutil.UniqueSet)
				}
				fallbackOwners[id].Insert(rootID)
			}
		}
		for id := range flushedIDs {
			emit(id, id, false)
		}
		return candidates, fallbackOwners
	}

	type ancestorCoverage int
	const (
		noAncestorCoverage ancestorCoverage = iota
		partialAncestorCoverage
		completeAncestorCoverage
	)

	type coverageResult struct {
		coverage ancestorCoverage
		owners   typeutil.UniqueSet
	}
	newCoverageChecker := func(candidates typeutil.UniqueSet, fallbackOwners map[UniqueID]typeutil.UniqueSet) func(UniqueID) coverageResult {
		coverageCache := make(map[UniqueID]coverageResult)
		trackOwners := fallbackOwners != nil
		var coverage func(UniqueID) coverageResult
		coverage = func(id UniqueID) coverageResult {
			if cached, ok := coverageCache[id]; ok {
				return cached
			}

			segment, ok := validSegmentInfos[id]
			if !ok || segment == nil || len(segment.GetCompactionFrom()) == 0 {
				return coverageResult{coverage: noAncestorCoverage}
			}

			result := coverageResult{}
			if trackOwners {
				result.owners = make(typeutil.UniqueSet)
			}
			anyCovered := false
			allCovered := true
			for _, parentID := range segment.GetCompactionFrom() {
				parentResult := coverageResult{coverage: noAncestorCoverage}
				if candidates.Contain(parentID) {
					parentResult.coverage = completeAncestorCoverage
					if trackOwners {
						parentResult.owners = fallbackOwners[parentID]
					}
				} else {
					parentResult = coverage(parentID)
				}
				anyCovered = anyCovered || parentResult.coverage != noAncestorCoverage
				allCovered = allCovered && parentResult.coverage == completeAncestorCoverage
				if trackOwners {
					for ownerID := range parentResult.owners {
						result.owners.Insert(ownerID)
					}
				}
			}

			switch {
			case allCovered:
				result.coverage = completeAncestorCoverage
			case anyCovered:
				result.coverage = partialAncestorCoverage
			default:
				result.coverage = noAncestorCoverage
			}
			coverageCache[id] = result
			return result
		}
		return coverage
	}

	// Close partial overlaps created by M:N compactions. Expand the descendant
	// when all of its direct parents are ready; otherwise cancel the root
	// fallback that introduced the overlapping ancestors and keep its child.
	var candidates typeutil.UniqueSet
	blockedRoots := make(typeutil.UniqueSet)
	for {
		var fallbackOwners map[UniqueID]typeutil.UniqueSet
		candidates, fallbackOwners = buildCandidates()
		coverage := newCoverageChecker(candidates, fallbackOwners)
		toExpand := make(typeutil.UniqueSet)
		toCancel := make(typeutil.UniqueSet)
		hasPartialCoverage := false
		for id := range candidates {
			result := coverage(id)
			if result.coverage != partialAncestorCoverage {
				continue
			}
			hasPartialCoverage = true
			segment := validSegmentInfos[id]
			if segment != nil && len(segment.GetCompactionFrom()) > 0 && !blockedRoots.Contain(id) && allParentsReady(segment.GetCompactionFrom()...) {
				toExpand.Insert(id)
			} else {
				toCancel.Insert(result.owners.Collect()...)
			}
		}
		if !hasPartialCoverage {
			break
		}

		changed := false
		for id := range toExpand {
			if !toCancel.Contain(id) {
				expanded.Insert(id)
				changed = true
			}
		}
		for id := range toCancel {
			expanded.Remove(id)
			blockedRoots.Insert(id)
			changed = true
		}
		if !changed {
			// The original leaf frontier is the safe fallback for malformed
			// metadata that cannot attribute a partial overlap to an expansion.
			candidates = typeutil.NewUniqueSet(flushedIDs.Collect()...)
			break
		}
	}

	// Deduplicate against the immutable candidate frontier. An ancestor may be
	// selected by another branch of an M:N compaction, so walk the complete
	// ancestry here, but never introduce additional segments during this phase.
	coverage := newCoverageChecker(candidates, nil)
	finalFrontier := make(typeutil.UniqueSet)
	for id := range candidates {
		if coverage(id).coverage != completeAncestorCoverage {
			finalFrontier.Insert(id)
		}
	}
	droppedIDs.Remove(finalFrontier.Collect()...)

	return finalFrontier, droppedIDs
}

func (h *ServerHandler) GetCurrentSegmentsView(ctx context.Context, channel RWChannel, partitionIDs ...UniqueID) *SegmentsView {
	validPartitions := lo.Filter(partitionIDs, func(partitionID int64, _ int) bool { return partitionID > allPartitionID })
	filterWithPartition := len(validPartitions) > 0
	validPartitionsMap := make(map[int64]bool)
	validPartitionsMap[common.AllPartitionsID] = true
	for _, partitionID := range validPartitions {
		validPartitionsMap[partitionID] = true
	}

	var (
		flushedIDs   = make(typeutil.UniqueSet)
		droppedIDs   = make(typeutil.UniqueSet)
		growingIDs   = make(typeutil.UniqueSet)
		importingIDs = make(typeutil.UniqueSet)
		levelZeroIDs = make(typeutil.UniqueSet)
	)

	// cannot use GetSegmentsByChannel since dropped segments are needed here
	segments := h.s.meta.GetRealSegmentsForChannel(channel.GetName())

	validSegmentInfos := make(map[int64]*SegmentInfo)
	for _, s := range segments {
		if filterWithPartition && !validPartitionsMap[s.GetPartitionID()] {
			continue
		}
		if s.GetStartPosition() == nil && s.GetDmlPosition() == nil {
			continue
		}

		validSegmentInfos[s.GetID()] = s
		switch {
		case s.GetState() == commonpb.SegmentState_Dropped:
			droppedIDs.Insert(s.GetID())
		case s.GetState() == commonpb.SegmentState_Importing:
			importingIDs.Insert(s.GetID())
		case s.GetLevel() == datapb.SegmentLevel_L0:
			levelZeroIDs.Insert(s.GetID())
		case s.GetState() == commonpb.SegmentState_Growing:
			growingIDs.Insert(s.GetID())
		default:
			flushedIDs.Insert(s.GetID())
		}
	}

	alwaysReady := func(UniqueID) bool { return true }
	flushedIDs, droppedIDs = retrieveSegment(validSegmentInfos, flushedIDs, droppedIDs, alwaysReady, alwaysReady)

	mlog.Info(ctx, "GetCurrentSegmentsView",
		mlog.FieldCollectionID(channel.GetCollectionID()),
		mlog.String("channel", channel.GetName()),
		mlog.Int("numOfSegments", len(segments)),
		mlog.Int("result flushed", len(flushedIDs)),
		mlog.Int("result growing", len(growingIDs)),
		mlog.Int("result importing", len(importingIDs)),
		mlog.Int("result L0", len(levelZeroIDs)),
	)

	return &SegmentsView{
		FlushedSegmentIDs:   flushedIDs.Collect(),
		GrowingSegmentIDs:   growingIDs.Collect(),
		DroppedSegmentIDs:   droppedIDs.Collect(),
		L0SegmentIDs:        levelZeroIDs.Collect(),
		ImportingSegmentIDs: importingIDs.Collect(),
	}
}

// getEarliestSegmentDMLPos returns the earliest dml position of segments,
// this is mainly for COMPATIBILITY with old version <=2.1.x
func (h *ServerHandler) getEarliestSegmentDMLPos(channel string, partitionIDs ...UniqueID) *msgpb.MsgPosition {
	var minPos *msgpb.MsgPosition
	var minPosSegID int64
	var minPosTs uint64
	segments := h.s.meta.SelectSegments(context.TODO(), WithChannel(channel))

	validPartitions := lo.Filter(partitionIDs, func(partitionID int64, _ int) bool { return partitionID > allPartitionID })
	partitionSet := typeutil.NewUniqueSet(validPartitions...)
	for _, s := range segments {
		if (partitionSet.Len() > 0 && !partitionSet.Contain(s.PartitionID)) ||
			(s.GetStartPosition() == nil && s.GetDmlPosition() == nil) {
			continue
		}
		if s.GetIsImporting() {
			// Skip bulk insert segments.
			continue
		}
		if s.GetState() == commonpb.SegmentState_Dropped {
			continue
		}

		var segmentPosition *msgpb.MsgPosition
		if s.GetDmlPosition() != nil {
			segmentPosition = s.GetDmlPosition()
		} else {
			segmentPosition = s.GetStartPosition()
		}
		if minPos == nil || segmentPosition.Timestamp < minPos.Timestamp {
			minPosSegID = s.GetID()
			minPosTs = segmentPosition.GetTimestamp()
			minPos = segmentPosition
		}
	}
	if minPos != nil {
		mlog.Info(context.TODO(), "getEarliestSegmentDMLPos done",
			mlog.FieldSegmentID(minPosSegID),
			mlog.Uint64("posTs", minPosTs),
			mlog.Time("posTime", tsoutil.PhysicalTime(minPosTs)))
	}
	return minPos
}

// getCollectionStartPos returns collection start position.
func (h *ServerHandler) getCollectionStartPos(channel RWChannel) *msgpb.MsgPosition {
	log := mlog.With(mlog.String("channel", channel.GetName()))
	if channel.GetStartPosition() != nil {
		return channel.GetStartPosition()
	}
	// use collection start position when segment position is not found
	collection, err := h.GetCollection(h.s.ctx, channel.GetCollectionID())
	if collection != nil && err == nil {
		startPosition := toMsgPosition(channel.GetName(), collection.StartPositions)
		// We should not set the timestamp to collectionInfo.CreatedAt
		// because after enabling streaming arch, every shard has its own timetick, no comparison can be applied cross shards timetick.
		// because when using the collection start position, we don't perform any sync operation of data,
		// so we can just use 0 here without introducing any repeated data to avoid filtering some DML whose timetick is less than collectionInfo.CreatedAt.
		// And after enabling new DDL framework, the collection start position will have its own timestamp, so we can use it directly.
		log.Info(context.TODO(), "NEITHER segment position or channel start position are found, setting channel seek position to collection start position",
			mlog.Uint64("posTs", startPosition.GetTimestamp()),
			mlog.Time("posTime", tsoutil.PhysicalTime(startPosition.GetTimestamp())),
		)
		return startPosition
	}
	return nil
}

// GetChannelSeekPosition gets channel seek position from:
//  1. Channel checkpoint meta;
//  2. Segments earliest dml position;
//  3. Collection start position;
//     And would return if any position is valid.
func (h *ServerHandler) GetChannelSeekPosition(channel RWChannel, partitionIDs ...UniqueID) *msgpb.MsgPosition {
	log := mlog.With(mlog.String("channel", channel.GetName()))
	var seekPosition *msgpb.MsgPosition
	seekPosition = h.s.meta.GetChannelCheckpoint(channel.GetName())
	if seekPosition != nil {
		return seekPosition
	}

	seekPosition = h.getEarliestSegmentDMLPos(channel.GetName(), partitionIDs...)
	if seekPosition != nil {
		log.Info(context.TODO(), "channel seek position set from earliest segment dml position",
			mlog.Uint64("posTs", seekPosition.Timestamp),
			mlog.Time("posTime", tsoutil.PhysicalTime(seekPosition.GetTimestamp())))
		return seekPosition
	}

	seekPosition = h.getCollectionStartPos(channel)
	if seekPosition != nil {
		log.Info(context.TODO(), "channel seek position set from collection start position",
			mlog.Uint64("posTs", seekPosition.Timestamp),
			mlog.Time("posTime", tsoutil.PhysicalTime(seekPosition.GetTimestamp())))
		return seekPosition
	}

	log.Warn(context.TODO(), "get channel checkpoint failed, channelCPMeta and earliestSegDMLPos and collStartPos are all invalid")
	return nil
}

// Deprecated: use toMsgPositionWithWALNames
func toMsgPosition(channel string, startPositions []*commonpb.KeyDataPair) *msgpb.MsgPosition {
	for _, sp := range startPositions {
		if sp.GetKey() != funcutil.ToPhysicalChannel(channel) {
			continue
		}
		return &msgpb.MsgPosition{
			ChannelName: channel,
			MsgID:       sp.GetData(),
		}
	}
	return nil
}

func toMsgPositionWithWALNames(channel string, startPositions []*commonpb.KeyDataPair, channelWALNames map[string]commonpb.WALName) *msgpb.MsgPosition {
	for _, sp := range startPositions {
		pChannel := funcutil.ToPhysicalChannel(channel)
		if sp.GetKey() != pChannel {
			continue
		}
		return &msgpb.MsgPosition{
			ChannelName: channel,
			MsgID:       sp.GetData(),
			WALName:     channelWALNames[pChannel],
		}
	}
	return nil
}

// trimSegmentInfo returns a shallow copy of datapb.SegmentInfo and sets ALL binlog info to nil
func trimSegmentInfo(info *datapb.SegmentInfo) *datapb.SegmentInfo {
	return &datapb.SegmentInfo{
		ID:             info.ID,
		CollectionID:   info.CollectionID,
		PartitionID:    info.PartitionID,
		InsertChannel:  info.InsertChannel,
		NumOfRows:      info.NumOfRows,
		State:          info.State,
		MaxRowNum:      info.MaxRowNum,
		LastExpireTime: info.LastExpireTime,
		StartPosition:  info.StartPosition,
		DmlPosition:    info.DmlPosition,
	}
}

// HasCollection returns whether the collection exist from user's perspective.
func (h *ServerHandler) HasCollection(ctx context.Context, collectionID UniqueID) (bool, error) {
	var hasCollection bool
	ctx2, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if err := retry.Do(ctx2, func() error {
		has, err := h.s.broker.HasCollection(ctx2, collectionID)
		if err != nil {
			mlog.RatedInfo(ctx, rate.Limit(60), "datacoord ServerHandler HasCollection retry failed", mlog.Err(err))
			return err
		}
		hasCollection = has
		return nil
	}, retry.Attempts(5)); err != nil {
		mlog.Error(ctx2, "datacoord ServerHandler HasCollection finally failed",
			mlog.FieldCollectionID(collectionID),
			mlog.Err(err))
		// A workaround for https://github.com/milvus-io/milvus/issues/26863. The collection may be considered as not
		// dropped when any exception happened, but there are chances that finally the collection will be cleaned.
		return true, nil
	}
	return hasCollection, nil
}

// GetCollection returns collection info with specified collection id
func (h *ServerHandler) GetCollection(ctx context.Context, collectionID UniqueID) (*collectionInfo, error) {
	ctx2, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	var coll *collectionInfo
	if err := retry.Do(ctx2, func() error {
		var err error
		coll, err = h.s.getCollectionFromRootCoord(ctx2, collectionID)
		if err != nil {
			mlog.Warn(ctx, "failed to get collection from rootcoord", mlog.FieldCollectionID(collectionID), mlog.Err(err))
			return err
		}
		return nil
	}, retry.Attempts(5)); err != nil {
		mlog.Warn(ctx2, "datacoord ServerHandler GetCollection finally failed",
			mlog.FieldCollectionID(collectionID),
			mlog.Err(err))
		return nil, err
	}
	return coll, nil
}

// CheckShouldDropChannel returns whether specified channel is marked to be removed
func (h *ServerHandler) CheckShouldDropChannel(channel string) bool {
	return h.s.meta.catalog.ShouldDropChannel(h.s.ctx, channel)
}

// FinishDropChannel cleans up the remove flag for channels
// this function is a wrapper of server.meta.FinishDropChannel
func (h *ServerHandler) FinishDropChannel(channel string, collectionID int64) error {
	err := h.s.meta.catalog.DropChannel(h.s.ctx, channel)
	if err != nil {
		mlog.Warn(context.TODO(), "DropChannel failed", mlog.String("vChannel", channel), mlog.Err(err))
		return err
	}
	mlog.Info(context.TODO(), "DropChannel succeeded", mlog.String("channel", channel))
	// Channel checkpoints are cleaned up during garbage collection.
	// Metric cleanup is independent of collection metadata ownership.
	metrics.CleanupDataCoordWithCollectionID(collectionID)

	return nil
}

func (h *ServerHandler) ListLoadedSegments(ctx context.Context) ([]int64, error) {
	return h.s.listLoadedSegments(ctx)
}

// GetSnapshotSeekPositions returns every channel seek position used to create a snapshot.
// The returned min timestamp is kept as SnapshotInfo.create_ts for compatibility.
// Note: if channel has tt lag, the snapshot ts also has tt lag.
func (h *ServerHandler) GetSnapshotSeekPositions(ctx context.Context, collectionID UniqueID, partitionIDs ...UniqueID) ([]*msgpb.MsgPosition, uint64, error) {
	channels, err := h.s.getChannelsByCollectionID(ctx, collectionID)
	if err != nil {
		return nil, 0, err
	}
	if len(channels) == 0 {
		return nil, 0, merr.WrapErrServiceInternal("no channel found for snapshot")
	}

	positions := make([]*msgpb.MsgPosition, 0, len(channels))
	minTs := uint64(math.MaxUint64)
	for _, channel := range channels {
		seekPosition := h.GetChannelSeekPosition(channel, partitionIDs...)
		if seekPosition == nil {
			return nil, 0, merr.WrapErrServiceInternal("no valid channel seek position for snapshot")
		}
		cloned := proto.Clone(seekPosition).(*msgpb.MsgPosition)
		cloned.ChannelName = channel.GetName()
		if cloned.GetTimestamp() < minTs {
			minTs = cloned.GetTimestamp()
		}
		positions = append(positions, cloned)
	}

	sort.Slice(positions, func(i, j int) bool {
		return positions[i].GetChannelName() < positions[j].GetChannelName()
	})
	return positions, minTs, nil
}

// hasCommittedManifest reports whether a Storage V3 manifest references
// committed files. ManifestEarliest is only the placeholder assigned to a new
// Growing segment before its first manifest commit.
func hasCommittedManifest(info *SegmentInfo) (bool, error) {
	if info.GetStorageVersion() != storage.StorageV3 || info.GetManifestPath() == "" {
		return false, nil
	}

	_, version, err := packed.UnmarshalManifestPath(info.GetManifestPath())
	if err != nil {
		return false, merr.WrapErrDataIntegrity(err,
			"invalid manifest path for segment %d", info.GetID())
	}
	return version > packed.ManifestEarliest, nil
}

// GenSnapshot generates a point-in-time snapshot of a collection's data and metadata.
//
// This function captures a consistent view of a collection at a specific timestamp, including:
// - Collection schema and configuration
// - Partition metadata (excluding auto-created partitions)
// - Segment data (binlogs, deltalogs, statslogs)
// - All index types (vector/scalar, text, JSON key)
// - Compaction history deltalogs
//
// Process flow:
//  1. Retrieve collection schema and partition information
//  2. Filter user-created partitions (exclude default and auto-created partitions)
//  3. Generate per-channel snapshot seek positions ensuring data consistency
//  4. Collect current index metadata for the collection
//  5. Select segments with data that started before each channel seek timestamp
//  6. Decompress binlog paths for segment data
//  7. Gather delta logs from compacted segments
//  8. Build segment descriptions with all binlog and index file paths
//  9. Assemble complete snapshot data structure
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - collectionID: ID of collection to snapshot
//
// Returns:
//   - snapshotstorage.SnapshotData: Complete snapshot with collection metadata and segment descriptions
//   - error: If collection not found, timestamp generation fails, or binlog operations fail
//
// Partition filtering logic:
//   - Collections without partition key: Include only explicitly user-created partitions
//     (exclude "_default" and "_default_*" auto-sharded partitions)
//   - Collections with partition key: Include all partitions (filtering handled elsewhere)
//
// Segment selection criteria:
// - Must have data (binlogs, deltalogs, or a committed V3 manifest present)
// - StartPosition timestamp < channel seek timestamp (data started before snapshot)
// - State != Dropped (still valid)
// - Not importing (stable segments only)
//
// Index handling:
// - Includes collection index definitions
// - Captures finished segment index files with full paths
// - Includes text indexes and JSON key indexes
// - Preserves index parameters and versions
//
// Why decompress binlogs:
// - Binlogs are stored compressed in metadata for space efficiency
// - Snapshot needs full paths for file copying during restore
// - Decompression expands compressed paths to complete S3/MinIO paths
//
// Use case:
// - Creating backup snapshots for disaster recovery
// - Point-in-time restore for data rollback
// - Collection cloning to different database/cluster
func (h *ServerHandler) GenSnapshot(ctx context.Context, collectionID UniqueID) (*snapshotstorage.SnapshotData, error) {
	// get coll info
	resp, err := h.s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	showPartitionResp, err := h.s.broker.ShowPartitions(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	partitionIDs := showPartitionResp.GetPartitionIDs()
	partitionNames := showPartitionResp.GetPartitionNames()

	partitionMapping := make(map[string]int64)
	for idx, name := range partitionNames {
		partitionMapping[name] = partitionIDs[idx]
	}

	// generate snapshot seek positions with current partition ids
	channelSeekPositions, snapshotTs, err := h.GetSnapshotSeekPositions(ctx, collectionID, partitionIDs...)
	if err != nil {
		return nil, err
	}
	channelSeekTs := make(map[string]uint64, len(channelSeekPositions))
	for _, position := range channelSeekPositions {
		if position.GetChannelName() == "" {
			return nil, merr.WrapErrServiceInternal("empty snapshot channel seek position")
		}
		channelSeekTs[position.GetChannelName()] = position.GetTimestamp()
	}

	indexes := h.s.meta.indexMeta.GetIndexesForCollection(collectionID, "")
	indexInfos := lo.FilterMap(indexes, func(index *model.Index, _ int) (*indexpb.IndexInfo, bool) {
		return &indexpb.IndexInfo{
			IndexID:         index.IndexID,
			CollectionID:    index.CollectionID,
			FieldID:         index.FieldID,
			IndexName:       index.IndexName,
			TypeParams:      index.TypeParams,
			IndexParams:     index.IndexParams,
			IsAutoIndex:     index.IsAutoIndex,
			UserIndexParams: index.UserIndexParams,
		}, true
	})

	// get segment info
	candidateSegments := h.s.meta.SelectSegments(ctx, WithCollection(collectionID), SegmentFilterFunc(func(info *SegmentInfo) bool {
		return info.GetState() != commonpb.SegmentState_Dropped && !info.GetIsImporting()
	}))
	segments := make([]*SegmentInfo, 0, len(candidateSegments))
	for _, info := range candidateSegments {
		segmentHasData := len(info.GetBinlogs()) > 0 || len(info.GetDeltalogs()) > 0
		if !segmentHasData {
			segmentHasData, err = hasCommittedManifest(info)
			if err != nil {
				return nil, err
			}
		}
		if !segmentHasData {
			continue
		}

		seekTs, ok := channelSeekTs[info.GetInsertChannel()]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg(
				"missing snapshot channel seek position for segment channel %s",
				info.GetInsertChannel())
		}
		if segmentEffectiveTs(info.SegmentInfo) < seekTs {
			segments = append(segments, info)
		}
	}

	if len(segments) == 0 {
		mlog.Info(ctx, "no segments found for collection when generating snapshot",
			mlog.FieldCollectionID(collectionID),
			mlog.Uint64("snapshotTs", snapshotTs))
	}

	segmentInfos := lo.Map(segments, func(segment *SegmentInfo, _ int) *datapb.SegmentInfo {
		return proto.Clone(segment.SegmentInfo).(*datapb.SegmentInfo)
	})

	err = binlog.DecompressMultiBinLogs(segmentInfos)
	if err != nil {
		mlog.Error(ctx, "decompress segment binlogs failed when generating snapshot",
			mlog.FieldCollectionID(collectionID),
			mlog.Uint64("snapshotTs", snapshotTs),
			mlog.Err(err))
		return nil, err
	}

	// get delta logs from compactTo segments
	lo.ForEach(segmentInfos, func(segInfo *datapb.SegmentInfo, _ int) {
		deltalogs, err := h.GetDeltaLogFromCompactTo(ctx, segInfo.GetID())
		if err != nil {
			mlog.Error(ctx, "get delta logs from compactTo failed when generating snapshot",
				mlog.FieldCollectionID(collectionID),
				mlog.Uint64("snapshotTs", snapshotTs),
				mlog.FieldSegmentID(segInfo.GetID()),
				mlog.Err(err))
			return
		}
		segInfo.Deltalogs = append(segInfo.GetDeltalogs(), deltalogs...)
	})

	segDescList := lo.Map(segmentInfos, func(segInfo *datapb.SegmentInfo, _ int) *datapb.SegmentDescription {
		segID := segInfo.GetID()
		indexesFiles := uncompressIndexFiles(h, collectionID, segID)
		uncompressedJSONStats := make(map[int64]*datapb.JsonKeyStats)
		for id, jsonStats := range segInfo.GetJsonKeyStats() {
			uncompressedJSONStats[id] = uncompressJSONStats(h, segInfo, jsonStats)
		}
		return &datapb.SegmentDescription{
			SegmentId:         segInfo.GetID(),
			SegmentLevel:      segInfo.GetLevel(),
			PartitionId:       segInfo.GetPartitionID(),
			ChannelName:       segInfo.GetInsertChannel(),
			NumOfRows:         segInfo.GetNumOfRows(),
			StartPosition:     segInfo.GetStartPosition(),
			DmlPosition:       segInfo.GetDmlPosition(),
			StorageVersion:    segInfo.GetStorageVersion(),
			IsSorted:          segInfo.GetIsSorted(),
			Binlogs:           segInfo.GetBinlogs(),
			Deltalogs:         segInfo.GetDeltalogs(),
			Statslogs:         segInfo.GetStatslogs(),
			Bm25Statslogs:     segInfo.GetBm25Statslogs(),
			IndexFiles:        indexesFiles,
			JsonKeyIndexFiles: uncompressedJSONStats,
			TextIndexFiles:    segInfo.GetTextStatsLogs(),
			ManifestPath:      segInfo.GetManifestPath(),
		}
	})

	// Clone schema and add consistency level to properties
	// This is needed because mustConsumeConsistencyLevel in restore expects consistency level in schema.Properties
	schema := proto.Clone(resp.GetSchema()).(*schemapb.CollectionSchema)
	schema.Properties = append(schema.Properties, &commonpb.KeyValuePair{
		Key:   common.ConsistencyLevel,
		Value: strconv.Itoa(int(resp.GetConsistencyLevel())),
	})

	return &snapshotstorage.SnapshotData{
		SnapshotInfo: &datapb.SnapshotInfo{
			CollectionId:         collectionID,
			PartitionIds:         partitionIDs,
			CreateTs:             int64(snapshotTs),
			ChannelSeekPositions: channelSeekPositions,
		},
		Collection: &datapb.CollectionDescription{
			Schema:              schema,
			NumShards:           int64(resp.GetShardsNum()),
			NumPartitions:       resp.GetNumPartitions(),
			ConsistencyLevel:    resp.GetConsistencyLevel(),
			Properties:          common.CloneKeyValuePairs(resp.GetProperties()),
			Partitions:          partitionMapping,
			VirtualChannelNames: resp.GetVirtualChannelNames(),
		},
		Indexes:  indexInfos,
		Segments: segDescList,
	}, nil
}

func uncompressJSONStats(h *ServerHandler, segInfo *datapb.SegmentInfo, jsonStats *datapb.JsonKeyStats) *datapb.JsonKeyStats {
	uncompressedJSONStats := proto.Clone(jsonStats).(*datapb.JsonKeyStats)
	statsMap := map[int64]*datapb.JsonKeyStats{jsonStats.GetFieldID(): uncompressedJSONStats}
	metautil.BuildJSONKeyStatsPaths(h.s.meta.chunkManager.RootPath(), segInfo, statsMap)
	return uncompressedJSONStats
}

func uncompressIndexFiles(h *ServerHandler, collectionID int64, segID int64) []*indexpb.IndexFilePathInfo {
	segIdxes := h.s.meta.indexMeta.getSegmentIndexes(collectionID, segID)
	indexesFiles := make([]*indexpb.IndexFilePathInfo, 0)
	for _, segIdx := range segIdxes {
		if segIdx.IndexState == commonpb.IndexState_Finished {
			fieldID := h.s.meta.indexMeta.GetFieldIDByIndexID(segIdx.CollectionID, segIdx.IndexID)
			indexName := h.s.meta.indexMeta.GetIndexNameByID(segIdx.CollectionID, segIdx.IndexID)

			builder := metautil.NewIndexPathBuilder(h.s.meta.chunkManager.RootPath(),
				segIdx.IndexStorePathVersion, segIdx.CollectionID,
				segIdx.PartitionID, segIdx.SegmentID,
				segIdx.BuildID, segIdx.IndexVersion)
			indexFilePaths := builder.BuildFilePaths(segIdx.IndexFileKeys)
			indexParams := h.s.meta.indexMeta.GetIndexParams(segIdx.CollectionID, segIdx.IndexID)
			indexParams = append(indexParams, h.s.meta.indexMeta.GetTypeParams(segIdx.CollectionID, segIdx.IndexID)...)

			indexesFiles = append(indexesFiles, &indexpb.IndexFilePathInfo{
				SegmentID:                 segID,
				FieldID:                   fieldID,
				IndexID:                   segIdx.IndexID,
				BuildID:                   segIdx.BuildID,
				IndexName:                 indexName,
				IndexParams:               indexParams,
				IndexFilePaths:            indexFilePaths,
				SerializedSize:            segIdx.IndexSerializedSize,
				MemSize:                   segIdx.IndexMemSize,
				IndexVersion:              segIdx.IndexVersion,
				NumRows:                   segIdx.NumRows,
				CurrentIndexVersion:       segIdx.CurrentIndexVersion,
				CurrentScalarIndexVersion: segIdx.CurrentScalarIndexVersion,
				IndexStorePathVersion:     segIdx.IndexStorePathVersion,
			})
		}
	}
	return indexesFiles
}

func (h *ServerHandler) GetDeltaLogFromCompactTo(ctx context.Context, segmentID UniqueID) ([]*datapb.FieldBinlog, error) {
	var getChildrenDelta func(id UniqueID) ([]*datapb.FieldBinlog, error)
	getChildrenDelta = func(id UniqueID) ([]*datapb.FieldBinlog, error) {
		children, ok := h.s.meta.GetCompactionTo(id)
		// double-check the segment, maybe the segment is being dropped concurrently.
		if !ok {
			mlog.Warn(ctx, "failed to get segment, this may have been cleaned", mlog.FieldSegmentID(id))
			err := merr.WrapErrSegmentNotFound(id)
			return nil, err
		}
		allDeltaLogs := make([]*datapb.FieldBinlog, 0)
		for _, child := range children {
			clonedChild := child.Clone()
			// child segment should decompress binlog path
			if err := binlog.DecompressBinLog(storage.DeleteBinlog, clonedChild.GetCollectionID(), clonedChild.GetPartitionID(), clonedChild.GetID(), clonedChild.GetDeltalogs()); err != nil {
				mlog.Warn(ctx, "failed to decompress delta binlog", mlog.FieldSegmentID(clonedChild.GetID()), mlog.Err(err))
				return nil, err
			}
			allDeltaLogs = append(allDeltaLogs, clonedChild.GetDeltalogs()...)
			allChildrenDeltas, err := getChildrenDelta(child.GetID())
			if err != nil {
				return nil, err
			}
			allDeltaLogs = append(allDeltaLogs, allChildrenDeltas...)
		}

		return allDeltaLogs, nil
	}

	return getChildrenDelta(segmentID)
}
