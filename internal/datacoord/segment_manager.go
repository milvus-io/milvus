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
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/retry"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// allocPool pool of Allocation, to reduce allocation of Allocation
var allocPool = sync.Pool{
	New: func() interface{} {
		return &Allocation{}
	},
}

// getAllocation unifies way to retrieve allocation struct
func getAllocation(numOfRows int64) *Allocation {
	v := allocPool.Get()
	a, ok := v.(*Allocation)
	if !ok {
		a = &Allocation{}
	}
	if a == nil {
		return &Allocation{
			NumOfRows: numOfRows,
		}
	}
	a.NumOfRows = numOfRows
	a.ExpireTime = 0
	a.SegmentID = 0
	return a
}

// putAllocation puts an allocation for recycling
func putAllocation(a *Allocation) {
	allocPool.Put(a)
}

// Manager manages segment related operations.
//
//go:generate mockery --name=Manager --structname=MockManager --output=./  --filename=mock_segment_manager.go --with-expecter --inpackage
type Manager interface {
	// CreateSegment create new segment when segment not exist

	// AllocSegment allocates rows and record the allocation.
	AllocSegment(ctx context.Context, collectionID, partitionID UniqueID, channelName string, requestRows int64) ([]*Allocation, error)
	AllocImportSegment(ctx context.Context, taskID int64, collectionID UniqueID, partitionID UniqueID, channelName string, level datapb.SegmentLevel) (*SegmentInfo, error)
	// DropSegment drops the segment from manager.
	DropSegment(ctx context.Context, segmentID UniqueID)
	// FlushImportSegments set importing segment state to Flushed.
	FlushImportSegments(ctx context.Context, collectionID UniqueID, segmentIDs []UniqueID) error
	// SealAllSegments seals all segments of collection with collectionID and return sealed segments.
	// If segIDs is not empty, also seals segments in segIDs.
	SealAllSegments(ctx context.Context, collectionID UniqueID, segIDs []UniqueID) ([]UniqueID, error)
	// GetFlushableSegments returns flushable segment ids
	GetFlushableSegments(ctx context.Context, channel string, ts Timestamp) ([]UniqueID, error)
	// ExpireAllocations notifies segment status to expire old allocations
	ExpireAllocations(channel string, ts Timestamp) error
	// DropSegmentsOfChannel drops all segments in a channel
	DropSegmentsOfChannel(ctx context.Context, channel string)
}

// Allocation records the allocation info
type Allocation struct {
	SegmentID  UniqueID
	NumOfRows  int64
	ExpireTime Timestamp
}

func (alloc *Allocation) String() string {
	t, _ := tsoutil.ParseTS(alloc.ExpireTime)
	return fmt.Sprintf("SegmentID: %d, NumOfRows: %d, ExpireTime: %v", alloc.SegmentID, alloc.NumOfRows, t)
}

// make sure SegmentManager implements Manager
var _ Manager = (*SegmentManager)(nil)

// SegmentManager handles L1 segment related logic
type SegmentManager struct {
	meta                *meta
	mu                  lock.RWMutex
	allocator           allocator
	helper              allocHelper
	segments            []UniqueID
	estimatePolicy      calUpperLimitPolicy
	allocPolicy         AllocatePolicy
	segmentSealPolicies []segmentSealPolicy
	channelSealPolicies []channelSealPolicy
	flushPolicy         flushPolicy
}

type allocHelper struct {
	afterCreateSegment func(segment *datapb.SegmentInfo) error
}

// allocOption allocation option applies to `SegmentManager`
type allocOption interface {
	apply(manager *SegmentManager)
}

// allocFunc function shortcut for allocOption
type allocFunc func(manager *SegmentManager)

// implement allocOption
func (f allocFunc) apply(manager *SegmentManager) {
	f(manager)
}

// get allocOption with allocHelper setting
func withAllocHelper(helper allocHelper) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.helper = helper })
}

// get default allocHelper, which does nothing
func defaultAllocHelper() allocHelper {
	return allocHelper{
		afterCreateSegment: func(segment *datapb.SegmentInfo) error { return nil },
	}
}

// get allocOption with estimatePolicy
func withCalUpperLimitPolicy(policy calUpperLimitPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.estimatePolicy = policy })
}

// get allocOption with allocPolicy
func withAllocPolicy(policy AllocatePolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.allocPolicy = policy })
}

// get allocOption with segmentSealPolicies
func withSegmentSealPolices(policies ...segmentSealPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) {
		// do override instead of append, to override default options
		manager.segmentSealPolicies = policies
	})
}

// get allocOption with channelSealPolicies
func withChannelSealPolices(policies ...channelSealPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) {
		// do override instead of append, to override default options
		manager.channelSealPolicies = policies
	})
}

// get allocOption with flushPolicy
func withFlushPolicy(policy flushPolicy) allocOption {
	return allocFunc(func(manager *SegmentManager) { manager.flushPolicy = policy })
}

func defaultCalUpperLimitPolicy() calUpperLimitPolicy {
	return calBySchemaPolicy
}

func defaultAllocatePolicy() AllocatePolicy {
	return AllocatePolicyL1
}

func defaultSegmentSealPolicy() []segmentSealPolicy {
	return []segmentSealPolicy{
		sealL1SegmentByBinlogFileNumber(Params.DataCoordCfg.SegmentMaxBinlogFileNumber.GetAsInt()),
		sealL1SegmentByLifetime(Params.DataCoordCfg.SegmentMaxLifetime.GetAsDuration(time.Second)),
		sealL1SegmentByCapacity(Params.DataCoordCfg.SegmentSealProportion.GetAsFloat()),
		sealL1SegmentByIdleTime(Params.DataCoordCfg.SegmentMaxIdleTime.GetAsDuration(time.Second), Params.DataCoordCfg.SegmentMinSizeFromIdleToSealed.GetAsFloat(), Params.DataCoordCfg.SegmentMaxSize.GetAsFloat()),
	}
}

func defaultFlushPolicy() flushPolicy {
	return flushPolicyL1
}

// newSegmentManager should be the only way to retrieve SegmentManager.
func newSegmentManager(meta *meta, allocator allocator, opts ...allocOption) (*SegmentManager, error) {
	manager := &SegmentManager{
		meta:                meta,
		allocator:           allocator,
		helper:              defaultAllocHelper(),
		segments:            make([]UniqueID, 0),
		estimatePolicy:      defaultCalUpperLimitPolicy(),
		allocPolicy:         defaultAllocatePolicy(),
		segmentSealPolicies: defaultSegmentSealPolicy(), // default only segment size policy
		channelSealPolicies: []channelSealPolicy{},      // no default channel seal policy
		flushPolicy:         defaultFlushPolicy(),
	}
	for _, opt := range opts {
		opt.apply(manager)
	}
	manager.loadSegmentsFromMeta()
	if err := manager.maybeResetLastExpireForSegments(); err != nil {
		return nil, err
	}
	return manager, nil
}

// loadSegmentsFromMeta generate corresponding segment status for each segment from meta
func (s *SegmentManager) loadSegmentsFromMeta() {
	segments := s.meta.GetUnFlushedSegments()
	segmentsID := make([]UniqueID, 0, len(segments))
	for _, segment := range segments {
		if segment.Level != datapb.SegmentLevel_L0 {
			segmentsID = append(segmentsID, segment.GetID())
		}
	}
	s.segments = segmentsID
}

func (s *SegmentManager) maybeResetLastExpireForSegments() error {
	// for all sealed and growing segments, need to reset last expire
	if len(s.segments) > 0 {
		var latestTs uint64
		allocateErr := retry.Do(context.Background(), func() error {
			ts, tryErr := s.genExpireTs(context.Background())
			log.Warn("failed to get ts from rootCoord for globalLastExpire", zap.Error(tryErr))
			if tryErr != nil {
				return tryErr
			}
			latestTs = ts
			return nil
		}, retry.Attempts(Params.DataCoordCfg.AllocLatestExpireAttempt.GetAsUint()), retry.Sleep(200*time.Millisecond))
		if allocateErr != nil {
			log.Warn("cannot allocate latest lastExpire from rootCoord", zap.Error(allocateErr))
			return errors.New("global max expire ts is unavailable for segment manager")
		}
		for _, sID := range s.segments {
			if segment := s.meta.GetSegment(sID); segment != nil && segment.GetState() == commonpb.SegmentState_Growing {
				s.meta.SetLastExpire(sID, latestTs)
			}
		}
	}
	return nil
}

// AllocSegment allocate segment per request collcation, partication, channel and rows
func (s *SegmentManager) AllocSegment(ctx context.Context, collectionID UniqueID,
	partitionID UniqueID, channelName string, requestRows int64,
) ([]*Allocation, error) {
	log := log.Ctx(ctx).
		With(zap.Int64("collectionID", collectionID)).
		With(zap.Int64("partitionID", partitionID)).
		With(zap.String("channelName", channelName)).
		With(zap.Int64("requestRows", requestRows))
	_, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "Alloc-Segment")
	defer sp.End()
	s.mu.Lock()
	defer s.mu.Unlock()

	// filter segments
	segments := make([]*SegmentInfo, 0)
	for _, segmentID := range s.segments {
		segment := s.meta.GetHealthySegment(segmentID)
		if segment == nil {
			log.Warn("Failed to get segment info from meta", zap.Int64("id", segmentID))
			continue
		}
		if !satisfy(segment, collectionID, partitionID, channelName) || !isGrowing(segment) || segment.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}
		segments = append(segments, segment)
	}

	// Apply allocation policy.
	maxCountPerSegment, err := s.estimateMaxNumOfRows(collectionID)
	if err != nil {
		return nil, err
	}
	newSegmentAllocations, existedSegmentAllocations := s.allocPolicy(segments,
		requestRows, int64(maxCountPerSegment), datapb.SegmentLevel_L1)

	// create new segments and add allocations
	expireTs, err := s.genExpireTs(ctx)
	if err != nil {
		return nil, err
	}
	for _, allocation := range newSegmentAllocations {
		segment, err := s.openNewSegment(ctx, collectionID, partitionID, channelName, commonpb.SegmentState_Growing, datapb.SegmentLevel_L1)
		if err != nil {
			log.Error("Failed to open new segment for segment allocation")
			return nil, err
		}
		allocation.ExpireTime = expireTs
		allocation.SegmentID = segment.GetID()
		if err := s.meta.AddAllocation(segment.GetID(), allocation); err != nil {
			return nil, err
		}
	}

	for _, allocation := range existedSegmentAllocations {
		allocation.ExpireTime = expireTs
		if err := s.meta.AddAllocation(allocation.SegmentID, allocation); err != nil {
			log.Error("Failed to add allocation to existed segment", zap.Int64("segmentID", allocation.SegmentID))
			return nil, err
		}
	}

	allocations := append(newSegmentAllocations, existedSegmentAllocations...)
	return allocations, nil
}

func satisfy(segment *SegmentInfo, collectionID, partitionID UniqueID, channel string) bool {
	return segment.GetCollectionID() == collectionID && segment.GetPartitionID() == partitionID &&
		segment.GetInsertChannel() == channel
}

func isGrowing(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Growing
}

func (s *SegmentManager) genExpireTs(ctx context.Context) (Timestamp, error) {
	ts, err := s.allocator.allocTimestamp(ctx)
	if err != nil {
		return 0, err
	}
	physicalTs, logicalTs := tsoutil.ParseTS(ts)
	expirePhysicalTs := physicalTs.Add(time.Duration(Params.DataCoordCfg.SegAssignmentExpiration.GetAsFloat()) * time.Millisecond)
	expireTs := tsoutil.ComposeTS(expirePhysicalTs.UnixNano()/int64(time.Millisecond), int64(logicalTs))
	return expireTs, nil
}

func (s *SegmentManager) AllocImportSegment(ctx context.Context, taskID int64, collectionID UniqueID,
	partitionID UniqueID, channelName string, level datapb.SegmentLevel,
) (*SegmentInfo, error) {
	log := log.Ctx(ctx)
	ctx, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "open-Segment")
	defer sp.End()
	id, err := s.allocator.allocID(ctx)
	if err != nil {
		log.Error("failed to open new segment while allocID", zap.Error(err))
		return nil, err
	}
	ts, err := s.allocator.allocTimestamp(ctx)
	if err != nil {
		return nil, err
	}
	position := &msgpb.MsgPosition{
		ChannelName: channelName,
		MsgID:       nil,
		Timestamp:   ts,
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:             id,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		InsertChannel:  channelName,
		NumOfRows:      0,
		State:          commonpb.SegmentState_Importing,
		MaxRowNum:      0,
		Level:          level,
		LastExpireTime: math.MaxUint64,
		StartPosition:  position,
		DmlPosition:    position,
	}
	segmentInfo.IsImporting = true
	segment := NewSegmentInfo(segmentInfo)
	if err := s.meta.AddSegment(ctx, segment); err != nil {
		log.Error("failed to add import segment", zap.Error(err))
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segments = append(s.segments, id)
	log.Info("add import segment done",
		zap.Int64("taskID", taskID),
		zap.Int64("collectionID", segmentInfo.CollectionID),
		zap.Int64("segmentID", segmentInfo.ID),
		zap.String("channel", segmentInfo.InsertChannel),
		zap.String("level", level.String()))

	return segment, nil
}

func (s *SegmentManager) openNewSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID,
	channelName string, segmentState commonpb.SegmentState, level datapb.SegmentLevel,
) (*SegmentInfo, error) {
	log := log.Ctx(ctx)
	ctx, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "open-Segment")
	defer sp.End()
	id, err := s.allocator.allocID(ctx)
	if err != nil {
		log.Error("failed to open new segment while allocID", zap.Error(err))
		return nil, err
	}
	maxNumOfRows, err := s.estimateMaxNumOfRows(collectionID)
	if err != nil {
		log.Error("failed to open new segment while estimateMaxNumOfRows", zap.Error(err))
		return nil, err
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:             id,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		InsertChannel:  channelName,
		NumOfRows:      0,
		State:          segmentState,
		MaxRowNum:      int64(maxNumOfRows),
		Level:          level,
		LastExpireTime: 0,
	}
	segment := NewSegmentInfo(segmentInfo)
	if err := s.meta.AddSegment(ctx, segment); err != nil {
		log.Error("failed to add segment to DataCoord", zap.Error(err))
		return nil, err
	}
	s.segments = append(s.segments, id)
	log.Info("datacoord: estimateTotalRows: ",
		zap.Int64("CollectionID", segmentInfo.CollectionID),
		zap.Int64("SegmentID", segmentInfo.ID),
		zap.Int("Rows", maxNumOfRows),
		zap.String("Channel", segmentInfo.InsertChannel))

	return segment, s.helper.afterCreateSegment(segmentInfo)
}

func (s *SegmentManager) estimateMaxNumOfRows(collectionID UniqueID) (int, error) {
	// it's ok to use meta.GetCollection here, since collection meta is set before using segmentManager
	collMeta := s.meta.GetCollection(collectionID)
	if collMeta == nil {
		return -1, fmt.Errorf("failed to get collection %d", collectionID)
	}
	return s.estimatePolicy(collMeta.Schema)
}

// DropSegment drop the segment from manager.
func (s *SegmentManager) DropSegment(ctx context.Context, segmentID UniqueID) {
	_, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "Drop-Segment")
	defer sp.End()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, id := range s.segments {
		if id == segmentID {
			s.segments = append(s.segments[:i], s.segments[i+1:]...)
			break
		}
	}
	segment := s.meta.GetHealthySegment(segmentID)
	if segment == nil {
		log.Warn("Failed to get segment", zap.Int64("id", segmentID))
		return
	}
	s.meta.SetAllocations(segmentID, []*Allocation{})
	for _, allocation := range segment.allocations {
		putAllocation(allocation)
	}
}

// FlushImportSegments set importing segment state to Flushed.
func (s *SegmentManager) FlushImportSegments(ctx context.Context, collectionID UniqueID, segmentIDs []UniqueID) error {
	_, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "Flush-Import-Segments")
	defer sp.End()

	s.mu.Lock()
	defer s.mu.Unlock()

	candidates := lo.Filter(segmentIDs, func(segmentID UniqueID, _ int) bool {
		info := s.meta.GetHealthySegment(segmentID)
		if info == nil {
			log.Warn("failed to get seg info from meta", zap.Int64("segmentID", segmentID))
			return false
		}
		if info.CollectionID != collectionID {
			return false
		}
		return info.State == commonpb.SegmentState_Importing
	})

	// We set the importing segment state directly to 'Flushed' rather than
	// 'Sealed' because all data has been imported, and there is no data
	// in the datanode flowgraph that needs to be synced.
	for _, id := range candidates {
		if err := s.meta.SetState(id, commonpb.SegmentState_Flushed); err != nil {
			return err
		}
	}
	return nil
}

// SealAllSegments seals all segments of collection with collectionID and return sealed segments
func (s *SegmentManager) SealAllSegments(ctx context.Context, collectionID UniqueID, segIDs []UniqueID) ([]UniqueID, error) {
	_, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "Seal-Segments")
	defer sp.End()

	s.mu.Lock()
	defer s.mu.Unlock()
	var ret []UniqueID
	segCandidates := s.segments
	if len(segIDs) != 0 {
		segCandidates = segIDs
	}

	sealedSegments := s.meta.GetSegments(segCandidates, func(segment *SegmentInfo) bool {
		return segment.CollectionID == collectionID && isSegmentHealthy(segment) && segment.State == commonpb.SegmentState_Sealed
	})
	growingSegments := s.meta.GetSegments(segCandidates, func(segment *SegmentInfo) bool {
		return segment.CollectionID == collectionID && isSegmentHealthy(segment) && segment.State == commonpb.SegmentState_Growing
	})
	ret = append(ret, sealedSegments...)

	for _, id := range growingSegments {
		if err := s.meta.SetState(id, commonpb.SegmentState_Sealed); err != nil {
			return nil, err
		}
		ret = append(ret, id)
	}
	return ret, nil
}

// GetFlushableSegments get segment ids with Sealed State and flushable (meets flushPolicy)
func (s *SegmentManager) GetFlushableSegments(ctx context.Context, channel string, t Timestamp) ([]UniqueID, error) {
	_, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "Get-Segments")
	defer sp.End()
	s.mu.Lock()
	defer s.mu.Unlock()
	// TODO:move tryToSealSegment and dropEmptySealedSegment outside
	if err := s.tryToSealSegment(t, channel); err != nil {
		return nil, err
	}

	s.cleanupSealedSegment(t, channel)

	ret := make([]UniqueID, 0, len(s.segments))
	for _, id := range s.segments {
		info := s.meta.GetHealthySegment(id)
		if info == nil || info.InsertChannel != channel {
			continue
		}
		if s.flushPolicy(info, t) {
			ret = append(ret, id)
		}
	}

	return ret, nil
}

// ExpireAllocations notify segment status to expire old allocations
func (s *SegmentManager) ExpireAllocations(channel string, ts Timestamp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range s.segments {
		segment := s.meta.GetHealthySegment(id)
		if segment == nil || segment.InsertChannel != channel {
			continue
		}
		allocations := make([]*Allocation, 0, len(segment.allocations))
		for i := 0; i < len(segment.allocations); i++ {
			if segment.allocations[i].ExpireTime <= ts {
				a := segment.allocations[i]
				putAllocation(a)
			} else {
				allocations = append(allocations, segment.allocations[i])
			}
		}
		s.meta.SetAllocations(segment.GetID(), allocations)
	}
	return nil
}

func (s *SegmentManager) cleanupSealedSegment(ts Timestamp, channel string) {
	valids := make([]int64, 0, len(s.segments))
	for _, id := range s.segments {
		segment := s.meta.GetHealthySegment(id)
		if segment == nil || segment.InsertChannel != channel {
			valids = append(valids, id)
			continue
		}

		if isEmptySealedSegment(segment, ts) {
			log.Info("remove empty sealed segment", zap.Int64("collection", segment.CollectionID), zap.Int64("segment", id))
			s.meta.SetState(id, commonpb.SegmentState_Dropped)
			continue
		}

		// clean up importing segment since the task failed.
		if segment.GetState() == commonpb.SegmentState_Importing && segment.GetLastExpireTime() < ts {
			log.Info("cleanup staled importing segment", zap.Int64("collection", segment.CollectionID), zap.Int64("segment", id))
			s.meta.SetState(id, commonpb.SegmentState_Dropped)
			continue
		}

		valids = append(valids, id)
	}
	s.segments = valids
}

func isEmptySealedSegment(segment *SegmentInfo, ts Timestamp) bool {
	return segment.GetState() == commonpb.SegmentState_Sealed && segment.GetLastExpireTime() <= ts && segment.currRows == 0
}

// tryToSealSegment applies segment & channel seal policies
func (s *SegmentManager) tryToSealSegment(ts Timestamp, channel string) error {
	channelInfo := make(map[string][]*SegmentInfo)
	for _, id := range s.segments {
		info := s.meta.GetHealthySegment(id)
		if info == nil || info.InsertChannel != channel {
			continue
		}
		channelInfo[info.InsertChannel] = append(channelInfo[info.InsertChannel], info)
		if info.State != commonpb.SegmentState_Growing {
			continue
		}
		// change shouldSeal to segment seal policy logic
		for _, policy := range s.segmentSealPolicies {
			if policy(info, ts) {
				if err := s.meta.SetState(id, commonpb.SegmentState_Sealed); err != nil {
					return err
				}
				break
			}
		}
	}
	for channel, segmentInfos := range channelInfo {
		for _, policy := range s.channelSealPolicies {
			vs := policy(channel, segmentInfos, ts)
			for _, info := range vs {
				if info.State != commonpb.SegmentState_Growing {
					continue
				}
				if err := s.meta.SetState(info.GetID(), commonpb.SegmentState_Sealed); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// DropSegmentsOfChannel drops all segments in a channel
func (s *SegmentManager) DropSegmentsOfChannel(ctx context.Context, channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	validSegments := make([]int64, 0, len(s.segments))
	for _, sid := range s.segments {
		segment := s.meta.GetHealthySegment(sid)
		if segment == nil {
			continue
		}
		if segment.GetInsertChannel() != channel {
			validSegments = append(validSegments, sid)
			continue
		}
		s.meta.SetAllocations(sid, nil)
		for _, allocation := range segment.allocations {
			putAllocation(allocation)
		}
	}

	s.segments = validSegments
}
