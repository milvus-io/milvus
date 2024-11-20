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
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
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
	// DropSegment drops the segment from manager.
	DropSegment(ctx context.Context, channel string, segmentID UniqueID)
	// SealAllSegments seals all segments of collection with collectionID and return sealed segments.
	// If segIDs is not empty, also seals segments in segIDs.
	SealAllSegments(ctx context.Context, channel string, segIDs []UniqueID) ([]UniqueID, error)
	// GetFlushableSegments returns flushable segment ids
	GetFlushableSegments(ctx context.Context, channel string, ts Timestamp) ([]UniqueID, error)
	// ExpireAllocations notifies segment status to expire old allocations
	ExpireAllocations(channel string, ts Timestamp)
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
	meta      *meta
	allocator allocator
	helper    allocHelper

	channelLock      *lock.KeyLock[string]
	channel2Segments *typeutil.ConcurrentMap[string, typeutil.UniqueSet]

	// Policies
	estimatePolicy      calUpperLimitPolicy
	allocPolicy         AllocatePolicy
	segmentSealPolicies []SegmentSealPolicy
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
func withSegmentSealPolices(policies ...SegmentSealPolicy) allocOption {
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

func defaultSegmentSealPolicy() []SegmentSealPolicy {
	return []SegmentSealPolicy{
		sealL1SegmentByBinlogFileNumber(Params.DataCoordCfg.SegmentMaxBinlogFileNumber.GetAsInt()),
		sealL1SegmentByLifetime(Params.DataCoordCfg.SegmentMaxLifetime.GetAsDuration(time.Second)),
		sealL1SegmentByCapacity(Params.DataCoordCfg.SegmentSealProportion.GetAsFloat()),
		sealL1SegmentByIdleTime(Params.DataCoordCfg.SegmentMaxIdleTime.GetAsDuration(time.Second), Params.DataCoordCfg.SegmentMinSizeFromIdleToSealed.GetAsFloat(), Params.DataCoordCfg.SegmentMaxSize.GetAsFloat()),
	}
}

func defaultChannelSealPolicy() []channelSealPolicy {
	return []channelSealPolicy{
		sealByTotalGrowingSegmentsSize(),
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
		channelLock:         lock.NewKeyLock[string](),
		channel2Segments:    typeutil.NewConcurrentMap[string, typeutil.UniqueSet](),
		estimatePolicy:      defaultCalUpperLimitPolicy(),
		allocPolicy:         defaultAllocatePolicy(),
		segmentSealPolicies: defaultSegmentSealPolicy(),
		channelSealPolicies: defaultChannelSealPolicy(),
		flushPolicy:         defaultFlushPolicy(),
	}
	for _, opt := range opts {
		opt.apply(manager)
	}
	latestTs, err := manager.genLastExpireTsForSegments()
	if err != nil {
		return nil, err
	}
	manager.loadSegmentsFromMeta(latestTs)
	return manager, nil
}

// loadSegmentsFromMeta generate corresponding segment status for each segment from meta
func (s *SegmentManager) loadSegmentsFromMeta(latestTs Timestamp) {
	unflushed := s.meta.GetUnFlushedSegments()
	unflushed = lo.Filter(unflushed, func(segment *SegmentInfo, _ int) bool {
		return segment.Level != datapb.SegmentLevel_L0
	})
	channel2Segments := lo.GroupBy(unflushed, func(segment *SegmentInfo) string {
		return segment.GetInsertChannel()
	})
	for channel, segmentInfos := range channel2Segments {
		segments := typeutil.NewUniqueSet()
		for _, segment := range segmentInfos {
			// for all sealed and growing segments, need to reset last expire
			if segment != nil && segment.GetState() == commonpb.SegmentState_Growing {
				s.meta.SetLastExpire(segment.GetID(), latestTs)
			}
			segments.Insert(segment.GetID())
		}
		s.channel2Segments.Insert(channel, segments)
	}
}

func (s *SegmentManager) genLastExpireTsForSegments() (Timestamp, error) {
	var latestTs uint64
	allocateErr := retry.Do(context.Background(), func() error {
		ts, tryErr := s.genExpireTs(context.Background())
		if tryErr != nil {
			log.Warn("failed to get ts from rootCoord for globalLastExpire", zap.Error(tryErr))
			return tryErr
		}
		latestTs = ts
		return nil
	}, retry.Attempts(Params.DataCoordCfg.AllocLatestExpireAttempt.GetAsUint()), retry.Sleep(200*time.Millisecond))
	if allocateErr != nil {
		log.Warn("cannot allocate latest lastExpire from rootCoord", zap.Error(allocateErr))
		return 0, errors.New("global max expire ts is unavailable for segment manager")
	}
	return latestTs, nil
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

	s.channelLock.Lock(channelName)
	defer s.channelLock.Unlock(channelName)

	// filter segments
	segmentInfos := make([]*SegmentInfo, 0)
	segments, _ := s.channel2Segments.GetOrInsert(channelName, typeutil.NewUniqueSet())
	segments.Range(func(segmentID int64) bool {
		segment := s.meta.GetHealthySegment(segmentID)
		if segment == nil {
			log.Warn("failed to get segment, remove it", zap.String("channel", channelName), zap.Int64("segmentID", segmentID))
			segments.Remove(segmentID)
			return true
		}

		if !isGrowing(segment) || segment.GetLevel() == datapb.SegmentLevel_L0 {
			return true
		}
		segmentInfos = append(segmentInfos, segment)
		return true
	})

	// Apply allocation policy.
	maxCountPerSegment, err := s.estimateMaxNumOfRows(collectionID)
	if err != nil {
		return nil, err
	}
	newSegmentAllocations, existedSegmentAllocations := s.allocPolicy(segmentInfos,
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
	segments, _ := s.channel2Segments.GetOrInsert(channelName, typeutil.NewUniqueSet())
	segments.Insert(id)
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
func (s *SegmentManager) DropSegment(ctx context.Context, channel string, segmentID UniqueID) {
	_, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "Drop-Segment")
	defer sp.End()

	s.channelLock.Lock(channel)
	defer s.channelLock.Unlock(channel)

	if segments, ok := s.channel2Segments.Get(channel); ok {
		segments.Remove(segmentID)
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

// SealAllSegments seals all segments of collection with collectionID and return sealed segments
func (s *SegmentManager) SealAllSegments(ctx context.Context, channel string, segIDs []UniqueID) ([]UniqueID, error) {
	_, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "Seal-Segments")
	defer sp.End()

	s.channelLock.Lock(channel)
	defer s.channelLock.Unlock(channel)

	segments, ok := s.channel2Segments.Get(channel)
	if !ok {
		return nil, nil
	}
	segCandidates := segments.Collect()
	if len(segIDs) != 0 {
		segCandidates = segIDs
	}

	var ret []UniqueID

	sealedSegments := s.meta.GetSegments(segCandidates, func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) && segment.State == commonpb.SegmentState_Sealed
	})
	growingSegments := s.meta.GetSegments(segCandidates, func(segment *SegmentInfo) bool {
		return isSegmentHealthy(segment) && segment.State == commonpb.SegmentState_Growing
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

	s.channelLock.Lock(channel)
	defer s.channelLock.Unlock(channel)

	// TODO:move tryToSealSegment and dropEmptySealedSegment outside
	if err := s.tryToSealSegment(t, channel); err != nil {
		return nil, err
	}

	// TODO: It's too frequent; perhaps each channel could check once per minute instead.
	s.cleanupSealedSegment(t, channel)

	segments, ok := s.channel2Segments.Get(channel)
	if !ok {
		return nil, nil
	}

	ret := make([]UniqueID, 0, segments.Len())
	segments.Range(func(segmentID int64) bool {
		info := s.meta.GetHealthySegment(segmentID)
		if info == nil {
			return true
		}
		if s.flushPolicy(info, t) {
			ret = append(ret, segmentID)
		}
		return true
	})

	return ret, nil
}

// ExpireAllocations notify segment status to expire old allocations
func (s *SegmentManager) ExpireAllocations(channel string, ts Timestamp) {
	s.channelLock.Lock(channel)
	defer s.channelLock.Unlock(channel)

	segments, ok := s.channel2Segments.Get(channel)
	if !ok {
		return
	}

	segments.Range(func(id int64) bool {
		segment := s.meta.GetHealthySegment(id)
		if segment == nil {
			log.Warn("failed to get segment, remove it", zap.String("channel", channel), zap.Int64("segmentID", id))
			segments.Remove(id)
			return true
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
		return true
	})
}

func (s *SegmentManager) cleanupSealedSegment(ts Timestamp, channel string) {
	segments, ok := s.channel2Segments.Get(channel)
	if !ok {
		return
	}
	segments.Range(func(id int64) bool {
		segment := s.meta.GetHealthySegment(id)
		if segment == nil {
			log.Warn("failed to get segment, remove it", zap.String("channel", channel), zap.Int64("segmentID", id))
			segments.Remove(id)
			return true
		}
		if isEmptySealedSegment(segment, ts) {
			log.Info("remove empty sealed segment", zap.Int64("collection", segment.CollectionID), zap.Int64("segment", id))
			if err := s.meta.SetState(id, commonpb.SegmentState_Dropped); err != nil {
				log.Warn("failed to set segment state to dropped", zap.String("channel", channel),
					zap.Int64("segmentID", id), zap.Error(err))
			} else {
				segments.Remove(id)
			}
		}
		return true
	})
}

func isEmptySealedSegment(segment *SegmentInfo, ts Timestamp) bool {
	return segment.GetState() == commonpb.SegmentState_Sealed && segment.GetLastExpireTime() <= ts && segment.currRows == 0
}

// tryToSealSegment applies segment & channel seal policies
func (s *SegmentManager) tryToSealSegment(ts Timestamp, channel string) error {
	segments, ok := s.channel2Segments.Get(channel)
	if !ok {
		return nil
	}

	channelInfo := make(map[string][]*SegmentInfo)
	sealedSegments := make(map[int64]struct{})

	var setStateErr error
	segments.Range(func(id int64) bool {
		info := s.meta.GetHealthySegment(id)
		if info == nil {
			return true
		}
		channelInfo[info.InsertChannel] = append(channelInfo[info.InsertChannel], info)
		if info.State != commonpb.SegmentState_Growing {
			return true
		}
		// change shouldSeal to segment seal policy logic
		for _, policy := range s.segmentSealPolicies {
			if shouldSeal, reason := policy.ShouldSeal(info, ts); shouldSeal {
				log.Info("Seal Segment for policy matched", zap.Int64("segmentID", info.GetID()), zap.String("reason", reason))
				if err := s.meta.SetState(id, commonpb.SegmentState_Sealed); err != nil {
					setStateErr = err
					return false
				}
				sealedSegments[id] = struct{}{}
				break
			}
		}
		return true
	})

	if setStateErr != nil {
		return setStateErr
	}

	for channel, segmentInfos := range channelInfo {
		for _, policy := range s.channelSealPolicies {
			vs, reason := policy(channel, segmentInfos, ts)
			for _, info := range vs {
				if _, ok := sealedSegments[info.GetID()]; ok {
					continue
				}
				if info.State != commonpb.SegmentState_Growing {
					continue
				}
				if err := s.meta.SetState(info.GetID(), commonpb.SegmentState_Sealed); err != nil {
					return err
				}
				log.Info("seal segment for channel seal policy matched",
					zap.Int64("segmentID", info.GetID()), zap.String("channel", channel), zap.String("reason", reason))
				sealedSegments[info.GetID()] = struct{}{}
			}
		}
	}
	return nil
}

// DropSegmentsOfChannel drops all segments in a channel
func (s *SegmentManager) DropSegmentsOfChannel(ctx context.Context, channel string) {
	s.channelLock.Lock(channel)
	defer s.channelLock.Unlock(channel)

	segments, ok := s.channel2Segments.Get(channel)
	if !ok {
		return
	}
	segments.Range(func(sid int64) bool {
		segment := s.meta.GetHealthySegment(sid)
		if segment == nil {
			log.Warn("failed to get segment, remove it", zap.String("channel", channel), zap.Int64("segmentID", sid))
			segments.Remove(sid)
			return true
		}
		s.meta.SetAllocations(sid, nil)
		for _, allocation := range segment.allocations {
			putAllocation(allocation)
		}
		return true
	})
	s.channel2Segments.Remove(channel)
}
