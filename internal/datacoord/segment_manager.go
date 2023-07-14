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
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	// allocPool pool of Allocation, to reduce allocation of Allocation
	allocPool = sync.Pool{
		New: func() interface{} {
			return &Allocation{}
		},
	}
)

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
type Manager interface {
	// AllocSegment allocates rows and record the allocation.
	AllocSegment(ctx context.Context, collectionID, partitionID UniqueID, channelName string, requestRows int64) ([]*Allocation, error)
	// allocSegmentForImport allocates one segment allocation for bulk insert.
	// TODO: Remove this method and AllocSegment() above instead.
	allocSegmentForImport(ctx context.Context, collectionID, partitionID UniqueID, channelName string, requestRows int64, taskID int64) (*Allocation, error)
	// DropSegment drops the segment from manager.
	DropSegment(ctx context.Context, segmentID UniqueID)
	// SealAllSegments seals all segments of collection with collectionID and return sealed segments.
	// If segIDs is not empty, also seals segments in segIDs.
	SealAllSegments(ctx context.Context, collectionID UniqueID, segIDs []UniqueID, isImporting bool) ([]UniqueID, error)
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

func (alloc Allocation) String() string {
	t, _ := tsoutil.ParseTS(alloc.ExpireTime)
	return fmt.Sprintf("SegmentID: %d, NumOfRows: %d, ExpireTime: %v", alloc.SegmentID, alloc.NumOfRows, t)
}

// make sure SegmentManager implements Manager
var _ Manager = (*SegmentManager)(nil)

// SegmentManager handles segment related logic
type SegmentManager struct {
	meta                *meta
	mu                  sync.RWMutex
	allocator           allocator
	helper              allocHelper
	segments            []UniqueID
	estimatePolicy      calUpperLimitPolicy
	allocPolicy         AllocatePolicy
	segmentSealPolicies []segmentSealPolicy
	channelSealPolicies []channelSealPolicy
	flushPolicy         flushPolicy
	rcc                 types.RootCoord
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
	return AllocatePolicyV1
}

func defaultSegmentSealPolicy() []segmentSealPolicy {
	return []segmentSealPolicy{
		sealByMaxBinlogFileNumberPolicy(Params.DataCoordCfg.SegmentMaxBinlogFileNumber.GetAsInt()),
		sealByLifetimePolicy(Params.DataCoordCfg.SegmentMaxLifetime.GetAsDuration(time.Second)),
		getSegmentCapacityPolicy(Params.DataCoordCfg.SegmentSealProportion.GetAsFloat()),
		sealLongTimeIdlePolicy(Params.DataCoordCfg.SegmentMaxIdleTime.GetAsDuration(time.Second), Params.DataCoordCfg.SegmentMinSizeFromIdleToSealed.GetAsFloat(), Params.DataCoordCfg.SegmentMaxSize.GetAsFloat()),
	}
}

func defaultFlushPolicy() flushPolicy {
	return flushPolicyV1
}

// newSegmentManager should be the only way to retrieve SegmentManager.
func newSegmentManager(meta *meta, allocator allocator, rcc types.RootCoord, opts ...allocOption) *SegmentManager {
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
		rcc:                 rcc,
	}
	for _, opt := range opts {
		opt.apply(manager)
	}
	manager.loadSegmentsFromMeta()
	return manager
}

// loadSegmentsFromMeta generate corresponding segment status for each segment from meta
func (s *SegmentManager) loadSegmentsFromMeta() {
	segments := s.meta.GetUnFlushedSegments()
	segmentsID := make([]UniqueID, 0, len(segments))
	for _, segment := range segments {
		segmentsID = append(segmentsID, segment.GetID())
	}
	s.segments = segmentsID
}

// AllocSegment allocate segment per request collcation, partication, channel and rows
func (s *SegmentManager) AllocSegment(ctx context.Context, collectionID UniqueID,
	partitionID UniqueID, channelName string, requestRows int64) ([]*Allocation, error) {

	_, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "Alloc-Segment")
	defer sp.End()
	s.mu.Lock()
	defer s.mu.Unlock()

	// filter segments
	segments := make([]*SegmentInfo, 0)
	for _, segmentID := range s.segments {
		segment := s.meta.GetHealthySegment(segmentID)
		if segment == nil {
			log.Warn("Failed to get seginfo from meta", zap.Int64("id", segmentID))
			continue
		}
		if !satisfy(segment, collectionID, partitionID, channelName) || !isGrowing(segment) {
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
		requestRows, int64(maxCountPerSegment))

	// create new segments and add allocations
	expireTs, err := s.genExpireTs(ctx, false)
	if err != nil {
		return nil, err
	}
	for _, allocation := range newSegmentAllocations {
		segment, err := s.openNewSegment(ctx, collectionID, partitionID, channelName, commonpb.SegmentState_Growing)
		if err != nil {
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
			return nil, err
		}
	}

	allocations := append(newSegmentAllocations, existedSegmentAllocations...)
	return allocations, nil
}

// allocSegmentForImport allocates one segment allocation for bulk insert.
func (s *SegmentManager) allocSegmentForImport(ctx context.Context, collectionID UniqueID,
	partitionID UniqueID, channelName string, requestRows int64, importTaskID int64) (*Allocation, error) {
	_, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "Alloc-ImportSegment")
	defer sp.End()
	s.mu.Lock()
	defer s.mu.Unlock()

	// Init allocation.
	allocation := getAllocation(requestRows)
	// Create new segments and add allocations to meta.
	// To avoid mixing up with growing segments, the segment state is "Importing"
	expireTs, err := s.genExpireTs(ctx, true)
	if err != nil {
		return nil, err
	}

	segment, err := s.openNewSegment(ctx, collectionID, partitionID, channelName, commonpb.SegmentState_Importing)
	if err != nil {
		return nil, err
	}
	// ReportImport with the new segment so RootCoord can add segment ref lock onto it.
	// TODO: This is a hack and will be removed once the whole ImportManager is migrated from RootCoord to DataCoord.
	if s.rcc == nil {
		log.Error("RootCoord client not set")
		return nil, errors.New("RootCoord client not set")
	}

	allocation.ExpireTime = expireTs
	allocation.SegmentID = segment.GetID()
	if err := s.meta.AddAllocation(segment.GetID(), allocation); err != nil {
		return nil, err
	}
	return allocation, nil
}

func satisfy(segment *SegmentInfo, collectionID, partitionID UniqueID, channel string) bool {
	return segment.GetCollectionID() == collectionID && segment.GetPartitionID() == partitionID &&
		segment.GetInsertChannel() == channel
}

func isGrowing(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Growing
}

func (s *SegmentManager) genExpireTs(ctx context.Context, isImported bool) (Timestamp, error) {
	ts, err := s.allocator.allocTimestamp(ctx)
	if err != nil {
		return 0, err
	}
	physicalTs, logicalTs := tsoutil.ParseTS(ts)
	expirePhysicalTs := physicalTs.Add(time.Duration(Params.DataCoordCfg.SegAssignmentExpiration.GetAsFloat()) * time.Millisecond)
	// for imported segment, clean up ImportTaskExpiration
	if isImported {
		expirePhysicalTs = physicalTs.Add(time.Duration(Params.RootCoordCfg.ImportTaskExpiration.GetAsFloat()) * time.Second)
	}
	expireTs := tsoutil.ComposeTS(expirePhysicalTs.UnixNano()/int64(time.Millisecond), int64(logicalTs))
	return expireTs, nil
}

func (s *SegmentManager) openNewSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID,
	channelName string, segmentState commonpb.SegmentState) (*SegmentInfo, error) {
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
		LastExpireTime: 0,
	}
	if segmentState == commonpb.SegmentState_Importing {
		segmentInfo.IsImporting = true
	}
	segment := NewSegmentInfo(segmentInfo)
	if err := s.meta.AddSegment(segment); err != nil {
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

// SealAllSegments seals all segments of collection with collectionID and return sealed segments
func (s *SegmentManager) SealAllSegments(ctx context.Context, collectionID UniqueID, segIDs []UniqueID, isImport bool) ([]UniqueID, error) {
	_, sp := otel.Tracer(typeutil.DataCoordRole).Start(ctx, "Seal-Segments")
	defer sp.End()
	s.mu.Lock()
	defer s.mu.Unlock()
	var ret []UniqueID
	segCandidates := s.segments
	if len(segIDs) != 0 {
		segCandidates = segIDs
	}
	for _, id := range segCandidates {
		info := s.meta.GetHealthySegment(id)
		if info == nil {
			log.Warn("failed to get seg info from meta", zap.Int64("segmentID", id))
			continue
		}
		if info.CollectionID != collectionID {
			continue
		}
		// idempotent sealed
		if info.State == commonpb.SegmentState_Sealed {
			ret = append(ret, id)
			continue
		}
		// segment can be sealed only if it is growing or if it's importing
		if (!isImport && info.State != commonpb.SegmentState_Growing) || (isImport && info.State != commonpb.SegmentState_Importing) {
			continue
		}
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
			log.Info("remove empty sealed segment", zap.Int64("collection", segment.CollectionID), zap.Any("segment", id))
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
