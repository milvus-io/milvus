// Copyright (C) 2019-2020 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datacoord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/rootcoord"

	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

var (
	// allocPool pool of Allocation, to reduce allocation of Allocation
	allocPool = sync.Pool{
		New: func() interface{} {
			return &Allocation{}
		},
	}
)

// getAllocation unified way to retrieve allocation struct
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

// putAllocation put allocation for recycling
func putAllocation(a *Allocation) {
	allocPool.Put(a)
}

// segmentMaxLifetime default segment max lifetime value
// TODO needs to be configurable
const segmentMaxLifetime = 24 * time.Hour

// Manager manage segment related operations.
type Manager interface {
	// AllocSegment allocates rows and record the allocation.
	AllocSegment(ctx context.Context, collectionID, partitionID UniqueID, channelName string, requestRows int64) ([]*Allocation, error)
	// DropSegment drops the segment from manager.
	DropSegment(ctx context.Context, segmentID UniqueID)
	// SealAllSegments seals all segments of collection with collectionID and return sealed segments
	SealAllSegments(ctx context.Context, collectionID UniqueID) ([]UniqueID, error)
	// GetFlushableSegments returns flushable segment ids
	GetFlushableSegments(ctx context.Context, channel string, ts Timestamp) ([]UniqueID, error)
	// ExpireAllocations notifies segment status to expire old allocations
	ExpireAllocations(channel string, ts Timestamp) error
}

// Allocation records the allocation info
type Allocation struct {
	SegmentID  UniqueID
	NumOfRows  int64
	ExpireTime Timestamp
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
}

type allocHelper struct {
	afterCreateSegment func(segment *datapb.SegmentInfo) error
}

// allocOption allction option applies to `SegmentManager`
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

func defaultAlocatePolicy() AllocatePolicy {
	return AllocatePolicyV1
}

func defaultSegmentSealPolicy() []segmentSealPolicy {
	return []segmentSealPolicy{
		sealByLifetimePolicy(segmentMaxLifetime),
		getSegmentCapacityPolicy(Params.SegmentSealProportion),
	}
}

func defaultFlushPolicy() flushPolicy {
	return flushPolicyV1
}

// newSegmentManager should be the only way to retrieve SegmentManager
func newSegmentManager(meta *meta, allocator allocator, opts ...allocOption) *SegmentManager {
	manager := &SegmentManager{
		meta:                meta,
		allocator:           allocator,
		helper:              defaultAllocHelper(),
		segments:            make([]UniqueID, 0),
		estimatePolicy:      defaultCalUpperLimitPolicy(),
		allocPolicy:         defaultAlocatePolicy(),
		segmentSealPolicies: defaultSegmentSealPolicy(), // default only segment size policy
		channelSealPolicies: []channelSealPolicy{},      // no default channel seal policy
		flushPolicy:         defaultFlushPolicy(),
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
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()

	// filter segments
	segments := make([]*SegmentInfo, 0)
	for _, segmentID := range s.segments {
		segment := s.meta.GetSegment(segmentID)
		if segment == nil {
			log.Warn("Failed to get seginfo from meta", zap.Int64("id", segmentID))
			continue
		}
		if segment.State == commonpb.SegmentState_Sealed || segment.CollectionID != collectionID ||
			segment.PartitionID != partitionID || segment.InsertChannel != channelName {
			continue
		}
		segments = append(segments, segment)
	}

	// apply allocate policy
	maxCountPerSegment, err := s.estimateMaxNumOfRows(collectionID)
	if err != nil {
		return nil, err
	}
	newSegmentAllocations, existedSegmentAllocations := s.allocPolicy(segments,
		requestRows, int64(maxCountPerSegment))

	// create new segments and add allocations
	expireTs, err := s.genExpireTs(ctx)
	if err != nil {
		return nil, err
	}
	for _, allocation := range newSegmentAllocations {
		segment, err := s.openNewSegment(ctx, collectionID, partitionID, channelName)
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

func (s *SegmentManager) genExpireTs(ctx context.Context) (Timestamp, error) {
	ts, err := s.allocator.allocTimestamp(ctx)
	if err != nil {
		return 0, err
	}
	physicalTs, logicalTs := tsoutil.ParseTS(ts)
	expirePhysicalTs := physicalTs.Add(time.Duration(Params.SegAssignmentExpiration) * time.Millisecond)
	expireTs := tsoutil.ComposeTS(expirePhysicalTs.UnixNano()/int64(time.Millisecond), int64(logicalTs))
	return expireTs, nil
}

func (s *SegmentManager) openNewSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID, channelName string) (*SegmentInfo, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	id, err := s.allocator.allocID(ctx)
	if err != nil {
		return nil, err
	}
	maxNumOfRows, err := s.estimateMaxNumOfRows(collectionID)
	if err != nil {
		return nil, err
	}

	startPosition := []byte{} // default start position
	coll := s.meta.GetCollection(collectionID)
	for _, pair := range coll.GetStartPositions() {
		if pair.Key == rootcoord.ToPhysicalChannel(channelName) { // pchan or vchan
			startPosition = pair.Data
			break
		}
	}

	segmentInfo := &datapb.SegmentInfo{
		ID:             id,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		InsertChannel:  channelName,
		NumOfRows:      0,
		State:          commonpb.SegmentState_Growing,
		MaxRowNum:      int64(maxNumOfRows),
		LastExpireTime: 0,
		StartPosition: &internalpb.MsgPosition{
			ChannelName: channelName,
			MsgID:       startPosition,
			MsgGroup:    "",
			Timestamp:   0,
		},
	}
	segment := NewSegmentInfo(segmentInfo)
	if err := s.meta.AddSegment(segment); err != nil {
		return nil, err
	}
	s.segments = append(s.segments, id)
	log.Debug("datacoord: estimateTotalRows: ",
		zap.Int64("CollectionID", segmentInfo.CollectionID),
		zap.Int64("SegmentID", segmentInfo.ID),
		zap.Int("Rows", maxNumOfRows),
		zap.String("Channel", segmentInfo.InsertChannel))

	return segment, s.helper.afterCreateSegment(segmentInfo)
}

func (s *SegmentManager) estimateMaxNumOfRows(collectionID UniqueID) (int, error) {
	collMeta := s.meta.GetCollection(collectionID)
	if collMeta == nil {
		return -1, fmt.Errorf("failed to get collection %d", collectionID)
	}
	return s.estimatePolicy(collMeta.Schema)
}

// DropSegment drop the segment from manager.
func (s *SegmentManager) DropSegment(ctx context.Context, segmentID UniqueID) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, id := range s.segments {
		if id == segmentID {
			s.segments = append(s.segments[:i], s.segments[i+1:]...)
			break
		}
	}
	segment := s.meta.GetSegment(segmentID)
	if segment == nil {
		log.Warn("failed to get segment", zap.Int64("id", segmentID))
		return
	}
	s.meta.SetAllocations(segmentID, []*Allocation{})
	for _, allocation := range segment.allocations {
		putAllocation(allocation)
	}
}

// SealAllSegments seals all segmetns of collection with collectionID and return sealed segments
func (s *SegmentManager) SealAllSegments(ctx context.Context, collectionID UniqueID) ([]UniqueID, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := make([]UniqueID, 0)
	for _, id := range s.segments {
		info := s.meta.GetSegment(id)
		if info == nil {
			log.Warn("Failed to get seg info from meta", zap.Int64("id", id))
			continue
		}
		if info.CollectionID != collectionID {
			continue
		}
		if info.State == commonpb.SegmentState_Sealed {
			ret = append(ret, id)
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
func (s *SegmentManager) GetFlushableSegments(ctx context.Context, channel string,
	t Timestamp) ([]UniqueID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	if err := s.tryToSealSegment(t, channel); err != nil {
		return nil, err
	}

	ret := make([]UniqueID, 0, len(s.segments))
	for _, id := range s.segments {
		info := s.meta.GetSegment(id)
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
		segment := s.meta.GetSegment(id)
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

// tryToSealSegment applies segment & channel seal policies
func (s *SegmentManager) tryToSealSegment(ts Timestamp, channel string) error {
	channelInfo := make(map[string][]*SegmentInfo)
	for _, id := range s.segments {
		info := s.meta.GetSegment(id)
		if info == nil || info.InsertChannel != channel {
			continue
		}
		channelInfo[info.InsertChannel] = append(channelInfo[info.InsertChannel], info)
		if info.State == commonpb.SegmentState_Sealed {
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
				if info.State == commonpb.SegmentState_Sealed {
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
