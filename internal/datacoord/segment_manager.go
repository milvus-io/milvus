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
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

var errRemainInSufficient = func(requestRows int64) error {
	return fmt.Errorf("segment remaining is insufficient for %d", requestRows)
}

// Manager manage segment related operations.
type Manager interface {
	// AllocSegment allocate rows and record the allocation.
	AllocSegment(ctx context.Context, collectionID, partitionID UniqueID, channelName string, requestRows int64) (UniqueID, int64, Timestamp, error)
	// DropSegment drop the segment from allocator.
	DropSegment(ctx context.Context, segmentID UniqueID)
	// SealAllSegments sealed all segmetns of collection with collectionID and return sealed segments
	SealAllSegments(ctx context.Context, collectionID UniqueID) ([]UniqueID, error)
	// GetFlushableSegments return flushable segment ids
	GetFlushableSegments(ctx context.Context, channel string, ts Timestamp) ([]UniqueID, error)
	// UpdateSegmentStats update segment status
	UpdateSegmentStats(stat *internalpb.SegmentStatisticsUpdates)
	// ExpireAllocations notify segment status to expire old allocations
	ExpireAllocations(channel string, ts Timestamp) error
}

// segmentStatus stores allocation entries and temporary row count
type segmentStatus struct {
	id          UniqueID
	allocations []*allocation
	currentRows int64
}

// allcation entry for segment allocation record
type allocation struct {
	numOfRows  int64
	expireTime Timestamp
}

// SegmentManager handles segment related logic
type SegmentManager struct {
	meta      *meta
	mu        sync.RWMutex
	allocator allocator
	helper    allocHelper
	stats     map[UniqueID]*segmentStatus //segment id -> status

	estimatePolicy calUpperLimitPolicy
	allocPolicy    allocatePolicy
	// sealPolicy     sealPolicy
	segmentSealPolicies []segmentSealPolicy
	channelSealPolicies []channelSealPolicy
	flushPolicy         flushPolicy

	allocPool sync.Pool
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
func withAllocPolicy(policy allocatePolicy) allocOption {
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
	return newCalBySchemaPolicy()
}

func defaultAlocatePolicy() allocatePolicy {
	return newAllocatePolicyV1()
}

func defaultSealPolicy() sealPolicy {
	return newSealPolicyV1()
}

func defaultSegmentSealPolicy() segmentSealPolicy {
	return getSegmentCapacityPolicy(Params.SegmentSealProportion)
}

func defaultFlushPolicy() flushPolicy {
	return newFlushPolicyV1()
}

// newSegmentManager should be the only way to retrieve SegmentManager
func newSegmentManager(meta *meta, allocator allocator, opts ...allocOption) *SegmentManager {
	manager := &SegmentManager{
		meta:      meta,
		allocator: allocator,
		helper:    defaultAllocHelper(),
		stats:     make(map[UniqueID]*segmentStatus),

		estimatePolicy:      defaultCalUpperLimitPolicy(),
		allocPolicy:         defaultAlocatePolicy(),
		segmentSealPolicies: []segmentSealPolicy{defaultSegmentSealPolicy()}, // default only segment size policy
		channelSealPolicies: []channelSealPolicy{},                           // no default channel seal policy
		flushPolicy:         defaultFlushPolicy(),

		allocPool: sync.Pool{
			New: func() interface{} {
				return &allocation{}
			},
		},
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
	ids := make([]UniqueID, 0, len(segments))
	for _, seg := range segments {
		ids = append(ids, seg.ID)
		stat := &segmentStatus{
			id:          seg.ID,
			allocations: make([]*allocation, 0, 16),
		}
		s.stats[seg.ID] = stat
	}
	log.Debug("Restore segment allocation", zap.Int64s("segments", ids))
}

// getAllocation unified way to retrieve allocation struct
func (s *SegmentManager) getAllocation(numOfRows int64, expireTs uint64) *allocation {
	v := s.allocPool.Get()
	if v == nil {
		return &allocation{
			numOfRows:  numOfRows,
			expireTime: expireTs,
		}
	}
	a, ok := v.(*allocation)
	if !ok {
		a = &allocation{}
	}
	a.numOfRows, a.expireTime = numOfRows, expireTs
	return a
}

// putAllocation put allocation for recycling
func (s *SegmentManager) putAllocation(a *allocation) {
	s.allocPool.Put(a)
}

// AllocSegment allocate segment per request collcation, partication, channel and rows
func (s *SegmentManager) AllocSegment(ctx context.Context, collectionID UniqueID,
	partitionID UniqueID, channelName string, requestRows int64) (segID UniqueID, retCount int64, expireTime Timestamp, err error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()

	var success bool
	var status *segmentStatus
	var info *datapb.SegmentInfo
	for _, segStatus := range s.stats {
		info = s.meta.GetSegment(segStatus.id)
		if info == nil {
			log.Warn("Failed to get seginfo from meta", zap.Int64("id", segStatus.id), zap.Error(err))
			continue
		}
		if info.State == commonpb.SegmentState_Sealed || info.CollectionID != collectionID ||
			info.PartitionID != partitionID || info.InsertChannel != channelName {
			continue
		}
		success, err = s.alloc(segStatus, info, requestRows)
		if err != nil {
			return
		}
		if success {
			status = segStatus
			break
		}
	}

	if !success {
		status, err = s.openNewSegment(ctx, collectionID, partitionID, channelName)
		if err != nil {
			return
		}
		info = s.meta.GetSegment(status.id)
		if info == nil {
			log.Warn("Failed to get seg into from meta", zap.Int64("id", status.id), zap.Error(err))
			return
		}
		success, err = s.alloc(status, info, requestRows)
		if err != nil {
			return
		}
		if !success {
			err = errRemainInSufficient(requestRows)
			return
		}
	}

	segID = status.id
	retCount = requestRows
	expireTime = info.LastExpireTime
	return
}

func (s *SegmentManager) alloc(status *segmentStatus, info *datapb.SegmentInfo, numOfRows int64) (bool, error) {
	var allocSize int64
	for _, allocItem := range status.allocations {
		allocSize += allocItem.numOfRows
	}

	if !s.allocPolicy.apply(info.MaxRowNum, status.currentRows, allocSize, numOfRows) {
		return false, nil
	}

	expireTs, err := s.genExpireTs()
	if err != nil {
		return false, err
	}

	alloc := s.getAllocation(numOfRows, expireTs)

	//safe here since info is a clone, used to pass expireTs out
	info.LastExpireTime = expireTs
	status.allocations = append(status.allocations, alloc)

	if err := s.meta.SetLastExpireTime(status.id, expireTs); err != nil {
		return false, err
	}
	return true, nil
}

func (s *SegmentManager) genExpireTs() (Timestamp, error) {
	ts, err := s.allocator.allocTimestamp()
	if err != nil {
		return 0, err
	}
	physicalTs, logicalTs := tsoutil.ParseTS(ts)
	expirePhysicalTs := physicalTs.Add(time.Duration(Params.SegAssignmentExpiration) * time.Millisecond)
	expireTs := tsoutil.ComposeTS(expirePhysicalTs.UnixNano()/int64(time.Millisecond), int64(logicalTs))
	return expireTs, nil
}

func (s *SegmentManager) openNewSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID, channelName string) (*segmentStatus, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	id, err := s.allocator.allocID()
	if err != nil {
		return nil, err
	}
	maxNumOfRows, err := s.estimateMaxNumOfRows(collectionID)
	if err != nil {
		return nil, err
	}
	status := &segmentStatus{
		id:          id,
		allocations: make([]*allocation, 0, 16),
		currentRows: 0,
	}
	s.stats[id] = status

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
			MsgID:       []byte{},
			MsgGroup:    "",
			Timestamp:   0,
		},
	}
	if err := s.meta.AddSegment(segmentInfo); err != nil {
		return nil, err
	}

	log.Debug("datacoord: estimateTotalRows: ",
		zap.Int64("CollectionID", segmentInfo.CollectionID),
		zap.Int64("SegmentID", segmentInfo.ID),
		zap.Int("Rows", maxNumOfRows),
		zap.String("Channel", segmentInfo.InsertChannel))

	s.helper.afterCreateSegment(segmentInfo)
	return status, nil
}

func (s *SegmentManager) estimateMaxNumOfRows(collectionID UniqueID) (int, error) {
	collMeta := s.meta.GetCollection(collectionID)
	if collMeta == nil {
		return -1, fmt.Errorf("Failed to get collection %d", collectionID)
	}
	return s.estimatePolicy.apply(collMeta.Schema)
}

func (s *SegmentManager) DropSegment(ctx context.Context, segmentID UniqueID) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	stat, ok := s.stats[segmentID]
	if ok && stat != nil {
		for _, allocation := range stat.allocations {
			s.putAllocation(allocation)
		}
	}
	delete(s.stats, segmentID)
}

func (s *SegmentManager) SealAllSegments(ctx context.Context, collectionID UniqueID) ([]UniqueID, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := make([]UniqueID, 0)
	for _, status := range s.stats {
		info := s.meta.GetSegment(status.id)
		if info == nil {
			log.Warn("Failed to get seg info from meta", zap.Int64("id", status.id))
			continue
		}
		if info.CollectionID != collectionID {
			continue
		}
		if info.State == commonpb.SegmentState_Sealed {
			ret = append(ret, status.id)
			continue
		}
		if err := s.meta.SetState(status.id, commonpb.SegmentState_Sealed); err != nil {
			return nil, err
		}
		ret = append(ret, status.id)
	}
	return ret, nil
}

func (s *SegmentManager) GetFlushableSegments(ctx context.Context, channel string,
	t Timestamp) ([]UniqueID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	if err := s.tryToSealSegment(t); err != nil {
		return nil, err
	}

	segments := s.meta.GetSegmentsByChannel(channel)
	mIDSegment := make(map[UniqueID]*datapb.SegmentInfo)
	for _, segment := range segments {
		mIDSegment[segment.ID] = segment
	}
	ret := make([]UniqueID, 0, len(segments))
	for _, status := range s.stats {
		info, has := mIDSegment[status.id]
		if !has {
			continue
		}
		if s.flushPolicy.apply(info, t) {
			ret = append(ret, status.id)
		}
	}

	return ret, nil
}

// UpdateSegmentStats update number of rows in memory
func (s *SegmentManager) UpdateSegmentStats(stat *internalpb.SegmentStatisticsUpdates) {
	s.mu.Lock()
	defer s.mu.Unlock()
	segment, ok := s.stats[stat.SegmentID]
	if !ok {
		return
	}
	segment.currentRows = stat.NumRows
}

// ExpireAllocations notify segment status to expire old allocations
func (s *SegmentManager) ExpireAllocations(channel string, ts Timestamp) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	segments := s.meta.GetSegmentsByChannel(channel)
	mIDSeg := make(map[UniqueID]struct{})
	for _, segment := range segments {
		mIDSeg[segment.ID] = struct{}{}
	}
	for _, status := range s.stats {
		_, ok := mIDSeg[status.id]
		if !ok {
			continue
		}
		for i := 0; i < len(status.allocations); i++ {
			if status.allocations[i].expireTime <= ts {
				a := status.allocations[i]
				status.allocations = append(status.allocations[:i], status.allocations[i+1:]...)
				s.putAllocation(a)
			}
		}
	}
	return nil
}

// tryToSealSegment applies segment & channel seal policies
func (s *SegmentManager) tryToSealSegment(ts Timestamp) error {
	channelInfo := make(map[string][]*datapb.SegmentInfo)
	mIDSegment := make(map[UniqueID]*datapb.SegmentInfo)
	for _, status := range s.stats {
		info := s.meta.GetSegment(status.id)
		if info == nil {
			log.Warn("Failed to get seg info from meta", zap.Int64("id", status.id))
			continue
		}
		mIDSegment[status.id] = info
		channelInfo[info.InsertChannel] = append(channelInfo[info.InsertChannel], info)
		if info.State == commonpb.SegmentState_Sealed {
			continue
		}
		// change shouldSeal to segment seal policy logic
		for _, policy := range s.segmentSealPolicies {
			if policy(status, info, ts) {
				if err := s.meta.SetState(status.id, commonpb.SegmentState_Sealed); err != nil {
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

// only for test
func (s *SegmentManager) SealSegment(ctx context.Context, segmentID UniqueID) error {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.meta.SetState(segmentID, commonpb.SegmentState_Sealed); err != nil {
		return err
	}
	return nil
}

func createNewSegmentHelper(stream msgstream.MsgStream) allocHelper {
	h := allocHelper{}
	h.afterCreateSegment = func(segment *datapb.SegmentInfo) error {
		infoMsg := &msgstream.SegmentInfoMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{0},
			},
			SegmentMsg: datapb.SegmentMsg{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_SegmentInfo,
					MsgID:     0,
					Timestamp: 0,
					SourceID:  Params.NodeID,
				},
				Segment: segment,
			},
		}
		msgPack := &msgstream.MsgPack{
			Msgs: []msgstream.TsMsg{infoMsg},
		}
		if err := stream.Produce(msgPack); err != nil {
			return err
		}
		return nil
	}
	return h
}
