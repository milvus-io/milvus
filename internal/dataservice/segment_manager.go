// Copyright (C) 2019-2020 Zilliz. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package dataservice

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

type errRemainInSufficient struct {
	requestRows int64
}

func newErrRemainInSufficient(requestRows int64) errRemainInSufficient {
	return errRemainInSufficient{requestRows: requestRows}
}

func (err errRemainInSufficient) Error() string {
	return fmt.Sprintf("segment remaining is insufficient for %d", err.requestRows)
}

// Manager manage segment related operations.
type Manager interface {
	// AllocSegment allocate rows and record the allocation.
	AllocSegment(ctx context.Context, collectionID, partitionID UniqueID, channelName string, requestRows int64) (UniqueID, int64, Timestamp, error)
	// DropSegment drop the segment from allocator.
	DropSegment(ctx context.Context, segmentID UniqueID)
	// SealAllSegments get all opened segment ids of collection. return success and failed segment ids
	SealAllSegments(ctx context.Context, collectionID UniqueID) error
	// GetFlushableSegments return flushable segment ids
	GetFlushableSegments(ctx context.Context, channel string, ts Timestamp) ([]UniqueID, error)
	// UpdateSegmentStats update segment status
	UpdateSegmentStats(stat *internalpb.SegmentStatisticsUpdates)
}

type segmentStatus struct {
	id             UniqueID
	collectionID   UniqueID
	partitionID    UniqueID
	sealed         bool
	total          int64
	insertChannel  string
	allocations    []*allocation
	lastExpireTime Timestamp
	currentRows    int64
}

type allocation struct {
	rowNums    int64
	expireTime Timestamp
}

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
}

type allocHelper struct {
	afterCreateSegment func(segment *datapb.SegmentInfo) error
}

type allocOption struct {
	apply func(manager *SegmentManager)
}

func withAllocHelper(helper allocHelper) allocOption {
	return allocOption{
		apply: func(manager *SegmentManager) { manager.helper = helper },
	}
}

func defaultAllocHelper() allocHelper {
	return allocHelper{
		afterCreateSegment: func(segment *datapb.SegmentInfo) error { return nil },
	}
}

func withCalUpperLimitPolicy(policy calUpperLimitPolicy) allocOption {
	return allocOption{
		apply: func(manager *SegmentManager) { manager.estimatePolicy = policy },
	}
}

func withAllocPolicy(policy allocatePolicy) allocOption {
	return allocOption{
		apply: func(manager *SegmentManager) { manager.allocPolicy = policy },
	}
}

// func withSealPolicy(policy sealPolicy) allocOption {
// 	return allocOption{
// 		apply: func(manager *SegmentManager) { manager.sealPolicy = policy },
// 	}
// }

func withSegmentSealPolices(policies ...segmentSealPolicy) allocOption {
	return allocOption{
		apply: func(manager *SegmentManager) {
			// do override instead of append, to override default options
			manager.segmentSealPolicies = policies
		},
	}
}

func withChannelSealPolices(policies ...channelSealPolicy) allocOption {
	return allocOption{
		apply: func(manager *SegmentManager) {
			// do override instead of append, to override default options
			manager.channelSealPolicies = policies
		},
	}
}

func withFlushPolicy(policy flushPolicy) allocOption {
	return allocOption{
		apply: func(manager *SegmentManager) { manager.flushPolicy = policy },
	}
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
	return getSegmentCapacityPolicy(Params.SegmentSizeFactor)
}

func defaultFlushPolicy() flushPolicy {
	return newFlushPolicyV1()
}

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
	}
	for _, opt := range opts {
		opt.apply(manager)
	}
	manager.loadSegmentsFromMeta()
	return manager
}

func (s *SegmentManager) loadSegmentsFromMeta() {
	segments := s.meta.GetUnFlushedSegments()
	ids := make([]UniqueID, 0, len(segments))
	for _, seg := range segments {
		ids = append(ids, seg.ID)
		stat := &segmentStatus{
			id:             seg.ID,
			collectionID:   seg.CollectionID,
			partitionID:    seg.PartitionID,
			total:          seg.MaxRowNum,
			allocations:    []*allocation{},
			insertChannel:  seg.InsertChannel,
			lastExpireTime: seg.LastExpireTime,
			sealed:         seg.State == commonpb.SegmentState_Sealed,
		}
		s.stats[seg.ID] = stat
	}
	log.Debug("Restore segment allocation", zap.Int64s("segments", ids))
}
func (s *SegmentManager) AllocSegment(ctx context.Context, collectionID UniqueID,
	partitionID UniqueID, channelName string, requestRows int64) (segID UniqueID, retCount int64, expireTime Timestamp, err error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()

	var success bool
	var status *segmentStatus
	for _, segStatus := range s.stats {
		if segStatus.sealed || segStatus.collectionID != collectionID ||
			segStatus.partitionID != partitionID || segStatus.insertChannel != channelName {
			continue
		}
		success, err = s.alloc(segStatus, requestRows)
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
		success, err = s.alloc(status, requestRows)
		if err != nil {
			return
		}
		if !success {
			err = newErrRemainInSufficient(requestRows)
			return
		}
	}

	segID = status.id
	retCount = requestRows
	expireTime = status.lastExpireTime

	return
}

func (s *SegmentManager) alloc(segStatus *segmentStatus, numRows int64) (bool, error) {
	var allocSize int64
	for _, allocation := range segStatus.allocations {
		allocSize += allocation.rowNums
	}
	if !s.allocPolicy.apply(segStatus.total, segStatus.currentRows, allocSize, numRows) {
		return false, nil
	}

	expireTs, err := s.genExpireTs()
	if err != nil {
		return false, err
	}

	alloc := &allocation{
		rowNums:    numRows,
		expireTime: expireTs,
	}
	segStatus.lastExpireTime = expireTs
	segStatus.allocations = append(segStatus.allocations, alloc)

	if err := s.meta.SetLastExpireTime(segStatus.id, expireTs); err != nil {
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
	expirePhysicalTs := physicalTs.Add(time.Duration(Params.SegIDAssignExpiration) * time.Millisecond)
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
	totalRows, err := s.estimateTotalRows(collectionID)
	if err != nil {
		return nil, err
	}
	segStatus := &segmentStatus{
		id:             id,
		collectionID:   collectionID,
		partitionID:    partitionID,
		sealed:         false,
		total:          int64(totalRows),
		insertChannel:  channelName,
		allocations:    []*allocation{},
		lastExpireTime: 0,
		currentRows:    0,
	}
	s.stats[id] = segStatus

	segmentInfo := &datapb.SegmentInfo{
		ID:             id,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		InsertChannel:  channelName,
		NumOfRows:      0,
		State:          commonpb.SegmentState_Growing,
		MaxRowNum:      int64(totalRows),
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

	log.Debug("dataservice: estimateTotalRows: ",
		zap.Int64("CollectionID", segmentInfo.CollectionID),
		zap.Int64("SegmentID", segmentInfo.ID),
		zap.Int("Rows", totalRows),
		zap.String("channel", segmentInfo.InsertChannel))

	s.helper.afterCreateSegment(segmentInfo)

	return segStatus, nil
}

func (s *SegmentManager) estimateTotalRows(collectionID UniqueID) (int, error) {
	collMeta, err := s.meta.GetCollection(collectionID)
	if err != nil {
		return -1, err
	}
	return s.estimatePolicy.apply(collMeta.Schema)
}

func (s *SegmentManager) DropSegment(ctx context.Context, segmentID UniqueID) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.stats, segmentID)
}

func (s *SegmentManager) SealAllSegments(ctx context.Context, collectionID UniqueID) error {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, status := range s.stats {
		if status.sealed || status.collectionID != collectionID {
			continue
		}
		if err := s.meta.SealSegment(status.id); err != nil {
			return err
		}
		status.sealed = true
	}
	return nil
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

	ret := make([]UniqueID, 0)
	for _, segStatus := range s.stats {
		if segStatus.insertChannel != channel {
			continue
		}
		if s.flushPolicy.apply(segStatus, t) {
			ret = append(ret, segStatus.id)
		}
	}

	return ret, nil
}

func (s *SegmentManager) UpdateSegmentStats(stat *internalpb.SegmentStatisticsUpdates) {
	s.mu.Lock()
	defer s.mu.Unlock()
	segment, ok := s.stats[stat.SegmentID]
	if !ok {
		return
	}
	segment.currentRows = stat.NumRows
}

// tryToSealSegment applies segment & channel seal policies
func (s *SegmentManager) tryToSealSegment(ts Timestamp) error {
	channelInfo := make(map[string][]*segmentStatus)
	for _, segStatus := range s.stats {
		channelInfo[segStatus.insertChannel] = append(channelInfo[segStatus.insertChannel], segStatus)
		if segStatus.sealed {
			continue
		}
		// change shouldSeal to segment seal policy logic
		for _, policy := range s.segmentSealPolicies {
			if policy(segStatus, ts) {
				if err := s.meta.SealSegment(segStatus.id); err != nil {
					return err
				}
				segStatus.sealed = true
				break
			}
		}

	}
	for channel, segs := range channelInfo {
		for _, policy := range s.channelSealPolicies {
			vs := policy(channel, segs, ts)
			for _, seg := range vs {
				if seg.sealed {
					continue
				}
				if err := s.meta.SealSegment(seg.id); err != nil {
					return err
				}
				seg.sealed = true
			}
		}
	}
	return nil
}

// func (s *SegmentManager) shouldSeal(segStatus *segmentStatus) (bool, error) {
// 	var allocSize int64
// 	for _, allocation := range segStatus.allocations {
// 		allocSize += allocation.rowNums
// 	}
// 	ret := s.sealPolicy.apply(segStatus.total, segStatus.currentRows, allocSize)
// 	return ret, nil
// }

// only for test
func (s *SegmentManager) SealSegment(ctx context.Context, segmentID UniqueID) error {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.meta.SealSegment(segmentID); err != nil {
		return err
	}
	s.stats[segmentID].sealed = true
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
