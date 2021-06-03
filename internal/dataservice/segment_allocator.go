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

// segmentAllocator is used to allocate rows for segments and record the allocations.
type segmentAllocator interface {
	// AllocSegment allocate rows and record the allocation.
	AllocSegment(ctx context.Context, collectionID, partitionID UniqueID, channelName string, requestRows int64) (UniqueID, int64, Timestamp, error)
	// DropSegment drop the segment from allocator.
	DropSegment(ctx context.Context, segmentID UniqueID)
	// SealAllSegments get all opened segment ids of collection. return success and failed segment ids
	SealAllSegments(ctx context.Context, collectionID UniqueID) error
	// GetFlushableSegments return flushable segment ids
	GetFlushableSegments(ctx context.Context, channel string, ts Timestamp) ([]UniqueID, error)
}

type channelSegmentAllocator struct {
	mt         *meta
	mu         sync.RWMutex
	allocator  allocator
	helper     allocHelper
	allocStats *segAllocStats

	estimatePolicy calUpperLimitPolicy
	allocPolicy    allocatePolicy
	sealPolicy     sealPolicy
	flushPolicy    flushPolicy
}

type allocHelper struct {
	afterCreateSegment func(segment *datapb.SegmentInfo) error
}

type allocOption struct {
	apply func(alloc *channelSegmentAllocator)
}

func withAllocHelper(helper allocHelper) allocOption {
	return allocOption{
		apply: func(alloc *channelSegmentAllocator) { alloc.helper = helper },
	}
}

func defaultAllocHelper() allocHelper {
	return allocHelper{
		afterCreateSegment: func(segment *datapb.SegmentInfo) error { return nil },
	}
}

func withCalUpperLimitPolicy(policy calUpperLimitPolicy) allocOption {
	return allocOption{
		apply: func(alloc *channelSegmentAllocator) { alloc.estimatePolicy = policy },
	}
}

func withAllocPolicy(policy allocatePolicy) allocOption {
	return allocOption{
		apply: func(alloc *channelSegmentAllocator) { alloc.allocPolicy = policy },
	}
}

func withSealPolicy(policy sealPolicy) allocOption {
	return allocOption{
		apply: func(alloc *channelSegmentAllocator) { alloc.sealPolicy = policy },
	}
}

func withFlushPolicy(policy flushPolicy) allocOption {
	return allocOption{
		apply: func(alloc *channelSegmentAllocator) { alloc.flushPolicy = policy },
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

func defaultFlushPolicy() flushPolicy {
	return newFlushPolicyV1()
}

func newSegmentAllocator(meta *meta, allocator allocator, opts ...allocOption) *channelSegmentAllocator {
	alloc := &channelSegmentAllocator{
		mt:         meta,
		allocator:  allocator,
		helper:     defaultAllocHelper(),
		allocStats: newAllocStats(meta),

		estimatePolicy: defaultCalUpperLimitPolicy(),
		allocPolicy:    defaultAlocatePolicy(),
		sealPolicy:     defaultSealPolicy(),
		flushPolicy:    defaultFlushPolicy(),
	}
	for _, opt := range opts {
		opt.apply(alloc)
	}
	return alloc
}

func (s *channelSegmentAllocator) AllocSegment(ctx context.Context, collectionID UniqueID,
	partitionID UniqueID, channelName string, requestRows int64) (segID UniqueID, retCount int64, expireTime Timestamp, err error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()

	var success bool
	var status *segAllocStatus
	segments := s.allocStats.getSegments(collectionID, partitionID, channelName)
	for _, segStatus := range segments {
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

func (s *channelSegmentAllocator) alloc(segStatus *segAllocStatus, numRows int64) (bool, error) {
	info, err := s.mt.GetSegment(segStatus.id)
	if err != nil {
		return false, err
	}
	allocSize := segStatus.getAllocationSize()
	if !s.allocPolicy.apply(segStatus.total, info.NumRows, allocSize, numRows) {
		return false, nil
	}

	expireTs, err := s.genExpireTs()
	if err != nil {
		return false, err
	}
	if err := s.allocStats.appendAllocation(segStatus.id, numRows, expireTs); err != nil {
		return false, err
	}

	return true, nil
}

func (s *channelSegmentAllocator) genExpireTs() (Timestamp, error) {
	ts, err := s.allocator.allocTimestamp()
	if err != nil {
		return 0, err
	}
	physicalTs, logicalTs := tsoutil.ParseTS(ts)
	expirePhysicalTs := physicalTs.Add(time.Duration(Params.SegIDAssignExpiration) * time.Millisecond)
	expireTs := tsoutil.ComposeTS(expirePhysicalTs.UnixNano()/int64(time.Millisecond), int64(logicalTs))
	return expireTs, nil
}

func (s *channelSegmentAllocator) openNewSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID, channelName string) (*segAllocStatus, error) {
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
	segmentInfo := &datapb.SegmentInfo{
		ID:             id,
		CollectionID:   collectionID,
		PartitionID:    partitionID,
		InsertChannel:  channelName,
		NumRows:        0,
		State:          commonpb.SegmentState_Growing,
		MaxRowNum:      int64(totalRows),
		LastExpireTime: 0,
	}

	if err := s.allocStats.addSegment(segmentInfo); err != nil {
		return nil, err
	}

	log.Debug("dataservice: estimateTotalRows: ",
		zap.Int64("CollectionID", segmentInfo.CollectionID),
		zap.Int64("SegmentID", segmentInfo.ID),
		zap.Int("Rows", totalRows))

	s.helper.afterCreateSegment(segmentInfo)

	return s.allocStats.getSegmentBy(segmentInfo.ID), nil
}

func (s *channelSegmentAllocator) estimateTotalRows(collectionID UniqueID) (int, error) {
	collMeta, err := s.mt.GetCollection(collectionID)
	if err != nil {
		return -1, err
	}
	return s.estimatePolicy.apply(collMeta.Schema)
}

func (s *channelSegmentAllocator) DropSegment(ctx context.Context, segmentID UniqueID) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocStats.dropSegment(segmentID)
}

func (s *channelSegmentAllocator) SealAllSegments(ctx context.Context, collectionID UniqueID) error {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocStats.sealSegmentsBy(collectionID)
	return nil
}

func (s *channelSegmentAllocator) GetFlushableSegments(ctx context.Context, channel string,
	t Timestamp) ([]UniqueID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	if err := s.tryToSealSegment(); err != nil {
		return nil, err
	}

	ret := make([]UniqueID, 0)
	segments := s.allocStats.getAllSegments()
	for _, segStatus := range segments {
		if segStatus.insertChannel != channel {
			continue
		}
		if s.flushPolicy.apply(segStatus, t) {
			ret = append(ret, segStatus.id)
		}
	}

	return ret, nil
}

func (s *channelSegmentAllocator) tryToSealSegment() error {
	segments := s.allocStats.getAllSegments()
	for _, segStatus := range segments {
		if segStatus.sealed {
			continue
		}
		sealed, err := s.checkSegmentSealed(segStatus)
		if err != nil {
			return err
		}
		if !sealed {
			continue
		}
		if err := s.allocStats.sealSegment(segStatus.id); err != nil {
			return err
		}
	}

	return nil
}

func (s *channelSegmentAllocator) checkSegmentSealed(segStatus *segAllocStatus) (bool, error) {
	segMeta, err := s.mt.GetSegment(segStatus.id)
	if err != nil {
		return false, err
	}

	ret := s.sealPolicy.apply(segStatus.total, segMeta.NumRows, segStatus.getAllocationSize())
	return ret, nil
}

// only for test
func (s *channelSegmentAllocator) SealSegment(ctx context.Context, segmentID UniqueID) error {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.allocStats.sealSegment(segmentID)
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
