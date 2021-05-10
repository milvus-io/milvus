// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
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
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/util/trace"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

type errRemainInSufficient struct {
	requestRows int
}

func newErrRemainInSufficient(requestRows int) errRemainInSufficient {
	return errRemainInSufficient{requestRows: requestRows}
}

func (err errRemainInSufficient) Error() string {
	return "segment remaining is insufficient for" + strconv.Itoa(err.requestRows)
}

// segmentAllocator is used to allocate rows for segments and record the allocations.
type segmentAllocatorInterface interface {
	// AllocSegment allocate rows and record the allocation.
	AllocSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID, channelName string, requestRows int) (UniqueID, int, Timestamp, error)
	// DropSegment drop the segment from allocator.
	DropSegment(ctx context.Context, segmentID UniqueID)
	// SealAllSegments get all opened segment ids of collection. return success and failed segment ids
	SealAllSegments(ctx context.Context, collectionID UniqueID) error
	// GetSealedSegments get all sealed segment.
	GetSealedSegments(ctx context.Context) ([]UniqueID, error)
	// ExpireAllocations check all allocations' expire time and remove the expired allocation.
	ExpireAllocations(ctx context.Context, timeTick Timestamp) error
	// IsAllocationsExpired check all allocations of segment expired.
	IsAllocationsExpired(ctx context.Context, segmentID UniqueID, ts Timestamp) (bool, error)
}

type segmentStatus struct {
	id             UniqueID
	collectionID   UniqueID
	partitionID    UniqueID
	total          int
	sealed         bool
	lastExpireTime Timestamp
	allocations    []*allocation
	insertChannel  string
}
type allocation struct {
	rowNums    int
	expireTime Timestamp
}
type segmentAllocator struct {
	mt                     *meta
	segments               map[UniqueID]*segmentStatus //segment id -> status
	segmentExpireDuration  int64
	segmentThreshold       float64
	segmentThresholdFactor float64
	mu                     sync.RWMutex
	allocator              allocatorInterface
	segmentInfoStream      msgstream.MsgStream
}

type Option struct {
	apply func(alloc *segmentAllocator)
}

func WithSegmentStream(stream msgstream.MsgStream) Option {
	return Option{
		apply: func(alloc *segmentAllocator) {
			alloc.segmentInfoStream = stream
		},
	}
}
func newSegmentAllocator(meta *meta, allocator allocatorInterface, opts ...Option) *segmentAllocator {
	alloc := &segmentAllocator{
		mt:                     meta,
		segments:               make(map[UniqueID]*segmentStatus),
		segmentExpireDuration:  Params.SegIDAssignExpiration,
		segmentThreshold:       Params.SegmentSize * 1024 * 1024,
		segmentThresholdFactor: Params.SegmentSizeFactor,
		allocator:              allocator,
	}
	for _, opt := range opts {
		opt.apply(alloc)
	}
	return alloc
}

func (s *segmentAllocator) open(segmentInfo *datapb.SegmentInfo) error {
	totalRows, err := s.estimateTotalRows(segmentInfo.CollectionID)
	if err != nil {
		return err
	}
	log.Debug("dataservice: estimateTotalRows: ",
		zap.Int64("CollectionID", segmentInfo.CollectionID),
		zap.Int64("SegmentID", segmentInfo.ID),
		zap.Int("Rows", totalRows))
	s.segments[segmentInfo.ID] = &segmentStatus{
		id:             segmentInfo.ID,
		collectionID:   segmentInfo.CollectionID,
		partitionID:    segmentInfo.PartitionID,
		total:          totalRows,
		sealed:         false,
		lastExpireTime: 0,
		insertChannel:  segmentInfo.InsertChannel,
	}
	return nil
}

func (s *segmentAllocator) AllocSegment(ctx context.Context, collectionID UniqueID,
	partitionID UniqueID, channelName string, requestRows int) (segID UniqueID, retCount int, expireTime Timestamp, err error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, segStatus := range s.segments {
		if segStatus.sealed || segStatus.collectionID != collectionID || segStatus.partitionID != partitionID ||
			segStatus.insertChannel != channelName {
			continue
		}
		var success bool
		success, err = s.alloc(segStatus, requestRows)
		if err != nil {
			return
		}
		if !success {
			continue
		}
		segID = segStatus.id
		retCount = requestRows
		expireTime = segStatus.lastExpireTime
		return
	}

	var segStatus *segmentStatus
	segStatus, err = s.openNewSegment(ctx, collectionID, partitionID, channelName)
	if err != nil {
		return
	}
	var success bool
	success, err = s.alloc(segStatus, requestRows)
	if err != nil {
		return
	}
	if !success {
		err = newErrRemainInSufficient(requestRows)
		return
	}

	segID = segStatus.id
	retCount = requestRows
	expireTime = segStatus.lastExpireTime
	return
}

func (s *segmentAllocator) alloc(segStatus *segmentStatus, numRows int) (bool, error) {
	totalOfAllocations := 0
	for _, allocation := range segStatus.allocations {
		totalOfAllocations += allocation.rowNums
	}
	segMeta, err := s.mt.GetSegment(segStatus.id)
	if err != nil {
		return false, err
	}
	free := segStatus.total - int(segMeta.NumRows) - totalOfAllocations
	log.Debug("dataservice::alloc: ",
		zap.Any("segMeta.NumRows", int(segMeta.NumRows)),
		zap.Any("totalOfAllocations", totalOfAllocations))
	if numRows > free {
		return false, nil
	}

	ts, err := s.allocator.allocTimestamp()
	if err != nil {
		return false, err
	}
	physicalTs, logicalTs := tsoutil.ParseTS(ts)
	expirePhysicalTs := physicalTs.Add(time.Duration(s.segmentExpireDuration) * time.Millisecond)
	expireTs := tsoutil.ComposeTS(expirePhysicalTs.UnixNano()/int64(time.Millisecond), int64(logicalTs))
	segStatus.lastExpireTime = expireTs
	segStatus.allocations = append(segStatus.allocations, &allocation{
		numRows,
		expireTs,
	})

	return true, nil
}

func (s *segmentAllocator) openNewSegment(ctx context.Context, collectionID UniqueID, partitionID UniqueID, channelName string) (*segmentStatus, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	id, err := s.allocator.allocID()
	if err != nil {
		return nil, err
	}
	segmentInfo, err := BuildSegment(collectionID, partitionID, id, channelName)
	if err != nil {
		return nil, err
	}
	if err = s.mt.AddSegment(segmentInfo); err != nil {
		return nil, err
	}
	if err = s.open(segmentInfo); err != nil {
		return nil, err
	}
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
			Segment: segmentInfo,
		},
	}
	msgPack := &msgstream.MsgPack{
		Msgs: []msgstream.TsMsg{infoMsg},
	}
	if s.segmentInfoStream != nil {
		if err = s.segmentInfoStream.Produce(msgPack); err != nil {
			return nil, err
		}
	}
	return s.segments[segmentInfo.ID], nil
}

func (s *segmentAllocator) estimateTotalRows(collectionID UniqueID) (int, error) {
	collMeta, err := s.mt.GetCollection(collectionID)
	if err != nil {
		return -1, err
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(collMeta.Schema)
	if err != nil {
		return -1, err
	}
	return int(s.segmentThreshold / float64(sizePerRecord)), nil
}

func (s *segmentAllocator) GetSealedSegments(ctx context.Context) ([]UniqueID, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]UniqueID, 0)
	for _, segStatus := range s.segments {
		if !segStatus.sealed {
			sealed, err := s.checkSegmentSealed(segStatus)
			if err != nil {
				return nil, err
			}
			if !sealed {
				continue
			}
			if err := s.sealSegmentInMeta(segStatus.id); err != nil {
				return nil, err
			}
			segStatus.sealed = sealed
		}
		if segStatus.sealed {
			keys = append(keys, segStatus.id)
		}
	}
	return keys, nil
}

func (s *segmentAllocator) checkSegmentSealed(segStatus *segmentStatus) (bool, error) {
	segMeta, err := s.mt.GetSegment(segStatus.id)
	if err != nil {
		return false, err
	}
	return float64(segMeta.NumRows) >= s.segmentThresholdFactor*float64(segStatus.total), nil
}

func (s *segmentAllocator) HasSegment(ctx context.Context, segmentID UniqueID) bool {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.segments[segmentID]
	return ok
}

func (s *segmentAllocator) sealSegmentInMeta(id UniqueID) error {
	ts, err := s.allocator.allocTimestamp()
	if err != nil {
		return err
	}
	return s.mt.SealSegment(id, ts)
}

func (s *segmentAllocator) DropSegment(ctx context.Context, segmentID UniqueID) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.segments, segmentID)
}

func (s *segmentAllocator) ExpireAllocations(ctx context.Context, timeTick Timestamp) error {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, segStatus := range s.segments {
		for i := 0; i < len(segStatus.allocations); i++ {
			if timeTick < segStatus.allocations[i].expireTime {
				continue
			}
			log.Debug("dataservice::ExpireAllocations: ",
				zap.Any("segStatus.id", segStatus.id),
				zap.Any("segStatus.allocations.rowNums", segStatus.allocations[i].rowNums))
			segStatus.allocations = append(segStatus.allocations[:i], segStatus.allocations[i+1:]...)
			i--
		}
	}
	return nil
}

func (s *segmentAllocator) IsAllocationsExpired(ctx context.Context, segmentID UniqueID, ts Timestamp) (bool, error) {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.RLock()
	defer s.mu.RUnlock()
	status, ok := s.segments[segmentID]
	if !ok {
		return false, fmt.Errorf("segment %d not found", segmentID)
	}
	return status.lastExpireTime <= ts, nil
}

func (s *segmentAllocator) SealAllSegments(ctx context.Context, collectionID UniqueID) error {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, status := range s.segments {
		if status.collectionID == collectionID {
			if status.sealed {
				continue
			}
			if err := s.sealSegmentInMeta(status.id); err != nil {
				return err
			}
			status.sealed = true
		}
	}
	return nil
}

// only for test
func (s *segmentAllocator) SealSegment(ctx context.Context, segmentID UniqueID) error {
	sp, _ := trace.StartSpanFromContext(ctx)
	defer sp.Finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	status, ok := s.segments[segmentID]
	if !ok {
		return nil
	}

	if err := s.sealSegmentInMeta(segmentID); err != nil {
		return err
	}
	status.sealed = true
	return nil
}
