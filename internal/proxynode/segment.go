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

package proxynode

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	SegCountPerRPC     = 20000
	ActiveTimeDuration = 100 //second
)

type Allocator = allocator.Allocator

type segRequest struct {
	allocator.BaseRequest
	count       uint32
	collID      UniqueID
	partitionID UniqueID
	segInfo     map[UniqueID]uint32
	channelName string
	timestamp   Timestamp
}

type segInfo struct {
	segID      UniqueID
	count      uint32
	expireTime Timestamp
}

type assignInfo struct {
	collID         UniqueID
	partitionID    UniqueID
	channelName    string
	segID          UniqueID
	segInfos       *list.List
	segCapacity    uint32
	lastInsertTime time.Time
}

func (info *segInfo) IsExpired(ts Timestamp) bool {
	return ts > info.expireTime || info.count <= 0
}

func (info *segInfo) Capacity(ts Timestamp) uint32 {
	if info.IsExpired(ts) {
		return 0
	}
	return info.count
}

func (info *segInfo) Assign(ts Timestamp, count uint32) uint32 {
	if info.IsExpired(ts) {
		return 0
	}
	ret := uint32(0)
	if info.count >= count {
		info.count -= count
		ret = count
	} else {
		info.count = 0
		ret = info.count
	}
	return ret
}

func (info *assignInfo) RemoveExpired(ts Timestamp) {
	for e := info.segInfos.Front(); e != nil; e = e.Next() {
		segInfo, ok := e.Value.(*segInfo)
		if !ok {
			log.Warn("can not cast to segInfo")
			continue
		}
		if segInfo.IsExpired(ts) {
			info.segInfos.Remove(e)
		}
	}
}

func (info *assignInfo) Capacity(ts Timestamp) uint32 {
	ret := uint32(0)
	for e := info.segInfos.Front(); e != nil; e = e.Next() {
		segInfo := e.Value.(*segInfo)
		ret += segInfo.Capacity(ts)
	}
	return ret
}

func (info *assignInfo) Assign(ts Timestamp, count uint32) (map[UniqueID]uint32, error) {
	capacity := info.Capacity(ts)
	if capacity < count {
		errMsg := fmt.Sprintf("AssignSegment Failed: capacity:%d is less than count:%d", capacity, count)
		return nil, errors.New(errMsg)
	}

	result := make(map[UniqueID]uint32)
	for e := info.segInfos.Front(); e != nil && count != 0; e = e.Next() {
		segInfo := e.Value.(*segInfo)
		cur := segInfo.Assign(ts, count)
		count -= cur
		if cur > 0 {
			result[segInfo.segID] += cur
		}
	}
	return result, nil
}

func (info *assignInfo) IsActive(now time.Time) bool {
	return now.Sub(info.lastInsertTime) <= ActiveTimeDuration*time.Second
}

type SegIDAssigner struct {
	Allocator
	assignInfos map[UniqueID]*list.List // collectionID -> *list.List
	segReqs     []*datapb.SegmentIDRequest
	getTickFunc func() Timestamp
	PeerID      UniqueID

	dataService types.DataService
	countPerRPC uint32
}

func NewSegIDAssigner(ctx context.Context, dataService types.DataService, getTickFunc func() Timestamp) (*SegIDAssigner, error) {
	ctx1, cancel := context.WithCancel(ctx)
	sa := &SegIDAssigner{
		Allocator: Allocator{
			Ctx:        ctx1,
			CancelFunc: cancel,
			Role:       "SegmentIDAllocator",
		},
		countPerRPC: SegCountPerRPC,
		dataService: dataService,
		assignInfos: make(map[UniqueID]*list.List),
		getTickFunc: getTickFunc,
	}
	sa.TChan = &allocator.Ticker{
		UpdateInterval: time.Second,
	}
	sa.Allocator.SyncFunc = sa.syncSegments
	sa.Allocator.ProcessFunc = sa.processFunc
	sa.Allocator.CheckSyncFunc = sa.checkSyncFunc
	sa.Allocator.PickCanDoFunc = sa.pickCanDoFunc
	sa.Init()
	return sa, nil
}

func (sa *SegIDAssigner) SetServiceClient(client types.DataService) {
	sa.dataService = client
}

func (sa *SegIDAssigner) collectExpired() {
	ts := sa.getTickFunc()
	for _, info := range sa.assignInfos {
		for e := info.Front(); e != nil; e = e.Next() {
			assign := e.Value.(*assignInfo)
			assign.RemoveExpired(ts)
			if assign.Capacity(ts) == 0 {
				info.Remove(e)
			}
		}
	}
}

func (sa *SegIDAssigner) pickCanDoFunc() {
	if sa.ToDoReqs == nil {
		return
	}
	records := make(map[UniqueID]map[UniqueID]map[string]uint32)
	newTodoReqs := sa.ToDoReqs[0:0]
	for _, req := range sa.ToDoReqs {
		segRequest := req.(*segRequest)
		collID := segRequest.collID
		partitionID := segRequest.partitionID
		channelName := segRequest.channelName

		if _, ok := records[collID]; !ok {
			records[collID] = make(map[UniqueID]map[string]uint32)
		}
		if _, ok := records[collID][partitionID]; !ok {
			records[collID][partitionID] = make(map[string]uint32)
		}

		if _, ok := records[collID][partitionID][channelName]; !ok {
			records[collID][partitionID][channelName] = 0
		}

		records[collID][partitionID][channelName] += segRequest.count
		assign, err := sa.getAssign(segRequest.collID, segRequest.partitionID, segRequest.channelName)
		if err != nil || assign.Capacity(segRequest.timestamp) < records[collID][partitionID][channelName] {
			sa.segReqs = append(sa.segReqs, &datapb.SegmentIDRequest{
				ChannelName:  channelName,
				Count:        segRequest.count,
				CollectionID: collID,
				PartitionID:  partitionID,
			})
			newTodoReqs = append(newTodoReqs, req)
		} else {
			sa.CanDoReqs = append(sa.CanDoReqs, req)
		}
	}
	sa.ToDoReqs = newTodoReqs
}

func (sa *SegIDAssigner) getAssign(collID UniqueID, partitionID UniqueID, channelName string) (*assignInfo, error) {
	assignInfos, ok := sa.assignInfos[collID]
	if !ok {
		return nil, fmt.Errorf("can not find collection %d", collID)
	}

	for e := assignInfos.Front(); e != nil; e = e.Next() {
		info := e.Value.(*assignInfo)
		if info.partitionID != partitionID || info.channelName != channelName {
			continue
		}
		return info, nil
	}
	return nil, fmt.Errorf("can not find assign info with collID %d, partitionID %d, channelName %s",
		collID, partitionID, channelName)
}

func (sa *SegIDAssigner) checkSyncFunc(timeout bool) bool {
	sa.collectExpired()
	return timeout || len(sa.segReqs) != 0
}

func (sa *SegIDAssigner) checkSegReqEqual(req1, req2 *datapb.SegmentIDRequest) bool {
	if req1 == nil || req2 == nil {
		return false
	}

	if req1 == req2 {
		return true
	}
	return req1.CollectionID == req2.CollectionID && req1.PartitionID == req2.PartitionID && req1.ChannelName == req2.ChannelName
}

func (sa *SegIDAssigner) reduceSegReqs() {

	if len(sa.segReqs) == 0 {
		return
	}

	var newSegReqs []*datapb.SegmentIDRequest
	for _, req1 := range sa.segReqs {
		var req2 *datapb.SegmentIDRequest
		for _, req3 := range newSegReqs {
			if sa.checkSegReqEqual(req1, req3) {
				req2 = req3
				break
			}
		}
		if req2 == nil { // not found
			newSegReqs = append(newSegReqs, req1)
		} else {
			req2.Count += req1.Count
		}
	}

	for _, req := range newSegReqs {
		if req.Count == 0 {
			req.Count = sa.countPerRPC
		}
	}
	sa.segReqs = newSegReqs
}

func (sa *SegIDAssigner) syncSegments() (bool, error) {
	if len(sa.segReqs) == 0 {
		return true, nil
	}
	sa.reduceSegReqs()
	req := &datapb.AssignSegmentIDRequest{
		NodeID:            sa.PeerID,
		PeerRole:          typeutil.ProxyNodeRole,
		SegmentIDRequests: sa.segReqs,
	}

	sa.segReqs = []*datapb.SegmentIDRequest{}
	resp, err := sa.dataService.AssignSegmentID(context.Background(), req)

	if err != nil {
		return false, fmt.Errorf("syncSegmentID Failed:%w", err)
	}

	now := time.Now()
	success := false
	for _, info := range resp.SegIDAssignments {
		if info.Status.GetErrorCode() != commonpb.ErrorCode_Success {
			log.Debug("proxynode", zap.String("SyncSegment Error", info.Status.Reason))
			continue
		}
		assign, err := sa.getAssign(info.CollectionID, info.PartitionID, info.ChannelName)
		segInfo := &segInfo{
			segID:      info.SegID,
			count:      info.Count,
			expireTime: info.ExpireTime,
		}
		if err != nil {
			colInfos, ok := sa.assignInfos[info.CollectionID]
			if !ok {
				colInfos = list.New()
			}
			segInfos := list.New()

			segInfos.PushBack(segInfo)
			assign = &assignInfo{
				collID:      info.CollectionID,
				partitionID: info.PartitionID,
				channelName: info.ChannelName,
				segInfos:    segInfos,
			}
			colInfos.PushBack(assign)
			sa.assignInfos[info.CollectionID] = colInfos
		} else {
			assign.segInfos.PushBack(segInfo)
		}
		assign.lastInsertTime = now
		success = true
	}
	return success, nil
}

func (sa *SegIDAssigner) processFunc(req allocator.Request) error {
	segRequest := req.(*segRequest)
	assign, err := sa.getAssign(segRequest.collID, segRequest.partitionID, segRequest.channelName)
	if err != nil {
		return err
	}
	result, err := assign.Assign(segRequest.timestamp, segRequest.count)
	segRequest.segInfo = result
	return err
}

func (sa *SegIDAssigner) GetSegmentID(collID UniqueID, partitionID UniqueID, channelName string, count uint32, ts Timestamp) (map[UniqueID]uint32, error) {
	req := &segRequest{
		BaseRequest: allocator.BaseRequest{Done: make(chan error), Valid: false},
		collID:      collID,
		partitionID: partitionID,
		channelName: channelName,
		count:       count,
		timestamp:   ts,
	}
	sa.Reqs <- req
	if err := req.Wait(); err != nil {
		return nil, fmt.Errorf("GetSegmentID failed: %s", err)
	}

	return req.segInfo, nil
}
