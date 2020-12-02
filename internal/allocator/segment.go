package allocator

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/cznic/mathutil"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

const (
	SegCountPerRPC     = 20000
	ActiveTimeDuration = 100 // Second
)

type assignInfo struct {
	collName       string
	partitionTag   string
	channelID      int32
	segInfo        map[UniqueID]uint32 // segmentID->count map
	expireTime     time.Time
	lastInsertTime time.Time
}

func (info *assignInfo) IsExpired(now time.Time) bool {
	return now.Sub(info.expireTime) >= 0
}

func (info *assignInfo) IsActive(now time.Time) bool {
	return now.Sub(info.lastInsertTime) <= ActiveTimeDuration*time.Second
}

func (info *assignInfo) IsEnough(count uint32) bool {
	total := uint32(0)
	for _, count := range info.segInfo {
		total += count
	}
	return total >= count
}

type SegIDAssigner struct {
	Allocator
	assignInfos map[string]*list.List // collectionName -> *list.List
	segReqs     []*internalpb.SegIDRequest
	canDoReqs   []request
}

func NewSegIDAssigner(ctx context.Context, masterAddr string) (*SegIDAssigner, error) {
	ctx1, cancel := context.WithCancel(ctx)
	sa := &SegIDAssigner{
		Allocator: Allocator{reqs: make(chan request, maxConcurrentRequests),
			ctx:           ctx1,
			cancel:        cancel,
			masterAddress: masterAddr,
			countPerRPC:   SegCountPerRPC,
		},
		assignInfos: make(map[string]*list.List),
	}
	sa.tChan = &ticker{
		updateInterval: time.Second,
	}
	sa.Allocator.syncFunc = sa.syncSegments
	sa.Allocator.processFunc = sa.processFunc
	sa.Allocator.checkFunc = sa.checkFunc
	return sa, nil
}

func (sa *SegIDAssigner) collectExpired() {
	now := time.Now()
	for _, info := range sa.assignInfos {
		for e := info.Front(); e != nil; e = e.Next() {
			assign := e.Value.(*assignInfo)
			if !assign.IsActive(now) || !assign.IsExpired(now) {
				continue
			}
			sa.segReqs = append(sa.segReqs, &internalpb.SegIDRequest{
				ChannelID:    assign.channelID,
				Count:        sa.countPerRPC,
				CollName:     assign.collName,
				PartitionTag: assign.partitionTag,
			})
		}
	}
}

func (sa *SegIDAssigner) checkToDoReqs() {
	if sa.toDoReqs == nil {
		return
	}
	now := time.Now()
	for _, req := range sa.toDoReqs {
		segRequest := req.(*segRequest)
		assign := sa.getAssign(segRequest.colName, segRequest.partition, segRequest.channelID)
		if assign == nil || assign.IsExpired(now) || !assign.IsEnough(segRequest.count) {
			sa.segReqs = append(sa.segReqs, &internalpb.SegIDRequest{
				ChannelID:    segRequest.channelID,
				Count:        segRequest.count,
				CollName:     segRequest.colName,
				PartitionTag: segRequest.partition,
			})
		}
	}
}

func (sa *SegIDAssigner) removeSegInfo(colName, partition string, channelID int32) {
	assignInfos, ok := sa.assignInfos[colName]
	if !ok {
		return
	}

	cnt := assignInfos.Len()
	if cnt == 0 {
		return
	}

	for e := assignInfos.Front(); e != nil; e = e.Next() {
		assign := e.Value.(*assignInfo)
		if assign.partitionTag != partition || assign.channelID != channelID {
			continue
		}
		assignInfos.Remove(e)
	}

}

func (sa *SegIDAssigner) getAssign(colName, partition string, channelID int32) *assignInfo {
	assignInfos, ok := sa.assignInfos[colName]
	if !ok {
		return nil
	}

	for e := assignInfos.Front(); e != nil; e = e.Next() {
		info := e.Value.(*assignInfo)
		if info.partitionTag != partition || info.channelID != channelID {
			continue
		}
		return info
	}
	return nil
}

func (sa *SegIDAssigner) checkFunc(timeout bool) bool {
	if timeout {
		sa.collectExpired()
	} else {
		sa.checkToDoReqs()
	}

	return len(sa.segReqs) != 0
}

func (sa *SegIDAssigner) syncSegments() {
	if len(sa.segReqs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := &internalpb.AssignSegIDRequest{
		PeerID:        1,
		Role:          internalpb.PeerRole_Proxy,
		PerChannelReq: sa.segReqs,
	}

	sa.segReqs = sa.segReqs[0:0]
	fmt.Println("OOOOO", req.PerChannelReq)
	resp, err := sa.masterClient.AssignSegmentID(ctx, req)

	if resp.Status.GetErrorCode() != commonpb.ErrorCode_SUCCESS {
		log.Println("GRPC AssignSegmentID Failed", resp, err)
		return
	}

	now := time.Now()
	expiredTime := now.Add(time.Millisecond * time.Duration(resp.ExpireDuration))
	for _, info := range resp.PerChannelAssignment {
		sa.removeSegInfo(info.CollName, info.PartitionTag, info.ChannelID)
	}

	for _, info := range resp.PerChannelAssignment {
		assign := sa.getAssign(info.CollName, info.PartitionTag, info.ChannelID)
		if assign == nil {
			colInfos, ok := sa.assignInfos[info.CollName]
			if !ok {
				colInfos = list.New()
			}
			segInfo := make(map[UniqueID]uint32)
			segInfo[info.SegID] = info.Count
			newAssign := &assignInfo{
				collName:     info.CollName,
				partitionTag: info.PartitionTag,
				channelID:    info.ChannelID,
				segInfo:      segInfo,
			}
			colInfos.PushBack(newAssign)
			sa.assignInfos[info.CollName] = colInfos
		} else {
			assign.segInfo[info.SegID] = info.Count
			assign.expireTime = expiredTime
			assign.lastInsertTime = now
		}
	}

	if err != nil {
		log.Panic("syncID Failed!!!!!")
		return
	}
}

func (sa *SegIDAssigner) processFunc(req request) error {
	segRequest := req.(*segRequest)
	assign := sa.getAssign(segRequest.colName, segRequest.partition, segRequest.channelID)
	if assign == nil {
		return errors.New("Failed to GetSegmentID")
	}

	keys := make([]UniqueID, len(assign.segInfo))
	i := 0
	for key := range assign.segInfo {
		keys[i] = key
		i++
	}
	reqCount := segRequest.count

	resultSegInfo := make(map[UniqueID]uint32)
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	for _, key := range keys {
		if reqCount <= 0 {
			break
		}
		cur := assign.segInfo[key]
		minCnt := mathutil.MinUint32(cur, reqCount)
		resultSegInfo[key] = minCnt
		cur -= minCnt
		reqCount -= minCnt
		if cur <= 0 {
			delete(assign.segInfo, key)
		} else {
			assign.segInfo[key] = cur
		}
	}
	segRequest.segInfo = resultSegInfo
	return nil
}

func (sa *SegIDAssigner) GetSegmentID(colName, partition string, channelID int32, count uint32) (map[UniqueID]uint32, error) {
	req := &segRequest{
		baseRequest: baseRequest{done: make(chan error), valid: false},
		colName:     colName,
		partition:   partition,
		channelID:   channelID,
		count:       count,
	}
	sa.reqs <- req
	req.Wait()

	if !req.IsValid() {
		return nil, errors.New("GetSegmentID Failed")
	}
	return req.segInfo, nil
}
