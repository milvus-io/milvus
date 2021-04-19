package allocator

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

const (
	SegCountPerRPC     = 20000
	ActiveTimeDuration = 100 // Second
)

type assignInfo struct {
	internalpb.SegIDAssignment
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
	return info.Count >= count
}

type SegIDAssigner struct {
	Allocator
	assignInfos map[string][]*assignInfo // collectionName -> [] *assignInfo
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
			//toDoReqs:      []request,
		},
		assignInfos: make(map[string][]*assignInfo),
		//segReqs:     make([]*internalpb.SegIDRequest, maxConcurrentRequests),
		//canDoReqs:   make([]request, maxConcurrentRequests),
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
	for _, colInfos := range sa.assignInfos {
		for _, assign := range colInfos {
			if !assign.IsActive(now) || !assign.IsExpired(now) {
				continue
			}
			sa.segReqs = append(sa.segReqs, &internalpb.SegIDRequest{
				ChannelID:    assign.ChannelID,
				Count:        sa.countPerRPC,
				CollName:     assign.CollName,
				PartitionTag: assign.PartitionTag,
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
		fmt.Println("DDDDD????", req)
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

func (sa *SegIDAssigner) getAssign(colName, partition string, channelID int32) *assignInfo {
	colInfos, ok := sa.assignInfos[colName]
	if !ok {
		return nil
	}
	for _, info := range colInfos {
		if info.PartitionTag != partition || info.ChannelID != channelID {
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
	log.Printf("resp: %v", resp)

	if resp.Status.GetErrorCode() != commonpb.ErrorCode_SUCCESS {
		log.Panic("GRPC AssignSegmentID Failed")
		return
	}

	now := time.Now()
	expiredTime := now.Add(time.Millisecond * time.Duration(resp.ExpireDuration))
	for _, info := range resp.PerChannelAssignment {
		assign := sa.getAssign(info.CollName, info.PartitionTag, info.ChannelID)
		if assign == nil {
			colInfos := sa.assignInfos[info.CollName]
			newAssign := &assignInfo{
				SegIDAssignment: *info,
				expireTime:      expiredTime,
				lastInsertTime:  now,
			}
			colInfos = append(colInfos, newAssign)
			sa.assignInfos[info.CollName] = colInfos
		} else {
			assign.SegIDAssignment = *info
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
	segRequest.segID = assign.SegID
	assign.Count -= segRequest.count
	fmt.Println("process segmentID")
	return nil
}

func (sa *SegIDAssigner) GetSegmentID(colName, partition string, channelID int32, count uint32) (UniqueID, error) {
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
		return 0, errors.New("GetSegmentID Failed")
	}
	return req.segID, nil
}
