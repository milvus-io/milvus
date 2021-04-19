package master

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/tsoutil"

	"github.com/zilliztech/milvus-distributed/internal/errors"

	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type Assignment struct {
	rowNums    int
	expireTime Timestamp
}

type Status struct {
	total          int
	lastExpireTime Timestamp
	assignments    []*Assignment
}

type SegmentAssigner struct {
	mt            *metaTable
	segmentStatus map[UniqueID]*Status //segment id -> status

	globalTSOAllocator    func() (Timestamp, error)
	segmentExpireDuration int64

	proxyTimeSyncChan chan *ms.TimeTickMsg
	ctx               context.Context
	cancel            context.CancelFunc
	waitGroup         sync.WaitGroup
	mu                sync.Mutex
}

type AssignResult struct {
	isSuccess  bool
	expireTime Timestamp
}

func (assigner *SegmentAssigner) OpenSegment(segmentID UniqueID, numRows int) error {
	assigner.mu.Lock()
	defer assigner.mu.Unlock()
	if _, ok := assigner.segmentStatus[segmentID]; ok {
		return errors.Errorf("can not reopen segment %d", segmentID)
	}

	newStatus := &Status{
		total:       numRows,
		assignments: make([]*Assignment, 0),
	}
	assigner.segmentStatus[segmentID] = newStatus
	return nil
}

func (assigner *SegmentAssigner) CloseSegment(segmentID UniqueID) error {
	assigner.mu.Lock()
	defer assigner.mu.Unlock()
	if _, ok := assigner.segmentStatus[segmentID]; !ok {
		return errors.Errorf("can not find segment %d", segmentID)
	}

	delete(assigner.segmentStatus, segmentID)
	return nil
}

func (assigner *SegmentAssigner) Assign(segmentID UniqueID, numRows int) (*AssignResult, error) {
	assigner.mu.Lock()
	defer assigner.mu.Unlock()

	res := &AssignResult{false, 0}
	status, ok := assigner.segmentStatus[segmentID]
	if !ok {
		return res, errors.Errorf("segment %d is not opened", segmentID)
	}

	allocated, err := assigner.totalOfAssignments(segmentID)
	if err != nil {
		return res, err
	}

	segMeta, err := assigner.mt.GetSegmentByID(segmentID)
	if err != nil {
		return res, err
	}
	free := status.total - int(segMeta.NumRows) - allocated
	if numRows > free {
		return res, nil
	}

	ts, err := assigner.globalTSOAllocator()
	if err != nil {
		return res, err
	}
	physicalTs, logicalTs := tsoutil.ParseTS(ts)
	expirePhysicalTs := physicalTs.Add(time.Duration(assigner.segmentExpireDuration) * time.Millisecond)
	expireTs := tsoutil.ComposeTS(expirePhysicalTs.UnixNano()/int64(time.Millisecond), int64(logicalTs))
	status.lastExpireTime = expireTs
	status.assignments = append(status.assignments, &Assignment{
		numRows,
		ts,
	})

	res.isSuccess = true
	res.expireTime = expireTs
	return res, nil
}

func (assigner *SegmentAssigner) CheckAssignmentExpired(segmentID UniqueID, timestamp Timestamp) (bool, error) {
	assigner.mu.Lock()
	defer assigner.mu.Unlock()
	status, ok := assigner.segmentStatus[segmentID]
	if !ok {
		return false, errors.Errorf("can not find segment %d", segmentID)
	}

	if timestamp >= status.lastExpireTime {
		return true, nil
	}

	return false, nil
}

func (assigner *SegmentAssigner) Start() {
	assigner.waitGroup.Add(1)
	go assigner.startProxyTimeSync()
}

func (assigner *SegmentAssigner) Close() {
	assigner.cancel()
	assigner.waitGroup.Wait()
}

func (assigner *SegmentAssigner) startProxyTimeSync() {
	defer assigner.waitGroup.Done()
	for {
		select {
		case <-assigner.ctx.Done():
			log.Println("proxy time sync stopped")
			return
		case msg := <-assigner.proxyTimeSyncChan:
			if err := assigner.syncProxyTimeStamp(msg.TimeTickMsg.Timestamp); err != nil {
				log.Println("proxy time sync error: " + err.Error())
			}
		}
	}
}

func (assigner *SegmentAssigner) totalOfAssignments(segmentID UniqueID) (int, error) {
	if _, ok := assigner.segmentStatus[segmentID]; !ok {
		return -1, errors.Errorf("can not find segment %d", segmentID)
	}

	status := assigner.segmentStatus[segmentID]
	res := 0
	for _, v := range status.assignments {
		res += v.rowNums
	}
	return res, nil
}

func (assigner *SegmentAssigner) syncProxyTimeStamp(timeTick Timestamp) error {
	assigner.mu.Lock()
	defer assigner.mu.Unlock()
	for _, status := range assigner.segmentStatus {
		for i := 0; i < len(status.assignments); {
			if timeTick >= status.assignments[i].expireTime {
				status.assignments[i] = status.assignments[len(status.assignments)-1]
				status.assignments = status.assignments[:len(status.assignments)-1]
				continue
			}
			i++
		}
	}

	return nil
}

func NewSegmentAssigner(ctx context.Context, metaTable *metaTable,
	globalTSOAllocator func() (Timestamp, error), proxyTimeSyncChan chan *ms.TimeTickMsg) *SegmentAssigner {
	assignCtx, cancel := context.WithCancel(ctx)
	return &SegmentAssigner{
		mt:                    metaTable,
		segmentStatus:         make(map[UniqueID]*Status),
		globalTSOAllocator:    globalTSOAllocator,
		segmentExpireDuration: Params.SegIDAssignExpiration,
		proxyTimeSyncChan:     proxyTimeSyncChan,
		ctx:                   assignCtx,
		cancel:                cancel,
	}
}
