package master

import (
	"context"

	"log"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type collectionStatus struct {
	segments []*segmentStatus
}
type segmentStatus struct {
	segmentID UniqueID
	total     int
	closable  bool
}

type channelRange struct {
	channelStart int32
	channelEnd   int32
}

type SegmentManager interface {
	Start()
	Close()
	AssignSegment(segIDReq []*datapb.SegIDRequest) ([]*datapb.SegIDAssignment, error)
	ForceClose(collID UniqueID) error
	DropCollection(collID UniqueID) error
}

type SegmentManagerImpl struct {
	metaTable              *metaTable
	channelRanges          []*channelRange
	collStatus             map[UniqueID]*collectionStatus // collection id to collection status
	defaultSizePerRecord   int64
	segmentThreshold       float64
	segmentThresholdFactor float64
	numOfChannels          int
	numOfQueryNodes        int
	globalIDAllocator      func() (UniqueID, error)
	globalTSOAllocator     func() (Timestamp, error)
	mu                     sync.RWMutex

	assigner *SegmentAssigner

	writeNodeTimeSyncChan chan *ms.TimeTickMsg
	flushScheduler        persistenceScheduler

	ctx       context.Context
	cancel    context.CancelFunc
	waitGroup sync.WaitGroup
}

func (manager *SegmentManagerImpl) AssignSegment(segIDReq []*datapb.SegIDRequest) ([]*datapb.SegIDAssignment, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	res := make([]*datapb.SegIDAssignment, 0)

	for _, req := range segIDReq {
		result := &datapb.SegIDAssignment{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
			},
		}
		collName := req.CollName
		paritionName := req.PartitionName
		count := req.Count
		channelID := req.ChannelID

		collMeta, err := manager.metaTable.GetCollectionByName(collName)
		if err != nil {
			result.Status.Reason = err.Error()
			res = append(res, result)
			continue
		}

		collID := collMeta.GetID()
		if !manager.metaTable.HasPartition(collID, paritionName) {
			result.Status.Reason = "partition tag " + paritionName + " can not find in coll " + strconv.FormatInt(collID, 10)
			res = append(res, result)
			continue
		}
		channelIDInt, _ := strconv.ParseInt(channelID, 10, 64)
		assignInfo, err := manager.assignSegment(collName, collID, paritionName, count, int32(channelIDInt))
		if err != nil {
			result.Status.Reason = err.Error()
			res = append(res, result)
			continue
		}

		res = append(res, assignInfo)
	}
	return res, nil
}

func (manager *SegmentManagerImpl) assignSegment(
	collName string,
	collID UniqueID,
	paritionName string,
	count uint32,
	channelID int32) (*datapb.SegIDAssignment, error) {

	collStatus, ok := manager.collStatus[collID]
	if !ok {
		collStatus = &collectionStatus{
			segments: make([]*segmentStatus, 0),
		}
		manager.collStatus[collID] = collStatus
	}
	for _, segStatus := range collStatus.segments {
		if segStatus.closable {
			continue
		}
		match, err := manager.isMatch(segStatus.segmentID, paritionName, channelID)
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}

		result, err := manager.assigner.Assign(segStatus.segmentID, int(count))
		if err != nil {
			return nil, err
		}
		if !result.isSuccess {
			continue
		}

		return &datapb.SegIDAssignment{
			SegID:         segStatus.segmentID,
			ChannelID:     channelID,
			Count:         count,
			CollName:      collName,
			PartitionName: paritionName,
			ExpireTime:    result.expireTime,
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_SUCCESS,
				Reason:    "",
			},
		}, nil

	}

	total, err := manager.estimateTotalRows(collName)
	if err != nil {
		return nil, err
	}
	if int(count) > total {
		return nil, errors.Errorf("request count %d is larger than total rows %d", count, total)
	}

	id, err := manager.openNewSegment(channelID, collID, paritionName, total)
	if err != nil {
		return nil, err
	}

	result, err := manager.assigner.Assign(id, int(count))
	if err != nil {
		return nil, err
	}
	if !result.isSuccess {
		return nil, errors.Errorf("assign failed for segment %d", id)
	}
	return &datapb.SegIDAssignment{
		SegID:         id,
		ChannelID:     channelID,
		Count:         count,
		CollName:      collName,
		PartitionName: paritionName,
		ExpireTime:    result.expireTime,
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_SUCCESS,
			Reason:    "",
		},
	}, nil
}

func (manager *SegmentManagerImpl) isMatch(segmentID UniqueID, paritionName string, channelID int32) (bool, error) {
	segMeta, err := manager.metaTable.GetSegmentByID(segmentID)
	if err != nil {
		return false, err
	}

	if channelID < segMeta.GetChannelStart() ||
		channelID > segMeta.GetChannelEnd() || segMeta.PartitionTag != paritionName {
		return false, nil
	}
	return true, nil
}

func (manager *SegmentManagerImpl) estimateTotalRows(collName string) (int, error) {
	collMeta, err := manager.metaTable.GetCollectionByName(collName)
	if err != nil {
		return -1, err
	}
	sizePerRecord, err := typeutil.EstimateSizePerRecord(collMeta.Schema)
	if err != nil {
		return -1, err
	}
	return int(manager.segmentThreshold / float64(sizePerRecord)), nil
}

func (manager *SegmentManagerImpl) openNewSegment(channelID int32, collID UniqueID, paritionName string, numRows int) (UniqueID, error) {
	// find the channel range
	channelStart, channelEnd := int32(-1), int32(-1)
	for _, r := range manager.channelRanges {
		if channelID >= r.channelStart && channelID <= r.channelEnd {
			channelStart = r.channelStart
			channelEnd = r.channelEnd
			break
		}
	}
	if channelStart == -1 {
		return -1, errors.Errorf("can't find the channel range which contains channel %d", channelID)
	}

	newID, err := manager.globalIDAllocator()
	if err != nil {
		return -1, err
	}
	openTime, err := manager.globalTSOAllocator()
	if err != nil {
		return -1, err
	}

	err = manager.metaTable.AddSegment(&etcdpb.SegmentMeta{
		SegmentID:    newID,
		CollectionID: collID,
		PartitionTag: paritionName,
		ChannelStart: channelStart,
		ChannelEnd:   channelEnd,
		OpenTime:     openTime,
		NumRows:      0,
		MemSize:      0,
	})
	if err != nil {
		return -1, err
	}

	err = manager.assigner.OpenSegment(newID, numRows)
	if err != nil {
		return -1, err
	}

	segStatus := &segmentStatus{
		segmentID: newID,
		total:     numRows,
		closable:  false,
	}

	collStatus := manager.collStatus[collID]
	collStatus.segments = append(collStatus.segments, segStatus)

	return newID, nil
}

func (manager *SegmentManagerImpl) Start() {
	manager.waitGroup.Add(1)
	go manager.startWriteNodeTimeSync()
}

func (manager *SegmentManagerImpl) Close() {
	manager.cancel()
	manager.waitGroup.Wait()
}

func (manager *SegmentManagerImpl) startWriteNodeTimeSync() {
	defer manager.waitGroup.Done()
	for {
		select {
		case <-manager.ctx.Done():
			log.Println("write node time sync stopped")
			return
		case msg := <-manager.writeNodeTimeSyncChan:
			if err := manager.syncWriteNodeTimestamp(msg.TimeTickMsg.Base.Timestamp); err != nil {
				log.Println("write node time sync error: " + err.Error())
			}
		}
	}
}

func (manager *SegmentManagerImpl) syncWriteNodeTimestamp(timeTick Timestamp) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for _, status := range manager.collStatus {
		for i := 0; i < len(status.segments); i++ {
			segStatus := status.segments[i]
			if !segStatus.closable {
				closable, err := manager.judgeSegmentClosable(segStatus)
				if err != nil {
					log.Printf("check segment closable error: %s", err.Error())
					continue
				}
				segStatus.closable = closable
				if !segStatus.closable {
					continue
				}
			}

			isExpired, err := manager.assigner.CheckAssignmentExpired(segStatus.segmentID, timeTick)
			if err != nil {
				return err
			}
			if !isExpired {
				continue
			}
			status.segments = append(status.segments[:i], status.segments[i+1:]...)
			i--
			ts, err := manager.globalTSOAllocator()
			if err != nil {
				log.Printf("allocate tso error: %s", err.Error())
				continue
			}
			if err = manager.metaTable.CloseSegment(segStatus.segmentID, ts); err != nil {
				log.Printf("meta table close segment error: %s", err.Error())
				continue
			}
			if err = manager.assigner.CloseSegment(segStatus.segmentID); err != nil {
				log.Printf("assigner close segment error: %s", err.Error())
				continue
			}
			if err = manager.flushScheduler.Enqueue(segStatus.segmentID); err != nil {
				log.Printf("flush scheduler enqueue error: %s", err.Error())
				continue
			}
		}
	}

	return nil
}

func (manager *SegmentManagerImpl) judgeSegmentClosable(status *segmentStatus) (bool, error) {
	segMeta, err := manager.metaTable.GetSegmentByID(status.segmentID)
	if err != nil {
		return false, err
	}

	if segMeta.NumRows >= int64(manager.segmentThresholdFactor*float64(status.total)) {
		return true, nil
	}
	return false, nil
}

func (manager *SegmentManagerImpl) initChannelRanges() error {
	div, rem := manager.numOfChannels/manager.numOfQueryNodes, manager.numOfChannels%manager.numOfQueryNodes
	for i, j := 0, 0; i < manager.numOfChannels; j++ {
		if j < rem {
			manager.channelRanges = append(manager.channelRanges, &channelRange{
				channelStart: int32(i),
				channelEnd:   int32(i + div),
			})
			i += div + 1
		} else {
			manager.channelRanges = append(manager.channelRanges, &channelRange{
				channelStart: int32(i),
				channelEnd:   int32(i + div - 1),
			})
			i += div
		}
	}
	return nil
}

// ForceClose set segments of collection with collID closable, segment will be closed after the assignments of it has expired
func (manager *SegmentManagerImpl) ForceClose(collID UniqueID) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	status, ok := manager.collStatus[collID]
	if !ok {
		return nil
	}

	for _, segStatus := range status.segments {
		segStatus.closable = true
	}
	return nil
}

func (manager *SegmentManagerImpl) DropCollection(collID UniqueID) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()

	status, ok := manager.collStatus[collID]
	if !ok {
		return nil
	}

	for _, segStatus := range status.segments {
		if err := manager.assigner.CloseSegment(segStatus.segmentID); err != nil {
			return err
		}
	}

	delete(manager.collStatus, collID)
	return nil
}

func NewSegmentManager(ctx context.Context,
	meta *metaTable,
	globalIDAllocator func() (UniqueID, error),
	globalTSOAllocator func() (Timestamp, error),
	syncWriteNodeChan chan *ms.TimeTickMsg,
	scheduler persistenceScheduler,
	assigner *SegmentAssigner) (*SegmentManagerImpl, error) {

	assignerCtx, cancel := context.WithCancel(ctx)
	segManager := &SegmentManagerImpl{
		metaTable:              meta,
		channelRanges:          make([]*channelRange, 0),
		collStatus:             make(map[UniqueID]*collectionStatus),
		segmentThreshold:       Params.SegmentSize * 1024 * 1024,
		segmentThresholdFactor: Params.SegmentSizeFactor,
		defaultSizePerRecord:   Params.DefaultRecordSize,
		numOfChannels:          Params.TopicNum,
		numOfQueryNodes:        Params.QueryNodeNum,
		globalIDAllocator:      globalIDAllocator,
		globalTSOAllocator:     globalTSOAllocator,

		assigner:              assigner,
		writeNodeTimeSyncChan: syncWriteNodeChan,
		flushScheduler:        scheduler,

		ctx:    assignerCtx,
		cancel: cancel,
	}

	if err := segManager.initChannelRanges(); err != nil {
		return nil, err
	}

	return segManager, nil
}

type mockSegmentManager struct {
}

func (manager *mockSegmentManager) Start() {
}

func (manager *mockSegmentManager) Close() {
}

func (manager *mockSegmentManager) AssignSegment(segIDReq []*datapb.SegIDRequest) ([]*datapb.SegIDAssignment, error) {
	return nil, nil
}

func (manager *mockSegmentManager) ForceClose(collID UniqueID) error {
	return nil
}

func (manager *mockSegmentManager) DropCollection(collID UniqueID) error {
	return nil
}

// only used in unit tests
func NewMockSegmentManager() SegmentManager {
	return &mockSegmentManager{}
}
