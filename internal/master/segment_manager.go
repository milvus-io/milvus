package master

import (
	"sync"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type collectionStatus struct {
	openedSegments []UniqueID
}

type assignment struct {
	MemSize    int64 // bytes
	AssignTime time.Time
}

type channelRange struct {
	channelStart int32
	channelEnd   int32
}

type segmentStatus struct {
	assignments []*assignment
}

type SegmentManager struct {
	metaTable              *metaTable
	statsStream            msgstream.MsgStream
	channelRanges          []*channelRange
	segmentStatus          map[UniqueID]*segmentStatus    // segment id to segment status
	collStatus             map[UniqueID]*collectionStatus // collection id to collection status
	defaultSizePerRecord   int64
	minimumAssignSize      int64
	segmentThreshold       float64
	segmentThresholdFactor float64
	segmentExpireDuration  int64
	numOfChannels          int
	numOfQueryNodes        int
	globalIDAllocator      func() (UniqueID, error)
	globalTSOAllocator     func() (Timestamp, error)
	mu                     sync.RWMutex
}

func (segMgr *SegmentManager) HandleQueryNodeMsgPack(msgPack *msgstream.MsgPack) error {
	segMgr.mu.Lock()
	defer segMgr.mu.Unlock()
	for _, msg := range msgPack.Msgs {
		statsMsg, ok := msg.(*msgstream.QueryNodeStatsMsg)
		if !ok {
			return errors.Errorf("Type of message is not QueryNodeStatsMsg")
		}

		for _, segStat := range statsMsg.GetSegStats() {
			err := segMgr.handleSegmentStat(segStat)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (segMgr *SegmentManager) handleSegmentStat(segStats *internalpb.SegmentStats) error {
	if !segStats.GetRecentlyModified() {
		return nil
	}
	segID := segStats.GetSegmentID()
	segMeta, err := segMgr.metaTable.GetSegmentByID(segID)
	if err != nil {
		return err
	}
	segMeta.NumRows = segStats.NumRows
	segMeta.MemSize = segStats.MemorySize

	if segStats.MemorySize > int64(segMgr.segmentThresholdFactor*segMgr.segmentThreshold) {
		if err := segMgr.metaTable.UpdateSegment(segMeta); err != nil {
			return err
		}
		return segMgr.closeSegment(segMeta)
	}
	return segMgr.metaTable.UpdateSegment(segMeta)
}

func (segMgr *SegmentManager) closeSegment(segMeta *etcdpb.SegmentMeta) error {
	if segMeta.GetCloseTime() == 0 {
		// close the segment and remove from collStatus
		collStatus, ok := segMgr.collStatus[segMeta.GetCollectionID()]
		if ok {
			openedSegments := collStatus.openedSegments
			for i, openedSegID := range openedSegments {
				if openedSegID == segMeta.SegmentID {
					openedSegments[i] = openedSegments[len(openedSegments)-1]
					collStatus.openedSegments = openedSegments[:len(openedSegments)-1]
					break
				}
			}
		}
		ts, err := segMgr.globalTSOAllocator()
		if err != nil {
			return err
		}
		segMeta.CloseTime = ts
	}

	err := segMgr.metaTable.CloseSegment(segMeta.SegmentID, segMeta.GetCloseTime())
	if err != nil {
		return err
	}
	return nil
}

func (segMgr *SegmentManager) AssignSegmentID(segIDReq []*internalpb.SegIDRequest) ([]*internalpb.SegIDAssignment, error) {
	segMgr.mu.Lock()
	defer segMgr.mu.Unlock()
	res := make([]*internalpb.SegIDAssignment, 0)
	for _, req := range segIDReq {
		collName := req.CollName
		partitionTag := req.PartitionTag
		count := req.Count
		channelID := req.ChannelID
		collMeta, err := segMgr.metaTable.GetCollectionByName(collName)
		if err != nil {
			return nil, err
		}

		collID := collMeta.GetID()
		if !segMgr.metaTable.HasCollection(collID) {
			return nil, errors.Errorf("can not find collection with id=%d", collID)
		}
		if !segMgr.metaTable.HasPartition(collID, partitionTag) {
			return nil, errors.Errorf("partition tag %s can not find in coll %d", partitionTag, collID)
		}
		collStatus, ok := segMgr.collStatus[collID]
		if !ok {
			collStatus = &collectionStatus{
				openedSegments: make([]UniqueID, 0),
			}
			segMgr.collStatus[collID] = collStatus
		}

		assignInfo, err := segMgr.assignSegment(collName, collID, partitionTag, count, channelID, collStatus)
		if err != nil {
			return nil, err
		}
		res = append(res, assignInfo)
	}
	return res, nil
}

func (segMgr *SegmentManager) assignSegment(collName string, collID UniqueID, partitionTag string, count uint32, channelID int32,
	collStatus *collectionStatus) (*internalpb.SegIDAssignment, error) {
	segmentThreshold := int64(segMgr.segmentThreshold)
	for _, segID := range collStatus.openedSegments {
		segMeta, _ := segMgr.metaTable.GetSegmentByID(segID)
		if segMeta.GetCloseTime() != 0 || channelID < segMeta.GetChannelStart() ||
			channelID > segMeta.GetChannelEnd() || segMeta.PartitionTag != partitionTag {
			continue
		}
		// check whether segment has enough mem size
		assignedMem := segMgr.checkAssignedSegExpire(segID)
		memSize := segMeta.MemSize
		neededMemSize := segMgr.calNeededSize(memSize, segMeta.NumRows, int64(count))
		if memSize+assignedMem+neededMemSize <= segmentThreshold {
			remainingSize := segmentThreshold - memSize - assignedMem
			allocMemSize := segMgr.calAllocMemSize(neededMemSize, remainingSize)
			segMgr.addAssignment(segID, allocMemSize)
			return &internalpb.SegIDAssignment{
				SegID:        segID,
				ChannelID:    channelID,
				Count:        uint32(segMgr.calNumRows(memSize, segMeta.NumRows, allocMemSize)),
				CollName:     collName,
				PartitionTag: partitionTag,
			}, nil
		}
	}
	neededMemSize := segMgr.defaultSizePerRecord * int64(count)
	if neededMemSize > segmentThreshold {
		return nil, errors.Errorf("request with count %d need about %d mem size which is larger than segment threshold",
			count, neededMemSize)
	}

	segMeta, err := segMgr.openNewSegment(channelID, collID, partitionTag)
	if err != nil {
		return nil, err
	}

	allocMemSize := segMgr.calAllocMemSize(neededMemSize, segmentThreshold)
	segMgr.addAssignment(segMeta.SegmentID, allocMemSize)
	return &internalpb.SegIDAssignment{
		SegID:        segMeta.SegmentID,
		ChannelID:    channelID,
		Count:        uint32(segMgr.calNumRows(0, 0, allocMemSize)),
		CollName:     collName,
		PartitionTag: partitionTag,
	}, nil
}

func (segMgr *SegmentManager) addAssignment(segID UniqueID, allocSize int64) {
	segStatus := segMgr.segmentStatus[segID]
	segStatus.assignments = append(segStatus.assignments, &assignment{
		MemSize:    allocSize,
		AssignTime: time.Now(),
	})
}

func (segMgr *SegmentManager) calNeededSize(memSize int64, numRows int64, count int64) int64 {
	var avgSize int64
	if memSize == 0 || numRows == 0 || memSize/numRows == 0 {
		avgSize = segMgr.defaultSizePerRecord
	} else {
		avgSize = memSize / numRows
	}
	return avgSize * count
}

func (segMgr *SegmentManager) calAllocMemSize(neededSize int64, remainSize int64) int64 {
	if neededSize > remainSize {
		return 0
	}
	if remainSize < segMgr.minimumAssignSize {
		return remainSize
	}
	if neededSize < segMgr.minimumAssignSize {
		return segMgr.minimumAssignSize
	}
	return neededSize
}

func (segMgr *SegmentManager) calNumRows(memSize int64, numRows int64, allocMemSize int64) int64 {
	var avgSize int64
	if memSize == 0 || numRows == 0 || memSize/numRows == 0 {
		avgSize = segMgr.defaultSizePerRecord
	} else {
		avgSize = memSize / numRows
	}
	return allocMemSize / avgSize
}

func (segMgr *SegmentManager) openNewSegment(channelID int32, collID UniqueID, partitionTag string) (*etcdpb.SegmentMeta, error) {
	// find the channel range
	channelStart, channelEnd := int32(-1), int32(-1)
	for _, r := range segMgr.channelRanges {
		if channelID >= r.channelStart && channelID <= r.channelEnd {
			channelStart = r.channelStart
			channelEnd = r.channelEnd
			break
		}
	}
	if channelStart == -1 {
		return nil, errors.Errorf("can't find the channel range which contains channel %d", channelID)
	}

	newID, err := segMgr.globalIDAllocator()
	if err != nil {
		return nil, err
	}
	openTime, err := segMgr.globalTSOAllocator()
	if err != nil {
		return nil, err
	}
	newSegMeta := &etcdpb.SegmentMeta{
		SegmentID:    newID,
		CollectionID: collID,
		PartitionTag: partitionTag,
		ChannelStart: channelStart,
		ChannelEnd:   channelEnd,
		OpenTime:     openTime,
		NumRows:      0,
		MemSize:      0,
	}

	err = segMgr.metaTable.AddSegment(newSegMeta)
	if err != nil {
		return nil, err
	}
	segMgr.segmentStatus[newID] = &segmentStatus{
		assignments: make([]*assignment, 0),
	}
	collStatus := segMgr.collStatus[collID]
	collStatus.openedSegments = append(collStatus.openedSegments, newSegMeta.SegmentID)
	return newSegMeta, nil
}

// checkAssignedSegExpire check the expire time of assignments and return the total sum of assignments that are not expired.
func (segMgr *SegmentManager) checkAssignedSegExpire(segID UniqueID) int64 {
	segStatus := segMgr.segmentStatus[segID]
	assignments := segStatus.assignments
	result := int64(0)
	i := 0
	for i < len(assignments) {
		assign := assignments[i]
		if time.Since(assign.AssignTime) >= time.Duration(segMgr.segmentExpireDuration)*time.Millisecond {
			assignments[i] = assignments[len(assignments)-1]
			assignments = assignments[:len(assignments)-1]
			continue
		}
		result += assign.MemSize
		i++
	}
	segStatus.assignments = assignments
	return result
}

func (segMgr *SegmentManager) createChannelRanges() error {
	div, rem := segMgr.numOfChannels/segMgr.numOfQueryNodes, segMgr.numOfChannels%segMgr.numOfQueryNodes
	for i, j := 0, 0; i < segMgr.numOfChannels; j++ {
		if j < rem {
			segMgr.channelRanges = append(segMgr.channelRanges, &channelRange{
				channelStart: int32(i),
				channelEnd:   int32(i + div),
			})
			i += div + 1
		} else {
			segMgr.channelRanges = append(segMgr.channelRanges, &channelRange{
				channelStart: int32(i),
				channelEnd:   int32(i + div - 1),
			})
			i += div
		}
	}
	return nil
}

func NewSegmentManager(meta *metaTable,
	globalIDAllocator func() (UniqueID, error),
	globalTSOAllocator func() (Timestamp, error),
) *SegmentManager {
	segMgr := &SegmentManager{
		metaTable:              meta,
		channelRanges:          make([]*channelRange, 0),
		segmentStatus:          make(map[UniqueID]*segmentStatus),
		collStatus:             make(map[UniqueID]*collectionStatus),
		segmentThreshold:       Params.SegmentSize * 1024 * 1024,
		segmentThresholdFactor: Params.SegmentSizeFactor,
		segmentExpireDuration:  Params.SegIDAssignExpiration,
		minimumAssignSize:      Params.MinSegIDAssignCnt * Params.DefaultRecordSize,
		defaultSizePerRecord:   Params.DefaultRecordSize,
		numOfChannels:          Params.TopicNum,
		numOfQueryNodes:        Params.QueryNodeNum,
		globalIDAllocator:      globalIDAllocator,
		globalTSOAllocator:     globalTSOAllocator,
	}
	segMgr.createChannelRanges()
	return segMgr
}
