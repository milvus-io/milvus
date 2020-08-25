package writer

import (
	"context"
	"github.com/czs007/suvlim/pulsar/schema"
	"github.com/czs007/suvlim/storage/pkg"
	"github.com/czs007/suvlim/storage/pkg/types"
	"strconv"
)

type writeNodeTimeSync struct {
	deleteTimeSync uint64
	insertTimeSync uint64
}

type writeNode struct {
	openSegmentId        string
	segmentCloseTime     uint64
	nextSegmentId        string
	nextSegmentCloseTime uint64
	kvStore              *types.Store
	timeSyncTable        *writeNodeTimeSync
}

func NewWriteNode(ctx context.Context,
	openSegmentId string,
	timeSync uint64,
	closeTime uint64,
	nextSegmentId string,
	nextCloseSegmentTime uint64) (*writeNode, error) {
	ctx = context.Background()
	store, err := storage.NewStore(ctx, "TIKV")
	writeTableTimeSync := &writeNodeTimeSync{deleteTimeSync: timeSync, insertTimeSync: timeSync}
	if err != nil {
		return nil, err
	}
	return &writeNode{
		kvStore:              store,
		openSegmentId:        openSegmentId,
		nextSegmentId:        nextSegmentId,
		segmentCloseTime:     closeTime,
		nextSegmentCloseTime: nextCloseSegmentTime,
		timeSyncTable:        writeTableTimeSync,
	}, nil
}

func (s *writeNode) InsertBatchData(ctx context.Context, data []schema.InsertMsg, time_sync uint64) error {
	var i int
	var storeKey string

	var keys [][]byte
	var binaryData [][]byte
	var timeStamps []uint64

	for i = 0; i < cap(data); i++ {
		storeKey = data[i].CollectionName + strconv.FormatInt(data[i].EntityId, 10)
		keys = append(keys, []byte(storeKey))
		binaryData = append(binaryData, data[i].Serialization())
		timeStamps = append(timeStamps, data[i].Timestamp)
	}

	if s.segmentCloseTime <= time_sync {
		s.openSegmentId = s.nextSegmentId
		s.segmentCloseTime = s.nextSegmentCloseTime
	}

	(*s.kvStore).PutRows(ctx, keys, binaryData, s.openSegmentId, timeStamps)
	s.UpdateInsertTimeSync(time_sync)
	return nil
}

func (s *writeNode) DeleteBatchData(ctx context.Context, data []schema.DeleteMsg, time_sync uint64) error {
	return nil
}

func (s *writeNode) AddNewSegment(segment_id string, close_segment_time uint64) error {
	s.nextSegmentId = segment_id
	s.nextSegmentCloseTime = close_segment_time
	return nil
}

func (s *writeNode) UpdateInsertTimeSync(time_sync uint64) {
	s.timeSyncTable.insertTimeSync = time_sync
}

func (s *writeNode) UpdateDeleteTimeSync(time_sync uint64) {
	s.timeSyncTable.deleteTimeSync = time_sync
}
