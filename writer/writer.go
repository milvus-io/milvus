package writer

import (
	"context"
	"github.com/czs007/suvlim/pulsar/schema"
	"github.com/czs007/suvlim/writer/mock"
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
	KvStore              *mock.TikvStore
	timeSyncTable        *writeNodeTimeSync
}

func NewWriteNode(ctx context.Context,
	openSegmentId string,
	closeTime uint64,
	nextSegmentId string,
	nextCloseSegmentTime uint64,
	timeSync uint64) (*writeNode, error) {
	store, err := mock.NewTikvStore()
	writeTableTimeSync := &writeNodeTimeSync{deleteTimeSync: timeSync, insertTimeSync: timeSync}
	if err != nil {
		return nil, err
	}
	return &writeNode{
		KvStore:              store,
		openSegmentId:        openSegmentId,
		nextSegmentId:        nextSegmentId,
		segmentCloseTime:     closeTime,
		nextSegmentCloseTime: nextCloseSegmentTime,
		timeSyncTable:        writeTableTimeSync,
	}, nil
}

func (s *writeNode) InsertBatchData(ctx context.Context, data []*schema.InsertMsg, timeSync uint64) error {
	var i int
	var storeKey string

	var keys [][]byte
	var binaryData [][]byte
	var timeStamps []uint64

	for i = 0; i < len(data); i++ {
		storeKey = data[i].CollectionName + strconv.FormatInt(data[i].EntityId, 10)
		keys = append(keys, []byte(storeKey))
		binaryData = append(binaryData, data[i].Serialization())
		timeStamps = append(timeStamps, data[i].Timestamp)
	}

	if s.segmentCloseTime <= timeSync {
		s.openSegmentId = s.nextSegmentId
		s.segmentCloseTime = s.nextSegmentCloseTime
	}

	err := (*s.KvStore).PutRows(ctx, keys, binaryData, s.openSegmentId, timeStamps)
	s.UpdateInsertTimeSync(timeSync)
	return err
}

func (s *writeNode) DeleteBatchData(ctx context.Context, data []*schema.DeleteMsg, timeSync uint64) error {
	var i int
	var storeKey string

	var keys [][]byte
	var timeStamps []uint64

	for i = 0; i < len(data); i++ {
		storeKey = data[i].CollectionName + strconv.FormatInt(data[i].EntityId, 10)
		keys = append(keys, []byte(storeKey))
		timeStamps = append(timeStamps, data[i].Timestamp)
	}

	segments := (*s.KvStore).GetSegment(ctx, keys)
	mock.DeliverSegmentIds(keys, segments)
	err := (*s.KvStore).DeleteRows(ctx, keys, timeStamps)
	s.UpdateDeleteTimeSync(timeSync)
	return err
}

func (s *writeNode) AddNewSegment(segmentId string, closeSegmentTime uint64) error {
	s.nextSegmentId = segmentId
	s.nextSegmentCloseTime = closeSegmentTime
	return nil
}

func (s *writeNode) UpdateInsertTimeSync(timeSync uint64) {
	s.timeSyncTable.insertTimeSync = timeSync
}

func (s *writeNode) UpdateDeleteTimeSync(timeSync uint64) {
	s.timeSyncTable.deleteTimeSync = timeSync
}

func (s *writeNode) UpdateCloseTime(closeTime uint64) {
	s.segmentCloseTime = closeTime
}
