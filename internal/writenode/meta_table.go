package writenode

import (
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/writerpb"
)

type metaTable struct {
	client          kv.TxnBase                       // client of a reliable kv service, i.e. etcd client
	segID2FlushMeta map[UniqueID]pb.SegmentFlushMeta // index id to index meta

	lock sync.RWMutex
}

func NewMetaTable(kv kv.TxnBase) (*metaTable, error) {
	mt := &metaTable{
		client: kv,
		lock:   sync.RWMutex{},
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *metaTable) reloadFromKV() error {
	mt.segID2FlushMeta = make(map[UniqueID]pb.SegmentFlushMeta)

	_, values, err := mt.client.LoadWithPrefix("writer/segment")
	if err != nil {
		return err
	}

	for _, value := range values {
		flushMeta := pb.SegmentFlushMeta{}
		err = proto.UnmarshalText(value, &flushMeta)
		if err != nil {
			return err
		}
		mt.segID2FlushMeta[flushMeta.SegmentID] = flushMeta
	}
	return nil
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveFlushMeta(meta *pb.SegmentFlushMeta) error {
	value := proto.MarshalTextString(meta)

	mt.segID2FlushMeta[meta.SegmentID] = *meta

	return mt.client.Save("/writer/segment/"+strconv.FormatInt(meta.SegmentID, 10), value)
}

func (mt *metaTable) AddSegmentFlush(segmentID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	_, ok := mt.segID2FlushMeta[segmentID]
	if ok {
		return errors.Errorf("segment already exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	meta := pb.SegmentFlushMeta{
		IsClosed:  false,
		SegmentID: segmentID,
	}
	return mt.saveFlushMeta(&meta)
}

func (mt *metaTable) getFlushCloseTime(segmentID UniqueID) (Timestamp, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return typeutil.ZeroTimestamp, errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	return meta.CloseTime, nil
}

func (mt *metaTable) SetFlushCloseTime(segmentID UniqueID, t Timestamp) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	meta.CloseTime = t
	return mt.saveFlushMeta(&meta)
}

func (mt *metaTable) SetFlushOpenTime(segmentID UniqueID, t Timestamp) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	meta.OpenTime = t
	return mt.saveFlushMeta(&meta)
}

func (mt *metaTable) getFlushOpenTime(segmentID UniqueID) (Timestamp, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return typeutil.ZeroTimestamp, errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	return meta.OpenTime, nil
}

func (mt *metaTable) CompleteFlush(segmentID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	meta.IsClosed = true
	return mt.saveFlushMeta(&meta)
}

func (mt *metaTable) checkFlushComplete(segmentID UniqueID) (bool, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return false, errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	return meta.IsClosed, nil
}

func (mt *metaTable) AppendBinlogPaths(segmentID UniqueID, fieldID int32, dataPaths []string) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	found := false
	for _, field := range meta.Fields {
		if field.FieldID == fieldID {
			field.BinlogPaths = append(field.BinlogPaths, dataPaths...)
			found = true
			break
		}
	}
	if !found {
		newField := &pb.FieldFlushMeta{
			FieldID:     fieldID,
			BinlogPaths: dataPaths,
		}
		meta.Fields = append(meta.Fields, newField)
	}

	return mt.saveFlushMeta(&meta)
}

func (mt *metaTable) getBinlogPaths(segmentID UniqueID) (map[int32][]string, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return nil, errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	ret := make(map[int32][]string)
	for _, field := range meta.Fields {
		ret[field.FieldID] = field.BinlogPaths
	}
	return ret, nil
}
