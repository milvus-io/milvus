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
	segID2FlushMeta map[UniqueID]pb.SegmentFlushMeta // segment id to flush meta
	collID2DdlMeta  map[UniqueID]*pb.DDLFlushMeta

	lock sync.RWMutex
}

func NewMetaTable(kv kv.TxnBase) (*metaTable, error) {
	mt := &metaTable{
		client: kv,
		lock:   sync.RWMutex{},
	}
	err := mt.reloadSegMetaFromKV()
	if err != nil {
		return nil, err
	}

	err = mt.reloadDdlMetaFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *metaTable) AppendDDLBinlogPaths(collID UniqueID, paths []string) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	_, ok := mt.collID2DdlMeta[collID]
	if !ok {
		mt.collID2DdlMeta[collID] = &pb.DDLFlushMeta{
			CollectionID: collID,
			BinlogPaths:  make([]string, 0),
		}
	}

	meta := mt.collID2DdlMeta[collID]
	meta.BinlogPaths = append(meta.BinlogPaths, paths...)

	return mt.saveDDLFlushMeta(meta)
}

func (mt *metaTable) AppendSegBinlogPaths(tsOpen Timestamp, segmentID UniqueID, fieldID int64, dataPaths []string) error {
	_, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		err := mt.addSegmentFlush(segmentID, tsOpen)
		if err != nil {
			return err
		}
	}

	meta := mt.segID2FlushMeta[segmentID]

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

	return mt.saveSegFlushMeta(&meta)
}

func (mt *metaTable) CompleteFlush(tsClose Timestamp, segmentID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	meta.IsClosed = true
	meta.CloseTime = tsClose

	return mt.saveSegFlushMeta(&meta)
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveDDLFlushMeta(meta *pb.DDLFlushMeta) error {
	value := proto.MarshalTextString(meta)

	mt.collID2DdlMeta[meta.CollectionID] = meta

	return mt.client.Save(Params.WriteNodeDDLKvSubPath+strconv.FormatInt(meta.CollectionID, 10), value)
}

func (mt *metaTable) reloadDdlMetaFromKV() error {
	mt.collID2DdlMeta = make(map[UniqueID]*pb.DDLFlushMeta)
	_, values, err := mt.client.LoadWithPrefix(Params.WriteNodeDDLKvSubPath)
	if err != nil {
		return err
	}

	for _, value := range values {
		ddlMeta := &pb.DDLFlushMeta{}
		err = proto.UnmarshalText(value, ddlMeta)
		if err != nil {
			return err
		}
		mt.collID2DdlMeta[ddlMeta.CollectionID] = ddlMeta
	}
	return nil
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveSegFlushMeta(meta *pb.SegmentFlushMeta) error {
	value := proto.MarshalTextString(meta)

	mt.segID2FlushMeta[meta.SegmentID] = *meta

	return mt.client.Save(Params.WriteNodeSegKvSubPath+strconv.FormatInt(meta.SegmentID, 10), value)
}

func (mt *metaTable) reloadSegMetaFromKV() error {
	mt.segID2FlushMeta = make(map[UniqueID]pb.SegmentFlushMeta)

	_, values, err := mt.client.LoadWithPrefix(Params.WriteNodeSegKvSubPath)
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

func (mt *metaTable) addSegmentFlush(segmentID UniqueID, timestamp Timestamp) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	_, ok := mt.segID2FlushMeta[segmentID]
	if ok {
		return errors.Errorf("segment already exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	meta := pb.SegmentFlushMeta{
		IsClosed:  false,
		SegmentID: segmentID,
		OpenTime:  timestamp,
	}
	return mt.saveSegFlushMeta(&meta)
}

func (mt *metaTable) getFlushCloseTime(segmentID UniqueID) (Timestamp, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return typeutil.ZeroTimestamp, errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	return meta.CloseTime, nil
}

func (mt *metaTable) getFlushOpenTime(segmentID UniqueID) (Timestamp, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return typeutil.ZeroTimestamp, errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	return meta.OpenTime, nil
}

func (mt *metaTable) checkFlushComplete(segmentID UniqueID) (bool, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return false, errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	return meta.IsClosed, nil
}

func (mt *metaTable) getSegBinlogPaths(segmentID UniqueID) (map[int64][]string, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return nil, errors.Errorf("segment not exists with ID = " + strconv.FormatInt(segmentID, 10))
	}
	ret := make(map[int64][]string)
	for _, field := range meta.Fields {
		ret[field.FieldID] = field.BinlogPaths
	}
	return ret, nil
}

func (mt *metaTable) getDDLBinlogPaths(collID UniqueID) (map[UniqueID][]string, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	meta, ok := mt.collID2DdlMeta[collID]
	if !ok {
		return nil, errors.Errorf("collection not exists with ID = " + strconv.FormatInt(collID, 10))
	}
	ret := make(map[UniqueID][]string)
	ret[meta.CollectionID] = meta.BinlogPaths
	return ret, nil
}
