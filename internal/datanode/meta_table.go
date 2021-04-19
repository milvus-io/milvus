package datanode

import (
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
)

type metaTable struct {
	client          kv.Base //
	segID2FlushMeta map[UniqueID]*datapb.SegmentFlushMeta
	collID2DdlMeta  map[UniqueID]*datapb.DDLFlushMeta

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

func (mt *metaTable) AppendSegBinlogPaths(segmentID UniqueID, fieldID int64, dataPaths []string) error {
	_, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		err := mt.addSegmentFlush(segmentID)
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
		newField := &datapb.FieldFlushMeta{
			FieldID:     fieldID,
			BinlogPaths: dataPaths,
		}
		meta.Fields = append(meta.Fields, newField)
	}

	return mt.saveSegFlushMeta(meta)
}

func (mt *metaTable) CompleteFlush(segmentID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return fmt.Errorf("segment not exists with ID = %v", segmentID)
	}
	meta.IsFlushed = true

	return mt.saveSegFlushMeta(meta)
}

func (mt *metaTable) reloadSegMetaFromKV() error {
	mt.segID2FlushMeta = make(map[UniqueID]*datapb.SegmentFlushMeta)

	_, values, err := mt.client.LoadWithPrefix(Params.SegFlushMetaSubPath)
	if err != nil {
		return err
	}

	for _, value := range values {
		flushMeta := &datapb.SegmentFlushMeta{}
		err = proto.UnmarshalText(value, flushMeta)
		if err != nil {
			return err
		}
		mt.segID2FlushMeta[flushMeta.SegmentID] = flushMeta
	}

	return nil
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveSegFlushMeta(meta *datapb.SegmentFlushMeta) error {
	value := proto.MarshalTextString(meta)

	mt.segID2FlushMeta[meta.SegmentID] = meta
	prefix := path.Join(Params.SegFlushMetaSubPath, strconv.FormatInt(meta.SegmentID, 10))

	return mt.client.Save(prefix, value)
}

func (mt *metaTable) addSegmentFlush(segmentID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta := &datapb.SegmentFlushMeta{
		IsFlushed: false,
		SegmentID: segmentID,
	}
	return mt.saveSegFlushMeta(meta)
}

func (mt *metaTable) hasSegmentFlush(segmentID UniqueID) bool {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	_, ok := mt.segID2FlushMeta[segmentID]
	return ok
}

func (mt *metaTable) checkFlushComplete(segmentID UniqueID) (bool, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return false, fmt.Errorf("segment not exists with ID = %v", segmentID)
	}
	return meta.IsFlushed, nil
}

func (mt *metaTable) getSegBinlogPaths(segmentID UniqueID) (map[int64][]string, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	meta, ok := mt.segID2FlushMeta[segmentID]
	if !ok {
		return nil, fmt.Errorf("segment not exists with ID = %v", segmentID)
	}
	ret := make(map[int64][]string)
	for _, field := range meta.Fields {
		ret[field.FieldID] = field.BinlogPaths
	}
	return ret, nil
}

// --- DDL ---
func (mt *metaTable) AppendDDLBinlogPaths(collID UniqueID, paths []string) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	_, ok := mt.collID2DdlMeta[collID]
	if !ok {
		mt.collID2DdlMeta[collID] = &datapb.DDLFlushMeta{
			CollectionID: collID,
			BinlogPaths:  make([]string, 0),
		}
	}

	meta := mt.collID2DdlMeta[collID]
	meta.BinlogPaths = append(meta.BinlogPaths, paths...)

	return mt.saveDDLFlushMeta(meta)
}

func (mt *metaTable) hasDDLFlushMeta(collID UniqueID) bool {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	_, ok := mt.collID2DdlMeta[collID]
	return ok
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveDDLFlushMeta(meta *datapb.DDLFlushMeta) error {
	value := proto.MarshalTextString(meta)

	mt.collID2DdlMeta[meta.CollectionID] = meta
	prefix := path.Join(Params.DDLFlushMetaSubPath, strconv.FormatInt(meta.CollectionID, 10))

	return mt.client.Save(prefix, value)
}

func (mt *metaTable) reloadDdlMetaFromKV() error {
	mt.collID2DdlMeta = make(map[UniqueID]*datapb.DDLFlushMeta)
	_, values, err := mt.client.LoadWithPrefix(Params.DDLFlushMetaSubPath)
	if err != nil {
		return err
	}

	for _, value := range values {
		ddlMeta := &datapb.DDLFlushMeta{}
		err = proto.UnmarshalText(value, ddlMeta)
		if err != nil {
			return err
		}
		mt.collID2DdlMeta[ddlMeta.CollectionID] = ddlMeta
	}
	return nil
}

func (mt *metaTable) getDDLBinlogPaths(collID UniqueID) (map[UniqueID][]string, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()
	meta, ok := mt.collID2DdlMeta[collID]
	if !ok {
		return nil, fmt.Errorf("collection not exists with ID = %v", collID)
	}
	ret := make(map[UniqueID][]string)
	ret[meta.CollectionID] = meta.BinlogPaths
	return ret, nil
}
