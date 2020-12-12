package indexbuilder

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/indexbuilderpb"
)

type metaTable struct {
	client       kv.TxnBase                // client of a reliable kv service, i.e. etcd client
	indexID2Meta map[UniqueID]pb.IndexMeta // index id to index meta

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
	mt.indexID2Meta = make(map[UniqueID]pb.IndexMeta)

	_, values, err := mt.client.LoadWithPrefix("indexes")
	if err != nil {
		return err
	}

	for _, value := range values {
		indexMeta := pb.IndexMeta{}
		err = proto.UnmarshalText(value, &indexMeta)
		if err != nil {
			return err
		}
		mt.indexID2Meta[indexMeta.IndexID] = indexMeta
	}
	return nil
}

// metaTable.lock.Lock() before call this function
func (mt *metaTable) saveIndexMeta(meta *pb.IndexMeta) error {
	value := proto.MarshalTextString(meta)

	mt.indexID2Meta[meta.IndexID] = *meta

	return mt.client.Save("/indexes/"+strconv.FormatInt(meta.IndexID, 10), value)
}

func (mt *metaTable) AddIndex(indexID UniqueID, req *pb.BuildIndexRequest) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	_, ok := mt.indexID2Meta[indexID]
	if ok {
		return errors.Errorf("index already exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	meta := &pb.IndexMeta{
		Status:  pb.IndexStatus_UNISSUED,
		IndexID: indexID,
		Req:     req,
	}
	mt.saveIndexMeta(meta)
	return nil
}

func (mt *metaTable) UpdateIndexStatus(indexID UniqueID, status pb.IndexStatus) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	meta.Status = status
	mt.saveIndexMeta(&meta)
	return nil
}

func (mt *metaTable) UpdateIndexEnqueTime(indexID UniqueID, t time.Time) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	meta.EnqueTime = t.UnixNano()
	mt.saveIndexMeta(&meta)
	return nil
}

func (mt *metaTable) UpdateIndexScheduleTime(indexID UniqueID, t time.Time) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	meta.ScheduleTime = t.UnixNano()
	mt.saveIndexMeta(&meta)
	return nil
}

func (mt *metaTable) CompleteIndex(indexID UniqueID, dataPaths []string) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	meta.Status = pb.IndexStatus_FINISHED
	meta.IndexFilePaths = dataPaths
	meta.BuildCompleteTime = time.Now().UnixNano()
	mt.saveIndexMeta(&meta)
	return nil
}

func (mt *metaTable) GetIndexDescription(indexID UniqueID) (*pb.DescribleIndexResponse, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	ret := &pb.DescribleIndexResponse{}
	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return ret, errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	ret.IndexStatus = meta.Status
	ret.EnqueTime = meta.EnqueTime
	ret.BuildCompleteTime = meta.BuildCompleteTime
	ret.ScheduleTime = meta.ScheduleTime
	return ret, nil
}

func (mt *metaTable) GetIndexFilePaths(indexID UniqueID) ([]string, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	meta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return nil, errors.Errorf("index not exists with ID = " + strconv.FormatInt(indexID, 10))
	}
	return meta.IndexFilePaths, nil
}

func (mt *metaTable) DeleteIndex(indexID UniqueID) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	indexMeta, ok := mt.indexID2Meta[indexID]
	if !ok {
		return errors.Errorf("can't find index. id = " + strconv.FormatInt(indexID, 10))
	}
	fmt.Print(indexMeta)

	return nil

}
