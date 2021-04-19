package indexbuilder

import (
	"fmt"
	"strconv"
	"sync"

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

func (mt *metaTable) AddIndex(meta *pb.IndexMeta) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	return nil
}

func (mt *metaTable) UpdateIndex(meta *pb.IndexMeta) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	return nil
}

func (mt *metaTable) GetIndexByID(indexID UniqueID) (*pb.IndexMeta, error) {
	mt.lock.RLock()
	defer mt.lock.RUnlock()

	sm, ok := mt.indexID2Meta[indexID]
	if !ok {
		return nil, errors.Errorf("can't find index id = %d", indexID)
	}
	return &sm, nil
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
