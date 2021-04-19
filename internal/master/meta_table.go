package master

import (
	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"strconv"
	"sync"
)

type metaTable struct {
	client        kv.Base                     // client of a reliable kv service, i.e. etcd client
	tenantId2Meta map[int64]pb.TenantMeta     // tenant id to tenant meta
	proxyId2Meta  map[int64]pb.ProxyMeta      // proxy id to proxy meta
	collId2Meta   map[int64]pb.CollectionMeta // collection id to collection meta
	collName2Id   map[string]int64            // collection name to collection id
	segId2Meta    map[int64]pb.SegmentMeta    // segment id to segment meta

	tenantLock sync.RWMutex
	proxyLock  sync.RWMutex
	ddLock     sync.RWMutex
}

//todo, load meta from etcd
func NewMetaTable(kv kv.Base) (*metaTable, error) {

	return &metaTable{
		client:        kv,
		tenantId2Meta: make(map[int64]pb.TenantMeta),
		proxyId2Meta:  make(map[int64]pb.ProxyMeta),
		collId2Meta:   make(map[int64]pb.CollectionMeta),
		collName2Id:   make(map[string]int64),
		segId2Meta:    make(map[int64]pb.SegmentMeta),
		tenantLock:    sync.RWMutex{},
		proxyLock:     sync.RWMutex{},
		ddLock:        sync.RWMutex{},
	}, nil
}

// mt.ddLock.Lock() before call this function
func (mt *metaTable) saveCollectionMeta(coll *pb.CollectionMeta) error {
	coll_bytes, err := proto.Marshal(coll)
	if err != nil {
		return err
	}
	err = mt.client.Save(strconv.FormatInt(coll.Id, 10), string(coll_bytes))
	if err != nil {
		return err
	}
	mt.collId2Meta[coll.Id] = *coll
	mt.collName2Id[coll.Schema.Name] = coll.Id
	return nil
}

// mt.ddLock.Lock() before call this function
func (mt *metaTable) saveSegmentMeta(seg *pb.SegmentMeta) error {
	seg_bytes, err := proto.Marshal(seg)
	if err != nil {
		return err
	}
	err = mt.client.Save(strconv.FormatInt(seg.SegmentId, 10), string(seg_bytes))
	if err != nil {
		return err
	}
	mt.segId2Meta[seg.SegmentId] = *seg
	return nil
}

func (mt *metaTable) AddCollection(coll *pb.CollectionMeta) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	return mt.saveCollectionMeta(coll)
}

func (mt *metaTable) GetCollectionByName(collectionName string) (*pb.CollectionMeta, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	vid, ok := mt.collName2Id[collectionName]
	if !ok {
		return nil, errors.Errorf("can't find collection: " + collectionName)
	}
	col, ok := mt.collId2Meta[vid]
	if !ok {
		return nil, errors.Errorf("can't find collection: " + collectionName)
	}
	return &col, nil
}

func (mt *metaTable) HasPartition(partitionTag, collectionName string) bool {
	col_meta, err := mt.GetCollectionByName(collectionName)
	if err != nil {
		return false
	}
	for _, tag := range col_meta.PartitionTags {
		if tag == partitionTag {
			return true
		}
	}
	return false
}

func (mt *metaTable) DeletePartition(partitionTag, collectionName string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	col_id, ok := mt.collName2Id[collectionName]
	if !ok {
		return errors.Errorf("can't find collection %s", collectionName)
	}
	col_meta, ok := mt.collId2Meta[col_id]
	if !ok {
		return errors.Errorf("can't find collection %s", collectionName)
	}
	pt := make([]string, 0, len(col_meta.PartitionTags))
	for _, t := range col_meta.PartitionTags {
		if t != partitionTag {
			pt = append(pt, t)
		}
	}
	if len(pt) == len(col_meta.PartitionTags) {
		return nil
	}

	seg := make([]int64, 0, len(col_meta.SegmentIds))
	for _, s := range col_meta.SegmentIds {
		sm, ok := mt.segId2Meta[s]
		if !ok {
			return errors.Errorf("can't find segment id = %d", s)
		}
		if sm.PartitionTag != partitionTag {
			seg = append(seg, s)
		}
	}
	col_meta.PartitionTags = pt
	col_meta.SegmentIds = seg

	return mt.saveCollectionMeta(&col_meta)
}

func (mt *metaTable) AddSegment(seg *pb.SegmentMeta) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	return mt.saveSegmentMeta(seg)
}

func (mt *metaTable) GetSegmentById(segId int64) (*pb.SegmentMeta, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	sm, ok := mt.segId2Meta[segId]
	if !ok {
		return nil, errors.Errorf("can't find segment id = %d", segId)
	}
	return &sm, nil
}
