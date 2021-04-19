package master

import (
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
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

func NewMetaTable(kv kv.Base) (*metaTable, error) {
	mt := &metaTable{
		client:     kv,
		tenantLock: sync.RWMutex{},
		proxyLock:  sync.RWMutex{},
		ddLock:     sync.RWMutex{},
	}
	err := mt.reloadFromKV()
	if err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *metaTable) reloadFromKV() error {

	mt.tenantId2Meta = make(map[int64]pb.TenantMeta)
	mt.proxyId2Meta = make(map[int64]pb.ProxyMeta)
	mt.collId2Meta = make(map[int64]pb.CollectionMeta)
	mt.collName2Id = make(map[string]int64)
	mt.segId2Meta = make(map[int64]pb.SegmentMeta)

	_, values, err := mt.client.LoadWithPrefix("tenant")
	if err != nil {
		return err
	}

	for _, value := range values {
		tenant_meta := pb.TenantMeta{}
		err := proto.Unmarshal([]byte(value), &tenant_meta)
		if err != nil {
			return err
		}
		mt.tenantId2Meta[tenant_meta.Id] = tenant_meta
	}

	_, values, err = mt.client.LoadWithPrefix("proxy")
	if err != nil {
		return err
	}

	for _, value := range values {
		proxy_meta := pb.ProxyMeta{}
		err = proto.Unmarshal([]byte(value), &proxy_meta)
		if err != nil {
			return err
		}
		mt.proxyId2Meta[proxy_meta.Id] = proxy_meta
	}

	_, values, err = mt.client.LoadWithPrefix("collection")
	if err != nil {
		return err
	}

	for _, value := range values {
		collection_meta := pb.CollectionMeta{}
		err = proto.Unmarshal([]byte(value), &collection_meta)
		if err != nil {
			return err
		}
		mt.collId2Meta[collection_meta.Id] = collection_meta
		mt.collName2Id[collection_meta.Schema.Name] = collection_meta.Id
	}

	_, values, err = mt.client.LoadWithPrefix("segment")
	if err != nil {
		return err
	}

	for _, value := range values {
		segment_meta := pb.SegmentMeta{}
		err = proto.Unmarshal([]byte(value), &segment_meta)
		if err != nil {
			return err
		}
		mt.segId2Meta[segment_meta.SegmentId] = segment_meta
	}

	return nil
}

// mt.ddLock.Lock() before call this function
func (mt *metaTable) saveCollectionMeta(coll *pb.CollectionMeta) error {
	coll_bytes, err := proto.Marshal(coll)
	if err != nil {
		return err
	}
	mt.collId2Meta[coll.Id] = *coll
	mt.collName2Id[coll.Schema.Name] = coll.Id
	return mt.client.Save("/collection/"+strconv.FormatInt(coll.Id, 10), string(coll_bytes))
}

// mt.ddLock.Lock() before call this function
func (mt *metaTable) saveSegmentMeta(seg *pb.SegmentMeta) error {
	seg_bytes, err := proto.Marshal(seg)
	if err != nil {
		return err
	}

	mt.segId2Meta[seg.SegmentId] = *seg

	return mt.client.Save("/segment/"+strconv.FormatInt(seg.SegmentId, 10), string(seg_bytes))
}

// mt.ddLock.Lock() before call this function
func (mt *metaTable) deleteSegmentMeta(segId int64) error {
	_, ok := mt.segId2Meta[segId]

	if ok {
		delete(mt.segId2Meta, segId)
	}

	return mt.client.Remove("/segment/" + strconv.FormatInt(segId, 10))
}

// mt.ddLock.Lock() before call this function
func (mt *metaTable) saveCollectionAndDeleteSegmentsMeta(coll *pb.CollectionMeta, segIds []int64) error {
	segIdStrs := make([]string, 0, len(segIds))
	for _, segId := range segIds {
		segIdStrs = append(segIdStrs, "/segment/"+strconv.FormatInt(segId, 10))
	}

	kvs := make(map[string]string)
	collStrs, err := proto.Marshal(coll)
	if err != nil {
		return err
	}

	kvs["/collection/"+strconv.FormatInt(coll.Id, 10)] = string(collStrs)

	for _, segId := range segIds {
		_, ok := mt.segId2Meta[segId]

		if ok {
			delete(mt.segId2Meta, segId)
		}
	}

	mt.collId2Meta[coll.Id] = *coll

	return mt.client.MultiSaveAndRemove(kvs, segIdStrs)
}

// mt.ddLock.Lock() before call this function
func (mt *metaTable) saveCollectionsAndSegmentsMeta(coll *pb.CollectionMeta, seg *pb.SegmentMeta) error {
	kvs := make(map[string]string, 0)
	coll_bytes, err := proto.Marshal(coll)
	if err != nil {
		return err
	}
	kvs["/collection/"+strconv.FormatInt(coll.Id, 10)] = string(coll_bytes)

	mt.collId2Meta[coll.Id] = *coll
	mt.collName2Id[coll.Schema.Name] = coll.Id

	seg_bytes, err := proto.Marshal(seg)
	if err != nil {
		return err
	}
	kvs["/segment/"+strconv.FormatInt(seg.SegmentId, 10)] = string(seg_bytes)

	mt.segId2Meta[seg.SegmentId] = *seg

	return mt.client.MultiSave(kvs)
}

// mt.ddLock.Lock() before call this function
func (mt *metaTable) deleteCollectionsAndSegmentsMeta(collId int64, segIds []int64) error {
	collIdStr := "/collection/" + strconv.FormatInt(collId, 10)

	totalIdStrs := make([]string, 0, 1+len(segIds))
	totalIdStrs = append(totalIdStrs, collIdStr)
	for _, singleId := range segIds {
		totalIdStrs = append(totalIdStrs, "/segment/"+strconv.FormatInt(singleId, 10))
	}

	coll_meta, ok := mt.collId2Meta[collId]

	if ok {
		delete(mt.collId2Meta, collId)
	}

	_, ok = mt.collName2Id[coll_meta.Schema.Name]

	if ok {
		delete(mt.collName2Id, coll_meta.Schema.Name)
	}

	for _, segId := range segIds {
		_, ok := mt.segId2Meta[segId]

		if ok {
			delete(mt.segId2Meta, segId)
		}
	}

	return mt.client.MultiRemove(totalIdStrs)
}

func (mt *metaTable) AddCollection(coll *pb.CollectionMeta) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	if len(coll.SegmentIds) != 0 {
		return errors.Errorf("segment should be empty when creating collection")
	}
	if len(coll.PartitionTags) != 0 {
		return errors.Errorf("segment should be empty when creating collection")
	}
	_, ok := mt.collName2Id[coll.Schema.Name]
	if ok {
		return errors.Errorf("collection alread exists with name = " + coll.Schema.Name)
	}
	err := mt.saveCollectionMeta(coll)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) DeleteCollection(collId int64) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	coll_meta, ok := mt.collId2Meta[collId]
	if !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(collId, 10))
	}

	err := mt.deleteCollectionsAndSegmentsMeta(collId, coll_meta.SegmentIds)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) HasCollection(collId int64) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	_, ok := mt.collId2Meta[collId]
	if !ok {
		return false
	}
	return true
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

func (mt *metaTable) AddPartition(collId int64, tag string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	coll, ok := mt.collId2Meta[collId]
	if !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(collId, 10))
	}

	for _, t := range coll.PartitionTags {
		if t == tag {
			return errors.Errorf("partition already exists.")
		}
	}
	coll.PartitionTags = append(coll.PartitionTags, tag)

	err := mt.saveCollectionMeta(&coll)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) HasPartition(collId int64, tag string) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	col, ok := mt.collId2Meta[collId]
	if !ok {
		return false
	}
	for _, partitionTag := range col.PartitionTags {
		if partitionTag == tag {
			return true
		}
	}
	return false
}

func (mt *metaTable) DeletePartition(collId int64, tag string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	coll_meta, ok := mt.collId2Meta[collId]
	if !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(collId, 10))
	}

	pt := make([]string, 0, len(coll_meta.PartitionTags))
	for _, t := range coll_meta.PartitionTags {
		if t != tag {
			pt = append(pt, t)
		}
	}
	if len(pt) == len(coll_meta.PartitionTags) {
		return nil
	}

	to_delete_seg := make([]int64, 0, len(coll_meta.SegmentIds))
	seg := make([]int64, 0, len(coll_meta.SegmentIds))
	for _, s := range coll_meta.SegmentIds {
		sm, ok := mt.segId2Meta[s]
		if !ok {
			return errors.Errorf("can't find segment id = %d", s)
		}
		if sm.PartitionTag != tag {
			seg = append(seg, s)
		} else {
			to_delete_seg = append(to_delete_seg, s)
		}
	}
	coll_meta.PartitionTags = pt
	coll_meta.SegmentIds = seg

	err := mt.saveCollectionAndDeleteSegmentsMeta(&coll_meta, to_delete_seg)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) AddSegment(seg *pb.SegmentMeta) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	collId := seg.CollectionId
	coll_meta := mt.collId2Meta[collId]
	coll_meta.SegmentIds = append(coll_meta.SegmentIds, seg.SegmentId)
	err := mt.saveCollectionsAndSegmentsMeta(&coll_meta, seg)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
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

func (mt *metaTable) DeleteSegment(segId int64) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	seg_meta, ok := mt.segId2Meta[segId]
	if !ok {
		return errors.Errorf("can't find segment. id = " + strconv.FormatInt(segId, 10))
	}

	coll_meta, ok := mt.collId2Meta[seg_meta.CollectionId]
	if !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(seg_meta.CollectionId, 10))
	}

	for i := 0; i < len(coll_meta.SegmentIds); i++ {
		if coll_meta.SegmentIds[i] == segId {
			coll_meta.SegmentIds = append(coll_meta.SegmentIds[:i], coll_meta.SegmentIds[i+1:]...)
		}
	}

	err := mt.saveCollectionAndDeleteSegmentsMeta(&coll_meta, []int64{segId})
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil

}
func (mt *metaTable) CloseSegment(segId int64, closeTs Timestamp, num_rows int64) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	seg_meta, ok := mt.segId2Meta[segId]
	if !ok {
		return errors.Errorf("can't find segment id = " + strconv.FormatInt(segId, 10))
	}

	seg_meta.CloseTime = uint64(closeTs)
	seg_meta.NumRows = num_rows

	err := mt.saveSegmentMeta(&seg_meta)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}
