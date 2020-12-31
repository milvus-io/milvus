package master

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
)

type metaTable struct {
	client           kv.TxnBase                       // client of a reliable kv service, i.e. etcd client
	tenantID2Meta    map[UniqueID]pb.TenantMeta       // tenant id to tenant meta
	proxyID2Meta     map[UniqueID]pb.ProxyMeta        // proxy id to proxy meta
	collID2Meta      map[UniqueID]pb.CollectionMeta   // collection id to collection meta
	collName2ID      map[string]UniqueID              // collection name to collection id
	segID2Meta       map[UniqueID]pb.SegmentMeta      // segment id to segment meta
	segID2IndexMetas map[UniqueID][]pb.FieldIndexMeta // segment id to array of field index meta

	tenantLock sync.RWMutex
	proxyLock  sync.RWMutex
	ddLock     sync.RWMutex
	indexLock  sync.RWMutex
}

func NewMetaTable(kv kv.TxnBase) (*metaTable, error) {
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

	mt.tenantID2Meta = make(map[UniqueID]pb.TenantMeta)
	mt.proxyID2Meta = make(map[UniqueID]pb.ProxyMeta)
	mt.collID2Meta = make(map[UniqueID]pb.CollectionMeta)
	mt.collName2ID = make(map[string]UniqueID)
	mt.segID2Meta = make(map[UniqueID]pb.SegmentMeta)
	mt.segID2IndexMetas = make(map[UniqueID][]pb.FieldIndexMeta)

	_, values, err := mt.client.LoadWithPrefix("tenant")
	if err != nil {
		return err
	}

	for _, value := range values {
		tenantMeta := pb.TenantMeta{}
		err := proto.UnmarshalText(value, &tenantMeta)
		if err != nil {
			return err
		}
		mt.tenantID2Meta[tenantMeta.ID] = tenantMeta
	}

	_, values, err = mt.client.LoadWithPrefix("proxy")
	if err != nil {
		return err
	}

	for _, value := range values {
		proxyMeta := pb.ProxyMeta{}
		err = proto.UnmarshalText(value, &proxyMeta)
		if err != nil {
			return err
		}
		mt.proxyID2Meta[proxyMeta.ID] = proxyMeta
	}

	_, values, err = mt.client.LoadWithPrefix("collection")
	if err != nil {
		return err
	}

	for _, value := range values {
		collectionMeta := pb.CollectionMeta{}
		err = proto.UnmarshalText(value, &collectionMeta)
		if err != nil {
			return err
		}
		mt.collID2Meta[collectionMeta.ID] = collectionMeta
		mt.collName2ID[collectionMeta.Schema.Name] = collectionMeta.ID
	}

	_, values, err = mt.client.LoadWithPrefix("segment")
	if err != nil {
		return err
	}

	for _, value := range values {
		segmentMeta := pb.SegmentMeta{}
		err = proto.UnmarshalText(value, &segmentMeta)
		if err != nil {
			return err
		}
		mt.segID2Meta[segmentMeta.SegmentID] = segmentMeta
	}

	_, values, err = mt.client.LoadWithPrefix("indexmeta")
	if err != nil {
		return err
	}

	for _, v := range values {
		indexMeta := pb.FieldIndexMeta{}
		err = proto.UnmarshalText(v, &indexMeta)
		if err != nil {
			return err
		}
		mt.segID2IndexMetas[indexMeta.SegmentID] = append(mt.segID2IndexMetas[indexMeta.SegmentID], indexMeta)
	}
	return nil
}

// metaTable.ddLock.Lock() before call this function
func (mt *metaTable) saveCollectionMeta(coll *pb.CollectionMeta) error {
	collBytes := proto.MarshalTextString(coll)
	mt.collID2Meta[coll.ID] = *coll
	mt.collName2ID[coll.Schema.Name] = coll.ID
	return mt.client.Save("/collection/"+strconv.FormatInt(coll.ID, 10), collBytes)
}

// metaTable.ddLock.Lock() before call this function
func (mt *metaTable) saveSegmentMeta(seg *pb.SegmentMeta) error {
	segBytes := proto.MarshalTextString(seg)

	mt.segID2Meta[seg.SegmentID] = *seg

	return mt.client.Save("/segment/"+strconv.FormatInt(seg.SegmentID, 10), segBytes)
}

// metaTable.ddLock.Lock() before call this function
func (mt *metaTable) saveCollectionAndDeleteSegmentsMeta(coll *pb.CollectionMeta, segIDs []UniqueID) error {
	segIDStrs := make([]string, 0, len(segIDs))
	for _, segID := range segIDs {
		segIDStrs = append(segIDStrs, "/segment/"+strconv.FormatInt(segID, 10))
	}

	kvs := make(map[string]string)
	collStrs := proto.MarshalTextString(coll)

	kvs["/collection/"+strconv.FormatInt(coll.ID, 10)] = collStrs

	for _, segID := range segIDs {
		_, ok := mt.segID2Meta[segID]

		if ok {
			delete(mt.segID2Meta, segID)
		}
	}

	mt.collID2Meta[coll.ID] = *coll

	return mt.client.MultiSaveAndRemove(kvs, segIDStrs)
}

// metaTable.ddLock.Lock() before call this function
func (mt *metaTable) saveCollectionsAndSegmentsMeta(coll *pb.CollectionMeta, seg *pb.SegmentMeta) error {
	kvs := make(map[string]string)
	collBytes := proto.MarshalTextString(coll)

	kvs["/collection/"+strconv.FormatInt(coll.ID, 10)] = collBytes

	mt.collID2Meta[coll.ID] = *coll
	mt.collName2ID[coll.Schema.Name] = coll.ID

	segBytes := proto.MarshalTextString(seg)

	kvs["/segment/"+strconv.FormatInt(seg.SegmentID, 10)] = segBytes

	mt.segID2Meta[seg.SegmentID] = *seg

	return mt.client.MultiSave(kvs)
}

// metaTable.ddLock.Lock() before call this function
func (mt *metaTable) deleteCollectionsAndSegmentsMeta(collID UniqueID, segIDs []UniqueID) error {
	collIDStr := "/collection/" + strconv.FormatInt(collID, 10)

	totalIDStrs := make([]string, 0, 1+len(segIDs))
	totalIDStrs = append(totalIDStrs, collIDStr)
	for _, singleID := range segIDs {
		totalIDStrs = append(totalIDStrs, "/segment/"+strconv.FormatInt(singleID, 10))
	}

	collMeta, ok := mt.collID2Meta[collID]

	if ok {
		delete(mt.collID2Meta, collID)
	}

	_, ok = mt.collName2ID[collMeta.Schema.Name]

	if ok {
		delete(mt.collName2ID, collMeta.Schema.Name)
	}

	for _, segID := range segIDs {
		_, ok := mt.segID2Meta[segID]

		if ok {
			delete(mt.segID2Meta, segID)
		}
	}

	return mt.client.MultiRemove(totalIDStrs)
}

func (mt *metaTable) AddCollection(coll *pb.CollectionMeta) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	if len(coll.SegmentIDs) != 0 {
		return errors.Errorf("segment should be empty when creating collection")
	}

	if len(coll.PartitionTags) == 0 {
		coll.PartitionTags = append(coll.PartitionTags, Params.DefaultPartitionTag)
	}
	_, ok := mt.collName2ID[coll.Schema.Name]
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

func (mt *metaTable) DeleteCollection(collID UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(collID, 10))
	}

	err := mt.deleteCollectionsAndSegmentsMeta(collID, collMeta.SegmentIDs)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}

	// remove index meta
	for _, v := range collMeta.SegmentIDs {
		if err := mt.removeSegmentIndexMeta(v); err != nil {
			return err
		}
	}
	return nil
}

func (mt *metaTable) HasCollection(collID UniqueID) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	_, ok := mt.collID2Meta[collID]
	return ok
}

func (mt *metaTable) GetCollectionByName(collectionName string) (*pb.CollectionMeta, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	vid, ok := mt.collName2ID[collectionName]
	if !ok {
		return nil, errors.Errorf("can't find collection: " + collectionName)
	}
	col, ok := mt.collID2Meta[vid]
	if !ok {
		return nil, errors.Errorf("can't find collection: " + collectionName)
	}
	return &col, nil
}

func (mt *metaTable) ListCollections() ([]string, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	colls := make([]string, 0, len(mt.collName2ID))
	for name := range mt.collName2ID {
		colls = append(colls, name)
	}
	return colls, nil
}

func (mt *metaTable) AddPartition(collID UniqueID, tag string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	coll, ok := mt.collID2Meta[collID]
	if !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(collID, 10))
	}

	// number of partition tags (except _default) should be limited to 4096 by default
	if int64(len(coll.PartitionTags)) > Params.MaxPartitionNum {
		return errors.New("maximum partition's number should be limit to " + strconv.FormatInt(Params.MaxPartitionNum, 10))
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

func (mt *metaTable) HasPartition(collID UniqueID, tag string) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	col, ok := mt.collID2Meta[collID]
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

func (mt *metaTable) DeletePartition(collID UniqueID, tag string) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if tag == Params.DefaultPartitionTag {
		return errors.New("default partition cannot be deleted")
	}

	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(collID, 10))
	}

	// check tag exists
	exist := false

	pt := make([]string, 0, len(collMeta.PartitionTags))
	for _, t := range collMeta.PartitionTags {
		if t != tag {
			pt = append(pt, t)
		} else {
			exist = true
		}
	}
	if !exist {
		return errors.New("partition " + tag + " does not exist")
	}
	if len(pt) == len(collMeta.PartitionTags) {
		return nil
	}

	toDeleteSeg := make([]UniqueID, 0, len(collMeta.SegmentIDs))
	seg := make([]UniqueID, 0, len(collMeta.SegmentIDs))
	for _, s := range collMeta.SegmentIDs {
		sm, ok := mt.segID2Meta[s]
		if !ok {
			return errors.Errorf("DeletePartition:can't find segment id = %d", s)
		}
		if sm.PartitionTag != tag {
			seg = append(seg, s)
		} else {
			toDeleteSeg = append(toDeleteSeg, s)
		}
	}
	collMeta.PartitionTags = pt
	collMeta.SegmentIDs = seg

	err := mt.saveCollectionAndDeleteSegmentsMeta(&collMeta, toDeleteSeg)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) AddSegment(seg *pb.SegmentMeta) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	collID := seg.CollectionID
	collMeta := mt.collID2Meta[collID]
	collMeta.SegmentIDs = append(collMeta.SegmentIDs, seg.SegmentID)
	err := mt.saveCollectionsAndSegmentsMeta(&collMeta, seg)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) UpdateSegment(seg *pb.SegmentMeta) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collID := seg.CollectionID
	collMeta := mt.collID2Meta[collID]
	isNewSegID := true
	for _, segID := range collMeta.SegmentIDs {
		if segID == seg.SegmentID {
			isNewSegID = false
			break
		}
	}
	if isNewSegID {
		collMeta.SegmentIDs = append(collMeta.SegmentIDs, seg.SegmentID)
		if err := mt.saveCollectionsAndSegmentsMeta(&collMeta, seg); err != nil {
			_ = mt.reloadFromKV()
			return err
		}
	} else {
		if err := mt.saveSegmentMeta(seg); err != nil {
			_ = mt.reloadFromKV()
			return err
		}
	}
	return nil
}

func (mt *metaTable) GetSegmentByID(segID UniqueID) (*pb.SegmentMeta, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	sm, ok := mt.segID2Meta[segID]
	if !ok {
		return nil, errors.Errorf("GetSegmentByID:can't find segment id = %d", segID)
	}
	return &sm, nil
}

func (mt *metaTable) DeleteSegment(segID UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	segMeta, ok := mt.segID2Meta[segID]
	if !ok {
		return errors.Errorf("DeleteSegment:can't find segment. id = " + strconv.FormatInt(segID, 10))
	}

	collMeta, ok := mt.collID2Meta[segMeta.CollectionID]
	if !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(segMeta.CollectionID, 10))
	}

	for i := 0; i < len(collMeta.SegmentIDs); i++ {
		if collMeta.SegmentIDs[i] == segID {
			collMeta.SegmentIDs = append(collMeta.SegmentIDs[:i], collMeta.SegmentIDs[i+1:]...)
		}
	}

	err := mt.saveCollectionAndDeleteSegmentsMeta(&collMeta, []UniqueID{segID})
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}

	return mt.removeSegmentIndexMeta(segID)
}
func (mt *metaTable) CloseSegment(segID UniqueID, closeTs Timestamp) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	segMeta, ok := mt.segID2Meta[segID]
	if !ok {
		return errors.Errorf("CloseSegment:can't find segment id = " + strconv.FormatInt(segID, 10))
	}

	segMeta.CloseTime = closeTs

	err := mt.saveSegmentMeta(&segMeta)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) AddFieldIndexMeta(meta *pb.FieldIndexMeta) error {
	mt.indexLock.Lock()
	defer mt.indexLock.Unlock()

	segID := meta.SegmentID
	if _, ok := mt.segID2IndexMetas[segID]; !ok {
		mt.segID2IndexMetas[segID] = make([]pb.FieldIndexMeta, 0)
	}
	for _, v := range mt.segID2IndexMetas[segID] {
		if v.FieldID == meta.FieldID && typeutil.CompareIndexParams(v.IndexParams, meta.IndexParams) {
			return fmt.Errorf("segment %d field id %d's index meta already exist", segID, meta.FieldID)
		}
	}
	mt.segID2IndexMetas[segID] = append(mt.segID2IndexMetas[segID], *meta)
	err := mt.saveFieldIndexMetaToEtcd(meta)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) saveFieldIndexMetaToEtcd(meta *pb.FieldIndexMeta) error {
	key := "/indexmeta/" + strconv.FormatInt(meta.SegmentID, 10) + strconv.FormatInt(meta.FieldID, 10) + strconv.FormatInt(meta.IndexID, 10)
	marshaledMeta := proto.MarshalTextString(meta)
	return mt.client.Save(key, marshaledMeta)
}

func (mt *metaTable) DeleteFieldIndexMeta(segID UniqueID, fieldID UniqueID, indexParams []*commonpb.KeyValuePair) error {
	mt.indexLock.Lock()
	defer mt.indexLock.Unlock()

	if _, ok := mt.segID2IndexMetas[segID]; !ok {
		return fmt.Errorf("can not find index meta of segment %d", segID)
	}

	for i, v := range mt.segID2IndexMetas[segID] {
		if v.FieldID == fieldID && typeutil.CompareIndexParams(v.IndexParams, indexParams) {
			mt.segID2IndexMetas[segID] = append(mt.segID2IndexMetas[segID][:i], mt.segID2IndexMetas[segID][i+1:]...)
			err := mt.deleteFieldIndexMetaToEtcd(segID, fieldID, v.IndexID)
			if err != nil {
				_ = mt.reloadFromKV()
				return err
			}
			return nil
		}
	}

	return fmt.Errorf("can not find index meta of field %d", fieldID)
}

func (mt *metaTable) deleteFieldIndexMetaToEtcd(segID UniqueID, fieldID UniqueID, indexID UniqueID) error {
	key := "/indexmeta/" + strconv.FormatInt(segID, 10) + strconv.FormatInt(fieldID, 10) + strconv.FormatInt(indexID, 10)
	return mt.client.Remove(key)
}

func (mt *metaTable) HasFieldIndexMeta(segID UniqueID, fieldID UniqueID, indexParams []*commonpb.KeyValuePair) (bool, error) {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	if _, ok := mt.segID2IndexMetas[segID]; !ok {
		return false, nil
	}

	for _, v := range mt.segID2IndexMetas[segID] {
		if v.FieldID == fieldID && typeutil.CompareIndexParams(v.IndexParams, indexParams) {
			return true, nil
		}
	}
	return false, nil
}

func (mt *metaTable) GetFieldIndexMeta(segID UniqueID, fieldID UniqueID, indexParams []*commonpb.KeyValuePair) (*pb.FieldIndexMeta, error) {
	mt.indexLock.RLock()
	defer mt.indexLock.RUnlock()

	if _, ok := mt.segID2IndexMetas[segID]; !ok {
		return nil, fmt.Errorf("can not find segment %d", segID)
	}

	for _, v := range mt.segID2IndexMetas[segID] {
		if v.FieldID == fieldID && typeutil.CompareIndexParams(v.IndexParams, indexParams) {
			return &v, nil
		}
	}
	return nil, fmt.Errorf("can not find field %d", fieldID)
}

func (mt *metaTable) UpdateFieldIndexMeta(meta *pb.FieldIndexMeta) error {
	mt.indexLock.Lock()
	defer mt.indexLock.Unlock()

	segID := meta.SegmentID
	if _, ok := mt.segID2IndexMetas[segID]; !ok {
		mt.segID2IndexMetas[segID] = make([]pb.FieldIndexMeta, 0)
	}
	for i, v := range mt.segID2IndexMetas[segID] {
		if v.FieldID == meta.FieldID && typeutil.CompareIndexParams(v.IndexParams, meta.IndexParams) {
			mt.segID2IndexMetas[segID][i] = *meta
			err := mt.deleteFieldIndexMetaToEtcd(segID, v.FieldID, v.IndexID)
			if err != nil {
				_ = mt.reloadFromKV()
				return err
			}
			err = mt.saveFieldIndexMetaToEtcd(meta)
			if err != nil {
				_ = mt.reloadFromKV()
				return err
			}
			return nil
		}
	}

	mt.segID2IndexMetas[segID] = append(mt.segID2IndexMetas[segID], *meta)
	err := mt.saveFieldIndexMetaToEtcd(meta)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) removeSegmentIndexMeta(segID UniqueID) error {
	mt.indexLock.Lock()
	defer mt.indexLock.Unlock()

	delete(mt.segID2IndexMetas, segID)
	keys, _, err := mt.client.LoadWithPrefix("indexmeta/" + strconv.FormatInt(segID, 10))
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	if err = mt.client.MultiRemove(keys); err != nil {
		_ = mt.reloadFromKV()
		return err
	}

	return nil
}

func (mt *metaTable) GetFieldIndexParams(collID UniqueID, fieldID UniqueID) ([]*commonpb.KeyValuePair, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if _, ok := mt.collID2Meta[collID]; !ok {
		return nil, fmt.Errorf("can not find collection with id %d", collID)
	}

	for _, fieldSchema := range mt.collID2Meta[collID].Schema.Fields {
		if fieldSchema.FieldID == fieldID {
			return fieldSchema.IndexParams, nil
		}
	}
	return nil, fmt.Errorf("can not find field %d in collection %d", fieldID, collID)
}

func (mt *metaTable) UpdateFieldIndexParams(collName string, fieldName string, indexParams []*commonpb.KeyValuePair) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	vid, ok := mt.collName2ID[collName]
	if !ok {
		return errors.Errorf("can't find collection: " + collName)
	}
	meta, ok := mt.collID2Meta[vid]
	if !ok {
		return errors.Errorf("can't find collection: " + collName)
	}

	for _, fieldSchema := range meta.Schema.Fields {
		if fieldSchema.Name == fieldName {
			fieldSchema.IndexParams = indexParams
			if err := mt.saveCollectionMeta(&meta); err != nil {
				_ = mt.reloadFromKV()
				return err
			}
			return nil
		}
	}

	return fmt.Errorf("can not find field with id %s", fieldName)
}

func (mt *metaTable) IsIndexable(collID UniqueID, fieldID UniqueID) (bool, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	if _, ok := mt.collID2Meta[collID]; !ok {
		return false, fmt.Errorf("can not find collection with id %d", collID)
	}

	for _, v := range mt.collID2Meta[collID].Schema.Fields {
		// field is vector type and index params is not empty
		if v.FieldID == fieldID && (v.DataType == schemapb.DataType_VECTOR_BINARY || v.DataType == schemapb.DataType_VECTOR_FLOAT) &&
			len(v.IndexParams) != 0 {
			return true, nil
		}
	}

	// fieldID is not in schema(eg: timestamp) or not indexable
	return false, nil
}
