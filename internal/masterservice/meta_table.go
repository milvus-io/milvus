package masterservice

import (
	"log"
	"path"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const (
	TenantMetaPrefix       = "tenant"
	ProxyMetaPrefix        = "proxy"
	CollectionMetaPrefix   = "collection"
	PartitionMetaPrefix    = "partition"
	SegmentIndexMetaPrefix = "segment-index"
	IndexMetaPrefix        = "index"
)

type metaTable struct {
	client             kv.TxnBase                                                       // client of a reliable kv service, i.e. etcd client
	tenantID2Meta      map[typeutil.UniqueID]pb.TenantMeta                              // tenant id to tenant meta
	proxyID2Meta       map[typeutil.UniqueID]pb.ProxyMeta                               // proxy id to proxy meta
	collID2Meta        map[typeutil.UniqueID]pb.CollectionInfo                          // collection id to collection meta,
	collName2ID        map[string]typeutil.UniqueID                                     // collection name to collection id
	partitionID2Meta   map[typeutil.UniqueID]pb.PartitionInfo                           // partition id -> partition meta
	segID2IndexMeta    map[typeutil.UniqueID]*map[typeutil.UniqueID]pb.SegmentIndexInfo // segment id -> index id -> segment index meta
	indexID2Meta       map[typeutil.UniqueID]pb.IndexInfo                               // index id ->index meta
	segID2CollID       map[typeutil.UniqueID]typeutil.UniqueID                          // segment id -> collection id
	partitionID2CollID map[typeutil.UniqueID]typeutil.UniqueID                          // partition id -> collection id

	tenantLock sync.RWMutex
	proxyLock  sync.RWMutex
	ddLock     sync.RWMutex
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

	mt.tenantID2Meta = make(map[typeutil.UniqueID]pb.TenantMeta)
	mt.proxyID2Meta = make(map[typeutil.UniqueID]pb.ProxyMeta)
	mt.collID2Meta = make(map[typeutil.UniqueID]pb.CollectionInfo)
	mt.collName2ID = make(map[string]typeutil.UniqueID)
	mt.partitionID2Meta = make(map[typeutil.UniqueID]pb.PartitionInfo)
	mt.segID2IndexMeta = make(map[typeutil.UniqueID]*map[typeutil.UniqueID]pb.SegmentIndexInfo)
	mt.indexID2Meta = make(map[typeutil.UniqueID]pb.IndexInfo)
	mt.partitionID2CollID = make(map[typeutil.UniqueID]typeutil.UniqueID)
	mt.segID2CollID = make(map[typeutil.UniqueID]typeutil.UniqueID)

	_, values, err := mt.client.LoadWithPrefix(TenantMetaPrefix)
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

	_, values, err = mt.client.LoadWithPrefix(ProxyMetaPrefix)
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

	_, values, err = mt.client.LoadWithPrefix(CollectionMetaPrefix)
	if err != nil {
		return err
	}

	for _, value := range values {
		collectionInfo := pb.CollectionInfo{}
		err = proto.UnmarshalText(value, &collectionInfo)
		if err != nil {
			return err
		}
		mt.collID2Meta[collectionInfo.ID] = collectionInfo
		mt.collName2ID[collectionInfo.Schema.Name] = collectionInfo.ID
		for _, partID := range collectionInfo.PartitionIDs {
			mt.partitionID2CollID[partID] = collectionInfo.ID
		}
	}

	_, values, err = mt.client.LoadWithPrefix(PartitionMetaPrefix)
	if err != nil {
		return err
	}
	for _, value := range values {
		partitionInfo := pb.PartitionInfo{}
		err = proto.UnmarshalText(value, &partitionInfo)
		if err != nil {
			return err
		}
		collID, ok := mt.partitionID2CollID[partitionInfo.PartitionID]
		if !ok {
			log.Printf("partition id %d not belong to any collection", partitionInfo.PartitionID)
			continue
		}
		mt.partitionID2Meta[partitionInfo.PartitionID] = partitionInfo
		for _, segID := range partitionInfo.SegmentIDs {
			mt.segID2CollID[segID] = collID
		}
	}

	_, values, err = mt.client.LoadWithPrefix(SegmentIndexMetaPrefix)
	if err != nil {
		return err
	}
	for _, value := range values {
		segmentIndexInfo := pb.SegmentIndexInfo{}
		err = proto.UnmarshalText(value, &segmentIndexInfo)
		if err != nil {
			return err
		}
		idx, ok := mt.segID2IndexMeta[segmentIndexInfo.SegmentID]
		if ok {
			(*idx)[segmentIndexInfo.IndexID] = segmentIndexInfo
		} else {
			meta := make(map[typeutil.UniqueID]pb.SegmentIndexInfo)
			meta[segmentIndexInfo.IndexID] = segmentIndexInfo
			mt.segID2IndexMeta[segmentIndexInfo.SegmentID] = &meta
		}
	}

	_, values, err = mt.client.LoadWithPrefix(IndexMetaPrefix)
	if err != nil {
		return err
	}
	for _, value := range values {
		meta := pb.IndexInfo{}
		err = proto.UnmarshalText(value, &meta)
		if err != nil {
			return err
		}
		mt.indexID2Meta[meta.IndexID] = meta
	}

	return nil
}

func (mt *metaTable) AddCollection(coll *pb.CollectionInfo, part *pb.PartitionInfo) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if len(part.SegmentIDs) != 0 {
		return errors.Errorf("segment should be empty when creating collection")
	}

	if len(coll.PartitionIDs) != 0 {
		return errors.Errorf("partitions should be empty when creating collection")
	}
	if _, ok := mt.collName2ID[coll.Schema.Name]; ok {
		return errors.Errorf("collection %s exist", coll.Schema.Name)
	}

	coll.PartitionIDs = append(coll.PartitionIDs, part.PartitionID)
	mt.collID2Meta[coll.ID] = *coll
	mt.collName2ID[coll.Schema.Name] = coll.ID
	mt.partitionID2Meta[part.PartitionID] = *part
	mt.partitionID2CollID[part.PartitionID] = coll.ID

	k1 := path.Join(CollectionMetaPrefix, strconv.FormatInt(coll.ID, 10))
	v1 := proto.MarshalTextString(coll)
	k2 := path.Join(PartitionMetaPrefix, strconv.FormatInt(part.PartitionID, 10))
	v2 := proto.MarshalTextString(part)
	meta := map[string]string{k1: v1, k2: v2}

	err := mt.client.MultiSave(meta)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) DeleteCollection(collID typeutil.UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(collID, 10))
	}

	metaKeys := []string{path.Join(CollectionMetaPrefix, strconv.FormatInt(collID, 10))}
	delete(mt.collID2Meta, collID)
	delete(mt.collName2ID, collMeta.Schema.Name)
	for _, partID := range collMeta.PartitionIDs {
		metaKeys = append(metaKeys, path.Join(PartitionMetaPrefix, strconv.FormatInt(partID, 10)))
		partMeta, ok := mt.partitionID2Meta[partID]
		if !ok {
			log.Printf("partition id = %d not exist", partID)
			continue
		}
		delete(mt.partitionID2Meta, partID)
		for _, segID := range partMeta.SegmentIDs {
			segIndexMeta, ok := mt.segID2IndexMeta[segID]
			if !ok {
				log.Printf("segment id = %d not exist", segID)
				continue
			}
			delete(mt.segID2IndexMeta, segID)
			for indexID, segIdxMeta := range *segIndexMeta {
				metaKeys = append(metaKeys, path.Join(SegmentIndexMetaPrefix, strconv.FormatInt(segID, 10), strconv.FormatInt(indexID, 10)))
				indexMeta, ok := mt.indexID2Meta[segIdxMeta.IndexID]
				if !ok {
					log.Printf("index id = %d not exist", segIdxMeta.IndexID)
					continue
				}
				delete(mt.indexID2Meta, segIdxMeta.IndexID)
				metaKeys = append(metaKeys, path.Join(IndexMetaPrefix, strconv.FormatInt(indexMeta.IndexID, 10)))
			}
		}
	}
	err := mt.client.MultiRemove(metaKeys)

	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}

	return nil
}

func (mt *metaTable) HasCollection(collID typeutil.UniqueID) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	_, ok := mt.collID2Meta[collID]
	return ok
}

func (mt *metaTable) GetCollectionByID(collectionID typeutil.UniqueID) (pb.CollectionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	col, ok := mt.collID2Meta[collectionID]
	if !ok {
		return pb.CollectionInfo{}, errors.Errorf("can't find collection id : %d", collectionID)
	}
	return col, nil
}

func (mt *metaTable) GetCollectionByName(collectionName string) (pb.CollectionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	vid, ok := mt.collName2ID[collectionName]
	if !ok {
		return pb.CollectionInfo{}, errors.Errorf("can't find collection: " + collectionName)
	}
	col, ok := mt.collID2Meta[vid]
	if !ok {
		return pb.CollectionInfo{}, errors.Errorf("can't find collection: " + collectionName)
	}
	return col, nil
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

func (mt *metaTable) AddPartition(collID typeutil.UniqueID, partitionName string, partitionID typeutil.UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	coll, ok := mt.collID2Meta[collID]
	if !ok {
		return errors.Errorf("can't find collection. id = " + strconv.FormatInt(collID, 10))
	}

	// number of partition tags (except _default) should be limited to 4096 by default
	if int64(len(coll.PartitionIDs)) > Params.MaxPartitionNum {
		return errors.New("maximum partition's number should be limit to " + strconv.FormatInt(Params.MaxPartitionNum, 10))
	}
	for _, t := range coll.PartitionIDs {
		part, ok := mt.partitionID2Meta[t]
		if !ok {
			log.Printf("partition id = %d not exist", t)
			continue
		}
		if part.PartitionName == partitionName {
			return errors.Errorf("partition name = %s already exists", partitionName)
		}
		if part.PartitionID == partitionID {
			return errors.Errorf("partition id = %d already exists", partitionID)
		}
	}
	partMeta := pb.PartitionInfo{
		PartitionName: partitionName,
		PartitionID:   partitionID,
		SegmentIDs:    make([]typeutil.UniqueID, 0, 16),
	}
	coll.PartitionIDs = append(coll.PartitionIDs, partitionID)
	mt.partitionID2Meta[partitionID] = partMeta
	mt.collID2Meta[collID] = coll
	mt.partitionID2CollID[partitionID] = collID

	k1 := path.Join(CollectionMetaPrefix, strconv.FormatInt(coll.ID, 10))
	v1 := proto.MarshalTextString(&coll)
	k2 := path.Join(PartitionMetaPrefix, strconv.FormatInt(partitionID, 10))
	v2 := proto.MarshalTextString(&partMeta)
	meta := map[string]string{k1: v1, k2: v2}

	err := mt.client.MultiSave(meta)

	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) HasPartition(collID typeutil.UniqueID, partitionName string) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	col, ok := mt.collID2Meta[collID]
	if !ok {
		return false
	}
	for _, partitionID := range col.PartitionIDs {
		meta, ok := mt.partitionID2Meta[partitionID]
		if ok {
			if meta.PartitionName == partitionName {
				return true
			}
		}
	}
	return false
}

func (mt *metaTable) DeletePartition(collID typeutil.UniqueID, partitionName string) (typeutil.UniqueID, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if partitionName == Params.DefaultPartitionName {
		return 0, errors.New("default partition cannot be deleted")
	}

	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return 0, errors.Errorf("can't find collection id = " + strconv.FormatInt(collID, 10))
	}

	// check tag exists
	exist := false

	pd := make([]typeutil.UniqueID, 0, len(collMeta.PartitionIDs))
	var partMeta pb.PartitionInfo
	for _, t := range collMeta.PartitionIDs {
		pm, ok := mt.partitionID2Meta[t]
		if ok {
			if pm.PartitionName != partitionName {
				pd = append(pd, pm.PartitionID)
			} else {
				partMeta = pm
				exist = true
			}

		}

	}
	if !exist {
		return 0, errors.New("partition " + partitionName + " does not exist")
	}
	delete(mt.partitionID2Meta, partMeta.PartitionID)
	collMeta.PartitionIDs = pd
	mt.collID2Meta[collID] = collMeta

	delMetaKeys := []string{path.Join(PartitionMetaPrefix, strconv.FormatInt(partMeta.PartitionID, 10))}
	for _, segID := range partMeta.SegmentIDs {
		segIndexMeta, ok := mt.segID2IndexMeta[segID]
		if !ok {
			log.Printf("segment id = %d has no index meta", segID)
			continue
		}
		delete(mt.segID2IndexMeta, segID)
		for indexID, segIdxMeta := range *segIndexMeta {
			delMetaKeys = append(delMetaKeys, path.Join(SegmentIndexMetaPrefix, strconv.FormatInt(segID, 10), strconv.FormatInt(indexID, 10)))
			indexMeta, ok := mt.indexID2Meta[segIdxMeta.IndexID]
			if !ok {
				log.Printf("index id = %d not exist", segIdxMeta.IndexID)
				continue
			}
			delete(mt.indexID2Meta, segIdxMeta.IndexID)
			delMetaKeys = append(delMetaKeys, path.Join(IndexMetaPrefix, strconv.FormatInt(indexMeta.IndexID, 10)))
		}
	}
	collKV := map[string]string{path.Join(CollectionMetaPrefix, strconv.FormatInt(collID, 10)): proto.MarshalTextString(&collMeta)}
	err := mt.client.MultiSaveAndRemove(collKV, delMetaKeys)

	if err != nil {
		_ = mt.reloadFromKV()
		return 0, err
	}
	return partMeta.PartitionID, nil
}

func (mt *metaTable) GetPartitionByID(partitionID typeutil.UniqueID) (pb.PartitionInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	partMeta, ok := mt.partitionID2Meta[partitionID]
	if !ok {
		return pb.PartitionInfo{}, errors.Errorf("partition id = %d not exist", partitionID)
	}
	return partMeta, nil
}

func (mt *metaTable) AddSegment(seg *datapb.SegmentInfo) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	collMeta, ok := mt.collID2Meta[seg.CollectionID]
	if !ok {
		return errors.Errorf("can't find collection id = " + strconv.FormatInt(seg.CollectionID, 10))
	}
	partMeta, ok := mt.partitionID2Meta[seg.PartitionID]
	if !ok {
		return errors.Errorf("can't find partition id = " + strconv.FormatInt(seg.PartitionID, 10))
	}
	exist := false
	for _, partID := range collMeta.PartitionIDs {
		if partID == seg.PartitionID {
			exist = true
			break
		}
	}
	if !exist {
		return errors.Errorf("partition id = %d, not belong to collection id = %d", seg.PartitionID, seg.CollectionID)
	}
	exist = false
	for _, segID := range partMeta.SegmentIDs {
		if segID == seg.SegmentID {
			exist = true
		}
	}
	if exist {
		return errors.Errorf("segment id = %d exist", seg.SegmentID)
	}
	partMeta.SegmentIDs = append(partMeta.SegmentIDs, seg.SegmentID)
	mt.partitionID2Meta[seg.PartitionID] = partMeta
	mt.segID2CollID[seg.SegmentID] = seg.CollectionID
	err := mt.client.Save(path.Join(PartitionMetaPrefix, strconv.FormatInt(seg.PartitionID, 10)), proto.MarshalTextString(&partMeta))

	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) AddIndex(seg *pb.SegmentIndexInfo, idx *pb.IndexInfo) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if seg.IndexID != idx.IndexID {
		return errors.Errorf("index id in segment is %d, in index info is %d, not equal", seg.IndexID, idx.IndexID)
	}

	segIdxMap, ok := mt.segID2IndexMeta[seg.SegmentID]
	if !ok {
		idxMap := map[typeutil.UniqueID]pb.SegmentIndexInfo{seg.IndexID: *seg}
		mt.segID2IndexMeta[seg.SegmentID] = &idxMap
	} else {
		_, ok := (*segIdxMap)[seg.IndexID]
		if ok {
			return errors.Errorf("index id = %d exist", seg.IndexID)
		}
	}
	_, ok = mt.indexID2Meta[idx.IndexID]
	if ok {
		return errors.Errorf("index id = %d exist", idx.IndexID)
	}
	(*(mt.segID2IndexMeta[seg.SegmentID]))[seg.IndexID] = *seg
	mt.indexID2Meta[idx.IndexID] = *idx
	k1 := path.Join(SegmentIndexMetaPrefix, strconv.FormatInt(seg.SegmentID, 10), strconv.FormatInt(seg.IndexID, 10))
	v1 := proto.MarshalTextString(seg)
	k2 := path.Join(IndexMetaPrefix, strconv.FormatInt(idx.IndexID, 10))
	v2 := proto.MarshalTextString(idx)
	meta := map[string]string{k1: v1, k2: v2}

	err := mt.client.MultiSave(meta)
	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}

func (mt *metaTable) GetSegmentIndexInfoByID(segID typeutil.UniqueID, filedID int64, idxName string) (pb.SegmentIndexInfo, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	segIdxMap, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return pb.SegmentIndexInfo{}, errors.Errorf("segment id %d not has any index", segID)
	}
	if len(*segIdxMap) == 0 {
		return pb.SegmentIndexInfo{}, errors.Errorf("segment id %d not has any index", segID)
	}

	if filedID == -1 && idxName == "" { // return any index
		for _, seg := range *segIdxMap {
			return seg, nil
		}
	} else {
		for idxID, seg := range *segIdxMap {
			idxMeta, ok := mt.indexID2Meta[idxID]
			if ok {
				if idxMeta.IndexName != idxName {
					continue
				}
				if seg.FieldID != filedID {
					continue
				}
				return seg, nil
			}
		}
	}
	return pb.SegmentIndexInfo{}, errors.Errorf("can't find index name = %s on segment = %d, with filed id = %d", idxName, segID, filedID)
}

func (mt *metaTable) GetFieldSchema(collName string, fieldName string) (schemapb.FieldSchema, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	collID, ok := mt.collName2ID[collName]
	if !ok {
		return schemapb.FieldSchema{}, errors.Errorf("collection %s not found", collName)
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return schemapb.FieldSchema{}, errors.Errorf("collection %s not found", collName)
	}

	for _, field := range collMeta.Schema.Fields {
		if field.Name == fieldName {
			return *field, nil
		}
	}
	return schemapb.FieldSchema{}, errors.Errorf("collection %s doesn't have filed %s", collName, fieldName)
}

//return true/false
func (mt *metaTable) IsSegmentIndexed(segID typeutil.UniqueID, fieldSchema *schemapb.FieldSchema, indexParams []*commonpb.KeyValuePair) bool {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	segIdx, ok := mt.segID2IndexMeta[segID]
	if !ok {
		return false
	}
	exist := false
	for idxID, meta := range *segIdx {
		if meta.FieldID != fieldSchema.FieldID {
			continue
		}
		idxMeta, ok := mt.indexID2Meta[idxID]
		if !ok {
			continue
		}
		if EqualKeyPairArray(indexParams, idxMeta.IndexParams) {
			exist = true
			break
		}
	}
	return exist
}

// return segment ids, type params, error
func (mt *metaTable) GetNotIndexedSegments(collName string, fieldName string, indexParams []*commonpb.KeyValuePair) ([]typeutil.UniqueID, schemapb.FieldSchema, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	collID, ok := mt.collName2ID[collName]
	if !ok {
		return nil, schemapb.FieldSchema{}, errors.Errorf("collection %s not found", collName)
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return nil, schemapb.FieldSchema{}, errors.Errorf("collection %s not found", collName)
	}
	fieldSchema, err := mt.GetFieldSchema(collName, fieldName)
	if err != nil {
		return nil, fieldSchema, err
	}

	rstID := make([]typeutil.UniqueID, 0, 16)
	for _, partID := range collMeta.PartitionIDs {
		partMeta, ok := mt.partitionID2Meta[partID]
		if ok {
			for _, segID := range partMeta.SegmentIDs {
				if exist := mt.IsSegmentIndexed(segID, &fieldSchema, indexParams); !exist {
					rstID = append(rstID, segID)
				}
			}
		}
	}
	return rstID, fieldSchema, nil
}

func (mt *metaTable) GetSegmentVectorFields(segID typeutil.UniqueID) ([]*schemapb.FieldSchema, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	collID, ok := mt.segID2CollID[segID]
	if !ok {
		return nil, errors.Errorf("segment id %d not belong to any collection", segID)
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return nil, errors.Errorf("segment id %d not belong to any collection which has dropped", segID)
	}
	rst := make([]*schemapb.FieldSchema, 0, 2)
	for _, f := range collMeta.Schema.Fields {
		if f.DataType == schemapb.DataType_VECTOR_BINARY || f.DataType == schemapb.DataType_VECTOR_FLOAT {
			field := proto.Clone(f)
			rst = append(rst, field.(*schemapb.FieldSchema))
		}
	}
	return rst, nil
}

func (mt *metaTable) GetIndexByName(collName string, fieldName string, indexName string) ([]pb.IndexInfo, error) {
	mt.ddLock.RLock()
	mt.ddLock.RUnlock()

	collID, ok := mt.collName2ID[collName]
	if !ok {
		return nil, errors.Errorf("collection %s not found", collName)
	}
	collMeta, ok := mt.collID2Meta[collID]
	if !ok {
		return nil, errors.Errorf("collection %s not found", collName)
	}
	fileSchema, err := mt.GetFieldSchema(collName, fieldName)
	if err != nil {
		return nil, err
	}

	rstIndex := make([]*pb.IndexInfo, 0, 16)

	for _, partID := range collMeta.PartitionIDs {
		partMeta, ok := mt.partitionID2Meta[partID]
		if ok {
			for _, segID := range partMeta.SegmentIDs {
				idxMeta, ok := mt.segID2IndexMeta[segID]
				if !ok {
					continue
				}
				for idxID, segMeta := range *idxMeta {
					if segMeta.FieldID != fileSchema.FieldID {
						continue
					}
					idxMeta, ok := mt.indexID2Meta[idxID]
					if !ok {
						continue
					}
					if indexName == "" {
						rstIndex = append(rstIndex, &idxMeta)
					} else if idxMeta.IndexName == indexName {
						rstIndex = append(rstIndex, &idxMeta)
					}
				}
			}
		}
	}
	rst := make([]pb.IndexInfo, 0, len(rstIndex))
	for i := range rstIndex {
		rst = append(rst, *rstIndex[i])
	}
	return rst, nil
}
