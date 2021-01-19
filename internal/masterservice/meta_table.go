package masterservice

import (
	"log"
	"path"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/kv"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/etcdpb"
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
	client           kv.TxnBase                                                       // client of a reliable kv service, i.e. etcd client
	tenantID2Meta    map[typeutil.UniqueID]pb.TenantMeta                              // tenant id to tenant meta
	proxyID2Meta     map[typeutil.UniqueID]pb.ProxyMeta                               // proxy id to proxy meta
	collID2Meta      map[typeutil.UniqueID]pb.CollectionInfo                          // collection id to collection meta,
	collName2ID      map[string]typeutil.UniqueID                                     // collection name to collection id
	partitionID2Meta map[typeutil.UniqueID]pb.PartitionInfo                           //partition id -> partition meta
	segID2IndexMeta  map[typeutil.UniqueID]*map[typeutil.UniqueID]pb.SegmentIndexInfo // segment id -> index id -> segment index meta
	indexID2Meta     map[typeutil.UniqueID]pb.IndexInfo                               //index id ->index meta

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
		mt.partitionID2Meta[partitionInfo.PartitionID] = partitionInfo
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
	coll.PartitionIDs = append(coll.PartitionIDs, part.PartitionID)
	mt.collID2Meta[coll.ID] = *coll
	mt.collName2ID[coll.Schema.Name] = coll.ID
	mt.partitionID2Meta[part.PartitionID] = *part

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
			log.Printf("segment id = %d not exist", segID)
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
	err := mt.client.Save(path.Join(PartitionMetaPrefix, strconv.FormatInt(seg.PartitionID, 10)), proto.MarshalTextString(&partMeta))

	if err != nil {
		_ = mt.reloadFromKV()
		return err
	}
	return nil
}
