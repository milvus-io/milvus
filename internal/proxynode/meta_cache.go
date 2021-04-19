package proxynode

import (
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type MasterClientInterface interface {
	DescribeCollection(in *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error)
	ShowPartitions(in *milvuspb.ShowPartitionRequest) (*milvuspb.ShowPartitionResponse, error)
}

type Cache interface {
	GetCollectionID(collectionName string) (typeutil.UniqueID, error)
	GetPartitionID(collectionName string, partitionName string) (typeutil.UniqueID, error)
	GetCollectionSchema(collectionName string) (*schemapb.CollectionSchema, error)
	RemoveCollection(collectionName string)
	RemovePartition(collectionName string, partitionName string)
}

type collectionInfo struct {
	collID   typeutil.UniqueID
	schema   *schemapb.CollectionSchema
	partInfo map[string]typeutil.UniqueID
}

type MetaCache struct {
	client MasterClientInterface

	collInfo map[string]*collectionInfo
	mu       sync.RWMutex
}

var globalMetaCache Cache

func InitMetaCache(client MasterClientInterface) error {
	var err error
	globalMetaCache, err = NewMetaCache(client)
	if err != nil {
		return err
	}
	return nil
}

func NewMetaCache(client MasterClientInterface) (*MetaCache, error) {
	return &MetaCache{
		client:   client,
		collInfo: map[string]*collectionInfo{},
	}, nil
}

func (m *MetaCache) readCollectionID(collectionName string) (typeutil.UniqueID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collInfo, ok := m.collInfo[collectionName]
	if !ok {
		return 0, errors.Errorf("can't find collection name:%s", collectionName)
	}
	return collInfo.collID, nil
}

func (m *MetaCache) readCollectionSchema(collectionName string) (*schemapb.CollectionSchema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collInfo, ok := m.collInfo[collectionName]
	if !ok {
		return nil, errors.Errorf("can't find collection name:%s", collectionName)
	}
	return collInfo.schema, nil
}

func (m *MetaCache) readPartitionID(collectionName string, partitionName string) (typeutil.UniqueID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collInfo, ok := m.collInfo[collectionName]
	if !ok {
		return 0, errors.Errorf("can't find collection name:%s", collectionName)
	}

	partitionID, ok := collInfo.partInfo[partitionName]
	if !ok {
		return 0, errors.Errorf("can't find partition name:%s", partitionName)
	}
	return partitionID, nil
}

func (m *MetaCache) GetCollectionID(collectionName string) (typeutil.UniqueID, error) {
	collID, err := m.readCollectionID(collectionName)
	if err == nil {
		return collID, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	req := &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_kDescribeCollection,
		},
		CollectionName: collectionName,
	}
	coll, err := m.client.DescribeCollection(req)
	if err != nil {
		return 0, err
	}
	if coll.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return 0, errors.Errorf("%s", coll.Status.Reason)
	}

	_, ok := m.collInfo[collectionName]
	if !ok {
		m.collInfo[collectionName] = &collectionInfo{}
	}
	m.collInfo[collectionName].schema = coll.Schema
	m.collInfo[collectionName].collID = coll.CollectionID

	return m.collInfo[collectionName].collID, nil
}
func (m *MetaCache) GetCollectionSchema(collectionName string) (*schemapb.CollectionSchema, error) {
	collSchema, err := m.readCollectionSchema(collectionName)
	if err == nil {
		return collSchema, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	req := &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_kDescribeCollection,
		},
		CollectionName: collectionName,
	}
	coll, err := m.client.DescribeCollection(req)
	if err != nil {
		return nil, err
	}
	if coll.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return nil, errors.Errorf("%s", coll.Status.Reason)
	}

	_, ok := m.collInfo[collectionName]
	if !ok {
		m.collInfo[collectionName] = &collectionInfo{}
	}
	m.collInfo[collectionName].schema = coll.Schema
	m.collInfo[collectionName].collID = coll.CollectionID

	return m.collInfo[collectionName].schema, nil
}

func (m *MetaCache) GetPartitionID(collectionName string, partitionName string) (typeutil.UniqueID, error) {
	partitionID, err := m.readPartitionID(collectionName, partitionName)
	if err == nil {
		return partitionID, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	req := &milvuspb.ShowPartitionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_kShowPartitions,
		},
		CollectionName: collectionName,
	}
	partitions, err := m.client.ShowPartitions(req)
	if err != nil {
		return 0, err
	}
	if partitions.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return 0, errors.Errorf("%s", partitions.Status.Reason)
	}
	if len(partitions.PartitionIDs) != len(partitions.PartitionNames) {
		return 0, errors.Errorf("partition ids len: %d doesn't equal Partition name len %d",
			len(partitions.PartitionIDs), len(partitions.PartitionNames))
	}

	_, ok := m.collInfo[collectionName]
	if !ok {
		m.collInfo[collectionName] = &collectionInfo{
			partInfo: map[string]typeutil.UniqueID{},
		}
	}
	partInfo := m.collInfo[collectionName].partInfo
	if partInfo == nil {
		partInfo = map[string]typeutil.UniqueID{}
	}

	for i := 0; i < len(partitions.PartitionIDs); i++ {
		_, ok := partInfo[partitions.PartitionNames[i]]
		if !ok {
			partInfo[partitions.PartitionNames[i]] = partitions.PartitionIDs[i]
		}
	}
	_, ok = partInfo[partitionName]
	if !ok {
		return 0, errors.Errorf("partitionID of partitionName:%s can not be find", partitionName)
	}

	return partInfo[partitionName], nil
}

func (m *MetaCache) RemoveCollection(collectionName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.collInfo, collectionName)
}

func (m *MetaCache) RemovePartition(collectionName, partitionName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.collInfo[collectionName]
	if !ok {
		return
	}
	partInfo := m.collInfo[collectionName].partInfo
	if partInfo == nil {
		return
	}
	delete(partInfo, partitionName)
}
