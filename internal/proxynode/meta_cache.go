package proxynode

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/types"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type Cache interface {
	GetCollectionID(ctx context.Context, collectionName string) (typeutil.UniqueID, error)
	GetPartitionID(ctx context.Context, collectionName string, partitionName string) (typeutil.UniqueID, error)
	GetCollectionSchema(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error)
	RemoveCollection(ctx context.Context, collectionName string)
	RemovePartition(ctx context.Context, collectionName string, partitionName string)
}

type collectionInfo struct {
	collID   typeutil.UniqueID
	schema   *schemapb.CollectionSchema
	partInfo map[string]typeutil.UniqueID
}

type MetaCache struct {
	client types.MasterService

	collInfo map[string]*collectionInfo
	mu       sync.RWMutex
}

var globalMetaCache Cache

func InitMetaCache(client types.MasterService) error {
	var err error
	globalMetaCache, err = NewMetaCache(client)
	if err != nil {
		return err
	}
	return nil
}

func NewMetaCache(client types.MasterService) (*MetaCache, error) {
	return &MetaCache{
		client:   client,
		collInfo: map[string]*collectionInfo{},
	}, nil
}

func (m *MetaCache) readCollectionID(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collInfo, ok := m.collInfo[collectionName]
	if !ok {
		return 0, fmt.Errorf("can't find collection name:%s", collectionName)
	}
	return collInfo.collID, nil
}

func (m *MetaCache) readCollectionSchema(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collInfo, ok := m.collInfo[collectionName]
	if !ok {
		return nil, fmt.Errorf("can't find collection name:%s", collectionName)
	}
	return collInfo.schema, nil
}

func (m *MetaCache) readPartitionID(ctx context.Context, collectionName string, partitionName string) (typeutil.UniqueID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	collInfo, ok := m.collInfo[collectionName]
	if !ok {
		return 0, fmt.Errorf("can't find collection name:%s", collectionName)
	}

	partitionID, ok := collInfo.partInfo[partitionName]
	if !ok {
		return 0, fmt.Errorf("can't find partition name:%s", partitionName)
	}
	return partitionID, nil
}

func (m *MetaCache) GetCollectionID(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
	collID, err := m.readCollectionID(ctx, collectionName)
	if err == nil {
		return collID, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	req := &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeCollection,
		},
		CollectionName: collectionName,
	}
	coll, err := m.client.DescribeCollection(ctx, req)
	if err != nil {
		return 0, err
	}
	if coll.Status.ErrorCode != commonpb.ErrorCode_Success {
		return 0, errors.New(coll.Status.Reason)
	}

	_, ok := m.collInfo[collectionName]
	if !ok {
		m.collInfo[collectionName] = &collectionInfo{}
	}
	m.collInfo[collectionName].schema = coll.Schema
	m.collInfo[collectionName].collID = coll.CollectionID

	return m.collInfo[collectionName].collID, nil
}
func (m *MetaCache) GetCollectionSchema(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
	collSchema, err := m.readCollectionSchema(ctx, collectionName)
	if err == nil {
		return collSchema, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	req := &milvuspb.DescribeCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeCollection,
		},
		CollectionName: collectionName,
	}
	coll, err := m.client.DescribeCollection(ctx, req)
	if err != nil {
		return nil, err
	}
	if coll.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(coll.Status.Reason)
	}

	_, ok := m.collInfo[collectionName]
	if !ok {
		m.collInfo[collectionName] = &collectionInfo{}
	}
	m.collInfo[collectionName].schema = coll.Schema
	m.collInfo[collectionName].collID = coll.CollectionID

	return m.collInfo[collectionName].schema, nil
}

func (m *MetaCache) GetPartitionID(ctx context.Context, collectionName string, partitionName string) (typeutil.UniqueID, error) {
	partitionID, err := m.readPartitionID(ctx, collectionName, partitionName)
	if err == nil {
		return partitionID, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	req := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowPartitions,
		},
		CollectionName: collectionName,
	}
	partitions, err := m.client.ShowPartitions(ctx, req)
	if err != nil {
		return 0, err
	}
	if partitions.Status.ErrorCode != commonpb.ErrorCode_Success {
		return 0, fmt.Errorf("%s", partitions.Status.Reason)
	}
	if len(partitions.PartitionIDs) != len(partitions.PartitionNames) {
		return 0, fmt.Errorf("partition ids len: %d doesn't equal Partition name len %d",
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
		return 0, fmt.Errorf("partitionID of partitionName:%s can not be find", partitionName)
	}

	return partInfo[partitionName], nil
}

func (m *MetaCache) RemoveCollection(ctx context.Context, collectionName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.collInfo, collectionName)
}

func (m *MetaCache) RemovePartition(ctx context.Context, collectionName, partitionName string) {
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
