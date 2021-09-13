// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/common"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Cache interface {
	GetCollectionID(ctx context.Context, collectionName string) (typeutil.UniqueID, error)
	GetCollectionInfo(ctx context.Context, collectionName string) (*collectionInfo, error)
	GetPartitionID(ctx context.Context, collectionName string, partitionName string) (typeutil.UniqueID, error)
	GetPartitions(ctx context.Context, collectionName string) (map[string]typeutil.UniqueID, error)
	GetPartitionInfo(ctx context.Context, collectionName string, partitionName string) (*partitionInfo, error)
	GetCollectionSchema(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error)
	RemoveCollection(ctx context.Context, collectionName string)
	RemovePartition(ctx context.Context, collectionName string, partitionName string)
}

type collectionInfo struct {
	collID              typeutil.UniqueID
	schema              *schemapb.CollectionSchema
	partInfo            map[string]*partitionInfo
	createdTimestamp    uint64
	createdUtcTimestamp uint64
}

type partitionInfo struct {
	partitionID         typeutil.UniqueID
	createdTimestamp    uint64
	createdUtcTimestamp uint64
}

type MetaCache struct {
	client types.RootCoord

	collInfo map[string]*collectionInfo
	mu       sync.RWMutex
}

var globalMetaCache Cache

func InitMetaCache(client types.RootCoord) error {
	var err error
	globalMetaCache, err = NewMetaCache(client)
	if err != nil {
		return err
	}
	return nil
}

func NewMetaCache(client types.RootCoord) (*MetaCache, error) {
	return &MetaCache{
		client:   client,
		collInfo: map[string]*collectionInfo{},
	}, nil
}

func (m *MetaCache) GetCollectionID(ctx context.Context, collectionName string) (typeutil.UniqueID, error) {
	m.mu.RLock()
	collInfo, ok := m.collInfo[collectionName]

	if !ok {
		m.mu.RUnlock()
		coll, err := m.describeCollection(ctx, collectionName)
		if err != nil {
			return 0, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.updateCollection(coll, collectionName)
		collInfo = m.collInfo[collectionName]
		return collInfo.collID, nil
	}
	defer m.mu.RUnlock()

	return collInfo.collID, nil
}

func (m *MetaCache) GetCollectionInfo(ctx context.Context, collectionName string) (*collectionInfo, error) {
	m.mu.RLock()
	var collInfo *collectionInfo
	collInfo, ok := m.collInfo[collectionName]
	m.mu.RUnlock()

	if !ok {
		coll, err := m.describeCollection(ctx, collectionName)
		if err != nil {
			return nil, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.updateCollection(coll, collectionName)
		collInfo = m.collInfo[collectionName]
	}

	return &collectionInfo{
		collID:              collInfo.collID,
		schema:              collInfo.schema,
		partInfo:            collInfo.partInfo,
		createdTimestamp:    collInfo.createdTimestamp,
		createdUtcTimestamp: collInfo.createdUtcTimestamp,
	}, nil
}

func (m *MetaCache) GetCollectionSchema(ctx context.Context, collectionName string) (*schemapb.CollectionSchema, error) {
	m.mu.RLock()
	collInfo, ok := m.collInfo[collectionName]

	if !ok {
		t0 := time.Now()
		m.mu.RUnlock()
		coll, err := m.describeCollection(ctx, collectionName)
		if err != nil {
			log.Warn("Failed to load collection from rootcoord ",
				zap.String("collection name ", collectionName),
				zap.Error(err))
			return nil, err
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		m.updateCollection(coll, collectionName)
		collInfo = m.collInfo[collectionName]
		log.Debug("Reload collection from rootcoord ",
			zap.String("collection name ", collectionName),
			zap.Any("time take ", time.Since(t0)))
		return collInfo.schema, nil
	}
	defer m.mu.RUnlock()

	return collInfo.schema, nil
}

func (m *MetaCache) updateCollection(coll *milvuspb.DescribeCollectionResponse, collectionName string) {
	_, ok := m.collInfo[collectionName]
	if !ok {
		m.collInfo[collectionName] = &collectionInfo{}
	}
	m.collInfo[collectionName].schema = coll.Schema
	m.collInfo[collectionName].collID = coll.CollectionID
	m.collInfo[collectionName].createdTimestamp = coll.CreatedTimestamp
	m.collInfo[collectionName].createdUtcTimestamp = coll.CreatedUtcTimestamp
}

func (m *MetaCache) GetPartitionID(ctx context.Context, collectionName string, partitionName string) (typeutil.UniqueID, error) {
	partInfo, err := m.GetPartitionInfo(ctx, collectionName, partitionName)
	if err != nil {
		return 0, err
	}
	return partInfo.partitionID, nil
}

func (m *MetaCache) GetPartitions(ctx context.Context, collectionName string) (map[string]typeutil.UniqueID, error) {
	_, err := m.GetCollectionID(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	m.mu.RLock()

	collInfo, ok := m.collInfo[collectionName]
	if !ok {
		m.mu.RUnlock()
		return nil, fmt.Errorf("can't find collection name:%s", collectionName)
	}

	if collInfo.partInfo == nil || len(collInfo.partInfo) == 0 {
		m.mu.RUnlock()

		partitions, err := m.showPartitions(ctx, collectionName)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		err = m.updatePartitions(partitions, collectionName)
		if err != nil {
			return nil, err
		}
		log.Debug("proxy", zap.Any("GetPartitions:partitions after update", partitions), zap.Any("collectionName", collectionName))
		ret := make(map[string]typeutil.UniqueID)
		partInfo := m.collInfo[collectionName].partInfo
		for k, v := range partInfo {
			ret[k] = v.partitionID
		}
		return ret, nil

	}
	defer m.mu.RUnlock()

	ret := make(map[string]typeutil.UniqueID)
	partInfo := m.collInfo[collectionName].partInfo
	for k, v := range partInfo {
		ret[k] = v.partitionID
	}

	return ret, nil
}

func (m *MetaCache) GetPartitionInfo(ctx context.Context, collectionName string, partitionName string) (*partitionInfo, error) {
	_, err := m.GetCollectionID(ctx, collectionName)
	if err != nil {
		return nil, err
	}

	m.mu.RLock()

	collInfo, ok := m.collInfo[collectionName]
	if !ok {
		m.mu.RUnlock()
		return nil, fmt.Errorf("can't find collection name:%s", collectionName)
	}

	var partInfo *partitionInfo
	partInfo, ok = collInfo.partInfo[partitionName]
	m.mu.RUnlock()

	if !ok {
		partitions, err := m.showPartitions(ctx, collectionName)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		err = m.updatePartitions(partitions, collectionName)
		if err != nil {
			return nil, err
		}
		log.Debug("proxy", zap.Any("GetPartitionID:partitions after update", partitions), zap.Any("collectionName", collectionName))

		partInfo, ok = m.collInfo[collectionName].partInfo[partitionName]
		if !ok {
			return nil, fmt.Errorf("partitionID of partitionName:%s can not be find", partitionName)
		}
	}
	return &partitionInfo{
		partitionID:         partInfo.partitionID,
		createdTimestamp:    partInfo.createdTimestamp,
		createdUtcTimestamp: partInfo.createdUtcTimestamp,
	}, nil
}

func (m *MetaCache) describeCollection(ctx context.Context, collectionName string) (*milvuspb.DescribeCollectionResponse, error) {
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
	resp := &milvuspb.DescribeCollectionResponse{
		Status: coll.Status,
		Schema: &schemapb.CollectionSchema{
			Name:        coll.Schema.Name,
			Description: coll.Schema.Description,
			AutoID:      coll.Schema.AutoID,
			Fields:      make([]*schemapb.FieldSchema, 0),
		},
		CollectionID:         coll.CollectionID,
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
		CreatedTimestamp:     coll.CreatedTimestamp,
		CreatedUtcTimestamp:  coll.CreatedUtcTimestamp,
	}
	for _, field := range coll.Schema.Fields {
		if field.FieldID >= common.StartOfUserFieldID {
			resp.Schema.Fields = append(resp.Schema.Fields, field)
		}
	}
	return resp, nil
}

func (m *MetaCache) showPartitions(ctx context.Context, collectionName string) (*milvuspb.ShowPartitionsResponse, error) {
	req := &milvuspb.ShowPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ShowPartitions,
		},
		CollectionName: collectionName,
	}

	partitions, err := m.client.ShowPartitions(ctx, req)
	if err != nil {
		return nil, err
	}
	if partitions.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, fmt.Errorf("%s", partitions.Status.Reason)
	}

	if len(partitions.PartitionIDs) != len(partitions.PartitionNames) {
		return nil, fmt.Errorf("partition ids len: %d doesn't equal Partition name len %d",
			len(partitions.PartitionIDs), len(partitions.PartitionNames))
	}

	return partitions, nil
}

func (m *MetaCache) updatePartitions(partitions *milvuspb.ShowPartitionsResponse, collectionName string) error {
	_, ok := m.collInfo[collectionName]
	if !ok {
		m.collInfo[collectionName] = &collectionInfo{
			partInfo: map[string]*partitionInfo{},
		}
	}
	partInfo := m.collInfo[collectionName].partInfo
	if partInfo == nil {
		partInfo = map[string]*partitionInfo{}
	}

	// check partitionID, createdTimestamp and utcstamp has sam element numbers
	if len(partitions.PartitionNames) != len(partitions.CreatedTimestamps) || len(partitions.PartitionNames) != len(partitions.CreatedUtcTimestamps) {
		return errors.New("partition names and timestamps number is not aligned, response " + partitions.String())
	}

	for i := 0; i < len(partitions.PartitionIDs); i++ {
		if _, ok := partInfo[partitions.PartitionNames[i]]; !ok {
			partInfo[partitions.PartitionNames[i]] = &partitionInfo{
				partitionID:         partitions.PartitionIDs[i],
				createdTimestamp:    partitions.CreatedTimestamps[i],
				createdUtcTimestamp: partitions.CreatedUtcTimestamps[i],
			}
		}
	}
	m.collInfo[collectionName].partInfo = partInfo
	return nil
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
