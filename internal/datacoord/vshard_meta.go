// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

type VshardMeta interface {
	GetVShardInfo(partitionID int64, channel string) []*datapb.VShardInfo
	ListVShardInfos() []*datapb.VShardInfo
	SaveVShardInfos([]*datapb.VShardInfo) error
	DropVShardInfo(*datapb.VShardInfo) error

	ListVShardTasks() []*datapb.VShardTask
	SaveVShardTask(*datapb.VShardTask) error
	DropVShardTask(*datapb.VShardTask) error
	GetVShardTasksByPartition(int64) []*datapb.VShardTask
	GetVShardTaskByID(int64, int64) *datapb.VShardTask

	SaveVShardInfosAndVshardTask([]*datapb.VShardInfo, *datapb.VShardTask) error
}

var _ VshardMeta = (*VshardMetaImpl)(nil)

type VshardMetaImpl struct {
	sync.RWMutex
	ctx              context.Context
	catalog          metastore.DataCoordCatalog
	vshardInfosCache map[string]map[string]*datapb.VShardInfo // partitionID+channel -> vshardDesc -> VShardInfo
	vshardTasksCache map[int64]map[int64]*datapb.VShardTask   // partitionID -> taskID -> vshardTask
}

func newVshardMetaImpl(ctx context.Context, catalog metastore.DataCoordCatalog) (*VshardMetaImpl, error) {
	meta := &VshardMetaImpl{
		RWMutex:          sync.RWMutex{},
		ctx:              ctx,
		catalog:          catalog,
		vshardInfosCache: make(map[string]map[string]*datapb.VShardInfo),
		vshardTasksCache: make(map[int64]map[int64]*datapb.VShardTask),
	}
	if err := meta.reloadFromKV(); err != nil {
		return nil, err
	}
	return meta, nil
}

func vShardInfoGroupKey(vshard *datapb.VShardInfo) string {
	return fmt.Sprintf("%d-%s", vshard.PartitionId, vshard.GetVchannel())
}

func (m *VshardMetaImpl) reloadFromKV() error {
	record := timerecord.NewTimeRecorder("VshardMeta-reloadFromKV")

	vshardMetas, err := m.catalog.ListVShardInfos(m.ctx)
	if err != nil {
		return err
	}

	for _, vshard := range vshardMetas {
		key := vShardInfoGroupKey(vshard)
		_, exist := m.vshardInfosCache[key]
		if !exist {
			m.vshardInfosCache[key] = make(map[string]*datapb.VShardInfo, 0)
		}
		m.vshardInfosCache[key][vshard.VshardDesc.String()] = vshard
	}

	tasks, err := m.catalog.ListVShardTasks(m.ctx)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		_, exist := m.vshardTasksCache[task.GetPartitionId()]
		if !exist {
			m.vshardTasksCache[task.GetPartitionId()] = make(map[int64]*datapb.VShardTask, 0)
		}
		m.vshardTasksCache[task.GetPartitionId()][task.GetId()] = task
	}

	log.Info("DataCoord VshardMetaImpl reloadFromKV done", zap.Duration("duration", record.ElapseSpan()))
	return nil
}

func (m *VshardMetaImpl) SaveVShardInfos(vshards []*datapb.VShardInfo) error {
	m.Lock()
	defer m.Unlock()
	if err := m.catalog.SaveVShardInfos(m.ctx, vshards); err != nil {
		log.Error("meta update: update VshardMeta info fail", zap.Error(err))
		return err
	}
	for _, vshard := range vshards {
		key := vShardInfoGroupKey(vshard)
		_, exist := m.vshardInfosCache[key]
		if !exist {
			m.vshardInfosCache[key] = make(map[string]*datapb.VShardInfo, 0)
		}
		m.vshardInfosCache[key][vshard.VshardDesc.String()] = vshard
	}
	return nil
}

func (m *VshardMetaImpl) DropVShardInfo(vshard *datapb.VShardInfo) error {
	m.Lock()
	defer m.Unlock()
	if err := m.catalog.DropVShardInfo(m.ctx, vshard); err != nil {
		log.Error("meta update: drop partitionVShardInfo info fail",
			zap.Int64("collectionID", vshard.GetCollectionId()),
			zap.Int64("partitionID", vshard.GetPartitionId()),
			zap.String("vchannel", vshard.GetVchannel()),
			zap.Error(err))
		return err
	}
	delete(m.vshardInfosCache, vShardInfoGroupKey(vshard))
	return nil
}

func (m *VshardMetaImpl) GetVShardInfo(partitionID int64, channel string) []*datapb.VShardInfo {
	m.Lock()
	defer m.Unlock()
	key := fmt.Sprintf("%d-%s", partitionID, channel)
	partitionVshardsMap, exist := m.vshardInfosCache[key]
	if !exist {
		return nil
	}
	partitionVshards := make([]*datapb.VShardInfo, 0, len(partitionVshardsMap))
	for _, value := range partitionVshardsMap {
		partitionVshards = append(partitionVshards, value)
	}
	return partitionVshards
}

func (m *VshardMetaImpl) ListVShardInfos() []*datapb.VShardInfo {
	m.Lock()
	defer m.Unlock()
	res := make([]*datapb.VShardInfo, 0)
	for _, vshardInfos := range m.vshardInfosCache {
		for _, vshardInfo := range vshardInfos {
			res = append(res, vshardInfo)
		}
	}
	return res
}

func (m *VshardMetaImpl) SaveVShardTask(task *datapb.VShardTask) error {
	m.Lock()
	defer m.Unlock()
	if err := m.catalog.SaveVShardTask(m.ctx, task); err != nil {
		log.Error("meta update: update VshardMeta info fail", zap.Error(err))
		return err
	}
	_, exist := m.vshardTasksCache[task.GetPartitionId()]
	if !exist {
		m.vshardTasksCache[task.GetPartitionId()] = make(map[int64]*datapb.VShardTask, 0)
	}
	m.vshardTasksCache[task.GetPartitionId()][task.GetId()] = task
	return nil
}

func (m *VshardMetaImpl) DropVShardTask(vshard *datapb.VShardTask) error {
	m.Lock()
	defer m.Unlock()
	if err := m.catalog.DropVShardTask(m.ctx, vshard); err != nil {
		log.Error("meta update: drop ReVShardTask info fail",
			zap.Int64("collectionID", vshard.GetCollectionId()),
			zap.Int64("partitionID", vshard.GetPartitionId()),
			zap.String("vchannel", vshard.GetVchannel()),
			zap.Int64("id", vshard.GetId()),
			zap.Error(err))
		return err
	}

	if _, exist := m.vshardTasksCache[vshard.GetPartitionId()]; exist {
		delete(m.vshardTasksCache[vshard.GetPartitionId()], vshard.GetId())
	}
	if len(m.vshardTasksCache[vshard.GetPartitionId()]) == 0 {
		delete(m.vshardTasksCache, vshard.GetPartitionId())
	}
	return nil
}

func (m *VshardMetaImpl) GetVShardTasksByPartition(partitionID int64) []*datapb.VShardTask {
	m.Lock()
	defer m.Unlock()
	partitionVshardsMap, exist := m.vshardTasksCache[partitionID]
	if !exist {
		return nil
	}
	partitionVshards := make([]*datapb.VShardTask, 0, len(partitionVshardsMap))
	for _, value := range partitionVshardsMap {
		partitionVshards = append(partitionVshards, value)
	}
	return partitionVshards
}

func (m *VshardMetaImpl) GetVShardTaskByID(partitionID, taskID int64) *datapb.VShardTask {
	m.Lock()
	defer m.Unlock()
	partitionVshardsMap, exist := m.vshardTasksCache[partitionID]
	if !exist {
		return nil
	}
	for id, value := range partitionVshardsMap {
		if id == taskID {
			return value
		}
	}
	return nil
}

func (m *VshardMetaImpl) SaveVShardInfosAndVshardTask(vshards []*datapb.VShardInfo, task *datapb.VShardTask) error {
	m.Lock()
	defer m.Unlock()
	if err := m.catalog.SaveVShardInfosAndVShardTasks(m.ctx, vshards, task); err != nil {
		log.Error("meta update: update VshardInfo and VshardTask fail", zap.Error(err))
		return err
	}
	// update vshard info cache
	for _, vshard := range vshards {
		key := vShardInfoGroupKey(vshard)
		_, exist := m.vshardInfosCache[key]
		if !exist {
			m.vshardInfosCache[key] = make(map[string]*datapb.VShardInfo, 0)
		}
		m.vshardInfosCache[key][vshard.VshardDesc.String()] = vshard
	}
	// update vshardTask cache
	_, exist := m.vshardTasksCache[task.GetPartitionId()]
	if !exist {
		m.vshardTasksCache[task.GetPartitionId()] = make(map[int64]*datapb.VShardTask, 0)
	}
	m.vshardTasksCache[task.GetPartitionId()][task.GetId()] = task
	return nil
}

func (m *VshardMetaImpl) ListVShardTasks() []*datapb.VShardTask {
	m.Lock()
	defer m.Unlock()
	res := make([]*datapb.VShardTask, 0)
	for _, vshardTasks := range m.vshardTasksCache {
		for _, task := range vshardTasks {
			res = append(res, task)
		}
	}
	return res
}
