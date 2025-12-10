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

package observers

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type FileResourceObserver struct {
	lock.RWMutex
	resources []*internalpb.FileResourceInfo
	version   uint64

	ctx context.Context

	distribution map[int64]uint64

	// version distribution
	nodeManager *session.NodeManager
	cluster     session.Cluster

	notifyCh  chan struct{}
	closeCh   chan struct{}
	wg        sync.WaitGroup
	sf        conc.Singleflight[any]
	once      sync.Once
	closeOnce sync.Once
}

func NewFileResourceObserver(ctx context.Context, nodeManager *session.NodeManager, cluster session.Cluster) *FileResourceObserver {
	return &FileResourceObserver{
		ctx:          ctx,
		nodeManager:  nodeManager,
		cluster:      cluster,
		distribution: map[int64]uint64{},

		notifyCh: make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
		sf:       conc.Singleflight[any]{},
	}
}

func (m *FileResourceObserver) getResources() ([]*internalpb.FileResourceInfo, uint64) {
	m.RLock()
	defer m.RUnlock()
	return m.resources, m.version
}

func (m *FileResourceObserver) syncLoop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.notifyCh:
			resources, version := m.getResources()
			err := m.sync(resources, version)
			if err != nil {
				// retry if error exist
				m.sf.Do("retry", func() (any, error) {
					time.Sleep(5 * time.Second)
					m.Notify()
					return nil, nil
				})
			}
		case <-m.closeCh:
			log.Info("file resource observer close")
			return
		case <-m.ctx.Done():
			log.Info("file resource observer context done")
			return
		}
	}
}

func (m *FileResourceObserver) Start() {
	if fileresource.IsSyncMode(paramtable.Get().QueryCoordCfg.FileResourceMode.GetValue()) {
		m.once.Do(func() {
			m.wg.Add(1)
			go m.syncLoop()
			m.Notify()
		})
	}
}

func (m *FileResourceObserver) Stop() {
	m.closeOnce.Do(func() {
		close(m.closeCh)
		m.wg.Wait()
	})
}

func (m *FileResourceObserver) Notify() {
	select {
	case m.notifyCh <- struct{}{}:
	default:
	}
}

func (m *FileResourceObserver) sync(resources []*internalpb.FileResourceInfo, version uint64) error {
	nodes := m.nodeManager.GetAll()
	var syncErr error

	newDistribution := make(map[int64]uint64)
	for _, node := range nodes {
		newDistribution[node.ID()] = m.distribution[node.ID()]
		if m.distribution[node.ID()] < version {
			status, err := m.cluster.SyncFileResource(m.ctx, node.ID(), &internalpb.SyncFileResourceRequest{
				Resources: resources,
				Version:   version,
			})
			if err != nil {
				log.Warn("sync file resource failed", zap.Int64("nodeID", node.ID()), zap.Error(err))
				syncErr = err
				continue
			}

			if err = merr.Error(status); err != nil {
				log.Warn("sync file resource failed", zap.Int64("nodeID", node.ID()), zap.Error(err))
				syncErr = err
				continue
			}

			newDistribution[node.ID()] = version
			log.Info("finish sync file resource to query node", zap.Int64("node", node.ID()), zap.Uint64("version", version))
		}
	}
	m.distribution = newDistribution

	if syncErr != nil {
		return syncErr
	}
	return nil
}

func (m *FileResourceObserver) UpdateResources(resources []*internalpb.FileResourceInfo, version uint64) {
	m.Lock()
	defer m.Unlock()
	m.resources = resources
	m.version = version
	m.Notify()
}
