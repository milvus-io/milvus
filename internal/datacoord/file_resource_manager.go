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

// Package datacoord contains core functions in datacoord
package datacoord

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type storageClient interface {
	Exist(ctx context.Context, filePath string) (bool, error)
}

type FileResourceManager struct {
	ctx context.Context

	// version distribution
	nodeManager  session.NodeManager
	distribution map[int64]uint64

	notifyCh chan struct{}
	sf       conc.Singleflight[any]
	once     sync.Once
	meta     *meta

	mode fileresource.Mode // data node file resource mode
	// close
	closeCh chan struct{}
	wg      sync.WaitGroup
}

func NewFileResourceManager(ctx context.Context, meta *meta, nodeManager session.NodeManager) *FileResourceManager {
	return &FileResourceManager{
		ctx:          ctx,
		nodeManager:  nodeManager,
		meta:         meta,
		distribution: map[int64]uint64{},

		closeCh: make(chan struct{}),
		sf:      conc.Singleflight[any]{},
		mode:    fileresource.ParseMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()),

		notifyCh: make(chan struct{}, 1),
	}
}

func (m *FileResourceManager) syncLoop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.notifyCh:
			err := m.sync()
			if err != nil {
				// retry if error exist
				m.sf.Do("retry", func() (any, error) {
					time.Sleep(5 * time.Second)
					m.Notify()
					return nil, nil
				})
			}
		case <-m.ctx.Done():
			return
		case <-m.closeCh:
			return
		}
	}
}

func (m *FileResourceManager) Start() {
	if m.mode == fileresource.SyncMode {
		m.once.Do(func() {
			m.wg.Add(1)
			go m.syncLoop()
		})
	}
}

func (m *FileResourceManager) Close() {
	close(m.closeCh)
	m.wg.Wait()
}

func (m *FileResourceManager) UpdateResources(resources []*internalpb.FileResourceInfo, version uint64) {
	m.meta.UpdateFileResources(m.ctx, resources, version)
	m.Notify()
}

// notify sync file resource to datanode
// if file resource mode was Sync
func (m *FileResourceManager) Notify() {
	if m == nil || m.mode != fileresource.SyncMode {
		log.Info("notify sync file resource to datanode, but file resource mode is not sync")
		return
	}

	select {
	case m.notifyCh <- struct{}{}:
		log.Info("notify sync file resource to datanode")
	default:
	}
}

func (m *FileResourceManager) sync() error {
	nodes := m.nodeManager.GetClientIDs()

	var syncErr error

	resources, version := m.meta.ListFileResources(m.ctx)

	newDistribution := make(map[int64]uint64)
	for _, node := range nodes {
		newDistribution[node] = m.distribution[node]
		if m.distribution[node] < version {
			c, err := m.nodeManager.GetClient(node)
			if err != nil {
				log.Warn("sync file resource failed, fetch client failed", zap.Error(err))
				syncErr = err
				continue
			}
			status, err := c.SyncFileResource(m.ctx, &internalpb.SyncFileResourceRequest{
				Resources: resources,
				Version:   version,
			})
			if err != nil {
				syncErr = err
				log.Warn("sync file resource failed", zap.Int64("nodeID", node), zap.Error(err))
				continue
			}

			if err = merr.Error(status); err != nil {
				log.Warn("sync file resource failed", zap.Int64("nodeID", node), zap.Error(err))
				syncErr = err
				continue
			}
			newDistribution[node] = version
			log.Info("finish sync file resource to data node", zap.Int64("node", node), zap.Uint64("version", version))
		}
	}
	m.distribution = newDistribution

	if syncErr != nil {
		return syncErr
	}
	return nil
}
