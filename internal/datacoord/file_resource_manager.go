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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/types"
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
	ctx  context.Context
	meta *meta

	// version distribution
	nodeManager  session.NodeManager
	distribution map[int64]uint64

	notifyCh chan struct{}
	sf       conc.Singleflight[any]
	once     sync.Once

	storage storageClient

	mixCoord types.MixCoord
	qnMode   fileresource.Mode // query node and streaming node file resource mode
	dnMode   fileresource.Mode // data node file resource mode
	// close
	closeCh chan struct{}
	wg      sync.WaitGroup
}

func NewFileResourceManager(ctx context.Context, mixCoord types.MixCoord, meta *meta, nodeManager session.NodeManager, storage storageClient) *FileResourceManager {
	return &FileResourceManager{
		ctx:          ctx,
		meta:         meta,
		nodeManager:  nodeManager,
		distribution: map[int64]uint64{},

		closeCh:  make(chan struct{}),
		sf:       conc.Singleflight[any]{},
		qnMode:   fileresource.ParseMode(paramtable.Get().CommonCfg.QNFileResourceMode.GetValue()),
		dnMode:   fileresource.ParseMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()),
		storage:  storage,
		mixCoord: mixCoord,

		notifyCh: make(chan struct{}, 1),
	}
}

func (m *FileResourceManager) AddFileResource(ctx context.Context, resource *internalpb.FileResourceInfo) error {
	exist, err := m.storage.Exist(ctx, resource.Path)
	if err != nil {
		return err
	}

	if !exist {
		return merr.WrapErrAsInputError(errors.Errorf("add file resource failed: file path %s not exist", resource.Path))
	}

	err = m.meta.AddFileResource(ctx, resource)
	if err != nil {
		return err
	}

	m.Notify()
	m.syncQcFileResource(ctx)
	return nil
}

func (m *FileResourceManager) RemoveFileResource(ctx context.Context, name string) error {
	err := m.meta.RemoveFileResource(ctx, name)
	if err != nil {
		return err
	}

	m.Notify()
	m.syncQcFileResource(ctx)
	return nil
}

func (m *FileResourceManager) syncQcFileResource(ctx context.Context) error {
	if m.qnMode != fileresource.SyncMode {
		return nil
	}
	resources, version := m.meta.ListFileResource(ctx)
	err := m.mixCoord.SyncQcFileResource(ctx, resources, version)
	if err != nil {
		log.Warn("sync qc file resource failed", zap.Error(err))
		return err
	}
	log.Info("finish sync qc file resource to query node", zap.Uint64("version", version), zap.Int("resources", len(resources)))
	return nil
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
	if fileresource.IsSyncMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()) {
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

// notify sync file resource to datanode
// if file resource mode was Sync
func (m *FileResourceManager) Notify() {
	if m == nil || m.dnMode != fileresource.SyncMode {
		return
	}

	select {
	case m.notifyCh <- struct{}{}:
	default:
	}
}

func (m *FileResourceManager) sync() error {
	nodes := m.nodeManager.GetClientIDs()

	var syncErr error

	resources, version := m.meta.ListFileResource(m.ctx)

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
