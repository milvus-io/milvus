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

package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	dcsession "github.com/milvus-io/milvus/internal/datacoord/session"
	qcsession "github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type NodeType int

const (
	QueryNode NodeType = 0 + iota
	DataNode
)

type NodeInfo struct {
	NodeID   int64
	NodeType NodeType
	Version  uint64
}

type FileResourceMeta interface {
	GetResources() ([]*internalpb.FileResourceInfo, uint64)
}

type FileResourceObserver struct {
	ctx  context.Context
	meta rootcoord.IMetaTable

	syncMu       sync.Mutex
	distribution *typeutil.ConcurrentMap[int64, *NodeInfo]

	// node manager
	qnManager *qcsession.NodeManager
	dnManager dcsession.NodeManager
	cluster   qcsession.Cluster

	// mode
	qnMode fileresource.Mode // tips: streaming node used as query node now
	dnMode fileresource.Mode

	notifyCh  chan struct{}
	closeCh   chan struct{}
	wg        sync.WaitGroup
	sf        conc.Singleflight[any]
	startonce sync.Once
	closeOnce sync.Once
}

func NewFileResourceObserver(ctx context.Context) *FileResourceObserver {
	return &FileResourceObserver{
		ctx:          ctx,
		distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),

		notifyCh: make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
		sf:       conc.Singleflight[any]{},
		dnMode:   fileresource.ParseMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()),
		qnMode:   fileresource.ParseMode(paramtable.Get().CommonCfg.QNFileResourceMode.GetValue()),
	}
}

// sleep and notify to sync
func (m *FileResourceObserver) RetryNotify() {
	go func() {
		m.sf.Do("retry", func() (any, error) {
			time.Sleep(3 * time.Second)
			m.Notify()
			return nil, nil
		})
	}()
}

func (m *FileResourceObserver) syncLoop() {
	defer m.wg.Done()
	for {
		select {
		case <-m.notifyCh:
			err := m.Sync()
			if err != nil {
				// retry if error exist
				m.RetryNotify()
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
	if m.qnMode == fileresource.SyncMode || m.dnMode == fileresource.SyncMode {
		m.startonce.Do(func() {
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

// if node sync at least once, it will be a valid node.
func (m *FileResourceObserver) CheckNodeSynced(nodeID int64) bool {
	// return false if meta is not ready
	if m.meta == nil {
		return false
	}

	resources, version := m.meta.ListFileResource(m.ctx)
	// skip check if no any resource
	if version == 0 || len(resources) == 0 {
		return true
	}
	_, ok := m.distribution.Get(nodeID)
	return ok
}

// check if all valid nodes sync the resource to current version
func (m *FileResourceObserver) CheckAllQnReady() error {
	// return error if meta is not ready
	if m.meta == nil {
		return merr.WrapErrParameterInvalidMsg("rootcoord meta is not ready")
	}

	resources, version := m.meta.ListFileResource(m.ctx)
	// skip check if no any resource
	if version == 0 || len(resources) == 0 {
		return nil
	}

	var err error
	m.distribution.Range(func(nodeID int64, node *NodeInfo) bool {
		if node.NodeType == QueryNode && node.Version < version {
			err = merr.WrapErrParameterInvalidMsg("node %d file resource not synced", nodeID)
			return false
		}
		return true
	})
	return err
}

func (m *FileResourceObserver) Sync() error {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()
	var syncErr error
	nodeIDs := []int64{}
	resources, targetVersion := m.meta.ListFileResource(m.ctx)

	// sync file resource to query node if file resource mode was Sync
	if m.qnMode == fileresource.SyncMode {
		qnnodes := m.qnManager.GetAll()
		for _, node := range qnnodes {
			if info, ok := m.distribution.Get(node.ID()); !ok || info.Version < targetVersion {
				status, err := m.cluster.SyncFileResource(m.ctx, node.ID(), &internalpb.SyncFileResourceRequest{
					Resources: resources,
					Version:   targetVersion,
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

				m.distribution.Insert(node.ID(), &NodeInfo{
					NodeID:   node.ID(),
					NodeType: QueryNode,
					Version:  targetVersion,
				})
				log.Info("finish sync file resource to query node", zap.Int64("node", node.ID()), zap.Uint64("version", targetVersion))
			}
		}

		for _, node := range qnnodes {
			nodeIDs = append(nodeIDs, node.ID())
		}
	}

	// sync file resource to data node if file resource mode was Sync
	if m.dnMode == fileresource.SyncMode {
		dnnodes := m.dnManager.GetClientIDs()

		for _, nodeID := range dnnodes {
			if info, ok := m.distribution.Get(nodeID); !ok || info.Version < targetVersion {
				c, err := m.dnManager.GetClient(nodeID)
				if err != nil {
					log.Warn("sync file resource failed, fetch client failed", zap.Error(err))
					syncErr = err
					continue
				}
				status, err := c.SyncFileResource(m.ctx, &internalpb.SyncFileResourceRequest{
					Resources: resources,
					Version:   targetVersion,
				})
				if err != nil {
					syncErr = err
					log.Warn("sync file resource failed", zap.Int64("nodeID", nodeID), zap.Error(err))
					continue
				}

				if err = merr.Error(status); err != nil {
					log.Warn("sync file resource failed", zap.Int64("nodeID", nodeID), zap.Error(err))
					syncErr = err
					continue
				}

				m.distribution.Insert(nodeID, &NodeInfo{
					NodeID:   nodeID,
					NodeType: DataNode,
					Version:  targetVersion,
				})
				log.Info("finish sync file resource to data node", zap.Int64("nodeID", nodeID), zap.Uint64("version", targetVersion))
			}
		}

		nodeIDs = append(nodeIDs, dnnodes...)
	}

	// delete node from distribution if node is not in manager
	m.distribution.Range(func(nodeID int64, node *NodeInfo) bool {
		if !lo.Contains(nodeIDs, nodeID) {
			m.distribution.Remove(nodeID)
		}
		return true
	})

	if syncErr != nil {
		return syncErr
	}
	return nil
}

func (m *FileResourceObserver) InitMeta(meta rootcoord.IMetaTable) {
	m.meta = meta
}

func (m *FileResourceObserver) InitQueryCoord(manager *qcsession.NodeManager, cluster qcsession.Cluster) {
	m.qnManager = manager
	m.cluster = cluster
}

func (m *FileResourceObserver) InitDataCoord(manager dcsession.NodeManager) {
	m.dnManager = manager
}
