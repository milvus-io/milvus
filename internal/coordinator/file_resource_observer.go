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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	dcsession "github.com/milvus-io/milvus/internal/datacoord/session"
	qcsession "github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type NodeType int

const (
	QueryNode NodeType = 0 + iota
	DataNode
	ProxyNode
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
	ctx    context.Context
	cancel context.CancelFunc
	meta   rootcoord.IMetaTable

	syncMu       sync.Mutex
	distribution *typeutil.ConcurrentMap[int64, *NodeInfo]

	// node manager
	qnManager *qcsession.NodeManager
	dnManager dcsession.NodeManager
	cluster   qcsession.Cluster
	proxies   proxyutil.ProxyClientManagerInterface

	// mode
	qnMode fileresource.Mode // tips: streaming node used as query node now
	dnMode fileresource.Mode
	pnMode fileresource.Mode

	notifyCh          chan struct{}
	closeCh           chan struct{}
	retryInterval     time.Duration
	reconcileInterval time.Duration
	wg                sync.WaitGroup
	startonce         sync.Once
	closeOnce         sync.Once
}

func NewFileResourceObserver(ctx context.Context) *FileResourceObserver {
	ctx, cancel := context.WithCancel(ctx) //nolint:gosec // cancel is stored and called in Stop()
	return &FileResourceObserver{
		ctx:          ctx,
		cancel:       cancel,
		distribution: typeutil.NewConcurrentMap[int64, *NodeInfo](),

		notifyCh:          make(chan struct{}, 1),
		closeCh:           make(chan struct{}),
		retryInterval:     3 * time.Second,
		reconcileInterval: time.Minute,
		dnMode:            fileresource.GetDataNodeMode(),
		qnMode:            fileresource.GetQueryNodeMode(),
		pnMode:            fileresource.GetProxyMode(),
	}
}

func (m *FileResourceObserver) syncLoop() {
	defer m.wg.Done()

	if m.retryInterval <= 0 {
		m.retryInterval = 3 * time.Second
	}
	if m.reconcileInterval <= 0 {
		m.reconcileInterval = time.Minute
	}
	reconcileTicker := time.NewTicker(m.reconcileInterval)
	defer reconcileTicker.Stop()
	var retryTimer *time.Timer
	var retryCh <-chan time.Time
	defer func() {
		if retryTimer != nil {
			retryTimer.Stop()
		}
	}()

	reconcile := func() {
		if err := m.Sync(); err != nil {
			if retryTimer == nil {
				retryTimer = time.NewTimer(m.retryInterval)
			} else {
				if !retryTimer.Stop() {
					select {
					case <-retryTimer.C:
					default:
					}
				}
				retryTimer.Reset(m.retryInterval)
			}
			retryCh = retryTimer.C
			return
		}
		if retryTimer != nil && !retryTimer.Stop() {
			select {
			case <-retryTimer.C:
			default:
			}
		}
		retryCh = nil
	}

	for {
		select {
		case <-m.notifyCh:
			reconcile()
		case <-retryCh:
			retryCh = nil
			reconcile()
		case <-reconcileTicker.C:
			reconcile()
		case <-m.closeCh:
			mlog.Info(m.ctx, "file resource observer close")
			return
		case <-m.ctx.Done():
			mlog.Info(m.ctx, "file resource observer context done")
			return
		}
	}
}

func (m *FileResourceObserver) Start() {
	if m.qnMode == fileresource.SyncMode || m.dnMode == fileresource.SyncMode || m.pnMode == fileresource.SyncMode {
		m.startonce.Do(func() {
			m.wg.Add(1)
			go m.syncLoop()
			m.Notify()
		})
	}
}

func (m *FileResourceObserver) Stop() {
	m.closeOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
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

// CheckNodeSynced reports whether the node has completed at least one file resource sync
// since startup. Runtime resource version changes do not invalidate node readiness.
func (m *FileResourceObserver) CheckNodeSynced(nodeID int64) bool {
	if m.qnMode != fileresource.SyncMode && m.dnMode != fileresource.SyncMode && m.pnMode != fileresource.SyncMode {
		return true
	}

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
		return merr.WrapErrServiceUnavailable("rootcoord meta is not ready")
	}

	resources, version := m.meta.ListFileResource(m.ctx)
	// skip check if no any resource
	if version == 0 || len(resources) == 0 || m.qnMode != fileresource.SyncMode {
		return nil
	}
	if m.qnManager == nil {
		return merr.WrapErrServiceUnavailable("querycoord node manager is not ready")
	}

	for _, node := range m.qnManager.GetAll() {
		info, ok := m.distribution.Get(node.ID())
		if !ok || info.Version < version {
			return merr.WrapErrServiceUnavailableMsg("file resource not synced, node-%d", node.ID())
		}
	}
	return nil
}

func (m *FileResourceObserver) syncContext() (context.Context, context.CancelFunc) {
	maxDuration := paramtable.Get().CommonCfg.FileResourceSyncMaxDuration.GetAsDurationByParse()
	if maxDuration > 0 {
		return context.WithTimeout(m.ctx, maxDuration)
	}
	return context.WithCancel(m.ctx)
}

func (m *FileResourceObserver) Sync() error {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()
	if m.meta == nil {
		return merr.WrapErrServiceUnavailable("rootcoord meta is not ready")
	}
	var syncErr error
	nodeIDs := []int64{}
	resources, targetVersion := m.meta.ListFileResource(m.ctx)

	// sync file resource to query node if file resource mode was Sync
	if m.qnMode == fileresource.SyncMode {
		if m.qnManager == nil || m.cluster == nil {
			return merr.WrapErrServiceUnavailable("querycoord file resource sync is not ready")
		}
		qnnodes := m.qnManager.GetAll()
		for _, node := range qnnodes {
			if info, ok := m.distribution.Get(node.ID()); !ok || info.Version < targetVersion {
				syncCtx, cancel := m.syncContext()
				status, err := m.cluster.SyncFileResource(syncCtx, node.ID(), &internalpb.SyncFileResourceRequest{
					Resources: resources,
					Version:   targetVersion,
				})
				cancel()
				if err != nil {
					mlog.Warn(m.ctx, "sync file resource failed", mlog.FieldNodeID(node.ID()), mlog.Err(err))
					syncErr = err
					continue
				}

				if err = merr.Error(status); err != nil {
					mlog.Warn(m.ctx, "sync file resource failed", mlog.FieldNodeID(node.ID()), mlog.Err(err))
					syncErr = err
					continue
				}

				m.distribution.Insert(node.ID(), &NodeInfo{
					NodeID:   node.ID(),
					NodeType: QueryNode,
					Version:  targetVersion,
				})
				mlog.Info(m.ctx, "finish sync file resource to query node", mlog.Int64("node", node.ID()), mlog.Uint64("version", targetVersion))
			}
		}

		for _, node := range qnnodes {
			nodeIDs = append(nodeIDs, node.ID())
		}
	}

	// sync file resource to data node if file resource mode was Sync
	if m.dnMode == fileresource.SyncMode {
		if m.dnManager == nil {
			return merr.WrapErrServiceUnavailable("datacoord file resource sync is not ready")
		}
		dnnodes := m.dnManager.GetClientIDs()

		for _, nodeID := range dnnodes {
			if info, ok := m.distribution.Get(nodeID); !ok || info.Version < targetVersion {
				c, err := m.dnManager.GetClient(nodeID)
				if err != nil {
					mlog.Warn(m.ctx, "sync file resource failed, fetch client failed", mlog.Err(err))
					syncErr = err
					continue
				}
				syncCtx, cancel := m.syncContext()
				status, err := c.SyncFileResource(syncCtx, &internalpb.SyncFileResourceRequest{
					Resources: resources,
					Version:   targetVersion,
				})
				cancel()
				if err != nil {
					syncErr = err
					mlog.Warn(m.ctx, "sync file resource failed", mlog.FieldNodeID(nodeID), mlog.Err(err))
					continue
				}

				if err = merr.Error(status); err != nil {
					mlog.Warn(m.ctx, "sync file resource failed", mlog.FieldNodeID(nodeID), mlog.Err(err))
					syncErr = err
					continue
				}

				m.distribution.Insert(nodeID, &NodeInfo{
					NodeID:   nodeID,
					NodeType: DataNode,
					Version:  targetVersion,
				})
				mlog.Info(m.ctx, "finish sync file resource to data node", mlog.FieldNodeID(nodeID), mlog.Uint64("version", targetVersion))
			}
		}

		nodeIDs = append(nodeIDs, dnnodes...)
	}

	// sync file resource to proxy if file resource mode was Sync
	if m.pnMode == fileresource.SyncMode && m.proxies != nil {
		proxyClients := m.proxies.GetProxyClients()
		proxyClients.Range(func(nodeID int64, client types.ProxyClient) bool {
			if info, ok := m.distribution.Get(nodeID); ok && info.Version >= targetVersion {
				return true
			}
			syncCtx, cancel := m.syncContext()
			status, err := client.SyncFileResource(syncCtx, &internalpb.SyncFileResourceRequest{
				Resources: resources,
				Version:   targetVersion,
			})
			cancel()
			if errors.Is(err, merr.ErrServiceUnimplemented) {
				return true
			}
			if err != nil {
				mlog.Warn(m.ctx, "sync file resource failed", mlog.FieldNodeID(nodeID), mlog.Err(err))
				syncErr = err
				return true
			}

			if err = merr.Error(status); err != nil {
				mlog.Warn(m.ctx, "sync file resource failed", mlog.FieldNodeID(nodeID), mlog.Err(err))
				syncErr = err
				return true
			}

			m.distribution.Insert(nodeID, &NodeInfo{
				NodeID:   nodeID,
				NodeType: ProxyNode,
				Version:  targetVersion,
			})
			mlog.Info(m.ctx, "finish sync file resource to proxy", mlog.FieldNodeID(nodeID), mlog.Uint64("version", targetVersion))
			return true
		})
		proxyClients.Range(func(nodeID int64, _ types.ProxyClient) bool {
			nodeIDs = append(nodeIDs, nodeID)
			return true
		})
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

func (m *FileResourceObserver) InitProxyManager(manager proxyutil.ProxyClientManagerInterface) {
	m.proxies = manager
}
