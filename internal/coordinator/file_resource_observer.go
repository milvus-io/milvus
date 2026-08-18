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
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"

	dcsession "github.com/milvus-io/milvus/internal/datacoord/session"
	qcsession "github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type NodeType int

const (
	QueryNode NodeType = 0 + iota
	DataNode
	Proxy
)

type NodeInfo struct {
	NodeID   int64
	NodeType NodeType
	Version  uint64
}

type FileResourceMeta interface {
	GetResources() ([]*internalpb.FileResourceInfo, uint64)
}

// gateState is the snapshot CheckNodesSynced judges against without a lock.
type gateState struct {
	version uint64
	gated   bool
}

type FileResourceObserver struct {
	ctx  context.Context
	meta rootcoord.IMetaTable

	syncMu       sync.Mutex
	distribution *typeutil.ConcurrentMap[int64, *NodeInfo]

	// node manager
	qnManager    *qcsession.NodeManager
	dnManager    dcsession.NodeManager
	cluster      qcsession.Cluster
	proxyManager proxyutil.ProxyClientManagerInterface

	// mode
	qnMode    fileresource.Mode // tips: streaming node used as query node now
	dnMode    fileresource.Mode
	proxyMode fileresource.Mode

	// gateSnapshot caches the one fact CheckNodesSynced needs from the meta
	// table - whether file resources are registered, and at which version - so
	// the task executors' per-dispatch gate never takes rootcoord's ddLock.
	// Refreshed by every Sync pass; between a resource change and the next
	// pass the gate may briefly judge against the previous version, which is
	// the same convergence window the node syncs themselves have.
	gateSnapshot atomic.Pointer[gateState]

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

		notifyCh:  make(chan struct{}, 1),
		closeCh:   make(chan struct{}),
		sf:        conc.Singleflight[any]{},
		dnMode:    fileresource.ParseMode(paramtable.Get().CommonCfg.DNFileResourceMode.GetValue()),
		qnMode:    fileresource.ParseMode(paramtable.Get().CommonCfg.QNFileResourceMode.GetValue()),
		proxyMode: fileresource.ParseMode(paramtable.Get().CommonCfg.ProxyFileResourceMode.GetValue()),
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
			mlog.Info(m.ctx, "file resource observer close")
			return
		case <-m.ctx.Done():
			mlog.Info(m.ctx, "file resource observer context done")
			return
		}
	}
}

func (m *FileResourceObserver) Start() {
	if m.qnMode == fileresource.SyncMode || m.dnMode == fileresource.SyncMode || m.proxyMode == fileresource.SyncMode {
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
		return merr.WrapErrServiceUnavailable("rootcoord meta is not ready")
	}

	resources, version := m.meta.ListFileResource(m.ctx)
	// skip check if no any resource
	if version == 0 || len(resources) == 0 {
		return nil
	}

	var err error
	m.distribution.Range(func(_ int64, node *NodeInfo) bool {
		if node.NodeType == QueryNode && node.Version < version {
			err = merr.WrapErrServiceUnavailableMsg("file resource not synced, node-%d", node.NodeID)
			return false
		}
		return true
	})
	return err
}

// CheckNodesSynced reports whether the given query nodes hold the current
// analyzer file resources.
//
// This is the query-side counterpart of CheckAllQnReady. A DDL only needs
// whatever process validates analyzer parameters to hold the files; a node
// that actually runs an analyzer over data - BM25, raw text match - needs them
// on its own disk. Callers pass the nodes they are about to put data on, so a
// lagging node they are not using cannot block them.
//
// A node with no record has confirmed nothing and must not be read as ready: a
// node registers its session, which is what puts it in a resource group and in
// the task executor, before its first sync has been asked for. The comparison
// is against the version rather than mere presence for the same reason - a node
// that acknowledged one sync keeps its record, so presence alone would pass a
// node still downloading the version registered after it.
func (m *FileResourceObserver) CheckNodesSynced(nodeIDs []int64) error {
	if len(nodeIDs) == 0 {
		return nil
	}
	// Mode first: it is a plain field, and on a deployment that does not sync
	// query nodes the gate must cost nothing.
	if m.qnMode != fileresource.SyncMode {
		return nil
	}
	if m.meta == nil {
		return merr.WrapErrServiceUnavailable("rootcoord meta is not ready")
	}

	// The cached snapshot keeps this off rootcoord's ddLock: the task
	// executors consult the gate for every pending grow action on every
	// dispatch, and taking a DDL lock there couples querycoord's scheduling
	// tick to rootcoord's DDL throughput. Only the first call before any Sync
	// pass reads the meta table directly.
	snap := m.gateSnapshot.Load()
	if snap == nil {
		resources, version := m.meta.ListFileResource(m.ctx)
		snap = &gateState{version: version, gated: version != 0 && len(resources) > 0}
		// CAS, not Store: a Sync pass finishing right now has fresher data,
		// and this lazy first read must not clobber it with an older view.
		if !m.gateSnapshot.CompareAndSwap(nil, snap) {
			snap = m.gateSnapshot.Load()
		}
	}
	if !snap.gated {
		return nil
	}
	version := snap.version

	for _, nodeID := range nodeIDs {
		info, ok := m.distribution.Get(nodeID)
		if !ok {
			return merr.WrapErrServiceUnavailableMsg(
				"node %d has not synced any analyzer file resource yet", nodeID)
		}
		if info.Version < version {
			return merr.WrapErrServiceUnavailableMsg(
				"node %d analyzer file resource version %d is behind %d", nodeID, info.Version, version)
		}
	}
	return nil
}

func (m *FileResourceObserver) Sync() error {
	m.syncMu.Lock()
	defer m.syncMu.Unlock()
	var syncErr error
	activeNodes := make(map[int64]struct{})
	resources, targetVersion := m.meta.ListFileResource(m.ctx)
	m.gateSnapshot.Store(&gateState{version: targetVersion, gated: targetVersion != 0 && len(resources) > 0})

	// sync file resource to query node if file resource mode was Sync
	if m.qnMode == fileresource.SyncMode {
		qnnodes := m.qnManager.GetAll()
		for _, node := range qnnodes {
			if info, ok := m.distribution.Get(node.ID()); !ok || info.Version < targetVersion {
				status, err := m.cluster.SyncFileResource(m.ctx, node.ID(), &internalpb.SyncFileResourceRequest{
					Resources: resources,
					Version:   targetVersion,
				})
				// A node that does not implement the RPC cannot be asked to
				// download anything, and holding grow actions off it forever
				// would wedge a rolling upgrade on its oldest node. It is
				// recorded as synced: whatever it serves, it served before
				// file resources existed, which is the compatibility floor.
				if errors.Is(err, merr.ErrServiceUnimplemented) {
					err = nil
					status = merr.Success()
				}
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
			activeNodes[node.ID()] = struct{}{}
		}
	}

	// sync file resource to data node if file resource mode was Sync
	if m.dnMode == fileresource.SyncMode {
		dnnodes := m.dnManager.GetClientIDs()

		for _, nodeID := range dnnodes {
			if info, ok := m.distribution.Get(nodeID); !ok || info.Version < targetVersion {
				c, err := m.dnManager.GetClient(nodeID)
				if err != nil {
					mlog.Warn(m.ctx, "sync file resource failed, fetch client failed", mlog.Err(err))
					syncErr = err
					continue
				}
				status, err := c.SyncFileResource(m.ctx, &internalpb.SyncFileResourceRequest{
					Resources: resources,
					Version:   targetVersion,
				})
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

		for _, nodeID := range dnnodes {
			activeNodes[nodeID] = struct{}{}
		}
	}

	// sync file resource to proxy if file resource mode was Sync
	if m.proxyMode == fileresource.SyncMode && m.proxyManager != nil {
		proxyClients := m.proxyManager.GetProxyClients()
		proxyClients.Range(func(nodeID int64, client types.ProxyClient) bool {
			if info, ok := m.distribution.Get(nodeID); !ok || info.Version < targetVersion {
				status, err := client.SyncFileResource(m.ctx, &internalpb.SyncFileResourceRequest{
					Resources: resources,
					Version:   targetVersion,
				})
				if errors.Is(err, merr.ErrServiceUnimplemented) {
					err = nil
				} else if err == nil {
					err = merr.Error(status)
				}
				if err != nil {
					mlog.Warn(m.ctx, "sync file resource failed", mlog.FieldNodeID(nodeID), mlog.String("nodeType", "proxy"), mlog.Err(err))
					syncErr = err
					return true
				}
				m.distribution.Insert(nodeID, &NodeInfo{
					NodeID:   nodeID,
					NodeType: Proxy,
					Version:  targetVersion,
				})
				mlog.Info(m.ctx, "finish sync file resource to proxy", mlog.FieldNodeID(nodeID), mlog.Uint64("version", targetVersion))
			}
			return true
		})

		proxyClients.Range(func(nodeID int64, _ types.ProxyClient) bool {
			activeNodes[nodeID] = struct{}{}
			return true
		})
	}

	// delete node from distribution if node is not in manager
	m.distribution.Range(func(nodeID int64, _ *NodeInfo) bool {
		if _, ok := activeNodes[nodeID]; !ok {
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

func (m *FileResourceObserver) InitProxy(manager proxyutil.ProxyClientManagerInterface) {
	m.proxyManager = manager
}
