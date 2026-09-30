// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type queryNodeCreatorFunc func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error)

type NodeInfo struct {
	NodeID      UniqueID
	Address     string
	Serviceable bool
	// ResourceGroup is the resource group of the REPLICA this node leads, not
	// of the node itself -- a replica may borrow nodes from another group, so
	// the two are not the same thing. Empty means unknown: the coordinator
	// predates the field, or the entry came from somewhere that does not set
	// it. It must not be read as "no resource group", and an unknown entry
	// never matches a named group in FilterByResourceGroup.
	//
	// The one reader is FilterByResourceGroup, applied by LBPolicyImpl.selectNode
	// when ChannelWorkload.ResourceGroup names a group. Nothing on the proxy's
	// request path sets that field yet, so deployed traffic still builds its
	// candidate sets purely from Serviceable; the filter ships with the tag so
	// that the constraints on consuming it (see FilterByResourceGroup) live in
	// one place rather than with each future caller.
	ResourceGroup string
}

func (n NodeInfo) String() string {
	return fmt.Sprintf("<NodeID: %d, serviceable: %v, address: %s, rg: %s>", n.NodeID, n.Serviceable, n.Address, n.ResourceGroup)
}

// FilterByResourceGroup returns the leaders in leaders that belong to a replica
// in rg. rg == "" is the absence of a scope and returns leaders unchanged,
// matching the utils-layer surfaces on the coordinator. An entry whose tag is
// unknown (empty -- an old coordinator) never matches a named group.
//
// It filters the candidate list OF ONE CHANNEL. It must never be used to drop
// channels from the shard-leader map: LBPolicyImpl.Execute derives its fan-out
// from GetShardLeaderList() and never cross-checks the channel count against
// the collection's shard number, so a dropped channel is not an error -- it is
// a successful query over a subset of the shards, with no signal anywhere. A
// channel the group cannot serve has to surface from selectNode as a retriable
// error instead, which is what applying the scope there guarantees.
func FilterByResourceGroup(leaders []NodeInfo, rg string) []NodeInfo {
	if rg == "" {
		return leaders
	}
	scoped := make([]NodeInfo, 0, len(leaders))
	for _, node := range leaders {
		if node.ResourceGroup == rg {
			scoped = append(scoped, node)
		}
	}
	return scoped
}

type shardClient struct {
	sync.RWMutex
	info     NodeInfo
	poolSize int
	clients  []types.QueryNodeClient
	creator  queryNodeCreatorFunc

	initialized atomic.Bool
	isClosed    bool

	idx             atomic.Int64
	lastActiveTs    *atomic.Int64
	expiredDuration time.Duration
}

func newShardClient(info NodeInfo, creator queryNodeCreatorFunc, expiredDuration time.Duration) *shardClient {
	return &shardClient{
		info:            info,
		creator:         creator,
		lastActiveTs:    atomic.NewInt64(time.Now().UnixNano()),
		expiredDuration: expiredDuration,
	}
}

func (n *shardClient) getClient(ctx context.Context) (types.QueryNodeClient, error) {
	n.lastActiveTs.Store(time.Now().UnixNano())
	if !n.initialized.Load() {
		n.Lock()
		if !n.initialized.Load() {
			if err := n.initClients(ctx); err != nil {
				n.Unlock()
				return nil, err
			}
		}
		n.Unlock()
	}

	// Attempt to get a connection from the idle connection pool, supporting context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		client, err := n.roundRobinSelectClient()
		if err != nil {
			return nil, err
		}
		return client, nil
	}
}

func (n *shardClient) initClients(ctx context.Context) error {
	poolSize := paramtable.Get().ProxyCfg.QueryNodePoolingSize.GetAsInt()
	if poolSize <= 0 {
		poolSize = 1
	}

	clients := make([]types.QueryNodeClient, 0, poolSize)
	for i := 0; i < poolSize; i++ {
		client, err := n.creator(ctx, n.info.Address, n.info.NodeID)
		if err != nil {
			// Roll back already created clients
			for _, c := range clients {
				c.Close()
			}
			mlog.Info(context.TODO(), "failed to create client for node", mlog.Int64("nodeID", n.info.NodeID), mlog.Err(err))
			return errors.Wrap(err, fmt.Sprintf("create client for node=%d failed", n.info.NodeID))
		}
		clients = append(clients, client)
	}

	n.initialized.Store(true)
	n.poolSize = poolSize
	n.clients = clients
	return nil
}

func (n *shardClient) roundRobinSelectClient() (types.QueryNodeClient, error) {
	n.RLock()
	defer n.RUnlock()
	if n.isClosed {
		return nil, merr.WrapErrServiceUnavailable("client is closed")
	}

	if len(n.clients) == 0 {
		return nil, merr.WrapErrServiceUnavailable("no available clients")
	}

	nextClientIndex := n.idx.Inc() % int64(len(n.clients))
	nextClient := n.clients[nextClientIndex]
	return nextClient, nil
}

// Notice: close client should only be called by shard client manager. and after close, the client must be removed from the manager.
// 1. the client hasn't been used for a long time
// 2. shard client manager has been closed.
func (n *shardClient) Close(force bool) bool {
	n.Lock()
	defer n.Unlock()
	if force || n.isExpired() {
		n.close()
	}

	return n.isClosed
}

func (n *shardClient) isExpired() bool {
	return time.Now().UnixNano()-n.lastActiveTs.Load() > n.expiredDuration.Nanoseconds()
}

func (n *shardClient) close() {
	n.isClosed = true

	for _, client := range n.clients {
		if err := client.Close(); err != nil {
			mlog.Warn(context.TODO(), "close grpc client failed", mlog.Err(err))
		}
	}
	n.clients = nil
}
