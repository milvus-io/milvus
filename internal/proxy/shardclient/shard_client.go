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
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type queryNodeCreatorFunc func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error)

type NodeInfo struct {
	NodeID      UniqueID
	Address     string
	Serviceable bool
}

func (n NodeInfo) String() string {
	return fmt.Sprintf("<NodeID: %d, serviceable: %v, address: %s>", n.NodeID, n.Serviceable, n.Address)
}

var errClosed = errors.New("client is closed")

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
			log.Info("failed to create client for node", zap.Int64("nodeID", n.info.NodeID), zap.Error(err))
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
		return nil, errClosed
	}

	if len(n.clients) == 0 {
		return nil, errors.New("no available clients")
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
			log.Warn("close grpc client failed", zap.Error(err))
		}
	}
	n.clients = nil
}
