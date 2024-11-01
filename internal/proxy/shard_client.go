package proxy

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type queryNodeCreatorFunc func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error)

type nodeInfo struct {
	nodeID  UniqueID
	address string
}

func (n nodeInfo) String() string {
	return fmt.Sprintf("<NodeID: %d>", n.nodeID)
}

var errClosed = errors.New("client is closed")

type shardClient struct {
	sync.RWMutex
	info     nodeInfo
	isClosed bool
	refCnt   int
	clients  []types.QueryNodeClient
	idx      atomic.Int64
	poolSize int
	pooling  bool

	initialized atomic.Bool
	creator     queryNodeCreatorFunc
}

func (n *shardClient) getClient(ctx context.Context) (types.QueryNodeClient, error) {
	if !n.initialized.Load() {
		n.Lock()
		if !n.initialized.Load() {
			if err := n.initClients(); err != nil {
				n.Unlock()
				return nil, err
			}
			n.initialized.Store(true)
		}
		n.Unlock()
	}

	n.RLock()
	defer n.RUnlock()
	if n.isClosed {
		return nil, errClosed
	}

	idx := n.idx.Inc()
	return n.clients[int(idx)%n.poolSize], nil
}

func (n *shardClient) inc() {
	n.Lock()
	defer n.Unlock()
	if n.isClosed {
		return
	}
	n.refCnt++
}

func (n *shardClient) close() {
	n.isClosed = true
	n.refCnt = 0

	for _, client := range n.clients {
		if err := client.Close(); err != nil {
			log.Warn("close grpc client failed", zap.Error(err))
		}
	}
	n.clients = nil
}

func (n *shardClient) dec() bool {
	n.Lock()
	defer n.Unlock()
	if n.isClosed {
		return true
	}
	if n.refCnt > 0 {
		n.refCnt--
	}
	if n.refCnt == 0 {
		n.close()
	}
	return n.refCnt == 0
}

func (n *shardClient) Close() {
	n.Lock()
	defer n.Unlock()
	n.close()
}

func newPoolingShardClient(info *nodeInfo, creator queryNodeCreatorFunc) (*shardClient, error) {
	return &shardClient{
		info: nodeInfo{
			nodeID:  info.nodeID,
			address: info.address,
		},
		refCnt:  1,
		pooling: true,
		creator: creator,
	}, nil
}

func (n *shardClient) initClients() error {
	num := paramtable.Get().ProxyCfg.QueryNodePoolingSize.GetAsInt()
	if num <= 0 {
		num = 1
	}
	clients := make([]types.QueryNodeClient, 0, num)
	for i := 0; i < num; i++ {
		client, err := n.creator(context.Background(), n.info.address, n.info.nodeID)
		if err != nil {
			// roll back already created clients
			for _, c := range clients[:i] {
				c.Close()
			}
			return errors.Wrap(err, fmt.Sprintf("create client for node=%d failed", n.info.nodeID))
		}
		clients = append(clients, client)
	}

	n.clients = clients
	n.poolSize = num
	return nil
}

type shardClientMgr interface {
	GetClient(ctx context.Context, nodeID UniqueID) (types.QueryNodeClient, error)
	UpdateShardLeaders(oldLeaders map[string][]nodeInfo, newLeaders map[string][]nodeInfo) error
	Close()
	SetClientCreatorFunc(creator queryNodeCreatorFunc)
}

type shardClientMgrImpl struct {
	clients struct {
		sync.RWMutex
		data map[UniqueID]*shardClient
	}
	clientCreator queryNodeCreatorFunc
}

// SessionOpt provides a way to set params in SessionManager
type shardClientMgrOpt func(s shardClientMgr)

func withShardClientCreator(creator queryNodeCreatorFunc) shardClientMgrOpt {
	return func(s shardClientMgr) { s.SetClientCreatorFunc(creator) }
}

func defaultQueryNodeClientCreator(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error) {
	return registry.GetInMemoryResolver().ResolveQueryNode(ctx, addr, nodeID)
}

// NewShardClientMgr creates a new shardClientMgr
func newShardClientMgr(options ...shardClientMgrOpt) *shardClientMgrImpl {
	s := &shardClientMgrImpl{
		clients: struct {
			sync.RWMutex
			data map[UniqueID]*shardClient
		}{data: make(map[UniqueID]*shardClient)},
		clientCreator: defaultQueryNodeClientCreator,
	}
	for _, opt := range options {
		opt(s)
	}
	return s
}

func (c *shardClientMgrImpl) SetClientCreatorFunc(creator queryNodeCreatorFunc) {
	c.clientCreator = creator
}

// Warning this method may modify parameter `oldLeaders`
func (c *shardClientMgrImpl) UpdateShardLeaders(oldLeaders map[string][]nodeInfo, newLeaders map[string][]nodeInfo) error {
	oldLocalMap := make(map[UniqueID]*nodeInfo)
	for _, nodes := range oldLeaders {
		for i := range nodes {
			n := &nodes[i]
			_, ok := oldLocalMap[n.nodeID]
			if !ok {
				oldLocalMap[n.nodeID] = n
			}
		}
	}
	newLocalMap := make(map[UniqueID]*nodeInfo)

	for _, nodes := range newLeaders {
		for i := range nodes {
			n := &nodes[i]
			_, ok := oldLocalMap[n.nodeID]
			if !ok {
				_, ok2 := newLocalMap[n.nodeID]
				if !ok2 {
					newLocalMap[n.nodeID] = n
				}
			}
			delete(oldLocalMap, n.nodeID)
		}
	}
	c.clients.Lock()
	defer c.clients.Unlock()

	for _, node := range newLocalMap {
		client, ok := c.clients.data[node.nodeID]
		if ok {
			client.inc()
		} else {
			// context.Background() is useless
			// TODO QueryNode NewClient remove ctx parameter
			// TODO Remove Init && Start interface in QueryNode client
			if c.clientCreator == nil {
				return fmt.Errorf("clientCreator function is nil")
			}
			client, err := newPoolingShardClient(node, c.clientCreator)
			if err != nil {
				return err
			}
			c.clients.data[node.nodeID] = client
		}
	}
	for _, node := range oldLocalMap {
		client, ok := c.clients.data[node.nodeID]
		if ok && client.dec() {
			delete(c.clients.data, node.nodeID)
		}
	}
	return nil
}

func (c *shardClientMgrImpl) GetClient(ctx context.Context, nodeID UniqueID) (types.QueryNodeClient, error) {
	c.clients.RLock()
	client, ok := c.clients.data[nodeID]
	c.clients.RUnlock()

	if !ok {
		return nil, fmt.Errorf("can not find client of node %d", nodeID)
	}
	return client.getClient(ctx)
}

// Close release clients
func (c *shardClientMgrImpl) Close() {
	c.clients.Lock()
	defer c.clients.Unlock()

	for _, s := range c.clients.data {
		s.Close()
	}
	c.clients.data = make(map[UniqueID]*shardClient)
}
