package proxy

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	clients  []types.QueryNodeClient
	idx      atomic.Int64
	poolSize int
	pooling  bool

	initialized atomic.Bool
	creator     queryNodeCreatorFunc

	refCnt *atomic.Int64
}

func (n *shardClient) getClient(ctx context.Context) (types.QueryNodeClient, error) {
	if !n.initialized.Load() {
		n.Lock()
		if !n.initialized.Load() {
			if err := n.initClients(ctx); err != nil {
				n.Unlock()
				return nil, err
			}
			n.initialized.Store(true)
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
		n.IncRef()
		return client, nil
	}
}

func (n *shardClient) DecRef() {
	ret := n.refCnt.Dec()
	if ret <= 0 {
		log.Warn("unexpected client ref count zero, please check  the release call", zap.Int64("refCount", ret), zap.Stack("caller"))
	}
}

func (n *shardClient) IncRef() {
	n.refCnt.Inc()
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

// Notice: close client should only be called by shard client manager. and after close, the client must be removed from the manager.
// 1. the client hasn't been used for a long time
// 2. shard client manager has been closed.
func (n *shardClient) Close() {
	n.Lock()
	defer n.Unlock()
	n.close()
}

func newShardClient(info nodeInfo, creator queryNodeCreatorFunc) (*shardClient, error) {
	num := paramtable.Get().ProxyCfg.QueryNodePoolingSize.GetAsInt()
	if num <= 0 {
		num = 1
	}

	return &shardClient{
		info: nodeInfo{
			nodeID:  info.nodeID,
			address: info.address,
		},
		poolSize: num,
		creator:  creator,
		refCnt:   atomic.NewInt64(1),
	}, nil
}

func (n *shardClient) initClients(ctx context.Context) error {
	clients := make([]types.QueryNodeClient, 0, n.poolSize)
	for i := 0; i < n.poolSize; i++ {
		client, err := n.creator(ctx, n.info.address, n.info.nodeID)
		if err != nil {
			// Roll back already created clients
			for _, c := range clients {
				c.Close()
			}
			return errors.Wrap(err, fmt.Sprintf("create client for node=%d failed", n.info.nodeID))
		}
		clients = append(clients, client)
	}

	n.clients = clients
	return nil
}

// roundRobinSelectClient selects a client in a round-robin manner
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

type shardClientMgr interface {
	GetClient(ctx context.Context, nodeInfo nodeInfo) (types.QueryNodeClient, error)
	ReleaseClientRef(nodeID int64)
	Close()
	SetClientCreatorFunc(creator queryNodeCreatorFunc)
}

const (
	defaultPurgeInterval   = 600 * time.Second
	defaultPurgeExpiredAge = 3
)

type shardClientMgrImpl struct {
	clients struct {
		sync.RWMutex
		data map[UniqueID]*shardClient
	}
	clientCreator queryNodeCreatorFunc

	closeCh chan struct{}

	purgeInterval   time.Duration
	purgeExpiredAge int
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
		clientCreator:   defaultQueryNodeClientCreator,
		closeCh:         make(chan struct{}),
		purgeInterval:   defaultPurgeInterval,
		purgeExpiredAge: defaultPurgeExpiredAge,
	}
	for _, opt := range options {
		opt(s)
	}

	go s.PurgeClient()
	return s
}

func (c *shardClientMgrImpl) SetClientCreatorFunc(creator queryNodeCreatorFunc) {
	c.clientCreator = creator
}

func (c *shardClientMgrImpl) GetClient(ctx context.Context, info nodeInfo) (types.QueryNodeClient, error) {
	c.clients.RLock()
	client, ok := c.clients.data[info.nodeID]
	c.clients.RUnlock()

	if !ok {
		c.clients.Lock()
		// Check again after acquiring the lock
		client, ok = c.clients.data[info.nodeID]
		if !ok {
			// Create a new client if it doesn't exist
			newClient, err := newShardClient(info, c.clientCreator)
			if err != nil {
				c.clients.Unlock()
				return nil, err
			}
			c.clients.data[info.nodeID] = newClient
			client = newClient
		}
		c.clients.Unlock()
	}

	return client.getClient(ctx)
}

// PurgeClient purges client if it is not used for a long time
func (c *shardClientMgrImpl) PurgeClient() {
	ticker := time.NewTicker(c.purgeInterval)
	defer ticker.Stop()

	// record node's age, if node reach 3 consecutive failures, try to purge it
	nodeAges := make(map[int64]int, 0)

	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			shardLocations := globalMetaCache.ListShardLocation()
			c.clients.Lock()
			for nodeID, client := range c.clients.data {
				if _, ok := shardLocations[nodeID]; !ok {
					nodeAges[nodeID] += 1
					if nodeAges[nodeID] > c.purgeExpiredAge {
						if client.refCnt.Load() <= 1 {
							client.Close()
							delete(c.clients.data, nodeID)
							log.Info("remove client due to not used for long time", zap.Int64("nodeID", nodeID))
						}
					}
				} else {
					nodeAges[nodeID] = 0
				}
			}
			c.clients.Unlock()
		}
	}
}

func (c *shardClientMgrImpl) ReleaseClientRef(nodeID int64) {
	c.clients.RLock()
	defer c.clients.RUnlock()
	if client, ok := c.clients.data[nodeID]; ok {
		client.DecRef()
	}
}

// Close release clients
func (c *shardClientMgrImpl) Close() {
	c.clients.Lock()
	defer c.clients.Unlock()
	close(c.closeCh)
	for _, s := range c.clients.data {
		s.Close()
	}
	c.clients.data = make(map[UniqueID]*shardClient)
}
