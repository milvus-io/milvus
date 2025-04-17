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
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type queryNodeCreatorFunc func(ctx context.Context, addr string, nodeID int64) (types.QueryNodeClient, error)

type nodeInfo struct {
	nodeID      UniqueID
	address     string
	serviceable bool
}

func (n nodeInfo) String() string {
	return fmt.Sprintf("<NodeID: %d, Address: %s, Serviceable: %t>", n.nodeID, n.address, n.serviceable)
}

var errClosed = errors.New("client is closed")

type shardClient struct {
	sync.RWMutex
	info     nodeInfo
	poolSize int
	clients  []types.QueryNodeClient
	creator  queryNodeCreatorFunc

	initialized atomic.Bool
	isClosed    bool

	idx             atomic.Int64
	lastActiveTs    *atomic.Int64
	expiredDuration time.Duration
}

func newShardClient(info nodeInfo, creator queryNodeCreatorFunc, expiredDuration time.Duration) *shardClient {
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
		client, err := n.creator(ctx, n.info.address, n.info.nodeID)
		if err != nil {
			// Roll back already created clients
			for _, c := range clients {
				c.Close()
			}
			log.Info("failed to create client for node", zap.Int64("nodeID", n.info.nodeID), zap.Error(err))
			return errors.Wrap(err, fmt.Sprintf("create client for node=%d failed", n.info.nodeID))
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

// roundRobinSelectClient selects a client in a round-robin manner
type shardClientMgr interface {
	GetClient(ctx context.Context, nodeInfo nodeInfo) (types.QueryNodeClient, error)
	Close()
	SetClientCreatorFunc(creator queryNodeCreatorFunc)
}

type shardClientMgrImpl struct {
	clients       *typeutil.ConcurrentMap[UniqueID, *shardClient]
	clientCreator queryNodeCreatorFunc
	closeCh       chan struct{}

	purgeInterval   time.Duration
	expiredDuration time.Duration
}

const (
	defaultPurgeInterval   = 600 * time.Second
	defaultExpiredDuration = 60 * time.Minute
)

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
		clients:         typeutil.NewConcurrentMap[UniqueID, *shardClient](),
		clientCreator:   defaultQueryNodeClientCreator,
		closeCh:         make(chan struct{}),
		purgeInterval:   defaultPurgeInterval,
		expiredDuration: defaultExpiredDuration,
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
	client, _ := c.clients.GetOrInsert(info.nodeID, newShardClient(info, c.clientCreator, c.expiredDuration))
	return client.getClient(ctx)
}

// PurgeClient purges client if it is not used for a long time
func (c *shardClientMgrImpl) PurgeClient() {
	ticker := time.NewTicker(c.purgeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closeCh:
			return
		case <-ticker.C:
			shardLocations := globalMetaCache.ListShardLocation()
			c.clients.Range(func(key UniqueID, value *shardClient) bool {
				if _, ok := shardLocations[key]; !ok {
					// if the client is not used for more than 1 hour, and it's not a delegator anymore, should remove it
					if value.isExpired() {
						closed := value.Close(false)
						if closed {
							c.clients.Remove(key)
							log.Info("remove idle node client", zap.Int64("nodeID", key))
						}
					}
				}
				return true
			})
		}
	}
}

// Close release clients
func (c *shardClientMgrImpl) Close() {
	close(c.closeCh)
	c.clients.Range(func(key UniqueID, value *shardClient) bool {
		value.Close(true)
		c.clients.Remove(key)
		return true
	})
}
