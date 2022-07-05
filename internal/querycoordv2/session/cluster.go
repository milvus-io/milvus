package session

import (
	"context"
	"sync"

	grpcquerynodeclient "github.com/milvus-io/milvus/internal/distributed/querynode/client"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

type Client struct {
}

// Cluster is used to sending requests to QueryNodes and manage connections
type Cluster struct {
	*clients
	nodeManager *NodeManager
}

type clients struct {
	sync.RWMutex
	clients map[string]*grpcquerynodeclient.Client // addr -> conn
}

func (c *clients) getOrCreateClient(ctx context.Context, addr string) (*grpcquerynodeclient.Client, error) {
	if cli := c.getClient(addr); cli != nil {
		return cli, nil
	}

	newCli, err := grpcquerynodeclient.NewClient(ctx, addr)
	if err != nil {
		return nil, err
	}
	c.setClient(addr, newCli)
	return c.getClient(addr), nil
}

func (c *clients) setClient(addr string, client *grpcquerynodeclient.Client) {
	c.Lock()
	defer c.Unlock()
	if _, ok := c.clients[addr]; ok {
		if err := client.Stop(); err != nil {
			log.Warn("close new created client error", zap.String("addr", addr), zap.Error(err))
			return
		}
		log.Info("use old client", zap.String("addr", addr))
	}
	c.clients[addr] = client
}

func (c *clients) getClient(addr string) *grpcquerynodeclient.Client {
	c.RLock()
	defer c.RUnlock()
	return c.clients[addr]
}

func newClients() clients {
	return clients{clients: make(map[string]*grpcquerynodeclient.Client)}
}
