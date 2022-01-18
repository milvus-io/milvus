package querynode

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/types"
)

var errDisposed = errors.New("client is disposed")

type NodeInfo struct {
	NodeID  int64
	Address string
}

type proxyCreatorFunc func(ctx context.Context, addr string) (types.Proxy, error)

type Session struct {
	sync.Mutex
	info          *NodeInfo
	client        types.Proxy
	clientCreator proxyCreatorFunc
	isDisposed    bool
}

func NewSession(info *NodeInfo, creator proxyCreatorFunc) *Session {
	return &Session{
		info:          info,
		clientCreator: creator,
	}
}

func (n *Session) GetOrCreateClient(ctx context.Context) (types.Proxy, error) {
	n.Lock()
	defer n.Unlock()

	if n.isDisposed {
		return nil, errDisposed
	}

	if n.client != nil {
		return n.client, nil
	}

	if n.clientCreator == nil {
		return nil, fmt.Errorf("unable to create client for %s because of a nil client creator", n.info.Address)
	}

	err := n.initClient(ctx)
	return n.client, err
}

func (n *Session) initClient(ctx context.Context) (err error) {
	if n.client, err = n.clientCreator(ctx, n.info.Address); err != nil {
		return
	}
	if err = n.client.Init(); err != nil {
		return
	}
	return n.client.Start()
}

func (n *Session) Dispose() {
	n.Lock()
	defer n.Unlock()

	if n.client != nil {
		n.client.Stop()
		n.client = nil
	}
	n.isDisposed = true
}
