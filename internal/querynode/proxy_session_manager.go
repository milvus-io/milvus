package querynode

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/internalpb"

	grpcproxyclient "github.com/milvus-io/milvus/internal/distributed/proxy/client"

	"github.com/milvus-io/milvus/internal/types"
)

type SessionManager struct {
	sessions struct {
		sync.RWMutex
		data map[int64]*Session
	}
	// sessions       sync.Map // UniqueID -> Session
	sessionCreator proxyCreatorFunc
}

// SessionOpt provides a way to set params in SessionManager
type SessionOpt func(c *SessionManager)

func withSessionCreator(creator proxyCreatorFunc) SessionOpt {
	return func(c *SessionManager) { c.sessionCreator = creator }
}

func defaultSessionCreator() proxyCreatorFunc {
	return func(ctx context.Context, addr string) (types.Proxy, error) {
		return grpcproxyclient.NewClient(ctx, addr)
	}
}

// NewSessionManager creates a new SessionManager
func NewSessionManager(options ...SessionOpt) *SessionManager {
	m := &SessionManager{
		sessions: struct {
			sync.RWMutex
			data map[int64]*Session
		}{data: make(map[int64]*Session)},
		sessionCreator: defaultSessionCreator(),
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

// AddSession creates a new session
func (c *SessionManager) AddSession(node *NodeInfo) {
	c.sessions.Lock()
	defer c.sessions.Unlock()
	log.Info("add proxy session", zap.Int64("node", node.NodeID))
	session := NewSession(node, c.sessionCreator)
	c.sessions.data[node.NodeID] = session
}

func (c *SessionManager) Startup(nodes []*NodeInfo) {
	for _, node := range nodes {
		c.AddSession(node)
	}
}

// DeleteSession removes the node session
func (c *SessionManager) DeleteSession(node *NodeInfo) {
	c.sessions.Lock()
	defer c.sessions.Unlock()
	log.Info("delete proxy session", zap.Int64("node", node.NodeID))
	if session, ok := c.sessions.data[node.NodeID]; ok {
		session.Dispose()
		delete(c.sessions.data, node.NodeID)
	}
}

// GetSessions gets all node sessions
func (c *SessionManager) GetSessions() []*Session {
	c.sessions.RLock()
	defer c.sessions.RUnlock()

	ret := make([]*Session, 0, len(c.sessions.data))
	for _, s := range c.sessions.data {
		ret = append(ret, s)
	}
	return ret
}

func (c *SessionManager) SendSearchResult(ctx context.Context, nodeID UniqueID, result *internalpb.SearchResults) error {
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Warn("failed to send search result, cannot get client", zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}

	resp, err := cli.SendSearchResult(ctx, result)
	if err := funcutil.VerifyResponse(resp, err); err != nil {
		log.Warn("failed to send search result", zap.Int64("node", nodeID), zap.Error(err))
		return err
	}

	log.Debug("success to send search result", zap.Int64("node", nodeID), zap.Any("base", result.Base))

	return nil
}

func (c *SessionManager) SendRetrieveResult(ctx context.Context, nodeID UniqueID, result *internalpb.RetrieveResults) error {
	cli, err := c.getClient(ctx, nodeID)
	if err != nil {
		log.Warn("failed to send retrieve result, cannot get client", zap.Int64("nodeID", nodeID), zap.Error(err))
		return err
	}

	resp, err := cli.SendRetrieveResult(ctx, result)
	if err := funcutil.VerifyResponse(resp, err); err != nil {
		log.Warn("failed to send retrieve result", zap.Int64("node", nodeID), zap.Error(err))
		return err
	}

	log.Debug("success to send retrieve result", zap.Int64("node", nodeID), zap.Any("base", result.Base))

	return nil
}

func (c *SessionManager) getClient(ctx context.Context, nodeID int64) (types.Proxy, error) {
	c.sessions.RLock()
	session, ok := c.sessions.data[nodeID]
	c.sessions.RUnlock()

	if !ok {
		return nil, fmt.Errorf("can not find session of node %d", nodeID)
	}

	return session.GetOrCreateClient(ctx)
}

// Close release sessions
func (c *SessionManager) Close() {
	c.sessions.Lock()
	defer c.sessions.Unlock()

	for _, s := range c.sessions.data {
		s.Dispose()
	}
	c.sessions.data = nil
}
