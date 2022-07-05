package session

import "sync"

type Manager interface {
	Add(node *NodeInfo)
	Remove(nodeID int64)
	Get(nodeID int64) *NodeInfo
	GetAll() []*NodeInfo
}

type NodeManager struct {
	mu    sync.RWMutex
	nodes map[int64]*NodeInfo
}

func (m *NodeManager) Add(node *NodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID()] = node
}

func (m *NodeManager) Remove(nodeID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
}

func (m *NodeManager) Get(nodeID int64) *NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodes[nodeID]
}

func (m *NodeManager) GetAll() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ret := make([]*NodeInfo, 0, len(m.nodes))
	for _, n := range m.nodes {
		ret = append(ret, n)
	}
	return ret
}

type NodeInfo struct {
	id   int64
	addr string
}

func (n *NodeInfo) ID() int64 {
	return n.id
}

func (n *NodeInfo) Addr() string {
	return n.addr
}

func NewNodeInfo(id int64, addr string) NodeInfo {
	return NodeInfo{id: id, addr: addr}
}
