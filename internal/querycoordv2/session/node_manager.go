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

package session

import (
	"fmt"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/metrics"
)

type Manager interface {
	Add(node *NodeInfo)
	Stopping(nodeID int64)
	Remove(nodeID int64)
	Get(nodeID int64) *NodeInfo
	GetAll() []*NodeInfo

	Suspend(nodeID int64) error
	Resume(nodeID int64) error
}

type NodeManager struct {
	mu    sync.RWMutex
	nodes map[int64]*NodeInfo
}

func (m *NodeManager) Add(node *NodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[node.ID()] = node
	metrics.QueryCoordNumQueryNodes.WithLabelValues().Set(float64(len(m.nodes)))
}

func (m *NodeManager) Remove(nodeID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.nodes, nodeID)
	metrics.QueryCoordNumQueryNodes.WithLabelValues().Set(float64(len(m.nodes)))
}

func (m *NodeManager) Stopping(nodeID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if nodeInfo, ok := m.nodes[nodeID]; ok {
		nodeInfo.SetState(NodeStateStopping)
	}
}

func (m *NodeManager) IsStoppingNode(nodeID int64) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	node := m.nodes[nodeID]
	if node == nil {
		return false, fmt.Errorf("nodeID[%d] isn't existed", nodeID)
	}
	return node.IsStoppingState(), nil
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

func NewNodeManager() *NodeManager {
	return &NodeManager{
		nodes: make(map[int64]*NodeInfo),
	}
}

type State int

const (
	NormalStateName   = "Normal"
	StoppingStateName = "Stopping"
	SuspendStateName  = "Suspend"
)

type ImmutableNodeInfo struct {
	NodeID   int64
	Address  string
	Hostname string
	Version  semver.Version
	Labels   map[string]string
}

const (
	NodeStateNormal State = iota
	NodeStateStopping
)

var stateNameMap = map[State]string{
	NodeStateNormal:   NormalStateName,
	NodeStateStopping: StoppingStateName,
}

func (s State) String() string {
	return stateNameMap[s]
}

type NodeInfo struct {
	stats
	mu            sync.RWMutex
	immutableInfo ImmutableNodeInfo
	state         State
	lastHeartbeat *atomic.Int64
}

func (n *NodeInfo) ID() int64 {
	return n.immutableInfo.NodeID
}

func (n *NodeInfo) Addr() string {
	return n.immutableInfo.Address
}

func (n *NodeInfo) Hostname() string {
	return n.immutableInfo.Hostname
}

func (n *NodeInfo) Labels() map[string]string {
	return n.immutableInfo.Labels
}

func (n *NodeInfo) IsEmbeddedQueryNodeInStreamingNode() bool {
	return n.immutableInfo.Labels[sessionutil.LabelStreamingNodeEmbeddedQueryNode] == "1"
}

func (n *NodeInfo) SegmentCnt() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.stats.getSegmentCnt()
}

func (n *NodeInfo) ChannelCnt() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.stats.getChannelCnt()
}

// return node's memory capacity in mb
func (n *NodeInfo) MemCapacity() float64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.stats.getMemCapacity()
}

func (n *NodeInfo) SetLastHeartbeat(time time.Time) {
	n.lastHeartbeat.Store(time.UnixNano())
}

func (n *NodeInfo) LastHeartbeat() time.Time {
	return time.Unix(0, n.lastHeartbeat.Load())
}

func (n *NodeInfo) IsStoppingState() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == NodeStateStopping
}

func (n *NodeInfo) SetState(s State) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = s
}

func (n *NodeInfo) GetState() State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *NodeInfo) UpdateStats(opts ...StatsOption) {
	n.mu.Lock()
	for _, opt := range opts {
		opt(n)
	}
	n.mu.Unlock()
}

func (n *NodeInfo) Version() semver.Version {
	return n.immutableInfo.Version
}

func NewNodeInfo(info ImmutableNodeInfo) *NodeInfo {
	return &NodeInfo{
		stats:         newStats(),
		immutableInfo: info,
		lastHeartbeat: atomic.NewInt64(0),
	}
}

type StatsOption func(*NodeInfo)

func WithSegmentCnt(cnt int) StatsOption {
	return func(n *NodeInfo) {
		n.setSegmentCnt(cnt)
	}
}

func WithChannelCnt(cnt int) StatsOption {
	return func(n *NodeInfo) {
		n.setChannelCnt(cnt)
	}
}

func WithMemCapacity(capacity float64) StatsOption {
	return func(n *NodeInfo) {
		n.setMemCapacity(capacity)
	}
}
