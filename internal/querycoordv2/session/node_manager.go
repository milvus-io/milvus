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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type Manager interface {
	Add(node *NodeInfo)
	Stopping(nodeID int64)
	Remove(nodeID int64)
	Get(nodeID int64) *NodeInfo
	GetAll() []*NodeInfo

	MarkResourceExhaustion(nodeID int64, duration time.Duration)
	IsResourceExhausted(nodeID int64) bool
	ClearExpiredResourceExhaustion()
	Start(ctx context.Context)
}

type NodeManager struct {
	mu        sync.RWMutex
	nodes     map[int64]*NodeInfo
	startOnce sync.Once
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
	NormalStateName   = "active"
	StoppingStateName = "stopping"
	SuspendStateName  = "suspended"
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

	// resourceExhaustionExpireAt is the timestamp when the resource exhaustion penalty expires.
	// When a query node reports resource exhaustion (OOM, disk full, etc.), it gets marked
	// with a penalty duration during which it won't receive new loading tasks.
	// Zero value means no active penalty.
	resourceExhaustionExpireAt time.Time
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

// IsInStandalone returns true if the node is in standalone.
func (n *NodeInfo) IsInStandalone() bool {
	return n.immutableInfo.Labels[sessionutil.LabelStandalone] == "1"
}

func (n *NodeInfo) IsEmbeddedQueryNodeInStreamingNode() bool {
	// Since 2.6.8, we introduce a new label rule for session,
	// the LegacyLabelStreamingNodeEmbeddedQueryNode is used before 2.6.8, so we need to check both key to keep compatibility.
	return n.immutableInfo.Labels[sessionutil.LabelStreamingNodeEmbeddedQueryNode] == "1" ||
		n.immutableInfo.Labels[sessionutil.LegacyLabelStreamingNodeEmbeddedQueryNode] == "1"
}

// ResourceGroupName returns the resource group name of the current node.
func (n *NodeInfo) ResourceGroupName() string {
	return n.immutableInfo.Labels[sessionutil.LabelResourceGroup]
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

func (n *NodeInfo) CPUNum() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.stats.getCPUNum()
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

func WithCPUNum(num int64) StatsOption {
	return func(n *NodeInfo) {
		n.setCPUNum(num)
	}
}

// MarkResourceExhaustion marks a query node as resource exhausted for the specified duration.
// During this period, the node won't receive new segment/channel loading tasks.
// If duration is 0 or negative, the resource exhaustion mark is cleared immediately.
// This is typically called when a query node reports resource exhaustion errors (OOM, disk full, etc.).
func (m *NodeManager) MarkResourceExhaustion(nodeID int64, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if node, ok := m.nodes[nodeID]; ok {
		node.mu.Lock()
		if duration > 0 {
			node.resourceExhaustionExpireAt = time.Now().Add(duration)
		} else {
			node.resourceExhaustionExpireAt = time.Time{}
		}
		node.mu.Unlock()
	}
}

// IsResourceExhausted checks if a query node is currently marked as resource exhausted.
// Returns true if the node has an active (non-expired) resource exhaustion mark.
// This is a pure read-only operation with no side effects - expired marks are not
// automatically cleared here. Use ClearExpiredResourceExhaustion for cleanup.
func (m *NodeManager) IsResourceExhausted(nodeID int64) bool {
	m.mu.RLock()
	node := m.nodes[nodeID]
	m.mu.RUnlock()

	if node == nil {
		return false
	}

	node.mu.RLock()
	defer node.mu.RUnlock()

	return !node.resourceExhaustionExpireAt.IsZero() &&
		time.Now().Before(node.resourceExhaustionExpireAt)
}

// ClearExpiredResourceExhaustion iterates through all nodes and clears any expired
// resource exhaustion marks. This is called periodically by the cleanup loop started
// via Start(). It only clears marks that have already expired; active marks are preserved.
func (m *NodeManager) ClearExpiredResourceExhaustion() {
	m.mu.RLock()
	nodes := make([]*NodeInfo, 0, len(m.nodes))
	for _, node := range m.nodes {
		nodes = append(nodes, node)
	}
	m.mu.RUnlock()

	now := time.Now()
	for _, node := range nodes {
		node.mu.Lock()
		if !node.resourceExhaustionExpireAt.IsZero() && !now.Before(node.resourceExhaustionExpireAt) {
			node.resourceExhaustionExpireAt = time.Time{}
		}
		node.mu.Unlock()
	}
}

// Start begins the background cleanup loop for expired resource exhaustion marks.
// The cleanup interval is controlled by queryCoord.resourceExhaustionCleanupInterval config.
// The loop will stop when the provided context is canceled.
// This method is idempotent - multiple calls will only start one cleanup loop.
func (m *NodeManager) Start(ctx context.Context) {
	m.startOnce.Do(func() {
		go m.cleanupLoop(ctx)
	})
}

// cleanupLoop is the internal goroutine that periodically clears expired resource
// exhaustion marks from all nodes. It supports dynamic interval refresh.
func (m *NodeManager) cleanupLoop(ctx context.Context) {
	interval := paramtable.Get().QueryCoordCfg.ResourceExhaustionCleanupInterval.GetAsDuration(time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("cleanupLoop stopped")
			return
		case <-ticker.C:
			m.ClearExpiredResourceExhaustion()
			// Support dynamic interval refresh
			newInterval := paramtable.Get().QueryCoordCfg.ResourceExhaustionCleanupInterval.GetAsDuration(time.Second)
			if newInterval != interval {
				interval = newInterval
				ticker.Reset(interval)
			}
		}
	}
}
