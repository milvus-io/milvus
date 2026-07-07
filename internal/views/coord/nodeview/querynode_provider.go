package nodeview

import (
	"context"
	"sort"
	"sync"

	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

var _ balancer.NodeProvider = (*QueryNodeProvider)(nil)

// QueryNodeClient supplies QueryNode facts discovered by the manager client.
type QueryNodeClient interface {
	RegisterNodeChangedNotifier(notifier func())
	GetAllQueryNodes(ctx context.Context) (map[int64]*qnmanager.NodeInfo, error)
}

// ResourceGroupManager supplies current resource-group to QueryNode bindings.
// *meta.ResourceManager satisfies this interface directly.
type ResourceGroupManager interface {
	ListResourceGroups(ctx context.Context) []string
	GetNodes(ctx context.Context, rgName string) ([]int64, error)
}

// QueryNodeProvider adapts QueryNode manager state plus resource-group bindings
// into the node snapshot consumed by the QV balancer.
type QueryNodeProvider struct {
	ctx       context.Context
	nodes     QueryNodeClient
	rgManager ResourceGroupManager

	mu       sync.RWMutex
	version  uint64
	snapshot *balancer.NodeSnapshot
}

func NewQueryNodeProvider(
	ctx context.Context,
	nodes QueryNodeClient,
	rgManager ResourceGroupManager,
) *QueryNodeProvider {
	return &QueryNodeProvider{
		ctx:       ctx,
		nodes:     nodes,
		rgManager: rgManager,
	}
}

func (p *QueryNodeProvider) Snapshot() *balancer.NodeSnapshot {
	nodes, err := p.nodes.GetAllQueryNodes(p.ctx)
	if err != nil {
		mlog.Warn(p.ctx, "get querynode manager nodes failed, use last node snapshot", mlog.Err(err))
		return p.lastSnapshot()
	}
	rgByNode, err := p.resourceGroupByNode()
	if err != nil {
		mlog.Warn(p.ctx, "get resource group node bindings failed, use last node snapshot", mlog.Err(err))
		return p.lastSnapshot()
	}

	infos := make(map[int64]*balancer.NodeInfo)
	for nodeID, node := range nodes {
		if node == nil {
			continue
		}
		rg := node.ServerLabels[sessionutil.LabelResourceGroup]
		if rg == "" {
			rg = rgByNode[nodeID]
		}
		if rg == "" {
			continue
		}
		infos[nodeID] = &balancer.NodeInfo{
			NodeID:        nodeID,
			Alive:         true,
			Stopping:      node.Stopping,
			ResourceGroup: rg,
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.version++
	p.snapshot = balancer.NewNodeSnapshot(p.version, infos)
	return p.snapshot
}

func (p *QueryNodeProvider) RegisterNodeChangedNotifier(notifier func()) {
	p.nodes.RegisterNodeChangedNotifier(notifier)
}

func (p *QueryNodeProvider) resourceGroupByNode() (map[int64]string, error) {
	rgs := p.rgManager.ListResourceGroups(p.ctx)
	sort.Strings(rgs)

	rgByNode := make(map[int64]string)
	for _, rg := range rgs {
		nodes, err := p.rgManager.GetNodes(p.ctx, rg)
		if err != nil {
			return nil, err
		}
		for _, nodeID := range nodes {
			if _, ok := rgByNode[nodeID]; !ok {
				rgByNode[nodeID] = rg
			}
		}
	}
	return rgByNode, nil
}

func (p *QueryNodeProvider) lastSnapshot() *balancer.NodeSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.snapshot != nil {
		return p.snapshot
	}
	return balancer.NewNodeSnapshot(0, map[int64]*balancer.NodeInfo{})
}
