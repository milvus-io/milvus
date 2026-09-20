package nodeview

import (
	"sync"

	qnmanager "github.com/milvus-io/milvus/internal/querynodev2/client/manager"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/views/coord/balancer"
)

// Both sources must synchronously replay and serialize committed updates with
// registration. Callbacks run at the source commit point, never as a pull worker.
// Production owners implement these contracts when the QV runtime is wired.
type QueryNodeStatePublisher interface {
	RegisterQueryNodeListener(func(int64, *qnmanager.NodeInfo)) func()
}
type ResourceGroupStatePublisher interface {
	RegisterResourceGroupListener(func(string, []int64)) func()
}

type nodeFacts struct {
	label    string
	stopping bool
}

// QueryNodePublisher joins node facts with RG assignments on publication.
// A session label has precedence; overlapping RG bindings choose the smallest
// name, matching the existing pull adapter. Cache subscribers only read results.
type QueryNodePublisher struct {
	mu           sync.Mutex
	facts        map[int64]nodeFacts
	groups       map[string]map[int64]struct{}
	bindings     map[int64]map[string]struct{}
	published    map[int64]*balancer.NodeInfo
	listeners    map[uint64]balancer.NodeListener
	nextListener uint64
	unsubscribe  []func()
}

func newQueryNodePublisher() *QueryNodePublisher {
	return &QueryNodePublisher{facts: make(map[int64]nodeFacts), groups: make(map[string]map[int64]struct{}), bindings: make(map[int64]map[string]struct{}), published: make(map[int64]*balancer.NodeInfo), listeners: make(map[uint64]balancer.NodeListener)}
}

func NewQueryNodePublisher(nodes QueryNodeStatePublisher, groups ResourceGroupStatePublisher) *QueryNodePublisher {
	p := newQueryNodePublisher()
	p.unsubscribe = append(p.unsubscribe, groups.RegisterResourceGroupListener(p.onGroup))
	p.unsubscribe = append(p.unsubscribe, nodes.RegisterQueryNodeListener(p.onNode))
	return p
}

func (p *QueryNodePublisher) Close() {
	for _, unsubscribe := range p.unsubscribe {
		unsubscribe()
	}
}

func (p *QueryNodePublisher) RegisterNodeListener(listener balancer.NodeListener) func() {
	p.mu.Lock()
	p.nextListener++
	id := p.nextListener
	p.listeners[id] = listener
	for nodeID, node := range p.published {
		listener(nodeID, node)
	}
	p.mu.Unlock()
	return func() { p.mu.Lock(); delete(p.listeners, id); p.mu.Unlock() }
}

func (p *QueryNodePublisher) onNode(id int64, info *qnmanager.NodeInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if info == nil {
		delete(p.facts, id)
	} else {
		p.facts[id] = nodeFacts{label: info.ServerLabels[sessionutil.LabelResourceGroup], stopping: info.Stopping}
	}
	p.publishLocked(id)
}

func (p *QueryNodePublisher) onGroup(name string, nodes []int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	affected := make(map[int64]struct{})
	for nodeID := range p.groups[name] {
		affected[nodeID] = struct{}{}
		delete(p.bindings[nodeID], name)
		if len(p.bindings[nodeID]) == 0 {
			delete(p.bindings, nodeID)
		}
	}
	delete(p.groups, name)
	if len(nodes) > 0 {
		p.groups[name] = make(map[int64]struct{}, len(nodes))
	}
	for _, nodeID := range nodes {
		affected[nodeID] = struct{}{}
		p.groups[name][nodeID] = struct{}{}
		if p.bindings[nodeID] == nil {
			p.bindings[nodeID] = make(map[string]struct{})
		}
		p.bindings[nodeID][name] = struct{}{}
	}
	for nodeID := range affected {
		p.publishLocked(nodeID)
	}
}

func (p *QueryNodePublisher) publishLocked(id int64) {
	facts, exists := p.facts[id]
	rg := facts.label
	if rg == "" {
		for name := range p.bindings[id] {
			if rg == "" || name < rg {
				rg = name
			}
		}
	}
	var next *balancer.NodeInfo
	if exists && rg != "" {
		next = &balancer.NodeInfo{NodeID: id, Alive: true, Stopping: facts.stopping, ResourceGroup: rg}
	}
	old := p.published[id]
	if (old == nil && next == nil) || (old != nil && next != nil && *old == *next) {
		return
	}
	if next == nil {
		delete(p.published, id)
	} else {
		p.published[id] = next
	}
	for _, listener := range p.listeners {
		listener(id, next)
	}
}
