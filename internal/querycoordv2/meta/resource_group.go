package meta

import (
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	DefaultResourceGroupName           = "__default_resource_group"
	defaultResourceGroupCapacity int32 = 1000000
	resourceGroupTransferBoost         = 10000
)

// newResourceGroupConfig create a new resource group config.
func newResourceGroupConfig(request int32, limit int32) *rgpb.ResourceGroupConfig {
	return &rgpb.ResourceGroupConfig{
		Requests: &rgpb.ResourceGroupLimit{
			NodeNum: request,
		},
		Limits: &rgpb.ResourceGroupLimit{
			NodeNum: limit,
		},
		TransferFrom: make([]*rgpb.ResourceGroupTransfer, 0),
		TransferTo:   make([]*rgpb.ResourceGroupTransfer, 0),
	}
}

type ResourceGroup struct {
	name    string
	nodes   typeutil.UniqueSet
	cfg     *rgpb.ResourceGroupConfig
	nodeMgr *session.NodeManager
}

// NewResourceGroup create resource group.
func NewResourceGroup(name string, cfg *rgpb.ResourceGroupConfig, nodeMgr *session.NodeManager) *ResourceGroup {
	rg := &ResourceGroup{
		name:    name,
		nodes:   typeutil.NewUniqueSet(),
		cfg:     cfg,
		nodeMgr: nodeMgr,
	}
	return rg
}

// NewResourceGroupFromMeta create resource group from meta.
func NewResourceGroupFromMeta(meta *querypb.ResourceGroup, nodeMgr *session.NodeManager) *ResourceGroup {
	// Backward compatibility, recover the config from capacity.
	if meta.Config == nil {
		// If meta.Config is nil, which means the meta is from old version.
		// DefaultResourceGroup has special configuration.
		if meta.Name == DefaultResourceGroupName {
			meta.Config = newResourceGroupConfig(0, meta.Capacity)
		} else {
			meta.Config = newResourceGroupConfig(meta.Capacity, meta.Capacity)
		}
	}
	rg := NewResourceGroup(meta.Name, meta.Config, nodeMgr)
	for _, node := range meta.GetNodes() {
		rg.nodes.Insert(node)
	}
	return rg
}

// GetName return resource group name.
func (rg *ResourceGroup) GetName() string {
	return rg.name
}

// go:deprecated GetCapacity return resource group capacity.
func (rg *ResourceGroup) GetCapacity() int {
	// Forward compatibility, recover the capacity from configuration.
	capacity := rg.cfg.Requests.NodeNum
	if rg.GetName() == DefaultResourceGroupName {
		// Default resource group's capacity is always DefaultResourceGroupCapacity.
		capacity = defaultResourceGroupCapacity
	}
	return int(capacity)
}

// GetConfig return resource group config.
// Do not change the config directly, use UpdateTxn to update config.
func (rg *ResourceGroup) GetConfig() *rgpb.ResourceGroupConfig {
	return rg.cfg
}

// GetConfigCloned return a cloned resource group config.
func (rg *ResourceGroup) GetConfigCloned() *rgpb.ResourceGroupConfig {
	return proto.Clone(rg.cfg).(*rgpb.ResourceGroupConfig)
}

// GetNodes return nodes of resource group which match required node labels
func (rg *ResourceGroup) GetNodes() []int64 {
	requiredNodeLabels := rg.GetConfig().GetNodeFilter().GetNodeLabels()
	if len(requiredNodeLabels) == 0 {
		return rg.nodes.Collect()
	}

	ret := make([]int64, 0)
	rg.nodes.Range(func(nodeID int64) bool {
		if rg.AcceptNode(nodeID) {
			ret = append(ret, nodeID)
		}
		return true
	})

	return ret
}

// NodeNum return node count of resource group which match required node labels
func (rg *ResourceGroup) NodeNum() int {
	return len(rg.GetNodes())
}

// ContainNode return whether resource group contain node.
func (rg *ResourceGroup) ContainNode(id int64) bool {
	return rg.nodes.Contain(id)
}

// OversizedNumOfNodes return oversized nodes count. `NodeNum - requests`
func (rg *ResourceGroup) OversizedNumOfNodes() int {
	oversized := rg.NodeNum() - int(rg.cfg.Requests.NodeNum)
	if oversized < 0 {
		oversized = 0
	}
	return oversized + len(rg.getDirtyNode())
}

// MissingNumOfNodes return lack nodes count. `requests - NodeNum`
func (rg *ResourceGroup) MissingNumOfNodes() int {
	missing := int(rg.cfg.Requests.NodeNum) - rg.NodeNum()
	if missing < 0 {
		return 0
	}
	return missing
}

// ReachLimitNumOfNodes return reach limit nodes count. `limits - NodeNum`
func (rg *ResourceGroup) ReachLimitNumOfNodes() int {
	reachLimit := int(rg.cfg.Limits.NodeNum) - rg.NodeNum()
	if reachLimit < 0 {
		return 0
	}
	return reachLimit
}

// RedundantOfNodes return redundant nodes count. `len(node) - limits` or len(dirty_nodes)
func (rg *ResourceGroup) RedundantNumOfNodes() int {
	redundant := rg.NodeNum() - int(rg.cfg.Limits.NodeNum)
	if redundant < 0 {
		redundant = 0
	}
	return redundant + len(rg.getDirtyNode())
}

func (rg *ResourceGroup) getDirtyNode() []int64 {
	dirtyNodes := make([]int64, 0)
	rg.nodes.Range(func(nodeID int64) bool {
		if !rg.AcceptNode(nodeID) {
			dirtyNodes = append(dirtyNodes, nodeID)
		}
		return true
	})

	return dirtyNodes
}

func (rg *ResourceGroup) SelectNodeForRG(targetRG *ResourceGroup) int64 {
	// try to move out dirty node
	for _, node := range rg.getDirtyNode() {
		if targetRG.AcceptNode(node) {
			return node
		}
	}

	// try to move out oversized node
	oversized := rg.NodeNum() - int(rg.cfg.Requests.NodeNum)
	if oversized > 0 {
		for _, node := range rg.GetNodes() {
			if targetRG.AcceptNode(node) {
				return node
			}
		}
	}

	return -1
}

// return node and priority.
func (rg *ResourceGroup) AcceptNode(nodeID int64) bool {
	if rg.GetName() == DefaultResourceGroupName {
		return true
	}

	nodeInfo := rg.nodeMgr.Get(nodeID)
	if nodeInfo == nil {
		return false
	}

	requiredNodeLabels := rg.GetConfig().GetNodeFilter().GetNodeLabels()
	if len(requiredNodeLabels) == 0 {
		return true
	}

	nodeLabels := nodeInfo.Labels()
	if len(nodeLabels) == 0 {
		return false
	}

	for _, labelPair := range requiredNodeLabels {
		valueInNode, ok := nodeLabels[labelPair.Key]
		if !ok || valueInNode != labelPair.Value {
			return false
		}
	}

	return true
}

// HasFrom return whether given resource group is in `from` of rg.
func (rg *ResourceGroup) HasFrom(rgName string) bool {
	for _, from := range rg.cfg.GetTransferFrom() {
		if from.ResourceGroup == rgName {
			return true
		}
	}
	return false
}

// HasTo return whether given resource group is in `to` of rg.
func (rg *ResourceGroup) HasTo(rgName string) bool {
	for _, to := range rg.cfg.GetTransferTo() {
		if to.ResourceGroup == rgName {
			return true
		}
	}
	return false
}

// GetMeta return resource group meta.
func (rg *ResourceGroup) GetMeta() *querypb.ResourceGroup {
	capacity := rg.GetCapacity()
	return &querypb.ResourceGroup{
		Name:     rg.name,
		Capacity: int32(capacity),
		Nodes:    rg.nodes.Collect(),
		Config:   rg.GetConfigCloned(),
	}
}

// Snapshot return a snapshot of resource group.
func (rg *ResourceGroup) Snapshot() *ResourceGroup {
	return &ResourceGroup{
		name:    rg.name,
		nodes:   rg.nodes.Clone(),
		cfg:     rg.GetConfigCloned(),
		nodeMgr: rg.nodeMgr,
	}
}

// MeetRequirement return whether resource group meet requirement.
// Return error with reason if not meet requirement.
func (rg *ResourceGroup) MeetRequirement() error {
	// if len(node) is less than requests, new node need to be assigned.
	if rg.MissingNumOfNodes() > 0 {
		return errors.Errorf(
			"has %d nodes, less than request %d",
			rg.NodeNum(),
			rg.cfg.Requests.NodeNum,
		)
	}
	// if len(node) is greater than limits, node need to be removed.
	if rg.RedundantNumOfNodes() > 0 {
		return errors.Errorf(
			"has %d nodes, greater than limit %d",
			rg.NodeNum(),
			rg.cfg.Requests.NodeNum,
		)
	}
	return nil
}

// CopyForWrite return a mutable resource group.
func (rg *ResourceGroup) CopyForWrite() *mutableResourceGroup {
	return &mutableResourceGroup{ResourceGroup: rg.Snapshot()}
}

// mutableResourceGroup is a mutable type (COW) for manipulating resource group meta info for replica manager.
type mutableResourceGroup struct {
	*ResourceGroup
}

// UpdateConfig update resource group config.
func (r *mutableResourceGroup) UpdateConfig(cfg *rgpb.ResourceGroupConfig) {
	r.cfg = cfg
}

// Assign node to resource group.
func (r *mutableResourceGroup) AssignNode(id int64) {
	r.nodes.Insert(id)
}

// Unassign node from resource group.
func (r *mutableResourceGroup) UnassignNode(id int64) {
	r.nodes.Remove(id)
}

// ToResourceGroup return updated resource group, After calling this method, the mutable resource group should not be used again.
func (r *mutableResourceGroup) ToResourceGroup() *ResourceGroup {
	rg := r.ResourceGroup
	r.ResourceGroup = nil
	return rg
}
