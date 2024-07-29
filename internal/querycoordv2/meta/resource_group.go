package meta

import (
	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
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
	name  string
	nodes typeutil.UniqueSet
	cfg   *rgpb.ResourceGroupConfig
}

// NewResourceGroup create resource group.
func NewResourceGroup(name string, cfg *rgpb.ResourceGroupConfig) *ResourceGroup {
	rg := &ResourceGroup{
		name:  name,
		nodes: typeutil.NewUniqueSet(),
		cfg:   cfg,
	}
	return rg
}

// NewResourceGroupFromMeta create resource group from meta.
func NewResourceGroupFromMeta(meta *querypb.ResourceGroup) *ResourceGroup {
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
	rg := NewResourceGroup(meta.Name, meta.Config)
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

// GetNodes return nodes of resource group.
func (rg *ResourceGroup) GetNodes() []int64 {
	return rg.nodes.Collect()
}

// NodeNum return node count of resource group.
func (rg *ResourceGroup) NodeNum() int {
	return rg.nodes.Len()
}

// ContainNode return whether resource group contain node.
func (rg *ResourceGroup) ContainNode(id int64) bool {
	return rg.nodes.Contain(id)
}

// OversizedNumOfNodes return oversized nodes count. `len(node) - requests`
func (rg *ResourceGroup) OversizedNumOfNodes() int {
	oversized := rg.nodes.Len() - int(rg.cfg.Requests.NodeNum)
	if oversized < 0 {
		return 0
	}
	return oversized
}

// MissingNumOfNodes return lack nodes count. `requests - len(node)`
func (rg *ResourceGroup) MissingNumOfNodes() int {
	missing := int(rg.cfg.Requests.NodeNum) - len(rg.nodes)
	if missing < 0 {
		return 0
	}
	return missing
}

// ReachLimitNumOfNodes return reach limit nodes count. `limits - len(node)`
func (rg *ResourceGroup) ReachLimitNumOfNodes() int {
	reachLimit := int(rg.cfg.Limits.NodeNum) - len(rg.nodes)
	if reachLimit < 0 {
		return 0
	}
	return reachLimit
}

// RedundantOfNodes return redundant nodes count. `len(node) - limits`
func (rg *ResourceGroup) RedundantNumOfNodes() int {
	redundant := len(rg.nodes) - int(rg.cfg.Limits.NodeNum)
	if redundant < 0 {
		return 0
	}
	return redundant
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
		name:  rg.name,
		nodes: rg.nodes.Clone(),
		cfg:   rg.GetConfigCloned(),
	}
}

// MeetRequirement return whether resource group meet requirement.
// Return error with reason if not meet requirement.
func (rg *ResourceGroup) MeetRequirement() error {
	// if len(node) is less than requests, new node need to be assigned.
	if rg.nodes.Len() < int(rg.cfg.Requests.NodeNum) {
		return errors.Errorf(
			"has %d nodes, less than request %d",
			rg.nodes.Len(),
			rg.cfg.Requests.NodeNum,
		)
	}
	// if len(node) is greater than limits, node need to be removed.
	if rg.nodes.Len() > int(rg.cfg.Limits.NodeNum) {
		return errors.Errorf(
			"has %d nodes, greater than limit %d",
			rg.nodes.Len(),
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
