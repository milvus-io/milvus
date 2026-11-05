package loadmgr

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// LoadConfig is the desired load configuration of one collection.
// It unifies the DDL-side config (CollectionLoadInfo + LoadReplicaConfig).
// Runtime node membership is derived from ResourceGroup by the Balancer.
type LoadConfig struct {
	Version                  uint64
	DbID                     int64
	CollectionID             int64
	PartitionIDs             []int64
	LoadFields               []*viewpb.LoadFieldConfig // Preload hints; empty follows the target schema, not query scope.
	RequiredFields           []*viewpb.LoadFieldConfig // Explicit internal query scope.
	UserSpecifiedReplicaMode bool
	Replicas                 []*ReplicaAssignment
}

// ReplicaAssignment pairs a replica ID with its desired resource-group
// constraint. The Balancer expands ResourceGroup to live nodes at plan time.
type ReplicaAssignment struct {
	ReplicaID     int64
	ResourceGroup string
	Priority      commonpb.LoadPriority
}

// Clone returns a deep copy of the LoadConfig. Safe for external mutation.
func (c *LoadConfig) Clone() *LoadConfig {
	if c == nil {
		return nil
	}
	out := &LoadConfig{
		Version:                  c.Version,
		DbID:                     c.DbID,
		CollectionID:             c.CollectionID,
		UserSpecifiedReplicaMode: c.UserSpecifiedReplicaMode,
		RequiredFields:           cloneFieldConfigs(c.RequiredFields),
	}
	if len(c.PartitionIDs) > 0 {
		out.PartitionIDs = append([]int64{}, c.PartitionIDs...)
	}
	if len(c.LoadFields) > 0 {
		out.LoadFields = make([]*viewpb.LoadFieldConfig, len(c.LoadFields))
		for i, f := range c.LoadFields {
			out.LoadFields[i] = proto.Clone(f).(*viewpb.LoadFieldConfig)
		}
	}
	if len(c.Replicas) > 0 {
		out.Replicas = make([]*ReplicaAssignment, len(c.Replicas))
		for i, r := range c.Replicas {
			out.Replicas[i] = r.Clone()
		}
	}
	return out
}

// Clone returns a deep copy of the ReplicaAssignment.
func (r *ReplicaAssignment) Clone() *ReplicaAssignment {
	if r == nil {
		return nil
	}
	out := &ReplicaAssignment{
		ReplicaID:     r.ReplicaID,
		ResourceGroup: r.ResourceGroup,
		Priority:      r.Priority,
	}
	return out
}

// FromAlterLoadConfigMessage builds a LoadConfig from a DDL message.
func FromAlterLoadConfigMessage(msg *messagespb.AlterLoadConfigMessageHeader) *LoadConfig {
	cfg := &LoadConfig{
		DbID:                     msg.GetDbId(),
		CollectionID:             msg.GetCollectionId(),
		PartitionIDs:             append([]int64{}, msg.GetPartitionIds()...),
		UserSpecifiedReplicaMode: msg.GetUserSpecifiedReplicaMode(),
		RequiredFields:           FieldsFromMessage(msg.GetRequiredFields()),
	}
	cfg.LoadFields = FieldsFromMessage(msg.GetLoadFields())
	for _, r := range msg.GetReplicas() {
		cfg.Replicas = append(cfg.Replicas, &ReplicaAssignment{
			ReplicaID:     r.GetReplicaId(),
			ResourceGroup: r.GetResourceGroupName(),
			Priority:      r.GetPriority(),
		})
	}
	return cfg
}

// buildFromPersisted reconstructs the complete versioned requirements.
func buildFromPersisted(info *viewpb.CollectionLoadConfig) *LoadConfig {
	cfg := &LoadConfig{
		Version: info.GetVersion(), DbID: info.GetDbID(), CollectionID: info.GetCollectionID(),
		UserSpecifiedReplicaMode: info.GetUserSpecifiedReplicaMode(),
		RequiredFields:           cloneFieldConfigs(info.GetRequiredFields()),
		LoadFields:               cloneFieldConfigs(info.GetPreloadFields()),
		PartitionIDs:             append([]int64(nil), info.GetPartitionIds()...),
	}
	for _, replica := range info.GetReplicas() {
		cfg.Replicas = append(cfg.Replicas, &ReplicaAssignment{
			ReplicaID: replica.GetReplicaId(), ResourceGroup: replica.GetResourceGroupName(), Priority: replica.GetPriority(),
		})
	}
	return cfg
}

// toCollectionLoadConfigProto persists references, not index definitions or parameters.
func (c *LoadConfig) toCollectionLoadConfigProto() *viewpb.CollectionLoadConfig {
	info := &viewpb.CollectionLoadConfig{
		Version:                  c.Version,
		CollectionID:             c.CollectionID,
		DbID:                     c.DbID,
		UserSpecifiedReplicaMode: c.UserSpecifiedReplicaMode,
		RequiredFields:           cloneFieldConfigs(c.RequiredFields),
		PartitionIds:             append([]int64(nil), c.PartitionIDs...),
	}
	info.PreloadFields = cloneFieldConfigs(c.LoadFields)
	for _, replica := range c.Replicas {
		info.Replicas = append(info.Replicas, &viewpb.LoadReplicaConfig{
			ReplicaId: replica.ReplicaID, ResourceGroupName: replica.ResourceGroup, Priority: replica.Priority,
		})
	}
	return info
}

func cloneFieldConfigs(fields []*viewpb.LoadFieldConfig) []*viewpb.LoadFieldConfig {
	if fields == nil {
		return nil
	}
	out := make([]*viewpb.LoadFieldConfig, len(fields))
	for i, field := range fields {
		out[i] = proto.Clone(field).(*viewpb.LoadFieldConfig)
	}
	return out
}

// FieldsFromMessage converts WAL intent into independent QueryView requirements.
// Publication resolves the complete selected index set from DataCoord, since the
// historical WAL message expresses at most one index per field.
func FieldsFromMessage(fields []*messagespb.LoadFieldConfig) []*viewpb.LoadFieldConfig {
	if fields == nil {
		return nil
	}
	out := make([]*viewpb.LoadFieldConfig, 0, len(fields))
	for _, field := range fields {
		next := &viewpb.LoadFieldConfig{FieldId: field.GetFieldId()}
		if id := field.GetIndexId(); id != 0 {
			next.IndexIds = []int64{id}
		}
		out = append(out, next)
	}
	return out
}

// FieldsToMessage projects load intent into the existing WAL message. Its scalar
// index hint is not the authoritative QueryView index selection; publication
// resolves every selected index from committed metadata.
func FieldsToMessage(fields []*viewpb.LoadFieldConfig) []*messagespb.LoadFieldConfig {
	if fields == nil {
		return nil
	}
	out := make([]*messagespb.LoadFieldConfig, 0, len(fields))
	for _, field := range fields {
		next := &messagespb.LoadFieldConfig{FieldId: field.GetFieldId()}
		if len(field.GetIndexIds()) != 0 {
			next.IndexId = field.IndexIds[0]
		}
		out = append(out, next)
	}
	return out
}
