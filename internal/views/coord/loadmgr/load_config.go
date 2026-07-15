package loadmgr

import (
	"sort"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// LoadConfig is the desired load configuration of one collection.
// It unifies the DDL-side config (CollectionLoadInfo + LoadReplicaConfig).
// Runtime node membership is derived from ResourceGroup by the Balancer.
type LoadConfig struct {
	DbID                     int64
	CollectionID             int64
	PartitionIDs             []int64
	LoadFields               []*messagespb.LoadFieldConfig
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
		DbID:                     c.DbID,
		CollectionID:             c.CollectionID,
		UserSpecifiedReplicaMode: c.UserSpecifiedReplicaMode,
	}
	if len(c.PartitionIDs) > 0 {
		out.PartitionIDs = append([]int64{}, c.PartitionIDs...)
	}
	if len(c.LoadFields) > 0 {
		out.LoadFields = make([]*messagespb.LoadFieldConfig, len(c.LoadFields))
		for i, f := range c.LoadFields {
			out.LoadFields[i] = &messagespb.LoadFieldConfig{FieldId: f.FieldId, IndexId: f.IndexId}
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
	}
	for _, f := range msg.GetLoadFields() {
		cfg.LoadFields = append(cfg.LoadFields, &messagespb.LoadFieldConfig{
			FieldId: f.GetFieldId(),
			IndexId: f.GetIndexId(),
		})
	}
	for _, r := range msg.GetReplicas() {
		cfg.Replicas = append(cfg.Replicas, &ReplicaAssignment{
			ReplicaID:     r.GetReplicaId(),
			ResourceGroup: r.GetResourceGroupName(),
			Priority:      r.GetPriority(),
		})
	}
	return cfg
}

// buildFromPersisted reconstructs a LoadConfig from the persisted proto triple
// (CollectionLoadInfo, PartitionLoadInfo list, Replica list for this collection).
// NOTE: Priority is not currently persisted in the Replica proto; recovered
// replicas get commonpb.LoadPriority default (0 / HIGH). TODO: persist Priority.
func buildFromPersisted(
	info *querypb.CollectionLoadInfo,
	partitions []*querypb.PartitionLoadInfo,
	replicas []*querypb.Replica,
) *LoadConfig {
	cfg := &LoadConfig{
		DbID:                     info.GetDbID(),
		CollectionID:             info.GetCollectionID(),
		UserSpecifiedReplicaMode: info.GetUserSpecifiedReplicaMode(),
	}

	// PartitionIDs: derived from PartitionLoadInfo list.
	cfg.PartitionIDs = make([]int64, 0, len(partitions))
	for _, p := range partitions {
		cfg.PartitionIDs = append(cfg.PartitionIDs, p.GetPartitionID())
	}
	sort.Slice(cfg.PartitionIDs, func(i, j int) bool { return cfg.PartitionIDs[i] < cfg.PartitionIDs[j] })

	// LoadFields: combine the []int64 field list with the FieldIndexID map.
	fieldIndex := info.GetFieldIndexID()
	for _, fieldID := range info.GetLoadFields() {
		cfg.LoadFields = append(cfg.LoadFields, &messagespb.LoadFieldConfig{
			FieldId: fieldID,
			IndexId: fieldIndex[fieldID],
		})
	}

	// Replicas belonging to this collection.
	for _, r := range replicas {
		if r.GetCollectionID() != cfg.CollectionID {
			continue
		}
		cfg.Replicas = append(cfg.Replicas, &ReplicaAssignment{
			ReplicaID:     r.GetID(),
			ResourceGroup: r.GetResourceGroup(),
		})
	}
	sort.Slice(cfg.Replicas, func(i, j int) bool {
		return cfg.Replicas[i].ReplicaID < cfg.Replicas[j].ReplicaID
	})
	return cfg
}

// toCollectionLoadInfoProto serializes the top-level collection fields.
// Partitions and Replicas are serialized separately.
func (c *LoadConfig) toCollectionLoadInfoProto() *querypb.CollectionLoadInfo {
	info := &querypb.CollectionLoadInfo{
		CollectionID:             c.CollectionID,
		DbID:                     c.DbID,
		ReplicaNumber:            int32(len(c.Replicas)),
		UserSpecifiedReplicaMode: c.UserSpecifiedReplicaMode,
	}
	if len(c.LoadFields) > 0 {
		info.LoadFields = make([]int64, 0, len(c.LoadFields))
		info.FieldIndexID = make(map[int64]int64, len(c.LoadFields))
		for _, f := range c.LoadFields {
			info.LoadFields = append(info.LoadFields, f.GetFieldId())
			if f.GetIndexId() != 0 {
				info.FieldIndexID[f.GetFieldId()] = f.GetIndexId()
			}
		}
	}
	return info
}

// toPartitionLoadInfoProtos serializes each partition to its proto.
func (c *LoadConfig) toPartitionLoadInfoProtos() []*querypb.PartitionLoadInfo {
	out := make([]*querypb.PartitionLoadInfo, 0, len(c.PartitionIDs))
	for _, pid := range c.PartitionIDs {
		out = append(out, &querypb.PartitionLoadInfo{
			CollectionID: c.CollectionID,
			PartitionID:  pid,
		})
	}
	return out
}

// toReplicaProto serializes one ReplicaAssignment to its proto.
func (r *ReplicaAssignment) toReplicaProto(collectionID int64) *querypb.Replica {
	return &querypb.Replica{
		ID:            r.ReplicaID,
		CollectionID:  collectionID,
		ResourceGroup: r.ResourceGroup,
		// TODO: persist Priority once a proto field is added.
	}
}
