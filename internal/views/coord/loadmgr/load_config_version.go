package loadmgr

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
)

// loadInfoVersion identifies the persisted loading requirements, not the order
// of cache publications. Deriving it from canonical content keeps it stable
// across recovery without changing the catalog format. Only equality is valid.
// Priority is excluded: it is scheduling policy and is not persisted today.
func loadInfoVersion(config *LoadConfig) uint64 {
	cfg := config.Clone()
	sort.Slice(cfg.PartitionIDs, func(i, j int) bool { return cfg.PartitionIDs[i] < cfg.PartitionIDs[j] })
	sort.Slice(cfg.LoadFields, func(i, j int) bool {
		if cfg.LoadFields[i].FieldId != cfg.LoadFields[j].FieldId {
			return cfg.LoadFields[i].FieldId < cfg.LoadFields[j].FieldId
		}
		return cfg.LoadFields[i].IndexId < cfg.LoadFields[j].IndexId
	})
	sort.Slice(cfg.Replicas, func(i, j int) bool { return cfg.Replicas[i].ReplicaID < cfg.Replicas[j].ReplicaID })
	data := []byte("milvus-query-view-load-config-v1")
	add := func(value int64) { data = binary.LittleEndian.AppendUint64(data, uint64(value)) }
	add(cfg.DbID)
	add(cfg.CollectionID)
	if cfg.UserSpecifiedReplicaMode {
		add(1)
	} else {
		add(0)
	}
	add(int64(len(cfg.PartitionIDs)))
	for _, id := range cfg.PartitionIDs {
		add(id)
	}
	add(int64(len(cfg.LoadFields)))
	for _, field := range cfg.LoadFields {
		add(field.FieldId)
		add(field.IndexId)
	}
	add(int64(len(cfg.Replicas)))
	for _, replica := range cfg.Replicas {
		add(replica.ReplicaID)
		add(int64(len(replica.ResourceGroup)))
		data = append(data, replica.ResourceGroup...)
	}
	sum := sha256.Sum256(data)
	// Zero denotes unknown. Reserve the high bit to distinguish this identity
	// domain from the small process-local counters used by earlier draft code.
	return binary.LittleEndian.Uint64(sum[:8]) | (uint64(1) << 63)
}
