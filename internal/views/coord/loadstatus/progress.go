package loadstatus

import (
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

// Progress describes loading against one collection's current desired state.
// Config is immutable. A zero total never means that loading is complete.
type Progress struct {
	Config *loadmgr.LoadConfig
	Total  int64
	Loaded int64
}

func (p Progress) Percentage() int64 {
	if p.Total == 0 {
		return 0
	}
	return p.Loaded * 100 / p.Total
}

// Ready uses the same completion criterion as the public loading percentage.
func (p Progress) Ready() bool {
	return p.Total > 0 && p.Loaded == p.Total
}

// Get counts the full vchannel-by-replica product, including shards not yet in
// the registry. vchannels must come from collection metadata, not resident views.
func Get(store *loadmgr.LoadConfigStore, registry *coordview.ShardViewRegistry, collectionID int64, vchannels []string) Progress {
	return GetAtSchemaVersion(store, registry, collectionID, vchannels, 0)
}

// GetAtSchemaVersion additionally fences completion on the Up views' schema.
// Newer schemas satisfy an earlier DDL; the load configuration is unchanged.
func GetAtSchemaVersion(store *loadmgr.LoadConfigStore, registry *coordview.ShardViewRegistry, collectionID int64, vchannels []string, schemaVersion int32) Progress {
	cfg, version := store.GetConfigWithVersion(collectionID)
	if cfg == nil {
		return Progress{}
	}
	shards := make([]qviews.ShardID, 0, len(vchannels)*len(cfg.Replicas))
	for _, replica := range cfg.Replicas {
		for _, channel := range vchannels {
			shards = append(shards, qviews.ShardID{ReplicaID: replica.ReplicaID, VChannel: channel})
		}
	}
	stats := registry.SnapshotForShards(shards).StatsMap()
	progress := Progress{Config: cfg, Total: int64(len(shards))}
	for _, shard := range shards {
		if stat := stats[shard]; stat != nil && stat.UpVersion != nil && stat.UpLoadInfoVersion == version && stat.UpSchemaVersion >= schemaVersion {
			progress.Loaded++
		}
	}
	// Config identities survive recovery. Also compare the immutable config
	// pointer to detect updates or release/reload with identical content while
	// the registry snapshot was being read.
	current, currentVersion := store.GetConfigWithVersion(collectionID)
	if current != cfg || currentVersion != version {
		return Progress{Config: current}
	}
	return progress
}
