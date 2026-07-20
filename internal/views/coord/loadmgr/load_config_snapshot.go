package loadmgr

type LoadConfigSnapshot struct {
	version         uint64
	configs         map[int64]*LoadConfig
	replicaToConfig map[int64]*LoadConfig
	configVersions  map[int64]uint64
}

func NewLoadConfigSnapshot(version uint64, configs map[int64]*LoadConfig) *LoadConfigSnapshot {
	configVersions := make(map[int64]uint64, len(configs))
	for collectionID := range configs {
		configVersions[collectionID] = 1
	}
	return NewLoadConfigSnapshotWithVersions(version, configs, configVersions)
}

func NewLoadConfigSnapshotWithVersions(version uint64, configs map[int64]*LoadConfig, versions map[int64]uint64) *LoadConfigSnapshot {
	configsCopy := make(map[int64]*LoadConfig, len(configs))
	replicaToConfig := make(map[int64]*LoadConfig)
	configVersions := make(map[int64]uint64, len(configs))
	for collectionID, cfg := range configs {
		configsCopy[collectionID] = cfg
		if versions != nil {
			configVersions[collectionID] = versions[collectionID]
		}
		if configVersions[collectionID] == 0 {
			configVersions[collectionID] = 1
		}
		for _, replica := range cfg.Replicas {
			replicaToConfig[replica.ReplicaID] = cfg
		}
	}
	return &LoadConfigSnapshot{
		version:         version,
		configs:         configsCopy,
		replicaToConfig: replicaToConfig,
		configVersions:  configVersions,
	}
}

func (s *LoadConfigSnapshot) Version() uint64 {
	if s == nil {
		return 0
	}
	return s.version
}

func (s *LoadConfigSnapshot) ConfigVersion(collectionID int64) uint64 {
	if s == nil {
		return 0
	}
	return s.configVersions[collectionID]
}

func (s *LoadConfigSnapshot) ConfigsMap() map[int64]*LoadConfig {
	if s == nil {
		return nil
	}
	return s.configs
}

func (s *LoadConfigSnapshot) ReplicaToConfigMap() map[int64]*LoadConfig {
	if s == nil {
		return nil
	}
	return s.replicaToConfig
}
