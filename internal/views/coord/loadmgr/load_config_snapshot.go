package loadmgr

type LoadConfigSnapshot struct {
	version         uint64
	configs         map[int64]*LoadConfig
	replicaToConfig map[int64]*LoadConfig
}

func NewLoadConfigSnapshot(version uint64, configs map[int64]*LoadConfig) *LoadConfigSnapshot {
	configsCopy := make(map[int64]*LoadConfig, len(configs))
	replicaToConfig := make(map[int64]*LoadConfig)
	for collectionID, cfg := range configs {
		configsCopy[collectionID] = cfg
		for _, replica := range cfg.Replicas {
			replicaToConfig[replica.ReplicaID] = cfg
		}
	}
	return &LoadConfigSnapshot{
		version:         version,
		configs:         configsCopy,
		replicaToConfig: replicaToConfig,
	}
}

func (s *LoadConfigSnapshot) Version() uint64 {
	if s == nil {
		return 0
	}
	return s.version
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
