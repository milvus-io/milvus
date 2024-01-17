package indexparamcheck

import "fmt"

// raftIVFChecker checks if a RAFT_IVF_Flat index can be built.
type raftIVFFlatChecker struct {
	ivfBaseChecker
}

// CheckTrain checks if ivf-flat index can be built with the specific index parameters.
func (c *raftIVFFlatChecker) CheckTrain(params map[string]string) error {
	if err := c.ivfBaseChecker.CheckTrain(params); err != nil {
		return err
	}
	if !CheckStrByValues(params, Metric, RaftMetrics) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", RaftMetrics)
	}

	setDefaultIfNotExist(params, RaftCacheDatasetOnDevice, "false")

	if !CheckStrByValues(params, RaftCacheDatasetOnDevice, []string{"true", "false"}) {
		return fmt.Errorf("raft index cache_dataset_on_device param only support true false")
	}

	return nil
}

func newRaftIVFFlatChecker() IndexChecker {
	return &raftIVFFlatChecker{}
}
