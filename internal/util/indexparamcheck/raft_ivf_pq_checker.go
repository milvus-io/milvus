package indexparamcheck

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// raftIVFPQChecker checks if a RAFT_IVF_PQ index can be built.
type raftIVFPQChecker struct {
	ivfBaseChecker
}

// CheckTrain checks if ivf-pq index can be built with the specific index parameters.
func (c *raftIVFPQChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	if err := c.ivfBaseChecker.CheckTrain(dataType, params); err != nil {
		return err
	}
	if !CheckStrByValues(params, Metric, RaftMetrics) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", RaftMetrics)
	}
	return c.checkPQParams(params)
}

func (c *raftIVFPQChecker) checkPQParams(params map[string]string) error {
	dimStr, dimensionExist := params[DIM]
	if !dimensionExist {
		return fmt.Errorf("dimension not found")
	}

	dimension, err := strconv.Atoi(dimStr)
	if err != nil { // invalid dimension
		return fmt.Errorf("invalid dimension: %s", dimStr)
	}

	// nbits can be set to default: 8
	nbitsStr, nbitsExist := params[NBITS]
	if nbitsExist {
		_, err := strconv.Atoi(nbitsStr)
		if err != nil { // invalid nbits
			return fmt.Errorf("invalid nbits: %s", nbitsStr)
		}
	}

	mStr, ok := params[IVFM]
	if !ok {
		return fmt.Errorf("parameter `m` not found")
	}
	m, err := strconv.Atoi(mStr)
	if err != nil { // invalid m
		return fmt.Errorf("invalid `m`: %s", mStr)
	}

	// here is the only difference with IVF_PQ
	if m == 0 {
		return nil
	}
	if dimension%m != 0 {
		return fmt.Errorf("dimension must be able to be divided by `m`, dimension: %d, m: %d", dimension, m)
	}

	setDefaultIfNotExist(params, RaftCacheDatasetOnDevice, "false")

	if !CheckStrByValues(params, RaftCacheDatasetOnDevice, []string{"true", "false"}) {
		return fmt.Errorf("raft index cache_dataset_on_device param only support true false")
	}

	return nil
}

func newRaftIVFPQChecker() IndexChecker {
	return &raftIVFPQChecker{}
}
