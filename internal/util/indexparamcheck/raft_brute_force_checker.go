package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type raftBruteForceChecker struct {
	floatVectorBaseChecker
}

// raftBrustForceChecker checks if a Brute_Force index can be built.
func (c raftBruteForceChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	if err := c.floatVectorBaseChecker.CheckTrain(dataType, params); err != nil {
		return err
	}
	if !CheckStrByValues(params, Metric, RaftMetrics) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", RaftMetrics)
	}
	return nil
}

func newRaftBruteForceChecker() IndexChecker {
	return &raftBruteForceChecker{}
}
