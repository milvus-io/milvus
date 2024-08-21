package indexparamcheck

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// diskannChecker checks if an diskann index can be built.
type cagraChecker struct {
	floatVectorBaseChecker
}

func (c *cagraChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	err := c.baseChecker.CheckTrain(dataType, params)
	if err != nil {
		return err
	}
	interDegree := int(0)
	graphDegree := int(0)
	interDegreeStr, interDegreeExist := params[CagraInterDegree]
	if interDegreeExist {
		interDegree, err = strconv.Atoi(interDegreeStr)
		if err != nil {
			return fmt.Errorf("invalid cagra inter degree: %s", interDegreeStr)
		}
	}
	graphDegreeStr, graphDegreeExist := params[CagraGraphDegree]
	if graphDegreeExist {
		graphDegree, err = strconv.Atoi(graphDegreeStr)
		if err != nil {
			return fmt.Errorf("invalid cagra graph degree: %s", graphDegreeStr)
		}
	}
	if graphDegreeExist && interDegreeExist && interDegree < graphDegree {
		return fmt.Errorf("Graph degree cannot be larger than intermediate graph degree")
	}

	if !CheckStrByValues(params, Metric, RaftMetrics) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", RaftMetrics)
	}

	setDefaultIfNotExist(params, CagraBuildAlgo, "NN_DESCENT")

	if !CheckStrByValues(params, CagraBuildAlgo, CagraBuildAlgoTypes) {
		return fmt.Errorf("cagra build algo type not supported, supported: %v", CagraBuildAlgoTypes)
	}

	setDefaultIfNotExist(params, RaftCacheDatasetOnDevice, "false")

	if !CheckStrByValues(params, RaftCacheDatasetOnDevice, []string{"true", "false"}) {
		return fmt.Errorf("raft index cache_dataset_on_device param only support true false")
	}

	return nil
}

func (c cagraChecker) StaticCheck(dataType schemapb.DataType, params map[string]string) error {
	return c.staticCheck(params)
}

func newCagraChecker() IndexChecker {
	return &cagraChecker{}
}
