package indexparamcheck

import (
	"fmt"
	"strconv"
)

// diskannChecker checks if an diskann index can be built.
type cagraChecker struct {
	floatVectorBaseChecker
}

func (c *cagraChecker) CheckTrain(params map[string]string) error {
	err := c.baseChecker.CheckTrain(params)
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

	if !CheckStrByValues(params, Metric, CagraMetrics) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", CagraMetrics)
	}

	return nil
}

func (c cagraChecker) StaticCheck(params map[string]string) error {
	return c.staticCheck(params)
}

func newCagraChecker() IndexChecker {
	return &cagraChecker{}
}
