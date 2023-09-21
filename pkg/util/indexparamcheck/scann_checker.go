package indexparamcheck

import (
	"fmt"
	"strconv"
)

// scaNNChecker checks if a SCANN index can be built.
type scaNNChecker struct {
	ivfBaseChecker
}

// CheckTrain checks if SCANN index can be built with the specific index parameters.
func (c *scaNNChecker) CheckTrain(params map[string]string) error {
	if err := c.ivfBaseChecker.CheckTrain(params); err != nil {
		return err
	}

	return c.checkScaNNParams(params)
}

func (c *scaNNChecker) checkScaNNParams(params map[string]string) error {
	dimStr, dimensionExist := params[DIM]
	if !dimensionExist {
		return fmt.Errorf("dimension not found")
	}

	dimension, err := strconv.Atoi(dimStr)
	if err != nil { // invalid dimension
		return fmt.Errorf("invalid dimension: %s", dimStr)
	}

	if (dimension % 2) != 0 {
		return fmt.Errorf("dimension must be able to be divided by 2, dimension: %d", dimension)
	}
	return nil
}

func newScaNNChecker() IndexChecker {
	return &scaNNChecker{}
}
