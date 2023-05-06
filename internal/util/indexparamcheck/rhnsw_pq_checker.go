package indexparamcheck

import (
	"fmt"
	"strconv"
)

type rHnswPQChecker struct {
	ivfPQChecker
}

func (c rHnswPQChecker) CheckTrain(params map[string]string) error {
	if err := c.floatVectorBaseChecker.CheckTrain(params); err != nil {
		return err
	}

	if !CheckIntByRange(params, EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction) {
		return errOutOfRange(EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction)
	}

	if !CheckIntByRange(params, HNSWM, HNSWMinM, HNSWMaxM) {
		return errOutOfRange(HNSWM, HNSWMinM, HNSWMaxM)
	}

	dimension, _ := strconv.Atoi(params[DIM])
	pqmStr, ok := params[PQM]
	if !ok {
		return fmt.Errorf("parameter %s not found", PQM)
	}
	pqm, err := strconv.Atoi(pqmStr)
	if err != nil || pqm == 0 {
		return fmt.Errorf("parameter %s is not invalid: %s", PQM, pqmStr)
	}

	return c.ivfPQChecker.checkCPUPQParams(dimension, pqm)
}

func newRHnswPQChecker() IndexChecker {
	return &rHnswPQChecker{}
}
