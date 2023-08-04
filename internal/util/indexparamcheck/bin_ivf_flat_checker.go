package indexparamcheck

import (
	"fmt"
)

type binIVFFlatChecker struct {
	binaryVectorBaseChecker
}

func (c binIVFFlatChecker) StaticCheck(params map[string]string) error {
	if !CheckStrByValues(params, Metric, BinIvfMetrics) {
		return fmt.Errorf("metric type not found or not supported, supported: %v", BinIvfMetrics)
	}
	if !CheckIntByRange(params, NLIST, MinNList, MaxNList) {
		return errOutOfRange(NLIST, MinNList, MaxNList)
	}
	return nil
}

func (c binIVFFlatChecker) CheckTrain(params map[string]string) error {
	// static check first
	// or it will throw binaryVectorBaseChecker error msg when has the wrong metric type
	if err := c.StaticCheck(params); err != nil {
		return err
	}

	return c.binaryVectorBaseChecker.CheckTrain(params)
}

func newBinIVFFlatChecker() IndexChecker {
	return &binIVFFlatChecker{}
}
