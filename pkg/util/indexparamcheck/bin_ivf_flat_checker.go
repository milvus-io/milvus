package indexparamcheck

import (
	"fmt"
)

type binIVFFlatChecker struct {
	binaryVectorBaseChecker
}

func (c binIVFFlatChecker) StaticCheck(params map[string]string) error {
	if !CheckStrByValues(params, Metric, BinIvfMetrics) {
		return fmt.Errorf("metric type %s not found or not supported, supported: %v", params[Metric], BinIvfMetrics)
	}

	if !CheckIntByRange(params, NLIST, MinNList, MaxNList) {
		return errOutOfRange(NLIST, MinNList, MaxNList)
	}

	return nil
}

func (c binIVFFlatChecker) CheckTrain(params map[string]string) error {
	if err := c.binaryVectorBaseChecker.CheckTrain(params); err != nil {
		return err
	}

	return c.StaticCheck(params)
}

func newBinIVFFlatChecker() IndexChecker {
	return &binIVFFlatChecker{}
}
