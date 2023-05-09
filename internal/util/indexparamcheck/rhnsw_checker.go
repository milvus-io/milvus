package indexparamcheck

type rHnswFlatChecker struct {
	floatVectorBaseChecker
}

func (c rHnswFlatChecker) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction) {
		return errOutOfRange(EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction)
	}

	if !CheckIntByRange(params, HNSWM, HNSWMinM, HNSWMaxM) {
		return errOutOfRange(HNSWM, HNSWMinM, HNSWMaxM)
	}

	return c.floatVectorBaseChecker.CheckTrain(params)
}

func newRHnswFlatChecker() IndexChecker {
	return &rHnswFlatChecker{}
}
