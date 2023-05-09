package indexparamcheck

type rHnswSQChecker struct {
	floatVectorBaseChecker
}

func (c rHnswSQChecker) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction) {
		return errOutOfRange(EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction)
	}

	if !CheckIntByRange(params, HNSWM, HNSWMinM, HNSWMaxM) {
		return errOutOfRange(HNSWM, HNSWMinM, HNSWMaxM)
	}

	return c.floatVectorBaseChecker.CheckTrain(params)
}

func newRHnswSQChecker() IndexChecker {
	return &rHnswSQChecker{}
}
