package indexparamcheck

type annoyChecker struct {
	floatVectorBaseChecker
}

func (c annoyChecker) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, NTREES, MinNTrees, MaxNTrees) {
		return errOutOfRange(NTREES, MinNTrees, MaxNTrees)
	}

	return c.floatVectorBaseChecker.CheckTrain(params)
}

func newAnnoyChecker() IndexChecker {
	return &annoyChecker{}
}
