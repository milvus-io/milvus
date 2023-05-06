package indexparamcheck

type nsgChecker struct {
	floatVectorBaseChecker
}

func (c nsgChecker) CheckTrain(params map[string]string) error {
	if err := c.floatVectorBaseChecker.CheckTrain(params); err != nil {
		return err
	}

	if !CheckIntByRange(params, KNNG, MinKNNG, MaxKNNG) {
		return errOutOfRange(KNNG, MinKNNG, MaxKNNG)
	}

	if !CheckIntByRange(params, SearchLength, MinSearchLength, MaxSearchLength) {
		return errOutOfRange(SearchLength, MinSearchLength, MaxSearchLength)
	}

	if !CheckIntByRange(params, OutDegree, MinOutDegree, MaxOutDegree) {
		return errOutOfRange(OutDegree, MinOutDegree, MaxOutDegree)
	}

	if !CheckIntByRange(params, CANDIDATE, MinCandidatePoolSize, MaxCandidatePoolSize) {
		return errOutOfRange(CANDIDATE, MinCandidatePoolSize, MaxCandidatePoolSize)
	}

	// skip checking the number of rows

	return nil
}

func newNsgChecker() IndexChecker {
	return &nsgChecker{}
}
