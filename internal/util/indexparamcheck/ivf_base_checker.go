package indexparamcheck

type ivfBaseChecker struct {
	floatVectorBaseChecker
}

func (c *ivfBaseChecker) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, NLIST, MinNList, MaxNList) {
		return errOutOfRange(NLIST, MinNList, MaxNList)
	}

	// skip check number of rows

	return c.floatVectorBaseChecker.CheckTrain(params)
}

func newIVFBaseChecker() IndexChecker {
	return &ivfBaseChecker{}
}
