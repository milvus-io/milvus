package indexparamcheck

type binFlatChecker struct {
	binaryVectorBaseChecker
}

// CheckTrain checks if a binary flat index can be built with the specific parameters.
func (c *binFlatChecker) CheckTrain(params map[string]string) error {
	return c.binaryVectorBaseChecker.CheckTrain(params)
}

func newBinFlatChecker() IndexChecker {
	return &binFlatChecker{}
}
