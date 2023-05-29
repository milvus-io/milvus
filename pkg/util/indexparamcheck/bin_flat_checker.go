package indexparamcheck

type binFlatChecker struct {
	binaryVectorBaseChecker
}

func (c binFlatChecker) CheckTrain(params map[string]string) error {
	return c.binaryVectorBaseChecker.CheckTrain(params)
}

func (c binFlatChecker) StaticCheck(params map[string]string) error {
	return c.staticCheck(params)
}

func newBinFlatChecker() IndexChecker {
	return &binFlatChecker{}
}
