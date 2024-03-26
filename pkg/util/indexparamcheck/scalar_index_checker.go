package indexparamcheck

type scalarIndexChecker struct {
	baseChecker
}

func (c scalarIndexChecker) CheckTrain(params map[string]string) error {
	return nil
}
