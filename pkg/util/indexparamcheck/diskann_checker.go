package indexparamcheck

// diskannChecker checks if an diskann index can be built.
type diskannChecker struct {
	floatVectorBaseChecker
}

func (c diskannChecker) StaticCheck(params map[string]string) error {
	return c.staticCheck(params)
}

func (c diskannChecker) CheckTrain(params map[string]string) error {
	if err := c.StaticCheck(params); err != nil {
		return err
	}
	return c.baseChecker.CheckTrain(params)
}

func newDiskannChecker() IndexChecker {
	return &diskannChecker{}
}
