package indexparamcheck

type flatChecker struct {
	floatVectorBaseChecker
}

func (c flatChecker) StaticCheck(m map[string]string) error {
	return c.staticCheck(m)
}

func newFlatChecker() IndexChecker {
	return &flatChecker{}
}
