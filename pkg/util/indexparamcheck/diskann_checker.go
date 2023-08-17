package indexparamcheck

// diskannChecker checks if an diskann index can be built.
type diskannChecker struct {
	floatVectorBaseChecker
}

func (c diskannChecker) StaticCheck(params map[string]string) error {
	return c.staticCheck(params)
}

func newDiskannChecker() IndexChecker {
	return &diskannChecker{}
}
