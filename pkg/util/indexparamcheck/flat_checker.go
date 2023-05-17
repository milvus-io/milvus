package indexparamcheck

type flatChecker struct {
	floatVectorBaseChecker
}

func newFlatChecker() IndexChecker {
	return &flatChecker{}
}
