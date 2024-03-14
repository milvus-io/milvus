package indexparamcheck

type sparseInvertedIndexChecker struct {
	sparseFloatVectorBaseChecker
}

func newSparseInvertedIndexChecker() *sparseInvertedIndexChecker {
	return &sparseInvertedIndexChecker{}
}
