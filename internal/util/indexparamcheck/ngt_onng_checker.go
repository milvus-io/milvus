package indexparamcheck

type ngtONNGChecker struct {
	floatVectorBaseChecker
}

func (c ngtONNGChecker) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, EdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return errOutOfRange(EdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize)
	}

	if !CheckIntByRange(params, OutgoingEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return errOutOfRange(OutgoingEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize)
	}

	if !CheckIntByRange(params, IncomingEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return errOutOfRange(IncomingEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize)
	}

	return c.floatVectorBaseChecker.CheckTrain(params)
}

func newNgtONNGChecker() IndexChecker {
	return &ngtONNGChecker{}
}
