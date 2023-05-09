package indexparamcheck

import (
	"fmt"
	"strconv"
)

type ngtPANNGChecker struct {
	floatVectorBaseChecker
}

func (c ngtPANNGChecker) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, EdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return errOutOfRange(EdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize)
	}

	if !CheckIntByRange(params, ForcedlyPrunedEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return errOutOfRange(ForcedlyPrunedEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize)
	}

	if !CheckIntByRange(params, SelectivelyPrunedEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return errOutOfRange(SelectivelyPrunedEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize)
	}

	selectivelyPrunedEdgeSize, _ := strconv.Atoi(params[SelectivelyPrunedEdgeSize])
	forcedlyPrunedEdgeSize, _ := strconv.Atoi(params[ForcedlyPrunedEdgeSize])
	if selectivelyPrunedEdgeSize >= forcedlyPrunedEdgeSize {
		return fmt.Errorf("%s (%d) should be less than %s (%d)",
			SelectivelyPrunedEdgeSize, selectivelyPrunedEdgeSize, ForcedlyPrunedEdgeSize, forcedlyPrunedEdgeSize)
	}

	return c.floatVectorBaseChecker.CheckTrain(params)
}

func newNgtPANNGChecker() IndexChecker {
	return &ngtPANNGChecker{}
}
