package indexparamcheck

// diskannChecker checks if an diskann index can be built.
type diskannChecker struct {
	floatVectorBaseChecker
}

func (c *diskannChecker) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, DIM, DiskAnnMinDim, DefaultMaxDim) {
		return errOutOfRange(DIM, DiskAnnMinDim, DefaultMaxDim)
	}
	return c.floatVectorBaseChecker.CheckTrain(params)
}

func newDiskannChecker() IndexChecker {
	return &diskannChecker{}
}
