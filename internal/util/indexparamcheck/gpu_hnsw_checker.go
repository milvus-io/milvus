package indexparamcheck

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// gpuHnswChecker validates GPU_HNSW index parameters.
// knowhere's ValidateIndexParams returns 0 for GPU_HNSW (unimplemented config validation),
// so we validate M and efConstruction ranges in Go.
type gpuHnswChecker struct {
	vecIndexChecker
}

func (c *gpuHnswChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	if err := c.StaticCheck(dataType, elementType, params); err != nil {
		return err
	}
	if !CheckIntByRange(params, EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction) {
		return errOutOfRange(params[EFConstruction], HNSWMinEfConstruction, HNSWMaxEfConstruction)
	}
	if !CheckIntByRange(params, HNSWM, HNSWMinM, HNSWMaxM) {
		return errOutOfRange(params[HNSWM], HNSWMinM, HNSWMaxM)
	}
	if !CheckIntByRange(params, DIM, 1, 1<<31-1) {
		return errOutOfRange(params[DIM], 1, 1<<31-1)
	}
	return nil
}

func newGpuHnswChecker() IndexChecker {
	return &gpuHnswChecker{}
}
