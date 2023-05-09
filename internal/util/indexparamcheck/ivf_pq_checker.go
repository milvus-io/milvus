package indexparamcheck

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/internal/util/funcutil"
)

// ivfPQChecker checks if a IVF_PQ index can be built.
type ivfPQChecker struct {
	ivfBaseChecker
}

// CheckTrain checks if ivf-pq index can be built with the specific index parameters.
func (c *ivfPQChecker) CheckTrain(params map[string]string) error {
	if err := c.ivfBaseChecker.CheckTrain(params); err != nil {
		return err
	}

	return c.checkPQParams(params)
}

func (c *ivfPQChecker) checkPQParams(params map[string]string) error {
	dimStr, dimensionExist := params[DIM]
	if !dimensionExist {
		return fmt.Errorf("dimension not found")
	}

	dimension, err := strconv.Atoi(dimStr)
	if err != nil { // invalid dimension
		return fmt.Errorf("invalid dimension: %s", dimStr)
	}

	// nbits can be set to default: 8
	nbitsStr, nbitsExist := params[NBITS]
	var nbits int
	if !nbitsExist {
		nbits = 8
	} else {
		nbits, err = strconv.Atoi(nbitsStr)
		if err != nil { // invalid nbits
			return fmt.Errorf("invalid nbits: %s", nbitsStr)
		}
	}

	mStr, ok := params[IVFM]
	if !ok {
		return fmt.Errorf("parameter `m` not found")
	}
	m, err := strconv.Atoi(mStr)
	if err != nil || m == 0 { // invalid m
		return fmt.Errorf("invalid `m`: %s", mStr)
	}

	mode, ok := params[IndexMode]
	if !ok {
		mode = CPUMode
	}

	// check cpu parameters first.
	if err := c.checkCPUPQParams(dimension, m); err != nil {
		return err
	}

	if mode == GPUMode {
		return c.checkGPUPQParams(dimension, m, nbits)
	}

	return nil
}

func (c *ivfPQChecker) checkCPUPQParams(dimension, m int) error {
	if (dimension % m) != 0 {
		return fmt.Errorf("dimension must be abled to be divided by `m`, dimension: %d, m: %d", dimension, m)
	}
	return nil
}

func (c *ivfPQChecker) checkGPUPQParams(dimension, m, nbits int) error {
	/*
	 * Faiss 1.6
	 * Only 1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 28, 32 dims per sub-quantizer are currently supported with
	 * no precomputed codes. Precomputed codes supports any number of dimensions, but will involve memory overheads.
	 */

	subDim := dimension / m
	if funcutil.SliceContain(supportSubQuantizer, m) && funcutil.SliceContain(supportDimPerSubQuantizer, subDim) && nbits == 8 {
		return nil
	}
	return fmt.Errorf("invalid pq parameters, dimension: %d, m: %d, nbits: %d", dimension, m, nbits)
}

func newIVFPQChecker() IndexChecker {
	return &ivfPQChecker{}
}
