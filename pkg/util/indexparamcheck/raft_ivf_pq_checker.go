package indexparamcheck

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/util/merr"
)

// raftIVFPQChecker checks if a RAFT_IVF_PQ index can be built.
type raftIVFPQChecker struct {
	ivfBaseChecker
}

// CheckTrain checks if ivf-pq index can be built with the specific index parameters.
func (c *raftIVFPQChecker) CheckTrain(params map[string]string) error {
	if err := c.ivfBaseChecker.CheckTrain(params); err != nil {
		return err
	}

	return c.checkPQParams(params)
}

func (c *raftIVFPQChecker) checkPQParams(params map[string]string) error {
	dimStr, dimensionExist := params[DIM]
	if !dimensionExist {
		return merr.WrapErrParameterInvalidMsg("dimension not found")
	}

	dimension, err := strconv.Atoi(dimStr)
	if err != nil { // invalid dimension
		return merr.WrapErrParameterInvalidMsg("invalid dimension: %s", dimStr)
	}

	// nbits can be set to default: 8
	nbitsStr, nbitsExist := params[NBITS]
	if nbitsExist {
		_, err := strconv.Atoi(nbitsStr)
		if err != nil { // invalid nbits
			return merr.WrapErrParameterInvalidMsg("invalid nbits: %s", nbitsStr)
		}
	}

	mStr, ok := params[IVFM]
	if !ok {
		return merr.WrapErrParameterInvalidMsg("parameter `m` not found")
	}
	m, err := strconv.Atoi(mStr)
	if err != nil { // invalid m
		return merr.WrapErrParameterInvalidMsg("invalid `m`: %s", mStr)
	}

	// here is the only difference with IVF_PQ
	if m == 0 {
		return nil
	}
	if dimension%m != 0 {
		return merr.WrapErrParameterInvalidMsg("dimension must be abled to be divided by `m`, dimension: %d, m: %d", dimension, m)
	}
	return nil
}

func newRaftIVFPQChecker() IndexChecker {
	return &raftIVFPQChecker{}
}
