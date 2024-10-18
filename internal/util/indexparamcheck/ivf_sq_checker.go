package indexparamcheck

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// ivfSQChecker checks if a IVF_SQ index can be built.
type ivfSQChecker struct {
	ivfBaseChecker
}

func (c *ivfSQChecker) checkNBits(params map[string]string) error {
	// cgo will set this key to DefaultNBits (8), which is the only value Milvus supports.
	_, exist := params[NBITS]
	if exist {
		// 8 is the only supported nbits.
		if !CheckIntByRange(params, NBITS, DefaultNBits, DefaultNBits) {
			return fmt.Errorf("nbits can be only set to 8 for IVF_SQ")
		}
	}
	return nil
}

// CheckTrain returns true if the index can be built with the specific index parameters.
func (c *ivfSQChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	if err := c.checkNBits(params); err != nil {
		return err
	}
	return c.ivfBaseChecker.CheckTrain(dataType, params)
}

func newIVFSQChecker() IndexChecker {
	return &ivfSQChecker{}
}
