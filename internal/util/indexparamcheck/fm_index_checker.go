package indexparamcheck

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// FmSaSampleRateKey is the optional suffix-array sampling rate (space vs.
	// locate latency; no effect on Count). Default 8 when unset.
	FmSaSampleRateKey = "fm_sa_sample_rate"

	fmSaSampleRateMin = 4
	fmSaSampleRateMax = 256

	// FmBlockBytesKey is the optional rank-directory block granularity in bytes
	// (a power-of-two multiple of 8 in [8, 128]); larger shrinks the resident
	// directory RAM at no throughput cost up to ~64. Default 64 when unset.
	FmBlockBytesKey = "fm_block_bytes"

	fmBlockBytesMin = 8
	fmBlockBytesMax = 128
)

// FMIndexChecker validates params for the FM-index scalar index, an exact
// byte-level substring index for VARCHAR that accelerates LIKE
// prefix/infix/suffix with no candidate recheck.
type FMIndexChecker struct {
	scalarIndexChecker
}

func newFMIndexChecker() *FMIndexChecker {
	return &FMIndexChecker{}
}

func (c *FMIndexChecker) CheckTrain(dataType schemapb.DataType, elementType schemapb.DataType, params map[string]string) error {
	if dataType != schemapb.DataType_VarChar {
		return merr.WrapErrParameterInvalidMsg("FM-index can only be created on VARCHAR field")
	}

	if rateStr, ok := params[FmSaSampleRateKey]; ok {
		rate, err := strconv.Atoi(rateStr)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("fm_sa_sample_rate for FM-index must be an integer, got: %s", rateStr)
		}
		if rate < fmSaSampleRateMin || rate > fmSaSampleRateMax {
			return merr.WrapErrParameterInvalidMsg("fm_sa_sample_rate for FM-index must be in [%d, %d], got: %d", fmSaSampleRateMin, fmSaSampleRateMax, rate)
		}
	}

	if bbStr, ok := params[FmBlockBytesKey]; ok {
		bb, err := strconv.Atoi(bbStr)
		if err != nil {
			return merr.WrapErrParameterInvalidMsg("fm_block_bytes for FM-index must be an integer, got: %s", bbStr)
		}
		// Power of two in [8, 128] (8/16/32/64/128): one 8-byte rank sample per
		// block, and the block must divide the packed words evenly.
		if bb < fmBlockBytesMin || bb > fmBlockBytesMax || (bb&(bb-1)) != 0 {
			return merr.WrapErrParameterInvalidMsg("fm_block_bytes for FM-index must be a power of two in [%d, %d], got: %d", fmBlockBytesMin, fmBlockBytesMax, bb)
		}
	}

	return c.scalarIndexChecker.CheckTrain(dataType, elementType, params)
}

func (c *FMIndexChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	// Keep this in lockstep with CheckTrain: VARCHAR only in this release.
	// JSON / TEXT / ARRAY are separate follow-ups.
	if field.GetDataType() != schemapb.DataType_VarChar {
		return merr.WrapErrParameterInvalidMsg("FM-index can only be created on VARCHAR field")
	}
	return nil
}
