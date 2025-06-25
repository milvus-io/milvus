package indexparamcheck

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	MinGramKey = "MIN_GRAM"
	MaxGramKey = "MAX_GRAM"
)

type NgramIndexChecker struct {
	scalarIndexChecker
}

func newNgramIndexChecker() *NgramIndexChecker {
	return &NgramIndexChecker{}
}

func (c *NgramIndexChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	if dataType != schemapb.DataType_VarChar {
		// todo(SpadeA): we may support it for json in the future
		return merr.WrapErrParameterInvalidMsg("Ngram index can only be created on VARCHAR field")
	}

	minGramStr, minGramExist := params[MinGramKey]
	maxGramStr, maxGramExist := params[MaxGramKey]
	if !minGramExist || !maxGramExist {
		return merr.WrapErrParameterInvalidMsg("Ngram index must specify both min_gram and max_gram")
	}

	minGram, err := strconv.Atoi(minGramStr)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("min_gram for Ngram index must be an integer, got: %s", minGramStr)
	}

	maxGram, err := strconv.Atoi(maxGramStr)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("max_gram for Ngram index must be an integer, got: %s", maxGramStr)
	}

	if minGram <= 0 || maxGram <= 0 || minGram > maxGram {
		return merr.WrapErrParameterInvalidMsg("invalid min_gram or max_gram value for Ngram index, min_gram: %d, max_gram: %d", minGram, maxGram)
	}

	return c.scalarIndexChecker.CheckTrain(dataType, params)
}

func (c *NgramIndexChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	dType := field.GetDataType()
	if !typeutil.IsStringType(dType) {
		return fmt.Errorf("ngram index can only be created on VARCHAR field")
	}
	return nil
}
