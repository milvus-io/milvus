package indexparamcheck

import (
	"strconv"

	"github.com/milvus-io/milvus/internal/util/funcutil"
)

func CheckIntByRange(params map[string]string, key string, min, max int) bool {
	valueStr, ok := params[key]
	if !ok {
		return false
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return false
	}

	return value >= min && value <= max
}

func CheckStrByValues(params map[string]string, key string, container []string) bool {
	value, ok := params[key]
	if !ok {
		return false
	}

	return funcutil.SliceContain(container, value)
}
