package indexparamcheck

import (
	"strconv"

	"github.com/milvus-io/milvus/internal/util/funcutil"
)

// Check whether the data corresponding to the key is within the range of [min, max]. 
// If the key does not exist, or the data cannot be converted to an int type number, or the number is not in the range [min, max], return false
// Otherwise, return true
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

// Check whether the data corresponding to the key appears in the string slice of container.
// If the key does not exist, or the data does not appear in the container, return false
// Otherwise return true
func CheckStrByValues(params map[string]string, key string, container []string) bool {
	value, ok := params[key]
	if !ok {
		return false
	}

	return funcutil.SliceContain(container, value)
}
