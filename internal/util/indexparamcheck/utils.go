package indexparamcheck

import (
	"strconv"

	"github.com/milvus-io/milvus/internal/util/funcutil"
)

// CheckIntByRange check if the data corresponding to the key is in the range of [min, max].
// Return false if:
//   1. the key does not exist, or
//   2. the data cannot be converted to an integer, or
//   3. the number is not in the range [min, max]
// Return true otherwise
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

// CheckStrByValues check whether the data corresponding to the key appears in the string slice of container.
// Return false if:
//   1. the key does not exist, or
//   2. the data does not appear in the container
// Return true otherwise
func CheckStrByValues(params map[string]string, key string, container []string) bool {
	value, ok := params[key]
	if !ok {
		return false
	}

	return funcutil.SliceContain(container, value)
}
