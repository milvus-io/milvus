package indexparamcheck

import "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

type scalarIndexChecker struct {
	baseChecker
}

// IsJSONArrayCastType reports whether castType names a supported typed JSON array.
func IsJSONArrayCastType(castType string) bool {
	switch castType {
	case "ARRAY_BOOL", "ARRAY_DOUBLE", "ARRAY_VARCHAR":
		return true
	default:
		return false
	}
}

func (c scalarIndexChecker) CheckTrain(dataType schemapb.DataType, _ schemapb.DataType, params map[string]string) error {
	return nil
}
