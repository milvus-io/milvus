package agg

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func isSupportedAggFieldType(aggregateName string, dt schemapb.DataType) bool {
	switch aggregateName {
	case kCount:
		// count(*) uses DataType_None; count(field) can work for any type since it
		// doesn't depend on the field's scalar representation in reducer.
		return true
	case kSum, kAvg:
		switch dt {
		case schemapb.DataType_Int8,
			schemapb.DataType_Int16,
			schemapb.DataType_Int32,
			schemapb.DataType_Int64,
			schemapb.DataType_Float,
			schemapb.DataType_Double:
			return true
		default:
			return false
		}
	case kMin, kMax:
		switch dt {
		case schemapb.DataType_Int8,
			schemapb.DataType_Int16,
			schemapb.DataType_Int32,
			schemapb.DataType_Int64,
			schemapb.DataType_Float,
			schemapb.DataType_Double,
			schemapb.DataType_VarChar,
			schemapb.DataType_String,
			schemapb.DataType_Timestamptz:
			return true
		default:
			return false
		}
	default:
		// operator validity is handled by NewAggregate; keep this conservative.
		return false
	}
}

// ValidateAggFieldType checks whether the input field type can be used by the
// given aggregation operator.
//
// Note: operator name is expected to be normalized (lowercase) by caller.
func ValidateAggFieldType(aggregateName string, dt schemapb.DataType) error {
	if !isSupportedAggFieldType(aggregateName, dt) {
		return fmt.Errorf("aggregation operator %s does not support data type %s", aggregateName, dt.String())
	}
	return nil
}
