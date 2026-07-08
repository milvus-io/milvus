package parameterutil

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// MaxDecimalPrecision is the largest precision supported in v1, bounded by the
// int64 unscaled-value storage representation (up to 18 safe decimal digits).
const MaxDecimalPrecision = 18

// GetPrecisionAndScale gets the precision and scale of a Decimal field from its type params.
func GetPrecisionAndScale(field *schemapb.FieldSchema) (precision int64, scale int64, err error) {
	if !typeutil.IsDecimalType(field.GetDataType()) {
		msg := fmt.Sprintf("%s is not of decimal type", field.GetDataType())
		return 0, 0, merr.WrapErrParameterInvalid(schemapb.DataType_Decimal, field.GetDataType(), msg)
	}
	h := typeutil.NewKvPairs(append(field.GetIndexParams(), field.GetTypeParams()...))

	precisionStr, err := h.Get(common.PrecisionKey)
	if err != nil {
		return 0, 0, merr.WrapErrParameterInvalid("precision key in type parameters", "not found", "precision not found")
	}
	precision, err = strconv.ParseInt(precisionStr, 10, 64)
	if err != nil {
		return 0, 0, merr.WrapErrParameterInvalid("value of precision should be an integer", precisionStr, "invalid precision")
	}
	if precision <= 0 || precision > MaxDecimalPrecision {
		return 0, 0, merr.WrapErrParameterInvalidMsg("precision must be in (0, %d], got %d", MaxDecimalPrecision, precision)
	}

	scaleStr, err := h.Get(common.ScaleKey)
	if err != nil {
		return 0, 0, merr.WrapErrParameterInvalid("scale key in type parameters", "not found", "scale not found")
	}
	scale, err = strconv.ParseInt(scaleStr, 10, 64)
	if err != nil {
		return 0, 0, merr.WrapErrParameterInvalid("value of scale should be an integer", scaleStr, "invalid scale")
	}
	if scale < 0 || scale > precision {
		return 0, 0, merr.WrapErrParameterInvalidMsg("scale must be in [0, precision(%d)], got %d", precision, scale)
	}

	return precision, scale, nil
}

// ValidateDecimalString checks that s is a valid decimal literal (optional leading '-',
// digits, optional '.' followed by digits) fitting within the given precision and scale.
// Parsing is done on the raw string, never via float conversion, so it never introduces
// the binary-rounding error Decimal exists to avoid.
func ValidateDecimalString(s string, precision, scale int64) error {
	rest := s
	if rest == "" {
		return merr.WrapErrParameterInvalidMsg("empty decimal value")
	}
	if rest[0] == '-' {
		rest = rest[1:]
	}

	intPart, fracPart, hasFrac := strings.Cut(rest, ".")
	if intPart == "" || !isAllDigits(intPart) {
		return merr.WrapErrParameterInvalidMsg("invalid decimal value %q: malformed integer part", s)
	}
	if hasFrac && (fracPart == "" || !isAllDigits(fracPart)) {
		return merr.WrapErrParameterInvalidMsg("invalid decimal value %q: malformed fractional part", s)
	}

	if int64(len(fracPart)) > scale {
		return merr.WrapErrParameterInvalidMsg("decimal value %q exceeds scale %d", s, scale)
	}

	trimmedInt := strings.TrimLeft(intPart, "0")
	if trimmedInt == "" {
		trimmedInt = "0"
	}
	totalDigits := int64(len(trimmedInt) + len(fracPart))
	if totalDigits > precision {
		return merr.WrapErrParameterInvalidMsg("decimal value %q exceeds precision %d", s, precision)
	}

	return nil
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
