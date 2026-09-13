package parameterutil

import (
	"encoding/binary"
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

// DecimalBytesLen is the exact width of the canonical Decimal wire encoding.
//
// A Decimal value travels in ScalarField.bytes_data as its signed unscaled
// integer (value * 10^scale) encoded as exactly 8 bytes, little-endian, two's
// complement — never as decimal text. Because MaxDecimalPrecision is 18, every
// representable value fits an int64, so the width is fixed rather than 8-or-16.
// This is a public contract shared with every SDK; see the BytesArray comment
// in milvus-proto schema.proto, and the C++ mirror in common/Decimal.h.
const DecimalBytesLen = 8

// EncodeUnscaledBytes encodes an unscaled int64 into the canonical wire form.
func EncodeUnscaledBytes(unscaled int64) []byte {
	b := make([]byte, DecimalBytesLen)
	binary.LittleEndian.PutUint64(b, uint64(unscaled))
	return b
}

// DecodeUnscaledBytes decodes the canonical wire form back into its unscaled
// int64 value. Null rows carry an empty placeholder and must be filtered out by
// the caller via valid_data before reaching here.
func DecodeUnscaledBytes(b []byte) (int64, error) {
	if len(b) != DecimalBytesLen {
		return 0, merr.WrapErrParameterInvalidMsg(
			"decimal value must be exactly %d bytes (little-endian unscaled int64), got %d", DecimalBytesLen, len(b))
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

// MaxUnscaledValue returns the largest absolute unscaled value representable at
// the given precision, i.e. 10^precision - 1.
func MaxUnscaledValue(precision int64) int64 {
	limit := int64(1)
	for i := int64(0); i < precision; i++ {
		limit *= 10
	}
	return limit - 1
}

// ValidateUnscaledValue checks that an unscaled value carries no more than
// `precision` significant digits.
func ValidateUnscaledValue(unscaled, precision int64) error {
	limit := MaxUnscaledValue(precision)
	if unscaled > limit || unscaled < -limit {
		return merr.WrapErrParameterInvalidMsg("decimal unscaled value %d exceeds precision %d", unscaled, precision)
	}
	return nil
}

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

	// DECIMAL(p, s) admits at most p total significant digits of which s are
	// fractional, so the integer part is bounded by p - s independently of how
	// many fractional digits the literal actually spells out. Counting only the
	// digits present would both admit over-precision values (DECIMAL(5,2) would
	// accept "12345", which scales up to 1234500 — seven digits) and reject
	// legal ones (DECIMAL(3,3) would refuse "0.001"). Leading zeros are not
	// significant, so "0.5" has zero integer digits.
	intDigits := int64(len(strings.TrimLeft(intPart, "0")))
	if intDigits > precision-scale {
		return merr.WrapErrParameterInvalidMsg(
			"decimal value %q exceeds precision %d with scale %d: at most %d digits are allowed before the decimal point",
			s, precision, scale, precision-scale)
	}

	return nil
}

// EncodeUnscaledInt64 converts a decimal literal (e.g. "19.99") into its unscaled
// integer representation at the given scale (e.g. scale=4 -> 199900), via pure
// string/integer arithmetic — never through a float — so it stays exact for every
// value ValidateDecimalString accepts.
func EncodeUnscaledInt64(literal string, precision, scale int64) (int64, error) {
	if err := ValidateDecimalString(literal, precision, scale); err != nil {
		return 0, err
	}

	rest := literal
	negative := false
	if rest[0] == '-' {
		negative = true
		rest = rest[1:]
	}

	intPart, fracPart, _ := strings.Cut(rest, ".")
	fracPart += strings.Repeat("0", int(scale)-len(fracPart))

	unscaled, err := strconv.ParseInt(intPart+fracPart, 10, 64)
	if err != nil {
		return 0, merr.WrapErrParameterInvalidMsg("decimal value %q overflows the unscaled int64 representation", literal)
	}
	if negative {
		unscaled = -unscaled
	}
	return unscaled, nil
}

func isAllDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
