package parameterutil

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func decimalField(typeParams ...*commonpb.KeyValuePair) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		DataType:   schemapb.DataType_Decimal,
		TypeParams: typeParams,
	}
}

func kv(key, value string) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{Key: key, Value: value}
}

func TestGetPrecisionAndScale(t *testing.T) {
	t.Run("not decimal type", func(t *testing.T) {
		f := &schemapb.FieldSchema{DataType: schemapb.DataType_Double}
		_, _, err := GetPrecisionAndScale(f)
		assert.Error(t, err)
	})

	t.Run("precision not found", func(t *testing.T) {
		f := decimalField()
		_, _, err := GetPrecisionAndScale(f)
		assert.Error(t, err)
	})

	t.Run("precision not int", func(t *testing.T) {
		f := decimalField(kv(common.PrecisionKey, "abc"), kv(common.ScaleKey, "2"))
		_, _, err := GetPrecisionAndScale(f)
		assert.Error(t, err)
	})

	t.Run("precision out of range", func(t *testing.T) {
		f := decimalField(kv(common.PrecisionKey, "0"), kv(common.ScaleKey, "0"))
		_, _, err := GetPrecisionAndScale(f)
		assert.Error(t, err)

		f = decimalField(kv(common.PrecisionKey, "19"), kv(common.ScaleKey, "0"))
		_, _, err = GetPrecisionAndScale(f)
		assert.Error(t, err)
	})

	t.Run("scale not found", func(t *testing.T) {
		f := decimalField(kv(common.PrecisionKey, "18"))
		_, _, err := GetPrecisionAndScale(f)
		assert.Error(t, err)
	})

	t.Run("scale not int", func(t *testing.T) {
		f := decimalField(kv(common.PrecisionKey, "18"), kv(common.ScaleKey, "x"))
		_, _, err := GetPrecisionAndScale(f)
		assert.Error(t, err)
	})

	t.Run("scale exceeds precision", func(t *testing.T) {
		f := decimalField(kv(common.PrecisionKey, "4"), kv(common.ScaleKey, "5"))
		_, _, err := GetPrecisionAndScale(f)
		assert.Error(t, err)
	})

	t.Run("scale negative", func(t *testing.T) {
		f := decimalField(kv(common.PrecisionKey, "4"), kv(common.ScaleKey, "-1"))
		_, _, err := GetPrecisionAndScale(f)
		assert.Error(t, err)
	})

	t.Run("valid", func(t *testing.T) {
		f := decimalField(kv(common.PrecisionKey, "18"), kv(common.ScaleKey, "4"))
		precision, scale, err := GetPrecisionAndScale(f)
		assert.NoError(t, err)
		assert.EqualValues(t, 18, precision)
		assert.EqualValues(t, 4, scale)
	})
}

func TestValidateDecimalString(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		precision int64
		scale     int64
		wantErr   bool
	}{
		{"empty", "", 18, 4, true},
		{"plain integer", "1234", 18, 4, false},
		{"negative integer", "-1234", 18, 4, false},
		{"simple decimal", "19.99", 18, 4, false},
		{"negative decimal", "-19.99", 18, 4, false},
		{"zero", "0", 18, 4, false},
		{"zero with fraction", "0.00", 18, 4, false},
		{"precision exceeded", "99999999999999999.99", 18, 2, true},       // 17 int digits + 2 frac = 19 > 18
		{"exact precision boundary", "9999999999999999.99", 18, 2, false}, // 16 int digits + 2 frac = 18 == 18
		{"scale exceeded", "1.12345", 18, 4, true},
		{"malformed trailing dot", "5.", 18, 4, true},
		{"malformed leading dot", ".5", 18, 4, true},
		{"non digit", "12a.3", 18, 4, true},
		{"only sign", "-", 18, 4, true},
		{"leading zeros trimmed for precision", "0007.12", 4, 2, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDecimalString(tt.value, tt.precision, tt.scale)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEncodeUnscaledInt64(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		precision int64
		scale     int64
		want      int64
		wantErr   bool
	}{
		{"simple", "19.99", 18, 4, 199900, false},
		{"negative", "-19.99", 18, 4, -199900, false},
		{"integer only", "1234", 18, 4, 12340000, false},
		{"zero", "0", 18, 4, 0, false},
		{"negative zero fraction", "-0.5", 18, 4, -5000, false},
		{"already at full scale", "1.1234", 18, 4, 11234, false},
		{"leading zeros", "0007.12", 4, 2, 712, false},
		{"scale zero", "42", 18, 0, 42, false},
		{"invalid propagates", "abc", 18, 4, 0, true},
		{"scale exceeded propagates", "1.99999", 18, 4, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EncodeUnscaledInt64(tt.value, tt.precision, tt.scale)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestUnscaledBytesRoundTrip(t *testing.T) {
	values := []int64{
		0, 1, -1, 199900, -199900, 12345,
		MaxUnscaledValue(MaxDecimalPrecision),
		-MaxUnscaledValue(MaxDecimalPrecision),
	}
	for _, v := range values {
		b := EncodeUnscaledBytes(v)
		assert.Len(t, b, DecimalBytesLen, "wire encoding must be fixed-width")
		got, err := DecodeUnscaledBytes(b)
		assert.NoError(t, err)
		assert.Equal(t, v, got)
	}
}

func TestDecodeUnscaledBytesRejectsWrongWidth(t *testing.T) {
	// Null rows carry an empty placeholder; callers must filter them via
	// valid_data rather than relying on the decoder to tolerate them.
	for _, b := range [][]byte{{}, {1}, make([]byte, 7), make([]byte, 9), make([]byte, 16)} {
		_, err := DecodeUnscaledBytes(b)
		assert.Error(t, err, "width %d must be rejected", len(b))
	}
}

func TestValidateUnscaledValue(t *testing.T) {
	// precision 5 admits at most 5 significant digits, i.e. |unscaled| <= 99999.
	assert.NoError(t, ValidateUnscaledValue(99999, 5))
	assert.NoError(t, ValidateUnscaledValue(-99999, 5))
	assert.Error(t, ValidateUnscaledValue(100000, 5))
	assert.Error(t, ValidateUnscaledValue(-100000, 5))

	limit := MaxUnscaledValue(MaxDecimalPrecision)
	assert.Equal(t, int64(999999999999999999), limit)
	assert.NoError(t, ValidateUnscaledValue(limit, MaxDecimalPrecision))
	assert.Error(t, ValidateUnscaledValue(limit+1, MaxDecimalPrecision))
}

// TestDecimalGoldenVectors pins the canonical wire encoding against the shared
// cross-language fixture. The C++ side consumes an identical copy at
// internal/core/unittest/testdata/decimal/golden_vectors.json; if these two ever
// disagree, Go and C++ would silently exchange different values for the same
// decimal literal.
func TestDecimalGoldenVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "decimal_golden_vectors.json"))
	require.NoError(t, err)

	var fixture struct {
		Cases []struct {
			Name       string `json:"name"`
			Literal    string `json:"literal"`
			Precision  int64  `json:"precision"`
			Scale      int64  `json:"scale"`
			Unscaled   int64  `json:"unscaled"`
			BytesLEHex string `json:"bytes_le_hex"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	require.NotEmpty(t, fixture.Cases)

	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			// literal -> unscaled (the SDK-facing conversion)
			unscaled, err := EncodeUnscaledInt64(tc.Literal, tc.Precision, tc.Scale)
			require.NoError(t, err)
			assert.Equal(t, tc.Unscaled, unscaled)

			// unscaled -> canonical bytes (what goes on the wire)
			wantBytes, err := hex.DecodeString(tc.BytesLEHex)
			require.NoError(t, err)
			assert.Equal(t, wantBytes, EncodeUnscaledBytes(unscaled))

			// bytes -> unscaled (what the server decodes)
			decoded, err := DecodeUnscaledBytes(wantBytes)
			require.NoError(t, err)
			assert.Equal(t, tc.Unscaled, decoded)

			assert.NoError(t, ValidateUnscaledValue(unscaled, tc.Precision))
		})
	}
}

// TestValidateDecimalStringPrecisionScaleInvariant pins the DECIMAL(p, s)
// invariant integer_digits <= p - s. Both cases below were mis-handled when
// validation counted only the fractional digits actually present in the input
// rather than the digits after padding to the field scale.
func TestValidateDecimalStringPrecisionScaleInvariant(t *testing.T) {
	// "12345" at DECIMAL(5,2) scales up to 1234500 — seven significant digits,
	// well past the declared precision of five — so it must be rejected.
	assert.Error(t, ValidateDecimalString("12345", 5, 2))
	assert.NoError(t, ValidateDecimalString("123.45", 5, 2))
	assert.NoError(t, ValidateDecimalString("123", 5, 2))
	assert.Error(t, ValidateDecimalString("1234", 5, 2))

	// "0.001" at DECIMAL(3,3) is legal: no significant integer digits, and
	// three fractional digits exactly fills the scale.
	assert.NoError(t, ValidateDecimalString("0.001", 3, 3))
	assert.NoError(t, ValidateDecimalString("0.999", 3, 3))
	assert.Error(t, ValidateDecimalString("1.001", 3, 3))

	// Leading zeros are not significant digits.
	assert.NoError(t, ValidateDecimalString("0007.12", 4, 2))
	assert.Error(t, ValidateDecimalString("0.9999", 3, 3))
}
