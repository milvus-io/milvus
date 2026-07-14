package parameterutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

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
		{"precision exceeded", "99999999999999999.99", 18, 2, true}, // 17 int digits + 2 frac = 19 > 18
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
