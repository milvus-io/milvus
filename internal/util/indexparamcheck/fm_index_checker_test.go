package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func Test_FMIndexChecker_CheckTrain(t *testing.T) {
	checker := newFMIndexChecker()

	t.Run("valid fmindex on varchar without sample rate", func(t *testing.T) {
		params := map[string]string{}
		err := checker.CheckTrain(schemapb.DataType_VarChar, schemapb.DataType_None, params)
		assert.NoError(t, err)
	})

	t.Run("valid fmindex on varchar with sample rate", func(t *testing.T) {
		for _, rate := range []string{"4", "32", "256", "+8"} {
			params := map[string]string{FmSaSampleRateKey: rate}
			err := checker.CheckTrain(schemapb.DataType_VarChar, schemapb.DataType_None, params)
			assert.NoError(t, err)
		}
	})

	t.Run("invalid fmindex on unsupported field type", func(t *testing.T) {
		// FM-index is VARCHAR-only in this release; JSON / TEXT / ARRAY are
		// follow-ups and must be rejected here.
		for _, dtype := range []schemapb.DataType{
			schemapb.DataType_Int64,
			schemapb.DataType_Bool,
			schemapb.DataType_Float,
			schemapb.DataType_JSON,
			schemapb.DataType_Array,
		} {
			params := map[string]string{}
			err := checker.CheckTrain(dtype, schemapb.DataType_None, params)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "FM-index can only be created on VARCHAR field")
		}
	})

	t.Run("invalid sample rate", func(t *testing.T) {
		testCases := []struct {
			name   string
			rate   string
			errMsg string
		}{
			{"non-integer", "abc", "fm_sa_sample_rate for FM-index must be an integer"},
			{"below min", "3", "fm_sa_sample_rate for FM-index must be in"},
			{"above max", "257", "fm_sa_sample_rate for FM-index must be in"},
		}
		for _, tc := range testCases {
			params := map[string]string{FmSaSampleRateKey: tc.rate}
			err := checker.CheckTrain(schemapb.DataType_VarChar, schemapb.DataType_None, params)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		}
	})

	t.Run("valid block bytes", func(t *testing.T) {
		for _, bb := range []string{"8", "16", "32", "64", "128", "+64"} {
			params := map[string]string{FmBlockBytesKey: bb}
			err := checker.CheckTrain(schemapb.DataType_VarChar, schemapb.DataType_None, params)
			assert.NoError(t, err)
		}
	})

	t.Run("invalid block bytes", func(t *testing.T) {
		testCases := []struct {
			name   string
			bb     string
			errMsg string
		}{
			{"non-integer", "abc", "fm_block_bytes for FM-index must be an integer"},
			{"not power of two", "24", "fm_block_bytes for FM-index must be a power of two"},
			{"below min", "4", "fm_block_bytes for FM-index must be a power of two"},
			{"above max", "256", "fm_block_bytes for FM-index must be a power of two"},
		}
		for _, tc := range testCases {
			params := map[string]string{FmBlockBytesKey: tc.bb}
			err := checker.CheckTrain(schemapb.DataType_VarChar, schemapb.DataType_None, params)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		}
	})
}

func Test_FMIndexChecker_CheckValidDataType(t *testing.T) {
	checker := newFMIndexChecker()

	t.Run("valid data type", func(t *testing.T) {
		field := &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}
		err := checker.CheckValidDataType(IndexFMINDEX, field)
		assert.NoError(t, err)
	})

	t.Run("invalid data types", func(t *testing.T) {
		invalidTypes := []schemapb.DataType{
			schemapb.DataType_Int64,
			schemapb.DataType_Float,
			schemapb.DataType_Bool,
			schemapb.DataType_Array,
			schemapb.DataType_JSON,
			schemapb.DataType_FloatVector,
		}
		for _, dtype := range invalidTypes {
			field := &schemapb.FieldSchema{DataType: dtype}
			err := checker.CheckValidDataType(IndexFMINDEX, field)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "FM-index can only be created on VARCHAR field")
		}
	})
}
