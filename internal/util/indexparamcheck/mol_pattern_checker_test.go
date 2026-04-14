package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_MOLPatternChecker_CheckTrain(t *testing.T) {
	checker := newMOLPatternChecker()

	t.Run("valid params", func(t *testing.T) {
		testCases := []map[string]string{
			{},
			{"n_bit": "2048"},
			{"n_bit": "64"},
			{"n_bit": "4096"},
		}

		for _, params := range testCases {
			err := checker.CheckTrain(schemapb.DataType_Mol, schemapb.DataType_None, params)
			assert.NoError(t, err)
		}
	})

	t.Run("invalid n_bit", func(t *testing.T) {
		testCases := []map[string]string{
			{"n_bit": "0"},
			{"n_bit": "63"},
			{"n_bit": "-1"},
			{"n_bit": "abc"},
			{"n_bit": "4097"},
			{"n_bit": "2147483648"},
		}

		for _, params := range testCases {
			err := checker.CheckTrain(schemapb.DataType_Mol, schemapb.DataType_None, params)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "n_bit for MOL_PATTERN index must be an integer in [64, 4096]")
		}
	})
}

func Test_MOLPatternChecker_CheckValidDataType(t *testing.T) {
	checker := newMOLPatternChecker()

	err := checker.CheckValidDataType(IndexMolPattern, &schemapb.FieldSchema{
		DataType: schemapb.DataType_Mol,
	})
	assert.NoError(t, err)

	err = checker.CheckValidDataType(IndexMolPattern, &schemapb.FieldSchema{
		DataType: schemapb.DataType_VarChar,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "MOL_PATTERN index can only be built on mol field")
}
