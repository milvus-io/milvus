package funcutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
)

func TestCntOfInternalResult(t *testing.T) {
	t.Run("invalid", func(t *testing.T) {
		res := &internalpb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{nil, nil},
		}
		_, err := CntOfInternalResult(res)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		res := WrapCntToInternalResult(5)
		cnt, err := CntOfInternalResult(res)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), cnt)
	})
}

func TestCntOfSegCoreResult(t *testing.T) {
	t.Run("invalid", func(t *testing.T) {
		res := &segcorepb.RetrieveResults{
			FieldsData: []*schemapb.FieldData{nil, nil},
		}
		_, err := CntOfSegCoreResult(res)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		res := WrapCntToSegCoreResult(5)
		cnt, err := CntOfSegCoreResult(res)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), cnt)
	})
}

func TestCntOfFieldData(t *testing.T) {
	t.Run("not scalars", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{},
		}
		_, err := CntOfFieldData(f)
		assert.Error(t, err)
	})

	t.Run("not long data", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{},
				},
			},
		}
		_, err := CntOfFieldData(f)
		assert.Error(t, err)
	})

	t.Run("more than one row", func(t *testing.T) {
		f := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
			},
		}
		_, err := CntOfFieldData(f)
		assert.Error(t, err)
	})

	t.Run("more than one row", func(t *testing.T) {
		f := WrapCntToFieldData(1000)
		cnt, err := CntOfFieldData(f)
		assert.NoError(t, err)
		assert.Equal(t, int64(1000), cnt)
	})
}
