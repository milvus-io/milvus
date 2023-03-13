package querynode

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/segcorepb"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/stretchr/testify/assert"
)

func Test_cntReducer_Reduce(t *testing.T) {

	t.Run("invalid", func(t *testing.T) {
		r := &cntReducer{}

		results := []*internalpb.RetrieveResults{
			{
				FieldsData: []*schemapb.FieldData{nil, nil},
			},
		}

		_, err := r.Reduce(results)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		r := &cntReducer{}

		results := []*internalpb.RetrieveResults{
			funcutil.WrapCntToInternalResult(1),
			funcutil.WrapCntToInternalResult(2),
			funcutil.WrapCntToInternalResult(3),
			funcutil.WrapCntToInternalResult(4),
		}

		res, err := r.Reduce(results)
		assert.NoError(t, err)

		total, err := funcutil.CntOfInternalResult(res)
		assert.NoError(t, err)
		assert.Equal(t, int64(1+2+3+4), total)
	})
}

func Test_cntReducerSegCore_Reduce(t *testing.T) {

	t.Run("invalid", func(t *testing.T) {
		r := &cntReducerSegCore{}

		results := []*segcorepb.RetrieveResults{
			{
				FieldsData: []*schemapb.FieldData{nil, nil},
			},
		}

		_, err := r.Reduce(results)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		r := &cntReducerSegCore{}

		results := []*segcorepb.RetrieveResults{
			funcutil.WrapCntToSegCoreResult(1),
			funcutil.WrapCntToSegCoreResult(2),
			funcutil.WrapCntToSegCoreResult(3),
			funcutil.WrapCntToSegCoreResult(4),
		}

		res, err := r.Reduce(results)
		assert.NoError(t, err)

		total, err := funcutil.CntOfSegCoreResult(res)
		assert.NoError(t, err)
		assert.Equal(t, int64(1+2+3+4), total)
	})
}
