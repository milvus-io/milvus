package proxy

/*import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
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

		total, err := funcutil.CntOfQueryResults(res)
		assert.NoError(t, err)
		assert.Equal(t, int64(1+2+3+4), total)
	})
}
*/
