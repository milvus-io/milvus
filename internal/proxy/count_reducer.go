package proxy

/*import (
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type cntReducer struct {
	collectionName string
}

func (r *cntReducer) Reduce(results []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	cnt := int64(0)
	for _, res := range results {
		c, err := funcutil.CntOfInternalResult(res)
		if err != nil {
			return nil, err
		}
		cnt += c
	}
	res := funcutil.WrapCntToQueryResults(cnt)
	res.Status = merr.Success()
	res.CollectionName = r.collectionName
	return res, nil
}*/
