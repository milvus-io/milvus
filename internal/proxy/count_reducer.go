package proxy

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

type cntReducer struct {
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
	return funcutil.WrapCntToQueryResults(cnt), nil
}
