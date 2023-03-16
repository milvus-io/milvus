package querynode

import (
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

type cntReducer struct {
}

func (r *cntReducer) Reduce(results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	cnt := int64(0)
	for _, res := range results {
		c, err := funcutil.CntOfInternalResult(res)
		if err != nil {
			return nil, err
		}
		cnt += c
	}
	return funcutil.WrapCntToInternalResult(cnt), nil
}

type cntReducerSegCore struct {
}

func (r *cntReducerSegCore) Reduce(results []*segcorepb.RetrieveResults) (*segcorepb.RetrieveResults, error) {
	cnt := int64(0)
	for _, res := range results {
		c, err := funcutil.CntOfSegCoreResult(res)
		if err != nil {
			return nil, err
		}
		cnt += c
	}
	return funcutil.WrapCntToSegCoreResult(cnt), nil
}
