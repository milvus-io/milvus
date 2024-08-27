package segments

import (
	"context"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type ScanReducer struct {
	req *querypb.QueryRequest
}

func (r *ScanReducer) Reduce(ctx context.Context, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	if r.req.GetReq().GetScanCtx() == nil {
		return nil, merr.ErrParameterInvalid
	}
	reqScanCtx := r.req.GetReq().GetScanCtx()
	if reqScanCtx.IsInitScan {
		reqScanCtx.MvccTs = r.req.GetReq().GetMvccTimestamp()
		ret := &internalpb.RetrieveResults{
			Status:  merr.Success(),
			Ids:     &schemapb.IDs{},
			ScanCtx: reqScanCtx,
		}
		return ret, nil
	}
	if len(results) != 1 {
		//hc---refine error handling here
		return nil, merr.ErrInvalidSearchResult
	}
	return results[0], nil
}

type ScanReducerSegCore struct {
}

func (r *ScanReducerSegCore) Reduce(ctx context.Context, results []*segcorepb.RetrieveResults, segments []Segment, plan *RetrievePlan) (*segcorepb.RetrieveResults, error) {
	if len(results) != 1 {
		return nil, merr.ErrInvalidSearchResult
	}
	return results[0], nil
}
