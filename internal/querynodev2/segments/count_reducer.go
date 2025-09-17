package segments

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
)

type cntReducer struct{}

func (r *cntReducer) Reduce(ctx context.Context, results []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	cnt := int64(0)
	allRetrieveCount := int64(0)
	relatedDataSize := int64(0)
	scannedRemoteBytes := int64(0)
	scannedTotalBytes := int64(0)
	for _, res := range results {
		allRetrieveCount += res.GetAllRetrieveCount()
		relatedDataSize += res.GetCostAggregation().GetTotalRelatedDataSize()
		scannedRemoteBytes += res.GetScannedRemoteBytes()
		scannedTotalBytes += res.GetScannedTotalBytes()
		c, err := funcutil.CntOfInternalResult(res)
		if err != nil {
			return nil, err
		}
		cnt += c
	}
	res := funcutil.WrapCntToInternalResult(cnt)
	res.AllRetrieveCount = allRetrieveCount
	res.CostAggregation = &internalpb.CostAggregation{
		TotalRelatedDataSize: relatedDataSize,
	}
	res.ScannedRemoteBytes = scannedRemoteBytes
	res.ScannedTotalBytes = scannedTotalBytes
	return res, nil
}

type cntReducerSegCore struct{}

func (r *cntReducerSegCore) Reduce(ctx context.Context, results []*segcorepb.RetrieveResults, _ []Segment, _ *segcore.RetrievePlan) (*segcorepb.RetrieveResults, error) {
	cnt := int64(0)
	allRetrieveCount := int64(0)
	scannedRemoteBytes := int64(0)
	scannedTotalBytes := int64(0)
	for _, res := range results {
		allRetrieveCount += res.GetAllRetrieveCount()
		scannedRemoteBytes += res.GetScannedRemoteBytes()
		scannedTotalBytes += res.GetScannedTotalBytes()
		c, err := funcutil.CntOfSegCoreResult(res)
		if err != nil {
			return nil, err
		}
		cnt += c
	}
	res := funcutil.WrapCntToSegCoreResult(cnt)
	res.AllRetrieveCount = allRetrieveCount
	res.ScannedRemoteBytes = scannedRemoteBytes
	res.ScannedTotalBytes = scannedTotalBytes
	return res, nil
}
