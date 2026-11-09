package queryclient

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/resource"
)

func (c *legacyClient) Materialize(ctx context.Context, req *LegacyQueryRequest, fingerprint uint64, maxOutputSize int64,
	consume func(ShardPlan, qviews.WorkNode, *internalpb.MaterializeResults) error,
) (err error) {
	defer func() { err = toMilvusError(err) }()
	if req.Plans == nil {
		return merr.WrapErrServiceInternalMsg("materialize requires retained query plans")
	}
	var mu sync.Mutex
	var unsupported error
	sizes := make(map[string]int64, len(req.Plans))
	group, groupCtx := errgroup.WithContext(ctx)
	for _, plan := range req.Plans {
		if len(plan.WorkNodes) == 0 {
			group.Go(func() error {
				return consume(plan, nil, &internalpb.MaterializeResults{Header: &internalpb.MaterializeHeader{
					Status: merr.Success(), FormatVersion: materialize.FormatVersion,
					MvccTimestamp: max(plan.Mvcc.GetGrowingTimetick(), plan.Mvcc.GetTransformingTimetick()),
					FeatureBits:   executionSummaryFeatureBits(plan.ExecutionSummary),
				}})
			})
		}
		for _, node := range plan.WorkNodes {
			group.Go(func() error {
				request := proto.Clone(req.Req).(*internalpb.RetrieveRequest)
				request.MvccTimestamp = legacyMVCCForNode(plan.Mvcc, node)
				result, callErr := c.shardClient.queryServiceClient.MaterializeOnView(groupCtx, node, &viewpb.MaterializeOnViewRequest{
					Request: &viewpb.MaterializeOnViewRequest_QueryByPk{QueryByPk: &viewpb.QueryOnViewRequest{
						LegacyReq: request, ShardId: plan.ShardID.IntoProto(), Version: plan.Version, Mvcc: plan.Mvcc,
						SegmentSelection: qviews.WorkNodeSelection(&viewpb.QueryPlan{WorkNodes: plan.Selections}, node).GetSegmentSelection(),
					}},
					IpcOptions: &viewpb.MaterializeIPCOptions{SchemaFingerprint: fingerprint, FormatVersion: materialize.FormatVersion},
				})
				if callErr == nil {
					if result.GetHeader() == nil {
						callErr = merr.WrapErrServiceInternalMsg("materialize: missing result header")
					} else {
						callErr = merr.Error(result.GetHeader().GetStatus())
					}
				}
				if callErr != nil {
					resource.ReleaseMaterializeBody(result)
				} else {
					result.Header.FeatureBits |= executionSummaryFeatureBits(plan.ExecutionSummary)
					result.Header.MvccTimestamp = max(plan.Mvcc.GetGrowingTimetick(), plan.Mvcc.GetTransformingTimetick())
					mu.Lock()
					sizes[plan.ShardID.VChannel] += result.GetHeader().GetResultSize()
					tooLarge := maxOutputSize > 0 && sizes[plan.ShardID.VChannel] > maxOutputSize
					mu.Unlock()
					if tooLarge {
						resource.ReleaseMaterializeBody(result)
						// This input budget counts duplicate PKs across workers.
						// Query must reduce them before enforcing the output quota.
						callErr = merr.Wrapf(merr.ErrServiceUnimplemented, "materialize: pre-reduce size exceeds budget %d", maxOutputSize)
					} else {
						callErr = consume(plan, node, result)
					}
				}
				// Preserve a required fallback until all workers finish, so the
				// caller can discard the replies and rerun as one operation.
				if errors.Is(toMilvusError(callErr), merr.ErrServiceUnimplemented) {
					mu.Lock()
					if unsupported == nil {
						unsupported = callErr
					}
					mu.Unlock()
					return nil
				}
				return callErr
			})
		}
	}
	err = group.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if unsupported != nil {
		return unsupported
	}
	return err
}
