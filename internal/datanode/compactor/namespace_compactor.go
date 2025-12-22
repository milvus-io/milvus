package compactor

import (
	"context"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// NamespaceCompactor compacts data with the same namespace together.
// Input segments must be sorted by namespace (partition key).
type NamespaceCompactor struct {
	*mixCompactionTask
}

func checkInputSorted(plan *datapb.CompactionPlan) bool {
	for _, segment := range plan.GetSegmentBinlogs() {
		if !segment.GetIsNamespaceSorted() {
			return false
		}
	}
	return true
}

func (c *NamespaceCompactor) Compact() (*datapb.CompactionPlanResult, error) {
	if !checkInputSorted(c.plan) {
		return nil, merr.WrapErrIllegalCompactionPlan("input segments must be sorted by namespace")
	}
	res, err := c.mixCompactionTask.Compact()
	if err != nil {
		return nil, err
	}
	// TODO: after compact
	return res, nil
}

func NewNamespaceCompactor(ctx context.Context, plan *datapb.CompactionPlan, binlogIO io.BinlogIO, compactionParams compaction.Params, sortByFieldIDs []int64) *NamespaceCompactor {
	return &NamespaceCompactor{
		mixCompactionTask: NewMixCompactionTask(ctx, binlogIO, plan, compactionParams, sortByFieldIDs),
	}
}
