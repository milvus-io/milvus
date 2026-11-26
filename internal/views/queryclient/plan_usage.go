package queryclient

import (
	"slices"

	"github.com/milvus-io/milvus/internal/util/querymeter"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func queryPlanReadUsage(plan ShardPlan, partitions []int64, retained bool) (*internalpb.QueryReadUsage, error) {
	if plan.Usage == nil {
		return nil, missingPlanUsage(plan)
	}
	if !retained {
		return sumPlannedReads(plan.Usage.GetReads())
	}
	reads := make([]*viewpb.PlannedColumnRead, 0, len(plan.Usage.GetRequeryPartitions()))
	for partitionID, read := range plan.Usage.GetRequeryPartitions() {
		if len(partitions) == 0 || slices.Contains(partitions, partitionID) {
			reads = append(reads, read)
		}
	}
	return sumPlannedReads(reads)
}

func searchPlanReadUsage(plan ShardPlan) (*internalpb.QueryReadUsage, error) {
	if plan.Usage == nil {
		return nil, missingPlanUsage(plan)
	}
	reads := plan.Usage.GetReads()
	if plan.FilterOnlyRan {
		if plan.Usage.GetFilterOnly() == nil {
			return nil, missingPlanUsage(plan)
		}
		reads = append(slices.Clone(reads), plan.Usage.GetFilterOnly())
	}
	return sumPlannedReads(reads)
}

func missingPlanUsage(plan ShardPlan) error {
	return merr.WrapErrServiceInternalMsg("query plan for shard %s did not include logical column sizes", plan.ShardID.String())
}

func sumPlannedReads(reads []*viewpb.PlannedColumnRead) (*internalpb.QueryReadUsage, error) {
	total := &internalpb.QueryReadUsage{}
	for _, read := range reads {
		part := &internalpb.QueryReadUsage{}
		columns := read.GetColumns()
		for _, fieldID := range read.GetFieldIds() {
			payload, known := columns.GetPayloadBytes()[fieldID]
			if !known {
				return nil, merr.WrapErrServiceInternalMsg("missing logical column size for field %d in query plan", fieldID)
			}
			if err := querymeter.AddColumn(part, fieldID, payload, columns.GetValidityBits()[fieldID]); err != nil {
				return nil, err
			}
		}
		usage, err := querymeter.ScaleSearchUsage(part, read.GetNq())
		if err != nil {
			return nil, err
		}
		if err := querymeter.AddReadUsage(total, usage); err != nil {
			return nil, err
		}
	}
	return total, nil
}
