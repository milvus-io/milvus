package adaptor

import (
	"math/bits"
	"slices"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newPlannedColumnRead(fields []int64, nq int64) *viewpb.PlannedColumnRead {
	columns := &viewpb.FieldLogicalBytes{PayloadBytes: make(map[int64]uint64, len(fields))}
	for _, fieldID := range fields {
		columns.PayloadBytes[fieldID] = 0
	}
	return &viewpb.PlannedColumnRead{Columns: columns, FieldIds: fields, Nq: nq}
}

func collectSealedPlanColumns(read *viewpb.PlannedColumnRead, plan *viewpb.QueryPlan, view *viewpb.QueryViewOfShard, partitions []int64, sub int) error {
	return visitSealedPlanSegments(plan, view, partitions, sub, func(_, segmentID int64) error {
		return addPlannedColumns(read, view.GetStreamingNode().GetSealedFieldBytes()[segmentID])
	})
}

func visitSealedPlanSegments(plan *viewpb.QueryPlan, view *viewpb.QueryViewOfShard, partitions []int64, sub int, visit func(partitionID, segmentID int64) error) error {
	for _, assignment := range view.GetQueryNode() {
		worker := qviews.WorkNodeSelection(plan, qviews.QueryNode{ID: assignment.GetNodeId()})
		if worker == nil {
			continue
		}
		selection := worker.GetSegmentSelection()
		if sub >= 0 && len(worker.GetSubSelections()) != 0 {
			if sub >= len(worker.GetSubSelections()) {
				return merr.WrapErrServiceInternalMsg("hybrid segment selections do not match planned reads")
			}
			selection = worker.GetSubSelections()[sub]
		}
		accepts := qviews.SegmentSelector(selection)
		for _, partition := range assignment.GetPartitions() {
			if len(partitions) != 0 && !slices.Contains(partitions, partition.GetPartitionId()) {
				continue
			}
			for _, id := range partition.GetSegmentIds() {
				if accepts(id) {
					if err := visit(partition.GetPartitionId(), id); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func addPlannedColumns(read *viewpb.PlannedColumnRead, size *viewpb.FieldLogicalBytes) error {
	columns := read.GetColumns()
	for fieldID, total := range columns.PayloadBytes {
		payload, known := size.GetPayloadBytes()[fieldID]
		if !known {
			delete(columns.PayloadBytes, fieldID)
			delete(columns.ValidityBits, fieldID)
			continue
		}
		payload, payloadOverflow := bits.Add64(total, payload, 0)
		validity, validityOverflow := bits.Add64(columns.ValidityBits[fieldID], size.GetValidityBits()[fieldID], 0)
		if payloadOverflow != 0 || validityOverflow != 0 {
			return merr.WrapErrServiceInternalMsg("planned logical size overflows for field %d", fieldID)
		}
		columns.PayloadBytes[fieldID] = payload
		if validity != 0 {
			if columns.ValidityBits == nil {
				columns.ValidityBits = make(map[int64]uint64)
			}
			columns.ValidityBits[fieldID] = validity
		}
	}
	return nil
}
