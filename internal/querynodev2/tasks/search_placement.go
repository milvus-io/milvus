// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// distanceColumnName is the per-segment export column that holds the
// distance returned by the vector search (search_result_export_c.cpp).
const distanceColumnName = "$distance"

// searchPlacement produces the virtual columns of a Search that the
// QueryNode computes: $seg_id, $seg_offset and $distance. The cross-segment
// reduce records, for every result row, the input it came from, its offset
// in that segment and its row in that input, so the columns are built from
// that record without another read of the segments.
type searchPlacement struct {
	items []*planpb.ProjectionItem
	// segmentIDs[i] is the ID of the segment behind reduce input i.
	segmentIDs []int64
	// segDFs are the reduce inputs; $distance is read from them.
	segDFs []*chain.DataFrame
	// nqOffsets[i] is the index of the first query of reduce range i: chunk j
	// of that range's sources is chunk nqOffsets[i]+j of every input.
	nqOffsets []int
}

// placementProjectionItems returns the plan's projection items that the
// QueryNode Go layer computes.
func placementProjectionItems(plan *planpb.PlanNode) []*planpb.ProjectionItem {
	var items []*planpb.ProjectionItem
	for _, item := range plan.GetProjectionItems() {
		switch item.GetVirtualColumn() {
		case planpb.VirtualColumn_VirtualColumnSegmentID,
			planpb.VirtualColumn_VirtualColumnSegmentOffset,
			planpb.VirtualColumn_VirtualColumnDistance:
			items = append(items, item)
		}
	}
	return items
}

// distanceColumn gathers $distance for the rows of sources from the reduce
// inputs.
func (p *searchPlacement) distanceColumn(rangeIdx int, sources [][]segmentSource, totalRows int) ([]float32, error) {
	if rangeIdx < 0 || rangeIdx >= len(p.nqOffsets) {
		return nil, merr.WrapErrServiceInternalMsg("search $distance: reduce range %d out of range [0, %d)", rangeIdx, len(p.nqOffsets))
	}
	nqOffset := p.nqOffsets[rangeIdx]
	data := make([]float32, 0, totalRows)
	for chunkIdx, chunk := range sources {
		for _, src := range chunk {
			if src.InputIdx < 0 || src.InputIdx >= len(p.segDFs) || p.segDFs[src.InputIdx] == nil {
				return nil, merr.WrapErrServiceInternalMsg("search $distance: reduce input %d is not available", src.InputIdx)
			}
			column := p.segDFs[src.InputIdx].Column(distanceColumnName)
			if column == nil {
				return nil, merr.WrapErrServiceInternalMsg("search $distance: reduce input %d has no %s column", src.InputIdx, distanceColumnName)
			}
			if nqOffset+chunkIdx >= len(column.Chunks()) {
				return nil, merr.WrapErrServiceInternalMsg("search $distance: query %d out of range [0, %d)", nqOffset+chunkIdx, len(column.Chunks()))
			}
			values, ok := column.Chunk(nqOffset + chunkIdx).(*array.Float32)
			if !ok {
				return nil, merr.WrapErrServiceInternalMsg("search $distance: column type is %s, want float32", column.DataType())
			}
			if src.OriginalIdx < 0 || src.OriginalIdx >= values.Len() {
				return nil, merr.WrapErrServiceInternalMsg("search $distance: row %d out of range [0, %d)", src.OriginalIdx, values.Len())
			}
			data = append(data, values.Value(src.OriginalIdx))
		}
	}
	return data, nil
}

// appendColumns appends one column per item to the result, under the item's
// output ID, in the row order of sources. rangeIdx is the reduce range the
// sources belong to.
func (p *searchPlacement) appendColumns(rangeIdx int, sources [][]segmentSource, result *schemapb.SearchResultData) error {
	if p == nil || len(p.items) == 0 {
		return nil
	}
	totalRows := 0
	for _, chunk := range sources {
		totalRows += len(chunk)
	}
	for _, item := range p.items {
		if item.GetVirtualColumn() == planpb.VirtualColumn_VirtualColumnDistance {
			data, err := p.distanceColumn(rangeIdx, sources, totalRows)
			if err != nil {
				return err
			}
			result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
				Type:    schemapb.DataType_Float,
				FieldId: item.GetOutputId(),
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: data}},
				}},
			})
			continue
		}
		data := make([]int64, 0, totalRows)
		for _, chunk := range sources {
			for _, src := range chunk {
				if src.InputIdx < 0 || src.InputIdx >= len(p.segmentIDs) {
					return merr.WrapErrServiceInternalMsg("search placement: reduce input %d out of range [0, %d)", src.InputIdx, len(p.segmentIDs))
				}
				if item.GetVirtualColumn() == planpb.VirtualColumn_VirtualColumnSegmentID {
					data = append(data, p.segmentIDs[src.InputIdx])
					continue
				}
				if src.SegOffset < 0 {
					return merr.WrapErrServiceInternalMsg("search placement: segment offset is not available for a result row of segment %d", p.segmentIDs[src.InputIdx])
				}
				data = append(data, src.SegOffset)
			}
		}
		result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
			Type:    schemapb.DataType_Int64,
			FieldId: item.GetOutputId(),
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: data}},
			}},
		})
	}
	return nil
}
