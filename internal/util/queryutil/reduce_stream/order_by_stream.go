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

package reducestream

import (
	"context"
	"io"

	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// recvOrderBy runs under recvMu. No row is final until all worker versions
// have arrived; only then can the vchannel select its ordered candidate window.
func (s *OrderedReduceStream) recvOrderBy() (*internalpb.RetrieveResults, error) {
	if s.outputChunks == nil {
		if err := s.prepareOrderBy(); err != nil {
			return nil, s.fail(err)
		}
	}
	if s.outputIndex == len(s.outputChunks) {
		s.finished = true
		if err := s.closeChildren(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
	chunk := s.outputChunks[s.outputIndex]
	s.outputChunks[s.outputIndex] = nil
	s.outputIndex++
	return chunk, nil
}

func (s *OrderedReduceStream) prepareOrderBy() error {
	positions := make(map[any]int)
	var units []retrieveUnit
	var rowSizes []int64
	var outputSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for {
		readyBuffers, err := s.getReadyBuffers()
		if err != nil {
			return err
		}
		if len(readyBuffers) == 0 {
			break
		}
		for _, buffer := range readyBuffers {
			calculator := queryutil.NewRowSizeCalculator(buffer.chunk)
			for buffer.hasUnit() {
				unit := buffer.pop()
				position, exists := positions[unit.pk()]
				// Match Batch: missing or equal timestamps retain the first row.
				if exists && (unit.timestamp() == 0 || unit.timestamp() <= units[position].timestamp()) {
					continue
				}
				rowSize := calculator.RowSize(unit.rowIndex)
				if exists {
					outputSize -= rowSizes[position]
					units[position], rowSizes[position] = unit, rowSize
				} else {
					positions[unit.pk()] = len(units)
					units = append(units, unit)
					rowSizes = append(rowSizes, rowSize)
				}
				outputSize += rowSize
				if maxOutputSize > 0 && outputSize > maxOutputSize {
					return merr.WrapErrParameterInvalidMsg("query results exceed the maxOutputSize Limit %d", maxOutputSize)
				}
			}
		}
	}

	// Index each retained source once, so nullable-column preparation is not
	// repeated for every selected row from the same Chunk.
	var results []*internalpb.RetrieveResults
	resultIndexes := make(map[*internalpb.RetrieveResults]int)
	rows := make([]queryutil.RowRef, len(units))
	for i, unit := range units {
		index, exists := resultIndexes[unit.result]
		if !exists {
			index = len(results)
			resultIndexes[unit.result] = index
			results = append(results, unit.result)
		}
		rows[i] = queryutil.NewRowRef(index, unit.rowIndex)
	}
	result, err := queryutil.BuildMergedRetrieveResults(results, rows, nil)
	if err != nil {
		return err
	}
	if len(units) > 0 {
		fields := make([]*orderby.OrderByField, len(s.orderByFields))
		for i, field := range s.orderByFields {
			data, err := (retrieveUnit{result: result}).fieldData(field.GetFieldId())
			if err != nil {
				return err
			}
			fields[i] = &orderby.OrderByField{
				FieldID: field.GetFieldId(), DataType: data.GetType(),
				Ascending: field.GetAscending(), NullsFirst: field.GetNullsFirst(),
			}
		}
		ordered, err := queryutil.NewOrderByLimitOperator(fields, s.orderByLimit).Run(context.Background(), nil, result)
		if err != nil {
			return err
		}
		result = ordered[0].(*internalpb.RetrieveResults)
	}
	result.Status = merr.Success()
	s.attachMetadata(result)
	s.outputChunks, err = SplitRetrieveResult(result, s.chunkBytes)
	return err
}
