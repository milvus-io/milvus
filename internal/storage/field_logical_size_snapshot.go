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

package storage

import (
	"math"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

const FieldLogicalSizeVersion = 1

// FieldLogicalSizesCover checks the accounting version and authoritative insert row count.
// Each field still needs its own entry in the snapshot.
func FieldLogicalSizesCover(sizes *datapb.FieldLogicalSizes, rows int64) bool {
	return sizes.GetVersion() == FieldLogicalSizeVersion && rows >= 0 && sizes.GetRowCount() == rows
}

func cloneFieldLogicalSizes(sizes *datapb.FieldLogicalSizes) *datapb.FieldLogicalSizes {
	if sizes == nil {
		return nil
	}
	out := &datapb.FieldLogicalSizes{Version: sizes.Version, RowCount: sizes.RowCount}
	if len(sizes.Fields) > 0 {
		out.Fields = make(map[int64]*datapb.FieldLogicalSize, len(sizes.Fields))
		for id, size := range sizes.Fields {
			if size != nil {
				out.Fields[id] = &datapb.FieldLogicalSize{PayloadBytes: size.PayloadBytes, ValidityBits: size.ValidityBits}
			}
		}
	}
	return out
}

// AppendFieldLogicalSizes joins disjoint batches. A field is known only if its
// entry covers both nonempty batches. Each returned entry is owned by the caller.
func AppendFieldLogicalSizes(previous *datapb.FieldLogicalSizes, previousRows int64, batch *datapb.FieldLogicalSizes, batchRows int64) *datapb.FieldLogicalSizes {
	if previousRows < 0 || batchRows < 0 || previousRows > math.MaxInt64-batchRows {
		return nil
	}
	if batchRows == 0 && FieldLogicalSizesCover(previous, previousRows) {
		return cloneFieldLogicalSizes(previous)
	}
	if !FieldLogicalSizesCover(batch, batchRows) {
		return nil
	}
	if previousRows == 0 {
		return cloneFieldLogicalSizes(batch)
	}
	if !FieldLogicalSizesCover(previous, previousRows) {
		return nil
	}
	out := &datapb.FieldLogicalSizes{
		Version:  FieldLogicalSizeVersion,
		RowCount: previousRows + batchRows,
		Fields:   make(map[int64]*datapb.FieldLogicalSize, min(len(previous.Fields), len(batch.Fields))),
	}
	for id, next := range batch.Fields {
		prior := previous.Fields[id]
		if prior == nil || next == nil || prior.PayloadBytes > math.MaxUint64-next.PayloadBytes || prior.ValidityBits > math.MaxUint64-next.ValidityBits {
			continue
		}
		out.Fields[id] = &datapb.FieldLogicalSize{
			PayloadBytes: prior.PayloadBytes + next.PayloadBytes,
			ValidityBits: prior.ValidityBits + next.ValidityBits,
		}
	}
	return out
}
