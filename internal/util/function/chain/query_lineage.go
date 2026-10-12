// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chain

import (
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// PRE query snapshots describe the stage entry, while the current payload may
// already have been expanded. Supply row origins from protected lineage before
// every operator without mutating the request-owned snapshot.
func preProcessContextForFrame(ctx *types.FuncContext, frame *DataFrame) (*types.FuncContext, error) {
	queries := ctx.QueryContext()
	if ctx.Stage() != types.StagePreProcess || queries == nil {
		return ctx, nil
	}
	if frame.NumChunks() != 1 || len(frame.lineage) != 1 {
		return nil, merr.WrapErrServiceInternal("PRE query context requires one group with lineage")
	}
	originalCount := 0
	if len(queries.OriginalQueries) > 0 {
		originalCount = len(queries.OriginalQueries[0].GetValues())
	}
	origins := make([]int64, len(frame.lineage[0]))
	identity := queries.RowQueryOrigins == nil && len(origins) == originalCount
	previous := int64(-1)
	for row, sources := range frame.lineage[0] {
		if len(sources) != 1 || sources[0].Group != 0 {
			return nil, merr.WrapErrFunctionFailedMsg("PRE row %d requires one parent query", row)
		}
		source := sources[0].Row
		if source < previous {
			return nil, merr.WrapErrFunctionFailedMsg("PRE parent query order changed at row %d", row)
		}
		previous = source
		origin := source
		if queries.RowQueryOrigins != nil {
			if source < 0 || source >= int64(len(queries.RowQueryOrigins)) {
				return nil, merr.WrapErrFunctionFailedMsg("PRE parent query %d is out of bounds", source)
			}
			origin = queries.RowQueryOrigins[source]
		}
		if origin < 0 || origin >= int64(originalCount) {
			return nil, merr.WrapErrFunctionFailedMsg("PRE original query %d is out of bounds", origin)
		}
		origins[row] = origin
		identity = identity && origin == int64(row)
	}
	if identity {
		return ctx, nil // Preserve legacy map/select execution versions.
	}
	snapshot := *queries
	snapshot.RowQueryOrigins = origins
	return ctx.WithQueryContext(&snapshot), nil
}
