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

package adaptor

import (
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// A rewritten baseline may have folded source Deletes through the split, or
// earlier history through its own cursor. It cannot serve an older fixed B.
// Invalidation lets the caller seek an executable retained view while keeping
// its original B; it must never silently promote the snapshot to this floor.
func requireSplitReadSnapshot(meta *viewpb.QueryViewMeta, mvcc *viewpb.QueryPlanMVCC) error {
	floor := max(meta.GetTransformStartAfterTimetick(), meta.GetLineage().GetSwitchTimetick())
	if mvcc.GetGrowingTimetick() < floor || mvcc.GetTransformingTimetick() < floor {
		return viewerror.NewViewInvalidated("fixed query snapshot (%d,%d) precedes view history floor %d", mvcc.GetGrowingTimetick(), mvcc.GetTransformingTimetick(), floor)
	}
	return nil
}
