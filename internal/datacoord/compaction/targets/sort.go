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

package targets

import (
	"context"
	"math"

	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type SortObservation struct {
	Match []*meta.SegmentInfo
	Rows  int64
}

// ObserveSort evaluates cluster scope independently of ownership, visibility,
// protection, and available admission. Import owns its own sort stage.
func ObserveSort(ctx context.Context, mt *meta.Meta) (SortObservation, error) {
	var observation SortObservation
	internal := make(map[int64]bool)
	var rowErr error
	for _, segment := range mt.SelectSegments(ctx, meta.SegmentFilterFunc(policy.MatchesSortCompaction)) {
		if err := ctx.Err(); err != nil {
			return SortObservation{}, err
		}
		collectionID := segment.GetCollectionID()
		inScope, checked := internal[collectionID]
		if !checked {
			collection := mt.GetCollection(collectionID)
			inScope = collection.IsCreated() && !collection.IsExternal()
			internal[collectionID] = inScope
		}
		if !inScope {
			continue
		}
		rows := segment.GetNumOfRows()
		if rows < 0 || rows > math.MaxInt64-observation.Rows {
			if rowErr == nil {
				rowErr = merr.WrapErrDataIntegrityMsg("invalid SORT observation row count at segment %d", segment.GetID())
			}
		} else {
			observation.Rows += rows
		}
		observation.Match = append(observation.Match, segment)
	}
	if err := ctx.Err(); err != nil {
		return SortObservation{}, err
	}
	return observation, rowErr
}
