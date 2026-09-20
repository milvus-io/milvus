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

package meta

import (
	"context"
	"reflect"
	"slices"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// refreshRows caches a complete recovery-view row snapshot for a loaded scope.
// The caller holds the group recovery lock; RPCs hold no metadata locks.
func (g *replicaPlacement) refreshRows(ctx context.Context, broker Broker, scope map[int64][]int64) error {
	// All triggers share a complete successful row snapshot. Slow RPCs hold only
	// this group's scheduling lock, never replica/collection/resource metadata locks.
	var refreshErr error
	for _, parts := range scope {
		// Spawn precedes load metadata registration. Keep fault recovery active,
		// but do not optimize with an unknown scope mistaken for empty data.
		if len(parts) == 0 {
			refreshErr = merr.WrapErrServiceUnavailable("replica placement load scope is not registered yet")
			break
		}
	}
	if !reflect.DeepEqual(scope, g.scope) || time.Since(g.refreshed) >= 30*time.Second {
		rows := make(map[int64]int64, len(scope))
		ids := make([]int64, 0, len(scope))
		for id := range scope {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		singleShard := make(map[int64]bool, len(scope))
		for _, id := range ids {
			if refreshErr != nil {
				break
			}
			parts := scope[id]
			channels, segments, err := broker.GetRecoveryInfoV2(ctx, id, parts...)
			if err != nil {
				refreshErr = err
				break
			}
			if len(channels) == 0 {
				refreshErr = merr.WrapErrServiceUnavailable("replica placement recovery view has no channels")
				break
			}
			singleShard[id] = len(channels) == 1
			if !singleShard[id] {
				continue
			}
			partitions := typeutil.NewSet(parts...)
			for _, segment := range segments {
				// L0 contains deletes, not data rows. Count the other levels selected
				// by DC, including Legacy/L1 and clustering-compacted L2 segments.
				if segment.GetLevel() == datapb.SegmentLevel_L0 {
					continue
				}
				// DC owns recovery frontier selection; retain selected Dropped
				// compaction parents and match the loaded partition scope.
				if partitions.Contain(segment.GetPartitionID()) || segment.GetPartitionID() == common.AllPartitionsID {
					rows[id] += segment.GetNumOfRows()
				}
			}
		}
		if refreshErr != nil {
			mlog.RatedWarn(ctx, 1, "replica placement row refresh failed; retain last complete snapshot", mlog.String("resourceGroup", g.name), mlog.Err(refreshErr))
		} else {
			g.rows, g.scope, g.singleShard, g.refreshed = rows, scope, singleShard, time.Now()
		}
	}

	return refreshErr
}
