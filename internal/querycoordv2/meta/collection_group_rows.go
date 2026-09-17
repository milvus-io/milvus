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
	"time"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// refreshRows caches a complete recovery-view row snapshot for a loaded scope.
// The caller holds the group recovery lock; RPCs hold no metadata locks.
func (g *collectionGroup) refreshRows(ctx context.Context, broker Broker, scope map[int64][]int64) error {
	// All triggers share a complete successful row snapshot. Slow RPCs hold only
	// this group's scheduling lock, never replica/collection/resource metadata locks.
	var refreshErr error
	for _, parts := range scope {
		// Spawn precedes load metadata registration. Keep fault recovery active,
		// but do not optimize with an unknown scope mistaken for empty data.
		if len(parts) == 0 {
			refreshErr = merr.WrapErrServiceUnavailable("collection group load scope is not registered yet")
			break
		}
	}
	if !reflect.DeepEqual(scope, g.scope) || time.Since(g.refreshed) >= 30*time.Second {
		rows := make(map[int64]int64, len(scope))
		for _, id := range g.members {
			if refreshErr != nil {
				break
			}
			parts, ok := scope[id]
			if !ok {
				continue
			}
			_, segments, err := broker.GetRecoveryInfoV2(ctx, id, parts...)
			if err != nil {
				refreshErr = err
				break
			}
			partitions := typeutil.NewSet(parts...)
			for _, segment := range segments {
				// Match the load scope, including collection-wide L0 data. DC owns recovery
				// frontier selection; in particular do not filter Dropped compaction parents.
				if partitions.Contain(segment.GetPartitionID()) || segment.GetPartitionID() == common.AllPartitionsID {
					rows[id] += segment.GetNumOfRows()
				}
			}
		}
		if refreshErr != nil {
			mlog.RatedWarn(ctx, 1, "collection group row refresh failed; retain last complete snapshot", mlog.String("group", g.name), mlog.Err(refreshErr))
		} else {
			g.rows, g.scope, g.refreshed = rows, scope, time.Now()
		}
	}

	return refreshErr
}
