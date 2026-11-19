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

package compactor

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// compactionTTLTime fixes physical purge at the source split boundary. A
// queued ordinary sort/schema plan must also not move its cutoff while waiting
// at a DataNode: its creation time precedes a subsequent split fence. Unknown
// legacy creation time conservatively retains TTL rows instead of purging at a
// later wall clock. Query execution continues to enforce TTL at its own MVCC.
func compactionTTLTime(plan *datapb.CompactionPlan, now time.Time) time.Time {
	if split := plan.GetSplitRewrite(); split != nil {
		fixed, _ := tsoutil.ParseTS(split.GetTtlCutoffTimetick())
		return fixed
	}
	created := time.Unix(max(plan.GetStartTime(), 0), 0)
	if created.After(now) {
		return now
	}
	return created
}
