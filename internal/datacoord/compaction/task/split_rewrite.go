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

package task

import (
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// A sort/schema task can have been queued before the fence. Resolve the source
// cutoff when constructing its worker request, rather than trusting that old
// task's persisted snapshot to know about a later split.
func sourceSplitRewrite(mt meta.CompactionMeta, task *datapb.CompactionTask) *datapb.SplitShardRewritePlan {
	if current, ok := mt.(interface {
		SplitRewritePlan(string) *datapb.SplitShardRewritePlan
	}); ok {
		if plan := current.SplitRewritePlan(task.GetChannel()); plan != nil {
			return plan
		}
	}
	return task.GetSplitRewrite()
}
