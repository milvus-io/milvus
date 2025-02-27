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

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const (
	// TODO: better to make them configurable, use default value if no config was set since we never explode these before.
	globalIDAllocatorKey     = "idTimestamp"
	globalIDAllocatorSubPath = "gid"
	globalTSOAllocatorKey    = "timestamp"
)

func checkPartitionNumber(ctx context.Context, newParNum int64, core *Core,
) error {
	partitionNumber := core.meta.GetTotalPartitionNumber(ctx)
	partitionNumber += int(newParNum)
	if partitionNumber > Params.QuotaConfig.MaxPartitionNum.GetAsInt() {
		return merr.WrapPartitionExceed(partitionNumber, Params.QuotaConfig.MaxPartitionNum.GetAsInt(),
			"failed checking constraint: total partition number exceeds the maximum partition limit of the cluster: ")
	}
	return nil
}
