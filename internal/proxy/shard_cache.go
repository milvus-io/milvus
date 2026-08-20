// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type shardLeaderCacheInvalidator interface {
	InvalidateShardLeaderCache(collectionIDs []int64)
}

func invalidateShardLeaderCacheOnQueryNodeError(
	cache shardLeaderCacheInvalidator,
	collectionID int64,
	err error,
) {
	if paramtable.Get().ProxyCfg.SkipInvalidateShardLeaderCacheOnTimeout.GetAsBool() && isCanceledOrTimeout(err) {
		return
	}
	cache.InvalidateShardLeaderCache([]int64{collectionID})
}

func isCanceledOrTimeout(err error) bool {
	if merr.IsCanceledOrTimeout(err) {
		return true
	}

	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}
