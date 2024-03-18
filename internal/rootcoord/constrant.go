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

	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	// TODO: better to make them configurable, use default value if no config was set since we never explode these before.
	globalIDAllocatorKey      = "idTimestamp"
	globalIDAllocatorSubPath  = "gid"
	globalTSOAllocatorKey     = "timestamp"
	globalTSOAllocatorSubPath = "tso"
)

func checkGeneralCapacity(ctx context.Context, newColNum int,
	newParNum int64,
	newShardNum int32,
	core *Core,
	ts typeutil.Timestamp,
) error {
	var addedNum int64 = 0
	if newColNum > 0 && newParNum > 0 && newShardNum > 0 {
		// create collections scenarios
		addedNum += int64(newColNum) * newParNum * int64(newShardNum)
	} else if newColNum == 0 && newShardNum == 0 && newParNum > 0 {
		// add partitions to existing collections
		addedNum += newParNum
	}

	var generalNum int64 = 0
	collectionsMap := core.meta.ListAllAvailCollections(ctx)
	for dbId, collectionIDs := range collectionsMap {
		db, err := core.meta.GetDatabaseByID(ctx, dbId, ts)
		if err == nil {
			for _, collectionId := range collectionIDs {
				collection, err := core.meta.GetCollectionByID(ctx, db.Name, collectionId, ts, true)
				if err == nil {
					partNum := int64(collection.GetPartitionNum(false))
					shardNum := int64(collection.ShardsNum)
					generalNum += partNum * shardNum
				}
			}
		}
	}

	generalNum += addedNum
	if generalNum > Params.RootCoordCfg.MaxGeneralCapacity.GetAsInt64() {
		return merr.WrapGeneralCapacityExceed(generalNum, Params.RootCoordCfg.MaxGeneralCapacity.GetAsInt64(),
			"failed checking constraint: sum_collections(parition*shard) exceeding the max general capacity:")
	}
	return nil
}
