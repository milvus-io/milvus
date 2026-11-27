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

package rootcoord

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
)

// splitSourceShardInfos returns one shard info in state ShardSplitting for
// every split source of the collection, in the order the splits were applied.
//
// A split source takes no DML and is not in the vchannel list, so these
// entries come after the entries of the shards that take DML and are not
// parallel to virtual_channel_names. A writer must not route to them; they
// tell it which vchannels may still hold rows, and idempotency windows, of
// keys written before a split. The residues are the keys the source owned,
// at the routing modulus of the response; none are given if the metadata
// cannot say (then the source may hold any key).
//
// RootCoord does not know when a cluster retires a source, so a source is
// listed for as long as the collection exists.
func splitSourceShardInfos(coll *model.Collection) []*schemapb.CollectionShardInfo {
	if len(coll.SplitLineages) == 0 {
		return nil
	}
	infos := make([]*schemapb.CollectionShardInfo, 0, len(coll.SplitLineages))
	for _, lineage := range coll.SplitLineages {
		info := &schemapb.CollectionShardInfo{
			State:        schemapb.ShardState_ShardSplitting,
			VchannelName: lineage.SourceVChannel,
		}
		if buckets := coll.SplitSourceBuckets(lineage.SourceVChannel); len(buckets) > 0 {
			info.Routing = &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: buckets},
			}
		}
		infos = append(infos, info)
	}
	return infos
}
