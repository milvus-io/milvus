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

package job

import (
	"time"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/samber/lo"
)

// waitCollectionReleased blocks until
// all channels and segments of given collection(partitions) are released,
// empty partition list means wait for collection released
func waitCollectionReleased(dist *meta.DistributionManager, collection int64, partitions ...int64) {
	partitionSet := typeutil.NewUniqueSet(partitions...)
	for {
		var (
			channels []*meta.DmChannel
			segments []*meta.Segment = dist.SegmentDistManager.GetByCollection(collection)
		)
		if partitionSet.Len() > 0 {
			segments = lo.Filter(segments, func(segment *meta.Segment, _ int) bool {
				return partitionSet.Contain(segment.GetPartitionID())
			})
		} else {
			channels = dist.ChannelDistManager.GetByCollection(collection)
		}

		if len(channels)+len(segments) == 0 {
			break
		}

		time.Sleep(200 * time.Millisecond)
	}
}
