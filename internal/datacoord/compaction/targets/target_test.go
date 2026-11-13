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
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func targetSegmentWithDataTS(id, collectionID, partitionID int64, channel string, createTS uint64, dataTS uint64, compacting bool, compactionFrom ...int64) *meta.SegmentInfo {
	return &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:                  id,
			CollectionID:        collectionID,
			PartitionID:         partitionID,
			InsertChannel:       channel,
			StartPosition:       &msgpb.MsgPosition{Timestamp: dataTS},
			State:               commonpb.SegmentState_Flushed,
			CreateTs:            createTS,
			CompactionFrom:      compactionFrom,
			NumOfRows:           100,
			IsImporting:         false,
			Level:               datapb.SegmentLevel_L1,
			StorageVersion:      2,
			IsSorted:            false,
			CreatedByCompaction: len(compactionFrom) > 0,
		},
		IsCompacting: compacting,
	}
}
