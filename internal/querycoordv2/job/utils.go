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
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/typeutil"
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

func loadPartitions(ctx context.Context, meta *meta.Meta, cluster session.Cluster,
	ignoreErr bool, collection int64, partitions ...int64) error {
	replicas := meta.ReplicaManager.GetByCollection(collection)
	loadReq := &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_LoadPartitions,
		},
		CollectionID: collection,
		PartitionIDs: partitions,
	}
	for _, replica := range replicas {
		for _, node := range replica.GetNodes() {
			status, err := cluster.LoadPartitions(ctx, node, loadReq)
			if ignoreErr {
				continue
			}
			if err != nil {
				return err
			}
			if status.GetErrorCode() != commonpb.ErrorCode_Success {
				return fmt.Errorf("QueryNode failed to loadPartition, nodeID=%d, err=%s", node, status.GetReason())
			}
		}
	}
	return nil
}

func releasePartitions(ctx context.Context, meta *meta.Meta, cluster session.Cluster,
	ignoreErr bool, collection int64, partitions ...int64) error {
	replicas := meta.ReplicaManager.GetByCollection(collection)
	releaseReq := &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_ReleasePartitions,
		},
		CollectionID: collection,
		PartitionIDs: partitions,
	}
	for _, replica := range replicas {
		for _, node := range replica.GetNodes() {
			status, err := cluster.ReleasePartitions(ctx, node, releaseReq)
			if ignoreErr {
				continue
			}
			if err != nil {
				return err
			}
			if status.GetErrorCode() != commonpb.ErrorCode_Success {
				return fmt.Errorf("QueryNode failed to releasePartitions, nodeID=%d, err=%s", node, status.GetReason())
			}
		}
	}
	return nil
}
