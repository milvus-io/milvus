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
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/checkers"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// waitCollectionReleased blocks until
// all channels and segments of given collection(partitions) are released,
// empty partition list means wait for collection released
func waitCollectionReleased(dist *meta.DistributionManager, checkerController *checkers.CheckerController, collection int64, partitions ...int64) {
	partitionSet := typeutil.NewUniqueSet(partitions...)
	for {
		var (
			channels []*meta.DmChannel
			segments []*meta.Segment = dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(collection))
		)
		if partitionSet.Len() > 0 {
			segments = lo.Filter(segments, func(segment *meta.Segment, _ int) bool {
				return partitionSet.Contain(segment.GetPartitionID())
			})
		} else {
			channels = dist.ChannelDistManager.GetByCollectionAndFilter(collection)
		}

		if len(channels)+len(segments) == 0 {
			break
		} else {
			log.Info("wait for release done", zap.Int64("collection", collection),
				zap.Int64s("partitions", partitions),
				zap.Int("channel", len(channels)),
				zap.Int("segments", len(segments)),
			)
		}

		// trigger check more frequently
		checkerController.Check()
		time.Sleep(200 * time.Millisecond)
	}
}

func loadPartitions(ctx context.Context,
	meta *meta.Meta,
	cluster session.Cluster,
	collection int64,
	partitions ...int64,
) error {
	_, span := otel.Tracer(typeutil.QueryCoordRole).Start(ctx, "loadPartitions")
	defer span.End()
	start := time.Now()

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
			// TODO: rollback LoadPartitions if failed
			if err != nil {
				return err
			}
			if !merr.Ok(status) {
				return merr.Error(status)
			}
		}
	}

	log.Ctx(ctx).Info("load partitions done", zap.Int64("collectionID", collection),
		zap.Int64s("partitionIDs", partitions), zap.Duration("dur", time.Since(start)))
	return nil
}

func releasePartitions(ctx context.Context,
	meta *meta.Meta,
	cluster session.Cluster,
	collection int64,
	partitions ...int64,
) {
	log := log.Ctx(ctx).With(zap.Int64("collection", collection), zap.Int64s("partitions", partitions))
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
			// Ignore error as the Delegator will be removed from the query node,
			// causing search and query requests to fail due to the absence of Delegator.
			if err != nil {
				log.Warn("failed to ReleasePartitions", zap.Int64("node", node), zap.Error(err))
				continue
			}
			if !merr.Ok(status) {
				log.Warn("failed to ReleasePartitions", zap.Int64("node", node), zap.Error(merr.Error(status)))
			}
		}
	}
}
