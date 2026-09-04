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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (c *Core) broadcastCreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) (int64, error) {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, in.GetDbName(), in.GetCollectionName())
	if err != nil {
		return 0, err
	}
	defer broadcaster.Close()

	collMeta, err := c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), typeutil.MaxTimestamp, true)
	if err != nil {
		return 0, err
	}
	if err := checkGeneralCapacity(ctx, 0, 1, 0, c); err != nil {
		return 0, err
	}
	// idempotency check using partition name index (O(1) instead of O(n))
	if partitionID, exists := c.meta.GetPartitionIDByName(collMeta.CollectionID, in.GetPartitionName()); exists {
		return partitionID, errIgnoerdCreatePartition
	}
	cfgMaxPartitionNum := Params.RootCoordCfg.MaxPartitionNum.GetAsInt()
	partitionNum := collMeta.GetPartitionNum(true)
	if partitionNum >= cfgMaxPartitionNum {
		return 0, merr.WrapErrParameterInvalidMsg("partition number (%d) exceeds max configuration (%d), collection: %s",
			partitionNum, cfgMaxPartitionNum, collMeta.Name)
	}

	partID, err := c.idAllocator.AllocOne()
	if err != nil {
		return 0, merr.Wrap(err, "failed to allocate partition ID")
	}

	channels := partitionDDLBroadcastChannels(collMeta.VirtualChannelNames)
	msg := message.NewCreatePartitionMessageBuilderV1().
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: collMeta.CollectionID,
			PartitionId:  partID,
		}).
		WithBody(&message.CreatePartitionRequest{
			Base:           commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_CreatePartition)),
			DbName:         in.GetDbName(),
			CollectionName: in.GetCollectionName(),
			PartitionName:  in.GetPartitionName(),
			DbID:           collMeta.DBID,
			CollectionID:   collMeta.CollectionID,
			PartitionID:    partID,
		}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	_, err = broadcaster.Broadcast(ctx, msg)
	return partID, err
}

func (c *DDLCallback) createPartitionV1AckCallback(ctx context.Context, result message.BroadcastResultCreatePartitionMessageV1) error {
	header := result.Message.Header()
	body := result.Message.MustBody()
	partition := &model.Partition{
		PartitionID:               header.PartitionId,
		PartitionName:             result.Message.MustBody().PartitionName,
		PartitionCreatedTimestamp: result.GetControlChannelResult().TimeTick,
		CollectionID:              header.CollectionId,
		State:                     pb.PartitionState_PartitionCreated,
	}
	if err := c.meta.AddPartition(ctx, partition); err != nil {
		return merr.Wrap(err, "failed to add partition meta")
	}
	return c.ExpireCaches(ctx, ce.NewBuilder().
		WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(body.DbName),
			ce.OptLPCMCollectionName(body.CollectionName),
			ce.OptLPCMCollectionID(header.CollectionId),
			ce.OptLPCMPartitionName(body.PartitionName),
			ce.OptLPCMMsgType(commonpb.MsgType_CreatePartition),
		))
}

// partitionDDLBroadcastChannels lists the channels a partition DDL must reach:
// the control channel plus EVERY vchannel the collection has.
//
// Not VirtualChannelNames[0:ShardsNum]. That indexing assumes the live shards
// are the first ShardsNum entries of the list, which a shard split breaks in
// both directions at once: the list grows with the split's targets while the
// retired sources stay in it, and ShardsNum counts only the routable shards. A
// collection rehashed from 2 to 3 has five vchannels and ShardsNum 3, so the
// slice is the two dead sources plus one of the three live shards -- the other
// two never learn the partition was created or dropped.
//
// A retired source is included deliberately. It still holds that partition's
// segments until adoption drops them, the streamingnode tracks partitions per
// vchannel, and neither partition handler is gated on the split fence, so the
// append succeeds and keeps the two views in agreement.
func partitionDDLBroadcastChannels(vchannels []string) []string {
	channels := make([]string, 0, len(vchannels)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	return append(channels, vchannels...)
}
