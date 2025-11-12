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
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastDropPartition(ctx context.Context, in *milvuspb.DropPartitionRequest) error {
	if in.GetPartitionName() == Params.CommonCfg.DefaultPartitionName.GetValue() {
		return errors.New("default partition cannot be deleted")
	}

	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, in.GetDbName(), in.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	collMeta, err := c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		// Is this idempotent?
		return err
	}

	partID := common.InvalidPartitionID
	for _, partition := range collMeta.Partitions {
		if partition.PartitionName == in.GetPartitionName() {
			partID = partition.PartitionID
			break
		}
	}
	if partID == common.InvalidPartitionID {
		return errIgnoredDropPartition
	}

	channels := make([]string, 0, collMeta.ShardsNum+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	for i := 0; i < int(collMeta.ShardsNum); i++ {
		channels = append(channels, collMeta.VirtualChannelNames[i])
	}

	msg := message.NewDropPartitionMessageBuilderV1().
		WithHeader(&message.DropPartitionMessageHeader{
			CollectionId: collMeta.CollectionID,
			PartitionId:  partID,
		}).
		WithBody(&message.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_DropPartition,
			},
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
	return err
}

func (c *DDLCallback) dropPartitionV1AckCallback(ctx context.Context, result message.BroadcastResultDropPartitionMessageV1) error {
	header := result.Message.Header()
	body := result.Message.MustBody()

	for vchannel := range result.Results {
		if funcutil.IsControlChannel(vchannel) {
			continue
		}
		// drop all historical partition data when the vchannel is acknowledged.
		if err := c.mixCoord.NotifyDropPartition(ctx, vchannel, []int64{header.PartitionId}); err != nil {
			return err
		}
	}
	if err := c.meta.DropPartition(ctx, header.CollectionId, header.PartitionId, result.GetControlChannelResult().TimeTick); err != nil {
		return err
	}
	// add the partition tombstone to the sweeper.
	c.tombstoneSweeper.AddTombstone(newPartitionTombstone(c.meta, c.broker, header.CollectionId, header.PartitionId))
	// expire the partition meta cache on proxy.
	return c.ExpireCaches(ctx, ce.NewBuilder().
		WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(body.DbName),
			ce.OptLPCMCollectionName(body.CollectionName),
			ce.OptLPCMCollectionID(header.CollectionId),
			ce.OptLPCMPartitionName(body.PartitionName),
			ce.OptLPCMMsgType(commonpb.MsgType_DropPartition),
		),
		result.GetControlChannelResult().TimeTick)
}

// newPartitionTombstone creates a new partition tombstone.
func newPartitionTombstone(meta IMetaTable, broker Broker, collectionID int64, partitionID int64) *partitionTombstone {
	return &partitionTombstone{
		meta:         meta,
		broker:       broker,
		collectionID: collectionID,
		partitionID:  partitionID,
	}
}

type partitionTombstone struct {
	meta         IMetaTable
	broker       Broker
	collectionID int64
	partitionID  int64
}

func (t *partitionTombstone) ID() string {
	return fmt.Sprintf("p:%d:%d", t.collectionID, t.partitionID)
}

func (t *partitionTombstone) ConfirmCanBeRemoved(ctx context.Context) (bool, error) {
	return t.broker.GcConfirm(ctx, t.collectionID, t.partitionID), nil
}

func (t *partitionTombstone) Remove(ctx context.Context) error {
	return t.meta.RemovePartition(ctx, t.collectionID, t.partitionID, 0)
}
