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
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastCreatePartition(ctx context.Context, in *milvuspb.CreatePartitionRequest) error {
	broadcaster, err := c.startBroadcastWithAliasOrCollectionLock(ctx, in.GetDbName(), in.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	collMeta, err := c.meta.GetCollectionByName(ctx, in.GetDbName(), in.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}
	if err := checkGeneralCapacity(ctx, 0, 1, 0, c); err != nil {
		return err
	}
	// idempotency check here.
	for _, partition := range collMeta.Partitions {
		if partition.PartitionName == in.GetPartitionName() {
			return errIgnoerdCreatePartition
		}
	}
	cfgMaxPartitionNum := Params.RootCoordCfg.MaxPartitionNum.GetAsInt()
	if len(collMeta.Partitions) >= cfgMaxPartitionNum {
		return fmt.Errorf("partition number (%d) exceeds max configuration (%d), collection: %s",
			len(collMeta.Partitions), cfgMaxPartitionNum, collMeta.Name)
	}

	partID, err := c.idAllocator.AllocOne()
	if err != nil {
		return errors.Wrap(err, "failed to allocate partition ID")
	}

	channels := make([]string, 0, collMeta.ShardsNum+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	for i := 0; i < int(collMeta.ShardsNum); i++ {
		channels = append(channels, collMeta.VirtualChannelNames[i])
	}
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
	return err
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
		return errors.Wrap(err, "failed to add partition meta")
	}
	return c.ExpireCaches(ctx, ce.NewBuilder().
		WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(body.DbName),
			ce.OptLPCMCollectionName(body.CollectionName),
			ce.OptLPCMCollectionID(header.CollectionId),
			ce.OptLPCMPartitionName(body.PartitionName),
			ce.OptLPCMMsgType(commonpb.MsgType_CreatePartition),
		),
		result.GetControlChannelResult().TimeTick)
}
