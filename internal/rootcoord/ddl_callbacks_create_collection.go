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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastCreateCollectionV1(ctx context.Context, req *milvuspb.CreateCollectionRequest) error {
	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(req.GetSchema(), schema); err != nil {
		return err
	}
	if req.GetShardsNum() <= 0 {
		req.ShardsNum = common.DefaultShardsNum
	}
	if _, err := typeutil.GetPartitionKeyFieldSchema(schema); err == nil {
		if req.GetNumPartitions() <= 0 {
			req.NumPartitions = common.DefaultPartitionsWithPartitionKey
		}
	} else {
		// we only support to create one partition when partition key is not enabled.
		req.NumPartitions = int64(1)
	}

	broadcaster, err := c.startBroadcastWithCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// prepare and validate the create collection message.
	createCollectionTask := createCollectionTask{
		Core:   c,
		Req:    req,
		header: &message.CreateCollectionMessageHeader{},
		body: &message.CreateCollectionRequest{
			DbName:           req.GetDbName(),
			CollectionName:   req.GetCollectionName(),
			CollectionSchema: schema,
		},
	}
	if err := createCollectionTask.Prepare(ctx); err != nil {
		return err
	}

	// setup the broadcast virtual channels and control channel, then make a broadcast message.
	broadcastChannel := make([]string, 0, createCollectionTask.Req.ShardsNum+1)
	broadcastChannel = append(broadcastChannel, streaming.WAL().ControlChannel())
	for i := 0; i < int(createCollectionTask.Req.ShardsNum); i++ {
		broadcastChannel = append(broadcastChannel, createCollectionTask.body.VirtualChannelNames[i])
	}
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(createCollectionTask.header).
		WithBody(createCollectionTask.body).
		WithBroadcast(broadcastChannel,
			message.NewSharedDBNameResourceKey(createCollectionTask.body.DbName),
			message.NewExclusiveCollectionNameResourceKey(createCollectionTask.body.DbName, createCollectionTask.body.CollectionName),
		).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

func (c *DDLCallback) createCollectionV1AckCallback(ctx context.Context, result message.BroadcastResultCreateCollectionMessageV1) error {
	msg := result.Message
	header := msg.Header()
	body := msg.MustBody()
	for vchannel, result := range result.Results {
		if !funcutil.IsControlChannel(vchannel) {
			// create shard info when virtual channel is created.
			if err := c.createCollectionShard(ctx, header, body, vchannel, result); err != nil {
				return errors.Wrap(err, "failed to create collection shard")
			}
		}
	}
	newCollInfo := newCollectionModelWithMessage(header, body, result)
	if err := c.meta.AddCollection(ctx, newCollInfo); err != nil {
		return errors.Wrap(err, "failed to add collection to meta table")
	}
	return c.ExpireCaches(ctx, ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
		ce.OptLPCMDBName(body.DbName),
		ce.OptLPCMCollectionName(body.CollectionName),
		ce.OptLPCMCollectionID(header.CollectionId),
		ce.OptLPCMMsgType(commonpb.MsgType_DropCollection)),
		newCollInfo.UpdateTimestamp,
	)
}

func (c *DDLCallback) createCollectionShard(ctx context.Context, header *message.CreateCollectionMessageHeader, body *message.CreateCollectionRequest, vchannel string, appendResult *message.AppendResult) error {
	// TODO: redundant channel watch by now, remove it in future.
	startPosition := adaptor.MustGetMQWrapperIDFromMessage(appendResult.MessageID).Serialize()
	// semantically, we should use the last confirmed message id to setup the start position.
	// same as following `newCollectionModelWithMessage`.
	resp, err := c.mixCoord.WatchChannels(ctx, &datapb.WatchChannelsRequest{
		CollectionID:    header.CollectionId,
		ChannelNames:    []string{vchannel},
		StartPositions:  []*commonpb.KeyDataPair{{Key: funcutil.ToPhysicalChannel(vchannel), Data: startPosition}},
		Schema:          body.CollectionSchema,
		CreateTimestamp: appendResult.TimeTick,
	})
	return merr.CheckRPCCall(resp.GetStatus(), err)
}

// newCollectionModelWithMessage creates a collection model with the given message.
func newCollectionModelWithMessage(header *message.CreateCollectionMessageHeader, body *message.CreateCollectionRequest, result message.BroadcastResultCreateCollectionMessageV1) *model.Collection {
	timetick := result.GetControlChannelResult().TimeTick

	// Setup the start position for the vchannels
	newCollInfo := newCollectionModel(header, body, timetick)
	startPosition := make(map[string][]byte, len(body.PhysicalChannelNames))
	for vchannel, appendResult := range result.Results {
		if funcutil.IsControlChannel(vchannel) {
			// use control channel timetick to setup the create time and update timestamp
			newCollInfo.CreateTime = appendResult.TimeTick
			newCollInfo.UpdateTimestamp = appendResult.TimeTick
			for _, partition := range newCollInfo.Partitions {
				partition.PartitionCreatedTimestamp = appendResult.TimeTick
			}
			continue
		}
		startPosition[funcutil.ToPhysicalChannel(vchannel)] = adaptor.MustGetMQWrapperIDFromMessage(appendResult.MessageID).Serialize()
		// semantically, we should use the last confirmed message id to setup the start position, like following:
		//   startPosition := adaptor.MustGetMQWrapperIDFromMessage(appendResult.LastConfirmedMessageID).Serialize()
		// but currently, the zero message id will be serialized to nil if using woodpecker,
		// some code assertions will panic if the start position is nil.
		// so we use the message id here, because the vchannel is created by CreateCollectionMessage,
		// so the message id will promise to consume all message in the vchannel like LastConfirmedMessageID.
	}
	newCollInfo.StartPositions = toKeyDataPairs(startPosition)
	return newCollInfo
}

// newCollectionModel creates a collection model with the given header, body and timestamp.
func newCollectionModel(header *message.CreateCollectionMessageHeader, body *message.CreateCollectionRequest, ts uint64) *model.Collection {
	partitions := make([]*model.Partition, 0, len(body.PartitionIDs))
	for idx, partition := range body.PartitionIDs {
		partitions = append(partitions, &model.Partition{
			PartitionID:               partition,
			PartitionName:             body.PartitionNames[idx],
			PartitionCreatedTimestamp: ts,
			CollectionID:              header.CollectionId,
			State:                     etcdpb.PartitionState_PartitionCreated,
		})
	}
	consistencyLevel, properties := mustConsumeConsistencyLevel(body.CollectionSchema.Properties)
	return &model.Collection{
		CollectionID:         header.CollectionId,
		DBID:                 header.DbId,
		Name:                 body.CollectionSchema.Name,
		DBName:               body.DbName,
		Description:          body.CollectionSchema.Description,
		AutoID:               body.CollectionSchema.AutoID,
		Fields:               model.UnmarshalFieldModels(body.CollectionSchema.Fields),
		StructArrayFields:    model.UnmarshalStructArrayFieldModels(body.CollectionSchema.StructArrayFields),
		Functions:            model.UnmarshalFunctionModels(body.CollectionSchema.Functions),
		VirtualChannelNames:  body.VirtualChannelNames,
		PhysicalChannelNames: body.PhysicalChannelNames,
		ShardsNum:            int32(len(body.VirtualChannelNames)),
		ConsistencyLevel:     consistencyLevel,
		CreateTime:           ts,
		State:                etcdpb.CollectionState_CollectionCreated,
		Partitions:           partitions,
		Properties:           properties,
		EnableDynamicField:   body.CollectionSchema.EnableDynamicField,
		UpdateTimestamp:      ts,
	}
}

// mustConsumeConsistencyLevel consumes the consistency level from the properties and returns the new properties.
// it panics if the consistency level is not found in the properties, because the consistency level is required.
func mustConsumeConsistencyLevel(properties []*commonpb.KeyValuePair) (commonpb.ConsistencyLevel, []*commonpb.KeyValuePair) {
	ok, consistencyLevel := getConsistencyLevel(properties...)
	if !ok {
		panic(fmt.Errorf("consistency level not found in properties"))
	}
	newProperties := make([]*commonpb.KeyValuePair, 0, len(properties)-1)
	for _, property := range properties {
		if property.Key == common.ConsistencyLevel {
			continue
		}
		newProperties = append(newProperties, property)
	}
	return consistencyLevel, newProperties
}
