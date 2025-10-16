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
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastCreateCollectionV1(ctx context.Context, req *milvuspb.CreateCollectionRequest) error {
	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(req.GetSchema(), schema); err != nil {
		return err
	}
	if req.GetShardsNum() == 0 {
		req.ShardsNum = common.DefaultShardsNum
	}
	if _, err := typeutil.GetPartitionKeyFieldSchema(schema); err == nil {
		req.NumPartitions = common.DefaultPartitionsWithPartitionKey
	}
	if req.GetNumPartitions() == 0 {
		req.NumPartitions = int64(1)
	}

	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(req.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
	)
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
		if errors.Is(err, errCreateCollectionWithSameSchema) {
			return nil
		}
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
				return err
			}
		}
	}
	newCollInfo := newCollectionModelWithMessage(header, body, result)
	if err := c.meta.AddCollection(ctx, newCollInfo); err != nil {
		return err
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
	startPosition := adaptor.MustGetMQWrapperIDFromMessage(appendResult.LastConfirmedMessageID).Serialize()
	resp, err := c.mixCoord.WatchChannels(ctx, &datapb.WatchChannelsRequest{
		CollectionID:    header.CollectionId,
		ChannelNames:    []string{vchannel},
		StartPositions:  []*commonpb.KeyDataPair{{Key: vchannel, Data: startPosition}},
		Schema:          body.CollectionSchema,
		CreateTimestamp: appendResult.TimeTick,
	})
	if err != nil {
		return err
	}
	if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		return fmt.Errorf("failed to watch channels, code: %s, reason: %s", resp.GetStatus().GetErrorCode(), resp.GetStatus().GetReason())
	}
	return nil
}

// mergeCollectionModel merges the given collection models into a single collection model.
func mergeCollectionModel(models ...*model.Collection) *model.Collection {
	if len(models) == 1 {
		return models[0]
	}
	// createTimeTick from cchannel
	createTimeTick := uint64(0)
	// startPosition from vchannel
	startPosition := make(map[string][]byte, len(models[0].PhysicalChannelNames))
	state := etcdpb.CollectionState_CollectionCreating
	for _, model := range models {
		if model.CreateTime != 0 && createTimeTick == 0 {
			createTimeTick = model.CreateTime
		}
		for key, value := range toMap(model.StartPositions) {
			if _, ok := startPosition[key]; !ok {
				startPosition[key] = value
			}
		}
		if len(startPosition) == len(models[0].PhysicalChannelNames) && createTimeTick != 0 {
			// if all vchannels and cchannel are created, the collection is created
			state = etcdpb.CollectionState_CollectionCreated
		}
	}

	mergedModel := models[0]
	mergedModel.CreateTime = createTimeTick
	for _, partition := range mergedModel.Partitions {
		partition.PartitionCreatedTimestamp = createTimeTick
	}
	mergedModel.StartPositions = toKeyDataPairs(startPosition)
	mergedModel.State = state
	return mergedModel
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
		startPosition[funcutil.ToPhysicalChannel(vchannel)] = adaptor.MustGetMQWrapperIDFromMessage(appendResult.LastConfirmedMessageID).Serialize()
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
	_, consistencyLevel := getConsistencyLevel(body.CollectionSchema.Properties...)
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
		Properties:           body.CollectionSchema.Properties,
		EnableDynamicField:   body.CollectionSchema.EnableDynamicField,
		UpdateTimestamp:      ts,
	}
}
