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
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// broadcastRestoreSnapshotV2 broadcasts a restore snapshot message via the DDL framework.
// This method validates, allocates job ID, and broadcasts the message.
// All actual restore logic (ID allocation, metadata creation, data restore) is in the callback.
// Returns the allocated job ID for async tracking.
func (c *Core) broadcastRestoreSnapshotV2(ctx context.Context, req *milvuspb.RestoreSnapshotRequest) (int64, error) {
	logger := log.Ctx(ctx).With(
		zap.String("snapshotName", req.GetName()),
		zap.String("collectionName", req.GetCollectionName()),
		zap.String("dbName", req.GetDbName()),
	)
	logger.Info("broadcast restore snapshot via DDL framework")

	// Step 1: Validate snapshot exists
	descResp, err := c.mixCoord.DescribeSnapshot(ctx, &datapb.DescribeSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeSnapshot),
		),
		Name:                  req.GetName(),
		IncludeCollectionInfo: false, // Only check existence
	})
	if err = merr.CheckRPCCall(descResp, err); err != nil {
		return 0, errors.Wrap(err, "snapshot not found")
	}

	// Step 2: Validate collection does not exist and name doesn't conflict with alias
	if err := c.validateCollectionNotExists(ctx, req.GetDbName(), req.GetCollectionName()); err != nil {
		return 0, err
	}

	// Step 3: Allocate job ID for async restore tracking
	jobID, err := c.idAllocator.AllocOne()
	if err != nil {
		return 0, errors.Wrap(err, "failed to allocate job ID")
	}
	logger = logger.With(zap.Int64("jobID", jobID))

	// Step 4: Acquire distributed lock via broadcast
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedClusterResourceKey(),
		message.NewExclusiveCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
		message.NewSharedSnapshotNameResourceKey(req.GetName()),
	)
	if err != nil {
		return 0, err
	}
	defer broadcaster.Close()

	// Step 5: Build and broadcast message
	msg := message.NewRestoreSnapshotMessageBuilderV2().
		WithHeader(&message.RestoreSnapshotMessageHeader{
			DbName:         req.GetDbName(),
			CollectionName: req.GetCollectionName(),
			SnapshotName:   req.GetName(),
			JobId:          jobID,
		}).
		WithBody(&message.RestoreSnapshotMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()},
			message.NewSharedDBNameResourceKey(req.GetDbName()),
			message.NewExclusiveCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
			message.NewSharedSnapshotNameResourceKey(req.GetName()),
		).
		MustBuildBroadcast()

	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return 0, err
	}

	logger.Info("restore snapshot broadcast completed")
	return jobID, nil
}

// validateCollectionNotExists checks that the collection does not exist and name doesn't conflict with alias.
func (c *Core) validateCollectionNotExists(ctx context.Context, dbName, collectionName string) error {
	// Check if the collection name duplicates an alias
	if _, err := c.meta.DescribeAlias(ctx, dbName, collectionName, typeutil.MaxTimestamp); err == nil {
		return fmt.Errorf("collection name [%s] conflicts with an existing alias", collectionName)
	}

	// Check if the collection already exists
	if _, err := c.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp); err == nil {
		return fmt.Errorf("collection %s already exists", collectionName)
	}

	return nil
}

// restoreSnapshotV2AckCallback is the callback function for restore snapshot message.
// It reuses createCollectionTask.Prepare() for collection creation logic.
func (c *DDLCallback) restoreSnapshotV2AckCallback(ctx context.Context, result message.BroadcastResultRestoreSnapshotMessageV2) error {
	header := result.Message.Header()

	logger := log.Ctx(ctx).With(
		zap.String("snapshotName", header.SnapshotName),
		zap.String("collectionName", header.CollectionName),
		zap.String("dbName", header.DbName),
		zap.Int64("jobID", header.JobId),
	)
	logger.Info("restore snapshot callback started")

	// Step 1: Get snapshot detail info from DataCoord
	descResp, err := c.mixCoord.DescribeSnapshot(ctx, &datapb.DescribeSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeSnapshot),
		),
		Name:                  header.SnapshotName,
		IncludeCollectionInfo: true,
	})
	if err = merr.CheckRPCCall(descResp, err); err != nil {
		return errors.Wrap(err, "failed to describe snapshot")
	}

	collInfo := descResp.GetCollectionInfo()
	collInfo.Schema.Name = header.CollectionName

	// Step 2: Create collection and partitions
	collID, dbID, broadcastChannel, err := c.createCollectionAndPartitions(ctx, header, collInfo, result)
	if err != nil {
		return err
	}
	logger = logger.With(zap.Int64("collectionID", collID))

	// Step 3: Create indexes
	if err := c.createIndexes(ctx, dbID, collID, descResp.GetIndexInfos(), result, broadcastChannel); err != nil {
		return err
	}

	// Step 4: Restore data
	restoreResp, err := c.mixCoord.RestoreSnapshotData(ctx, &datapb.RestoreSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_RestoreSnapshot),
		),
		Name:         header.SnapshotName,
		CollectionId: collID,
		JobId:        header.JobId,
	})
	if err := merr.CheckRPCCall(restoreResp, err); err != nil {
		logger.Warn("data restore returned error, please check job status",
			zap.String("reason", restoreResp.GetStatus().GetReason()))
	} else {
		logger.Info("data restore triggered successfully")
	}

	// Step 5: Expire caches
	return c.ExpireCaches(ctx, ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
		ce.OptLPCMDBName(header.DbName),
		ce.OptLPCMCollectionName(header.CollectionName),
		ce.OptLPCMCollectionID(collID),
		ce.OptLPCMMsgType(commonpb.MsgType_CreateCollection)))
}

// createCollectionAndPartitions creates the collection and user partitions from snapshot info.
// It reuses createCollectionTask.Prepare() to allocate IDs and vchannels, then triggers the
// createCollectionV1AckCallback via registry.CallMessageAckCallback.
//
// Important: Since RestoreSnapshot message is only broadcast to the control channel (vchannels
// are not known until createCollectionTask.Prepare() allocates them), result.Results only contains
// the control channel's AppendResult. To ensure the collection has valid StartPositions for all
// vchannels (required for QueryNode to properly Seek when loading), we construct appendResults
// with entries for all vchannels using the control channel's MessageID as the initial position.
// This is safe because for a newly created collection, any valid MessageID can serve as the
// starting point since no data exists yet on these vchannels.
func (c *DDLCallback) createCollectionAndPartitions(
	ctx context.Context,
	header *message.RestoreSnapshotMessageHeader,
	collInfo *datapb.CollectionDescription,
	result message.BroadcastResultRestoreSnapshotMessageV2,
) (collID int64, dbID int64, broadcastChannel []string, err error) {
	logger := log.Ctx(ctx).With(
		zap.String("dbName", header.DbName),
		zap.String("collectionName", header.CollectionName),
		zap.String("snapshotName", header.SnapshotName))
	logger.Info("creating collection and partitions from snapshot",
		zap.Int32("numShards", int32(collInfo.GetNumShards())),
		zap.Int64("numPartitions", collInfo.GetNumPartitions()),
		zap.Int("userCreatedPartitions", len(collInfo.GetUserCreatedPartitions())))

	// Build CreateCollectionRequest from snapshot info
	schemaBytes, err := proto.Marshal(collInfo.Schema)
	if err != nil {
		return 0, 0, nil, errors.Wrap(err, "failed to marshal schema")
	}

	createReq := &milvuspb.CreateCollectionRequest{
		DbName:         header.DbName,
		CollectionName: header.CollectionName,
		Schema:         schemaBytes,
		ShardsNum:      int32(collInfo.GetNumShards()),
		NumPartitions:  collInfo.GetNumPartitions() - int64(len(collInfo.GetUserCreatedPartitions())),
	}

	// Reuse createCollectionTask.Prepare() for all collection creation logic:
	// - Allocates collection ID, partition IDs
	// - Allocates virtual channels via streaming node manager
	// - Validates schema and other constraints
	createTask := &createCollectionTask{
		Core:   c.Core,
		Req:    createReq,
		header: &message.CreateCollectionMessageHeader{},
		body: &message.CreateCollectionRequest{
			DbName:           header.DbName,
			CollectionName:   header.CollectionName,
			CollectionSchema: collInfo.Schema,
		},
		preserveFieldID: true,
	}

	if err := createTask.Prepare(ctx); err != nil {
		logger.Error("failed to prepare create collection task", zap.Error(err))
		return 0, 0, nil, errors.Wrap(err, "failed to prepare create collection task")
	}

	// Set up the broadcast channels (control channel + all virtual channels).
	// These channels are where the CreateCollectionMessage would normally be broadcast.
	broadcastChannel = make([]string, 0, createTask.Req.ShardsNum+1)
	broadcastChannel = append(broadcastChannel, streaming.WAL().ControlChannel())
	for i := 0; i < int(createTask.Req.ShardsNum); i++ {
		broadcastChannel = append(broadcastChannel, createTask.body.VirtualChannelNames[i])
	}

	// Construct appendResults for all channels using the control channel's result.
	// Background: In normal CreateCollection flow, the message is broadcast to all vchannels,
	// and each vchannel returns its own AppendResult with a unique MessageID. These MessageIDs
	// are used as StartPositions for the collection.
	//
	// In RestoreSnapshot flow, we can't broadcast to vchannels upfront because they don't exist
	// until createCollectionTask.Prepare() allocates them (chicken-and-egg problem).
	// So we use the control channel's MessageID as StartPosition for all vchannels.
	// This ensures collection.StartPositions is not empty, allowing QueryNode's dispatcher
	// to properly call Seek() when loading the collection.
	appendResults := make(map[string]*message.AppendResult)
	for _, vchannel := range broadcastChannel {
		appendResults[vchannel] = &message.AppendResult{
			MessageID:              result.GetControlChannelResult().MessageID,
			LastConfirmedMessageID: result.GetControlChannelResult().LastConfirmedMessageID,
			TimeTick:               result.GetControlChannelResult().TimeTick,
		}
	}

	collID = createTask.header.CollectionId
	dbID = createTask.header.DbId
	broadcastID := result.Message.BroadcastHeader().BroadcastID

	logger = logger.With(zap.Int64("collectionID", collID), zap.Int64("dbID", dbID))
	logger.Info("collection task prepared, broadcasting create collection message",
		zap.Int("numChannels", len(broadcastChannel)))

	// Create collection
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(createTask.header).
		WithBody(createTask.body).
		WithBroadcast(broadcastChannel).
		MustBuildBroadcast().
		WithBroadcastID(broadcastID)

	if err := registry.CallMessageAckCallback(ctx, msg, appendResults); err != nil {
		logger.Error("failed to broadcast create collection message", zap.Error(err))
		return 0, 0, nil, errors.Wrap(err, "failed to broadcast create collection message")
	}
	logger.Info("collection created successfully")

	// Create user partitions
	userPartitions := collInfo.GetUserCreatedPartitions()
	if len(userPartitions) > 0 {
		logger.Info("creating user partitions", zap.Int("count", len(userPartitions)))
	}
	for _, partitionName := range userPartitions {
		partID, err := c.idAllocator.AllocOne()
		if err != nil {
			logger.Error("failed to allocate partition ID", zap.String("partitionName", partitionName), zap.Error(err))
			return 0, 0, nil, errors.Wrap(err, "failed to allocate partition ID")
		}

		logger.Info("creating partition", zap.String("partitionName", partitionName), zap.Int64("partitionID", partID))

		partMsg := message.NewCreatePartitionMessageBuilderV1().
			WithHeader(&message.CreatePartitionMessageHeader{
				CollectionId: collID,
				PartitionId:  partID,
			}).
			WithBody(&message.CreatePartitionRequest{
				Base:           commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_CreatePartition)),
				DbName:         header.DbName,
				CollectionName: header.CollectionName,
				PartitionName:  partitionName,
				DbID:           dbID,
				CollectionID:   collID,
				PartitionID:    partID,
			}).
			WithBroadcast(broadcastChannel).
			MustBuildBroadcast().
			WithBroadcastID(broadcastID)

		if err := registry.CallMessageAckCallback(ctx, partMsg, appendResults); err != nil {
			logger.Error("failed to create partition", zap.String("partitionName", partitionName), zap.Error(err))
			return 0, 0, nil, errors.Wrap(err, "failed to broadcast create partition message")
		}
		logger.Info("partition created successfully", zap.String("partitionName", partitionName), zap.Int64("partitionID", partID))
	}

	logger.Info("collection and partitions created successfully")
	return collID, dbID, broadcastChannel, nil
}

// createIndexes restores indexes from snapshot info.
func (c *DDLCallback) createIndexes(
	ctx context.Context,
	dbID, collID int64,
	indexInfos []*indexpb.IndexInfo,
	result message.BroadcastResultRestoreSnapshotMessageV2,
	broadcastChannel []string,
) error {
	logger := log.Ctx(ctx).With(zap.Int64("collectionID", collID), zap.Int64("dbID", dbID))
	logger.Info("restoring indexes", zap.Int("indexCount", len(indexInfos)))

	broadcastID := result.Message.BroadcastHeader().BroadcastID

	for _, indexInfo := range indexInfos {
		// Update CollectionID to the new restored collection ID
		indexInfo.CollectionID = collID

		logger.Info("creating index",
			zap.Int64("indexID", indexInfo.GetIndexID()),
			zap.String("indexName", indexInfo.GetIndexName()),
			zap.Int64("fieldID", indexInfo.GetFieldID()))

		createIndexMsg := message.NewCreateIndexMessageBuilderV2().
			WithHeader(&message.CreateIndexMessageHeader{
				DbId:         dbID,
				CollectionId: collID,
				FieldId:      indexInfo.GetFieldID(),
				IndexId:      indexInfo.GetIndexID(),
				IndexName:    indexInfo.GetIndexName(),
			}).
			WithBody(&message.CreateIndexMessageBody{
				FieldIndex: &indexpb.FieldIndex{
					IndexInfo:  indexInfo,
					Deleted:    false,
					CreateTime: uint64(time.Now().UnixNano()),
				},
			}).
			WithBroadcast([]string{streaming.WAL().ControlChannel()}).
			MustBuildBroadcast().
			WithBroadcastID(broadcastID)

		if err := registry.CallMessageAckCallback(ctx, createIndexMsg, result.Results); err != nil {
			logger.Error("failed to create index",
				zap.Int64("indexID", indexInfo.GetIndexID()),
				zap.String("indexName", indexInfo.GetIndexName()),
				zap.Error(err))
			return errors.Wrap(err, "failed to broadcast create index message")
		}

		logger.Info("index created successfully",
			zap.Int64("indexID", indexInfo.GetIndexID()),
			zap.String("indexName", indexInfo.GetIndexName()))
	}

	logger.Info("all indexes restored successfully")
	return nil
}
