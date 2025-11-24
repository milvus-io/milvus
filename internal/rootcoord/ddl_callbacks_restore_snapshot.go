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
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// broadcastRestoreSnapshotV2 broadcasts a CreateCollectionMessage with snapshot info via the DDL framework.
// This method validates, allocates job ID, prepares the collection task, and broadcasts the message.
// All actual restore logic (metadata creation, index creation, data restore) is handled by
// createCollectionV1AckCallback when it detects snapshot_name in the header.
// Returns the allocated job ID for async tracking.
func (c *Core) broadcastRestoreSnapshotV2(ctx context.Context, req *milvuspb.RestoreSnapshotRequest) (int64, error) {
	logger := log.Ctx(ctx).With(
		zap.String("snapshotName", req.GetName()),
		zap.String("collectionName", req.GetCollectionName()),
		zap.String("dbName", req.GetDbName()),
	)
	logger.Info("broadcast restore snapshot via DDL framework")

	// Step 1: Validate snapshot exists and get full snapshot info
	descResp, err := c.mixCoord.DescribeSnapshot(ctx, &datapb.DescribeSnapshotRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_DescribeSnapshot),
		),
		Name:                  req.GetName(),
		IncludeCollectionInfo: true, // Need full info to build CreateCollectionMessage
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

	// Step 5: Build CreateCollectionMessage from snapshot info
	collInfo := descResp.GetCollectionInfo()
	collInfo.Schema.Name = req.GetCollectionName()

	schemaBytes, err := proto.Marshal(collInfo.Schema)
	if err != nil {
		return 0, errors.Wrap(err, "failed to marshal schema")
	}

	createReq := &milvuspb.CreateCollectionRequest{
		DbName:         req.GetDbName(),
		CollectionName: req.GetCollectionName(),
		Schema:         schemaBytes,
		ShardsNum:      int32(collInfo.GetNumShards()),
		NumPartitions:  collInfo.GetNumPartitions() - int64(len(collInfo.GetUserCreatedPartitions())),
	}

	// Extract pchannels from snapshot vchannels to preserve pchannel mapping
	var preferredPChannels []string
	if len(collInfo.GetVirtualChannelNames()) > 0 {
		for _, vchannel := range collInfo.GetVirtualChannelNames() {
			pchannel := funcutil.ToPhysicalChannel(vchannel)
			preferredPChannels = append(preferredPChannels, pchannel)
		}
	}

	// Reuse createCollectionTask.Prepare() for all collection creation logic:
	// - Allocates collection ID, partition IDs
	// - Allocates virtual channels via streaming node manager
	// - Validates schema and other constraints
	createTask := &createCollectionTask{
		Core:   c,
		Req:    createReq,
		header: &message.CreateCollectionMessageHeader{},
		body: &message.CreateCollectionRequest{
			DbName:           req.GetDbName(),
			CollectionName:   req.GetCollectionName(),
			CollectionSchema: collInfo.Schema,
		},
		preserveFieldID:    true, // Preserve field IDs from snapshot
		preferredPChannels: preferredPChannels,
	}

	if err := createTask.Prepare(ctx); err != nil {
		logger.Error("failed to prepare create collection task", zap.Error(err))
		return 0, errors.Wrap(err, "failed to prepare create collection task")
	}

	// Step 6: Set snapshot info in header so createCollectionV1AckCallback can handle
	// partition creation, index creation, and data restore
	createTask.header.SnapshotName = req.GetName()
	createTask.header.JobId = jobID
	// Mark as restore operation and pass metadata directly to avoid second DescribeSnapshot RPC in callback
	createTask.header.RestoreFromSnapshot = true
	createTask.header.UserCreatedPartitions = collInfo.GetUserCreatedPartitions()
	createTask.header.IndexInfos = descResp.GetIndexInfos()

	// Step 7: Set up the broadcast channels (control channel + all virtual channels)
	broadcastChannel := make([]string, 0, createTask.Req.ShardsNum+1)
	broadcastChannel = append(broadcastChannel, streaming.WAL().ControlChannel())
	for i := 0; i < int(createTask.Req.ShardsNum); i++ {
		broadcastChannel = append(broadcastChannel, createTask.body.VirtualChannelNames[i])
	}

	logger = logger.With(
		zap.Int64("collectionID", createTask.header.CollectionId),
		zap.Int64("dbID", createTask.header.DbId),
	)
	logger.Info("collection task prepared, broadcasting create collection message with snapshot info",
		zap.Int("numChannels", len(broadcastChannel)),
		zap.Int32("numShards", int32(collInfo.GetNumShards())),
		zap.Int64("numPartitions", collInfo.GetNumPartitions()),
		zap.Int("userCreatedPartitions", len(collInfo.GetUserCreatedPartitions())))

	// Step 8: Build and broadcast CreateCollectionMessage
	// The DDL framework will automatically call createCollectionV1AckCallback,
	// which will detect header.SnapshotName and handle the full restore logic
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(createTask.header).
		WithBody(createTask.body).
		WithBroadcast(broadcastChannel,
			message.NewSharedDBNameResourceKey(req.GetDbName()),
			message.NewExclusiveCollectionNameResourceKey(req.GetDbName(), req.GetCollectionName()),
			message.NewSharedSnapshotNameResourceKey(req.GetName()),
		).
		MustBuildBroadcast()

	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return 0, err
	}

	logger.Info("restore snapshot broadcast completed - createCollectionV1AckCallback will handle the rest")
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
