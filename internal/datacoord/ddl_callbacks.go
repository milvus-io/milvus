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

package datacoord

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// RegisterDDLCallbacks registers the ddl callbacks.
func RegisterDDLCallbacks(s *Server) {
	ddlCallback := &DDLCallbacks{
		Server: s,
	}
	ddlCallback.registerIndexCallbacks()
	registry.RegisterFlushAllV2AckCallback(ddlCallback.flushAllV2AckCallback)
	ddlCallback.registerSnapshotCallbacks()
	ddlCallback.registerExternalCollectionCallbacks()
}

type DDLCallbacks struct {
	*Server
}

func (c *DDLCallbacks) registerIndexCallbacks() {
	registry.RegisterCreateIndexV2AckCallback(c.createIndexV2AckCallback)
	registry.RegisterAlterIndexV2AckCallback(c.alterIndexV2AckCallback)
	registry.RegisterDropIndexV2AckCallback(c.dropIndexV2Callback)
}

func (c *DDLCallbacks) registerSnapshotCallbacks() {
	registry.RegisterCreateSnapshotV2AckCallback(c.createSnapshotV2AckCallback)
	registry.RegisterDropSnapshotV2AckCallback(c.dropSnapshotV2AckCallback)
	registry.RegisterRestoreSnapshotV2AckCallback(c.restoreSnapshotV2AckCallback)
}

func (c *DDLCallbacks) registerExternalCollectionCallbacks() {
	registry.RegisterRefreshExternalCollectionV2AckCallback(c.refreshExternalCollectionV2AckCallback)
}

// startBroadcastWithCollectionID starts a broadcast with collection name.
func (s *Server) startBroadcastWithCollectionID(ctx context.Context, collectionID int64) (broadcaster.BroadcastAPI, error) {
	coll, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	dbName := coll.GetDbName()
	collectionName := coll.GetCollectionName()
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewSharedDBNameResourceKey(dbName), message.NewExclusiveCollectionNameResourceKey(dbName, collectionName))
	if err != nil {
		return nil, err
	}
	return broadcaster, nil
}

// startBroadcastForRestoreSnapshot starts a broadcast for restore snapshot operations.
// Unlike startBroadcastRestoreSnapshot, this function does NOT validate resources -
// it only creates the broadcaster with appropriate resource keys (collection + snapshot).
// Use this when you need a broadcaster before all resources are created (e.g., for index restoration).
func (s *Server) startBroadcastForRestoreSnapshot(ctx context.Context, collectionID int64, snapshotName string) (broadcaster.BroadcastAPI, error) {
	coll, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return nil, fmt.Errorf("collection %d does not exist: %w", collectionID, err)
	}
	dbName := coll.GetDbName()
	collectionName := coll.GetCollectionName()

	b, err := broadcast.StartBroadcastWithResourceKeys(
		ctx,
		message.NewSharedDBNameResourceKey(dbName),
		message.NewExclusiveCollectionNameResourceKey(dbName, collectionName),
		message.NewExclusiveSnapshotNameResourceKey(snapshotName),
	)
	if err != nil {
		return nil, err
	}

	log.Ctx(ctx).Info("broadcast started for restore snapshot",
		zap.Int64("collectionID", collectionID),
		zap.String("snapshotName", snapshotName))
	return b, nil
}

// validateRestoreSnapshotResources validates that all required resources exist for restore.
// This includes collection, partitions, and indexes.
func (s *Server) validateRestoreSnapshotResources(ctx context.Context, collectionID int64, snapshotData *SnapshotData) error {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))

	// Validate partitions exist
	partitionsResp, err := s.broker.ShowPartitions(ctx, collectionID)
	if err != nil {
		return fmt.Errorf("failed to get partitions for collection %d: %w", collectionID, err)
	}

	existingPartitions := make(map[string]bool)
	for _, name := range partitionsResp.GetPartitionNames() {
		existingPartitions[name] = true
	}

	for partName := range snapshotData.Collection.GetPartitions() {
		if !existingPartitions[partName] {
			return fmt.Errorf("partition %s does not exist in collection %d", partName, collectionID)
		}
	}
	log.Info("partitions validated", zap.Int("count", len(existingPartitions)))

	return nil
}

// startBroadcastRestoreSnapshot starts a broadcast for restore snapshot.
// It validates that all previously created resources (collection, partitions, indexes)
// exist before starting the broadcast.
// Deprecated: Use startBroadcastForRestoreSnapshot + validateRestoreSnapshotResources instead.
func (s *Server) startBroadcastRestoreSnapshot(
	ctx context.Context,
	collectionID int64,
	snapshotData *SnapshotData,
) (broadcaster.BroadcastAPI, error) {
	log := log.Ctx(ctx).With(zap.Int64("collectionID", collectionID))

	// ========== Validate Collection Exists ==========
	coll, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return nil, fmt.Errorf("collection %d does not exist: %w", collectionID, err)
	}
	dbName := coll.GetDbName()
	collectionName := coll.GetCollectionName()
	log.Info("collection validated",
		zap.String("dbName", dbName),
		zap.String("collectionName", collectionName))

	// ========== Validate Partitions Exist ==========
	partitionsResp, err := s.broker.ShowPartitions(ctx, collectionID)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions for collection %d: %w", collectionID, err)
	}

	// Build set of existing partition names
	existingPartitions := make(map[string]bool)
	for _, name := range partitionsResp.GetPartitionNames() {
		existingPartitions[name] = true
	}

	// Check all snapshot partitions exist
	for partName := range snapshotData.Collection.GetPartitions() {
		if !existingPartitions[partName] {
			return nil, fmt.Errorf("partition %s does not exist in collection %d",
				partName, collectionID)
		}
	}
	log.Info("partitions validated", zap.Int("count", len(existingPartitions)))

	// ========== Validate Indexes Exist ==========
	for _, indexInfo := range snapshotData.Indexes {
		// Check if index exists for this field
		indexes := s.meta.indexMeta.GetIndexesForCollection(collectionID, "")

		indexFound := false
		for _, idx := range indexes {
			if idx.FieldID == indexInfo.GetFieldID() && idx.IndexName == indexInfo.GetIndexName() {
				indexFound = true
				break
			}
		}
		if !indexFound {
			return nil, fmt.Errorf("index %s for field %d does not exist in collection %d",
				indexInfo.GetIndexName(), indexInfo.GetFieldID(), collectionID)
		}
	}
	log.Info("indexes validated", zap.Int("count", len(snapshotData.Indexes)))

	// ========== Start Broadcast ==========
	b, err := broadcast.StartBroadcastWithResourceKeys(
		ctx,
		message.NewSharedDBNameResourceKey(dbName),
		message.NewExclusiveCollectionNameResourceKey(dbName, collectionName),
		message.NewExclusiveSnapshotNameResourceKey(snapshotData.SnapshotInfo.GetName()),
	)
	if err != nil {
		return nil, err
	}

	log.Info("broadcast started for restore snapshot")
	return b, nil
}
