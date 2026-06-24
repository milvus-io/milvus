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

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
	ddlCallback.registerImportCallbacks()
	ddlCallback.registerBatchUpdateManifestCallbacks()
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
	registry.RegisterDropSnapshotsByCollectionV2AckCallback(c.dropSnapshotsByCollectionV2AckCallback)
}

func (c *DDLCallbacks) registerExternalCollectionCallbacks() {
	registry.RegisterRefreshExternalCollectionV2AckCallback(c.refreshExternalCollectionV2AckCallback)
}

func (c *DDLCallbacks) registerBatchUpdateManifestCallbacks() {
	registry.RegisterBatchUpdateManifestV2AckCallback(c.batchUpdateManifestV2AckCallback)
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
// It only creates the broadcaster with appropriate resource keys (DB, collection, snapshot)
// without performing resource validation.
// Use this when you need a broadcaster before all resources are created (e.g., for index restoration).
func (s *Server) startBroadcastForRestoreSnapshot(ctx context.Context, collectionID int64, snapshotName string) (broadcaster.BroadcastAPI, error) {
	coll, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return nil, merr.Wrapf(err, "collection %d does not exist", collectionID)
	}
	dbName := coll.GetDbName()
	collectionName := coll.GetCollectionName()

	b, err := broadcast.StartBroadcastWithResourceKeys(
		ctx,
		message.NewSharedDBNameResourceKey(dbName),
		message.NewExclusiveCollectionNameResourceKey(dbName, collectionName),
		message.NewExclusiveSnapshotNameResourceKey(collectionID, snapshotName),
	)
	if err != nil {
		return nil, err
	}

	mlog.Info(ctx, "broadcast started for restore snapshot",
		mlog.FieldCollectionID(collectionID),
		mlog.String("snapshotName", snapshotName))
	return b, nil
}

// startRestoreSnapshotLock acquires the Phase 0 restore lock set for RestoreSnapshot.
//
// It holds three locks that together serialize the full restore flow against
// concurrent DropSnapshot / CreateCollection on both the source snapshot and
// the target collection name:
//
//   - Shared lock on target database
//   - Exclusive lock on target collection name (reserves the name before the
//     collection is created in Phase 2)
//   - Exclusive lock on (sourceCollectionID, snapshotName) — namespaced by
//     collection so cross-collection same-name snapshots do not contend,
//     and serializes against DropSnapshot of the same source snapshot
//
// The returned broadcaster holds the locks only; Close() releases them
// without broadcasting any message. Callers are expected to increment
// the restore reference count while the lock is held, then Close() — the
// refcount becomes the persistent guard after the lock is released.
func (s *Server) startRestoreSnapshotLock(
	ctx context.Context,
	sourceCollectionID int64,
	snapshotName, targetDbName, targetCollectionName string,
) (broadcaster.BroadcastAPI, error) {
	b, err := broadcast.StartBroadcastWithResourceKeys(
		ctx,
		message.NewSharedDBNameResourceKey(targetDbName),
		message.NewExclusiveCollectionNameResourceKey(targetDbName, targetCollectionName),
		message.NewExclusiveSnapshotNameResourceKey(sourceCollectionID, snapshotName),
	)
	if err != nil {
		return nil, err
	}

	mlog.Info(ctx, "phase 0 restore lock acquired",
		mlog.Int64("sourceCollectionID", sourceCollectionID),
		mlog.String("snapshotName", snapshotName),
		mlog.String("targetDbName", targetDbName),
		mlog.String("targetCollectionName", targetCollectionName))
	return b, nil
}

// validateRestoreSnapshotResources validates that all required resources exist for restore.
// This includes snapshot, collection, partitions, and indexes.
func (s *Server) validateRestoreSnapshotResources(ctx context.Context, collectionID int64, snapshotData *SnapshotData) error {
	log := mlog.With(mlog.FieldCollectionID(collectionID))

	// ========== Validate Snapshot Exists ==========
	// Use source collection ID from snapshot data (not the target collectionID parameter)
	// because snapshots are stored under the source collection's namespace.
	sourceCollectionID := snapshotData.SnapshotInfo.GetCollectionId()
	snapshot, err := s.meta.snapshotMeta.GetSnapshot(ctx, sourceCollectionID, snapshotData.SnapshotInfo.GetName())
	if err != nil {
		return merr.Wrapf(err, "snapshot %s does not exist for collection %d",
			snapshotData.SnapshotInfo.GetName(), sourceCollectionID)
	}
	log.Info(ctx, "snapshot validated", mlog.String("snapshotName", snapshot.GetName()))

	// ========== Validate Collection Exists ==========
	coll, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return merr.Wrapf(err, "collection %d does not exist", collectionID)
	}
	dbName := coll.GetDbName()
	collectionName := coll.GetCollectionName()
	log.Info(ctx, "collection validated",
		mlog.FieldDbName(dbName),
		mlog.FieldCollectionName(collectionName))

	// ========== Validate Partitions Exist ==========
	partitionsResp, err := s.broker.ShowPartitions(ctx, collectionID)
	if err != nil {
		return merr.Wrapf(err, "failed to get partitions for collection %d", collectionID)
	}

	// Build set of existing partition names
	existingPartitions := make(map[string]bool)
	for _, name := range partitionsResp.GetPartitionNames() {
		existingPartitions[name] = true
	}

	// Check all snapshot partitions exist
	for partName := range snapshotData.Collection.GetPartitions() {
		if !existingPartitions[partName] {
			return merr.WrapErrPartitionNotFound(partName, fmt.Sprintf("partition does not exist in collection %d", collectionID))
		}
	}
	log.Info(ctx, "partitions validated", mlog.Int("count", len(existingPartitions)))

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
			return merr.WrapErrIndexNotFound(indexInfo.GetIndexName(), fmt.Sprintf("index for field %d does not exist in collection %d", indexInfo.GetFieldID(), collectionID))
		}
	}
	log.Info(ctx, "indexes validated", mlog.Int("count", len(snapshotData.Indexes)))

	return nil
}
