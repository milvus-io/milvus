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

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// createSnapshotV2AckCallback handles the callback for CreateSnapshot DDL message.
// ID allocation happens inside SnapshotManager.CreateSnapshot.
func (s *DDLCallbacks) createSnapshotV2AckCallback(ctx context.Context, result message.BroadcastResultCreateSnapshotMessageV2) error {
	header := result.Message.Header()
	log := mlog.With(
		mlog.FieldCollectionID(header.CollectionId),
		mlog.String("snapshotName", header.Name),
	)
	log.Info(ctx, "createSnapshotV2AckCallback received")

	// Create snapshot - ID is allocated inside CreateSnapshot
	snapshotID, err := s.snapshotManager.CreateSnapshot(ctx, header.CollectionId, header.Name, header.Description, header.CompactionProtectionSeconds)
	if err != nil {
		log.Error(ctx, "failed to create snapshot via DDL callback", mlog.Err(err))
		return err
	}

	log.Info(ctx, "snapshot created successfully via DDL callback", mlog.Int64("snapshotID", snapshotID))
	return nil
}

// dropSnapshotV2AckCallback handles the callback for DropSnapshot DDL message.
//
// Precondition: the service-layer DropSnapshot acquires the (collectionID, snapshotName)
// exclusive resource key lock and does a pin check under that lock before broadcasting.
// PinSnapshotData also takes the same key (shared mode), so no concurrent Pin can add
// pins while this callback runs. Therefore snapshotManager.DropSnapshot is guaranteed
// never to return ErrSnapshotPinned on this path — the only remaining failure modes are
// transient (etcd/network), which are correctly retried by the ack scheduler loop.
func (s *DDLCallbacks) dropSnapshotV2AckCallback(ctx context.Context, result message.BroadcastResultDropSnapshotMessageV2) error {
	header := result.Message.Header()
	log := mlog.With(
		mlog.String("snapshotName", header.Name),
		mlog.FieldCollectionID(header.CollectionId),
	)
	log.Info(ctx, "dropSnapshotV2AckCallback received")

	// Delete snapshot using SnapshotManager interface (idempotent)
	if err := s.snapshotManager.DropSnapshot(ctx, header.CollectionId, header.Name); err != nil {
		log.Error(ctx, "failed to drop snapshot via DDL callback", mlog.Err(err))
		return err
	}

	log.Info(ctx, "snapshot dropped successfully via DDL callback")
	return nil
}

// dropSnapshotsByCollectionV2AckCallback handles the callback for DropSnapshotsByCollection DDL message.
// Called during drop collection cascade to clean up all snapshots of the collection.
func (s *DDLCallbacks) dropSnapshotsByCollectionV2AckCallback(ctx context.Context, result message.BroadcastResultDropSnapshotsByCollectionMessageV2) error {
	msg := result.Message
	collectionID := msg.Header().GetCollectionId()

	log := mlog.With(mlog.FieldCollectionID(collectionID))
	log.Info(ctx, "dropSnapshotsByCollectionV2AckCallback received")

	if err := s.snapshotManager.DropSnapshotsByCollection(ctx, collectionID); err != nil {
		log.Error(ctx, "failed to drop snapshots by collection in callback", mlog.Err(err))
		return err
	}

	log.Info(ctx, "dropSnapshotsByCollectionV2AckCallback completed")
	return nil
}

// restoreSnapshotV2AckCallback handles the callback for RestoreSnapshot DDL message.
// It creates copy segment jobs for data restoration.
// NOTE: RestoreIndexes is now called synchronously in services.go before broadcast.
// NOTE: jobID is pre-allocated in RestoreSnapshot and passed via WAL message for idempotency.
func (s *DDLCallbacks) restoreSnapshotV2AckCallback(ctx context.Context, result message.BroadcastResultRestoreSnapshotMessageV2) error {
	header := result.Message.Header()
	log := mlog.With(
		mlog.String("snapshotName", header.SnapshotName),
		mlog.FieldCollectionID(header.CollectionId),
		mlog.FieldJobID(header.JobId),
		mlog.Bool("external", header.External),
		mlog.String("snapshotS3Location", redactSnapshotObjectPath(header.SnapshotS3Location)),
		mlog.Bool("externalSpecSet", header.GetExternalSpec() != ""),
	)
	log.Info(ctx, "restoreSnapshotV2AckCallback received")

	// Restore data (create copy segment job)
	// Use the pre-allocated jobID from the WAL message for idempotency
	var (
		jobID int64
		err   error
	)
	if header.GetExternal() {
		jobID, err = s.snapshotManager.RestoreExternalData(
			ctx,
			header.SourceCollectionId,
			header.SnapshotName,
			header.SnapshotS3Location,
			header.CollectionId,
			header.JobId,
			header.GetExternalSpec(),
		)
	} else {
		jobID, err = s.snapshotManager.RestoreData(ctx, header.SourceCollectionId, header.SnapshotName, header.CollectionId, header.JobId, header.PinId)
	}
	if err != nil {
		log.Error(ctx, "failed to restore data", mlog.Err(err))
		return err
	}

	log.Info(ctx, "restore snapshot callback completed, job created for async execution", mlog.FieldJobID(jobID))
	return nil
}
