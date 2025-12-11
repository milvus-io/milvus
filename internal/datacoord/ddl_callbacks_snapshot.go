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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// createSnapshotV2AckCallback handles the callback for CreateSnapshot DDL message.
// ID allocation happens inside SnapshotManager.CreateSnapshot.
func (s *DDLCallbacks) createSnapshotV2AckCallback(ctx context.Context, result message.BroadcastResultCreateSnapshotMessageV2) error {
	header := result.Message.Header()
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", header.CollectionId),
		zap.String("snapshotName", header.Name),
	)
	log.Info("createSnapshotV2AckCallback received")

	// Create snapshot - ID is allocated inside CreateSnapshot
	snapshotID, err := s.snapshotManager.CreateSnapshot(ctx, header.CollectionId, header.Name, header.Description)
	if err != nil {
		log.Error("failed to create snapshot via DDL callback", zap.Error(err))
		return err
	}

	log.Info("snapshot created successfully via DDL callback", zap.Int64("snapshotID", snapshotID))
	return nil
}

// dropSnapshotV2AckCallback handles the callback for DropSnapshot DDL message.
func (s *DDLCallbacks) dropSnapshotV2AckCallback(ctx context.Context, result message.BroadcastResultDropSnapshotMessageV2) error {
	header := result.Message.Header()
	log := log.Ctx(ctx).With(zap.String("snapshotName", header.Name))
	log.Info("dropSnapshotV2AckCallback received")

	// Delete snapshot using SnapshotManager interface (idempotent)
	if err := s.snapshotManager.DropSnapshot(ctx, header.Name); err != nil {
		log.Error("failed to drop snapshot via DDL callback", zap.Error(err))
		return err
	}

	log.Info("snapshot dropped successfully via DDL callback")
	return nil
}
