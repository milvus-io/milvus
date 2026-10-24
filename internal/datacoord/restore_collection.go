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
	"maps"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// RestoreCollectionManager is wired directly to RootCoord inside Coordinator.
// These operations prepare/apply the one RestoreSnapshot broadcast; none issues
// another creation broadcast or invokes a public DDL RPC.
type RestoreCollectionManager interface {
	PrepareRestoreCollection(context.Context, *milvuspb.CreateCollectionRequest, []string) (*messagespb.RestoreSnapshotMessageBody, func(), error)
	ApplyRestoreCollection(context.Context, message.BroadcastResultRestoreSnapshotMessageV2) error
	CompleteRestoreCollection(context.Context, int64) error
	CheckRestoreTargetAbsent(context.Context, string, string) error
}

func (s *Server) SetRestoreCollectionManager(manager RestoreCollectionManager) {
	s.restoreCollectionManager = manager
}

type restorePublication struct {
	starts            map[string]uint64
	createsCollection bool
}

func (sm *snapshotManager) broadcastRestoreCollection(ctx context.Context, snapshot *snapshotstorage.SnapshotData, name string, sourceID int64, target, db string, pinID int64, external bool, location, spec string) (int64, error) {
	b, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(db), message.NewExclusiveCollectionNameResourceKey(db, target))
	if err != nil {
		return 0, err
	}
	defer b.Close()
	if err := sm.collectionRestorer.CheckRestoreTargetAbsent(ctx, db, target); !errors.Is(err, merr.ErrCollectionNotFound) {
		return 0, err
	}
	schema := proto.Clone(snapshot.Collection.GetSchema()).(*schemapb.CollectionSchema)
	schema.Name, schema.DbName = target, db
	encoded, err := proto.Marshal(schema)
	if err != nil {
		return 0, err
	}
	properties := snapshot.Collection.GetProperties()
	if len(properties) == 0 {
		properties = schema.GetProperties()
	}
	body, cleanup, err := sm.collectionRestorer.PrepareRestoreCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName: db, CollectionName: target, Schema: encoded,
		ShardsNum: int32(snapshot.Collection.GetNumShards()), NumPartitions: snapshot.Collection.GetNumPartitions(),
		ConsistencyLevel: snapshot.Collection.GetConsistencyLevel(), Properties: common.CloneKeyValuePairs(properties),
	}, slices.Collect(maps.Keys(snapshot.Collection.GetPartitions())))
	if err != nil {
		return 0, err
	}
	owned := true
	defer func() {
		if owned {
			cleanup()
		}
	}()
	collectionID := body.GetCollectionHeader().GetCollectionId()
	for _, source := range snapshot.Indexes {
		id, err := sm.allocator.AllocID(ctx)
		if err != nil {
			return 0, err
		}
		index := &model.Index{
			CollectionID: collectionID, FieldID: source.GetFieldID(), IndexID: id,
			CreateTime: uint64(time.Now().UnixNano()), IndexName: source.GetIndexName(), TypeParams: DeleteParams(source.GetTypeParams(), []string{common.MmapEnabledKey}),
			IndexParams: source.GetIndexParams(), UserIndexParams: source.GetUserIndexParams(), IsAutoIndex: source.GetIsAutoIndex(),
		}
		if err := indexparamcheck.ValidateIndexParams(index); err != nil {
			return 0, err
		}
		if err := sm.checkJSONPathIndexVersion(index); err != nil {
			return 0, err
		}
		var version int32
		if sm.indexEngineVersionManager != nil {
			version = sm.indexEngineVersionManager.ResolveScalarIndexVersion()
		}
		if err := checkFMIndexEngineVersion(index.IndexParams, version); err != nil {
			return 0, err
		}
		body.Indexes = append(body.Indexes, model.MarshalIndexModel(index))
	}
	jobID, err := sm.allocator.AllocID(ctx)
	if err != nil {
		return 0, err
	}
	header := &message.RestoreSnapshotMessageHeader{
		SnapshotName: name, CollectionId: collectionID, JobId: jobID,
		SourceCollectionId: sourceID, PinId: pinID, External: external, SnapshotS3Location: location, ExternalSpec: spec,
	}
	if external {
		header.SnapshotFingerprint, err = snapshotstorage.SnapshotFingerprint(snapshot)
		if err != nil {
			return 0, err
		}
	}
	msg := message.NewRestoreSnapshotMessageBuilderV2().WithHeader(header).WithBody(body).
		WithUnreplicable().WithBroadcast(body.GetCollection().GetVirtualChannelNames(), message.OptBuildBroadcastAckSyncUp()).MustBuildBroadcast()
	_, err = b.Broadcast(ctx, msg)
	if err != nil && broadcaster.IsBroadcastTaskNotCreated(err) {
		return 0, err
	}
	// The durable broadcast owns the recipe and resource reservations even if
	// the caller stopped waiting. Its callback creates the asynchronous job.
	owned = false
	return jobID, nil
}

func (sm *snapshotManager) applyRestoreCollection(ctx context.Context, result message.BroadcastResultRestoreSnapshotMessageV2) error {
	if err := sm.collectionRestorer.ApplyRestoreCollection(ctx, result); err != nil {
		return err
	}
	for _, index := range result.Message.MustBody().GetIndexes() {
		if err := sm.meta.indexMeta.CreateIndex(ctx, model.UnmarshalIndexModel(index)); err != nil {
			return err
		}
	}
	starts := make(map[string]uint64)
	for channel, position := range result.Results {
		if !funcutil.IsControlChannel(channel) {
			starts[channel] = position.TimeTick
		}
	}
	h := result.Message.Header()
	_, err := sm.restoreData(ctx, h.GetSourceCollectionId(), h.GetSnapshotName(), h.GetSnapshotS3Location(), h.GetCollectionId(),
		h.GetJobId(), h.GetPinId(), h.GetExternal(), h.GetExternalSpec(), h.GetSnapshotFingerprint(),
		&restorePublication{starts: starts, createsCollection: true})
	return err
}
