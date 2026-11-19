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

package snapshot

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	dcindex "github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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
	IsCollectionDropped(context.Context, int64) (bool, error)
	CheckRestoreTargetAbsent(context.Context, string, string) error
}

type restorePublication struct {
	starts            map[string]uint64
	createsCollection bool
}

func (sm *Manager) broadcastRestoreCollection(ctx context.Context, snapshot *snapshotstorage.SnapshotData, name string, sourceID int64, target, db string, pinID int64, external bool, location, spec string) (int64, error) {
	b, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(db), message.NewExclusiveCollectionNameResourceKey(db, target))
	if err != nil {
		return 0, err
	}
	defer b.Close()
	// StartBroadcast also holds SharedCluster, so topology changes cannot race
	// this fresh check and collection creation. The source pin still protects
	// snapshot reads; its caller releases it if admission fails here.
	if err := checkResidueRestoreReplication(ctx, snapshot.Collection); err != nil {
		return 0, err
	}
	if err := sm.CollectionRestorer.CheckRestoreTargetAbsent(ctx, db, target); !errors.Is(err, merr.ErrCollectionNotFound) {
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
	body, cleanup, err := sm.CollectionRestorer.PrepareRestoreCollection(ctx, &milvuspb.CreateCollectionRequest{
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
	body.Routing, err = remapSnapshotRouting(snapshot.Collection, body.GetCollection().GetVirtualChannelNames())
	if err != nil {
		return 0, err
	}
	collectionID := body.GetCollectionHeader().GetCollectionId()
	for _, source := range snapshot.Indexes {
		id, err := sm.Allocator.AllocID(ctx)
		if err != nil {
			return 0, err
		}
		index := &model.Index{
			CollectionID: collectionID, FieldID: source.GetFieldID(), IndexID: id,
			CreateTime: uint64(time.Now().UnixNano()), IndexName: source.GetIndexName(), TypeParams: dcindex.DeleteParams(source.GetTypeParams(), []string{common.MmapEnabledKey}),
			IndexParams: source.GetIndexParams(), UserIndexParams: source.GetUserIndexParams(), IsAutoIndex: source.GetIsAutoIndex(),
		}
		if err := indexparamcheck.ValidateIndexParams(index); err != nil {
			return 0, err
		}
		if err := sm.CheckJSONPathIndexVersion(index); err != nil {
			return 0, err
		}
		var version int32
		if sm.IndexEngineVersionManager != nil {
			version = sm.IndexEngineVersionManager.ResolveScalarIndexVersion()
		}
		if err := dcindex.CheckFMIndexEngineVersion(index.IndexParams, version); err != nil {
			return 0, err
		}
		body.Indexes = append(body.Indexes, model.MarshalIndexModel(index))
	}
	jobID, err := sm.Allocator.AllocID(ctx)
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

// RestoreSnapshot is local (_ur), including the copied rows. Until its logical
// replay contract is supported, restoring explicit residue owners must not
// create user state missing from a configured secondary.
func checkResidueRestoreReplication(ctx context.Context, collection *datapb.CollectionDescription) error {
	if collection.GetRoutingModulus() == 0 {
		return nil
	}
	wal := streaming.WAL()
	if wal == nil || wal.Replicate() == nil {
		return merr.WrapErrServiceNotReadyMsg("replication configuration is unavailable for snapshot restore")
	}
	config, err := wal.Replicate().GetReplicateConfiguration(ctx)
	if err != nil {
		return merr.Wrap(err, "read replication configuration for residue snapshot restore")
	}
	// A successful nil configuration explicitly means no topology is configured.
	// Match DDL's replication admission, including a multi-cluster
	// configuration whose topology edges have not been installed yet.
	if len(config.GetCrossClusterTopology()) != 0 || len(config.GetClusters()) > 1 {
		return merr.WrapErrOperationNotSupportedMsg("restoring a residue-routed snapshot is not supported on a replicating cluster")
	}
	return nil
}

func (sm *Manager) ApplyRestoreCollection(ctx context.Context, result message.BroadcastResultRestoreSnapshotMessageV2) error {
	if err := sm.CollectionRestorer.ApplyRestoreCollection(ctx, result); err != nil {
		return err
	}
	for _, index := range result.Message.MustBody().GetIndexes() {
		if err := sm.IndexMeta.CreateIndex(ctx, model.UnmarshalIndexModel(index)); err != nil {
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

// remapSnapshotRouting uses the same sorted-channel pairing as the copy recipe.
// Metadata is never inferred from segment rows or the number of restored shards.
func remapSnapshotRouting(collection *datapb.CollectionDescription, targets []string) (*message.SplitShardRouting, error) {
	if collection.GetRoutingModulus() == 0 {
		if len(collection.GetShardInfos()) != 0 || collection.GetShardBy() != "" {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot carries routing shards without a routing modulus")
		}
		return nil, nil // Original modulo-N snapshot format.
	}
	for _, shard := range collection.GetShardInfos() {
		if shard.GetState() != schemapb.ShardState_ShardNormal {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot routing includes a non-writable shard")
		}
	}
	source, err := routing.RoutingFromShardInfos(collection.GetRoutingModulus(), collection.GetShardBy(), collection.GetShardInfos())
	if err != nil {
		return nil, merr.Wrap(err, "invalid snapshot routing")
	}
	if _, err := source.Table(); err != nil {
		return nil, merr.Wrap(err, "invalid snapshot routing ownership")
	}
	mapping, err := pairSnapshotChannels(collection.GetVirtualChannelNames(), targets)
	if err != nil {
		return nil, err
	}
	buckets := make(map[string][]uint64, len(source.Shards))
	for _, shard := range source.Shards {
		target, ok := mapping[shard.VChannel]
		if !ok {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot routing shard %s is absent from its channel list", shard.VChannel)
		}
		buckets[target] = shard.Buckets
	}
	if len(buckets) != len(mapping) {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot routing does not cover every snapshot channel")
	}
	result := routing.Routing{Modulus: source.Modulus, ShardBy: source.ShardBy}
	for _, target := range targets {
		result.Shards = append(result.Shards, routing.Shard{VChannel: target, Buckets: slices.Clone(buckets[target])})
	}
	return &message.SplitShardRouting{RoutingModulus: result.Modulus, ShardBy: result.ShardBy, Shards: result.ShardInfos()}, nil
}

func pairSnapshotChannels(sources, targets []string) (map[string]string, error) {
	if len(sources) != len(targets) {
		return nil, merr.WrapErrDataIntegrityMsg("channel count mismatch between snapshot and target collection: snapshot=%d, target=%d", len(sources), len(targets))
	}
	old, next := slices.Clone(sources), slices.Clone(targets)
	slices.Sort(old)
	slices.Sort(next)
	mapping := make(map[string]string, len(old))
	for i, source := range old {
		if source == "" || next[i] == "" || (i > 0 && (source == old[i-1] || next[i] == next[i-1])) {
			return nil, merr.WrapErrDataIntegrityMsg("snapshot channel mapping contains an empty or repeated channel")
		}
		mapping[source] = next[i]
	}
	return mapping, nil
}
