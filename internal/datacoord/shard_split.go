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
	"math"
	"slices"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/datacoord/bulkimport"
	"github.com/milvus-io/milvus/internal/datacoord/copysegment"
	"github.com/milvus-io/milvus/internal/datacoord/ddl"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This file wires the shard split manager (package split) into the server:
// the hooks of the SplitShard ack callback, the trigger, and the places where
// DataCoord must treat a split collection differently from what the vchannel
// list of RootCoord says.
//
// After a split RootCoord lists the targets and not the source. The source is
// still a shard of the DataView, with segments, until its adoption on this
// cluster. Every DataCoord path that takes "the vchannels of the collection"
// from RootCoord therefore misses the source; the ones where that matters
// read the split records here.

// SplitShardIssuer broadcasts the SplitShard message of a request and returns
// when its ack callback has returned. It runs admit first under the exclusive
// resource key of the collection and appends nothing if admit refuses.
// MixCoord sets it to RootCoord's BroadcastSplitShard.
type SplitShardIssuer func(ctx context.Context, request split.Request, admit func(ctx context.Context) error) error

// SetSplitShardIssuer sets how this DataCoord issues a shard split. Without an
// issuer the trigger refuses to issue.
func (s *Server) SetSplitShardIssuer(issuer SplitShardIssuer) {
	s.splitIssuerMu.Lock()
	defer s.splitIssuerMu.Unlock()
	s.splitIssuer = issuer
}

func (s *Server) issueSplitShard(ctx context.Context, request split.Request, admit func(ctx context.Context) error) error {
	s.splitIssuerMu.RLock()
	issuer := s.splitIssuer
	s.splitIssuerMu.RUnlock()
	if issuer == nil {
		return merr.WrapErrServiceUnavailableMsg("no issuer of shard splits is set on this DataCoord")
	}
	return issuer(ctx, request, admit)
}

// splitShardCallback returns the DataCoord half of the SplitShard callback,
// or an error while the metadata is not recovered.
func (s *Server) splitShardCallback() (*split.Callback, error) {
	if s.splitStore == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("the shard split records of DataCoord are not recovered")
	}
	return &split.Callback{
		Store: s.splitStore,
		DataView: func() split.ShardDeclarer {
			if s.dataViewManager == nil {
				return nil
			}
			return s.dataViewManager
		},
	}, nil
}

// SplitShardAckHook is the first DataCoord step of the SplitShard ack
// callback (split.Callback.OnAcked). RootCoord runs it before it publishes the
// new routing.
func (s *Server) SplitShardAckHook(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
	callback, err := s.splitShardCallback()
	if err != nil {
		return err
	}
	return callback.OnAcked(ctx, result)
}

// SplitShardAppliedHook is the last step of the SplitShard ack callback
// (split.Callback.OnApplied): it returns when the residual of the source has
// flushed.
func (s *Server) SplitShardAppliedHook(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
	callback, err := s.splitShardCallback()
	if err != nil {
		return err
	}
	return callback.OnApplied(ctx, result)
}

// ObserveSplitShardConsumerAck is the consumer ack observer of SplitShard
// messages (split.Callback.OnSourceConsumed).
func (s *Server) ObserveSplitShardConsumerAck(ctx context.Context, msg message.ImmutableMessage, completed bool) error {
	callback, err := s.splitShardCallback()
	if err != nil {
		return err
	}
	return callback.OnSourceConsumed(ctx, msg, completed)
}

// SplitShard issues the split of one shard, whatever its size and whether the
// trigger is enabled (split.Manager.Split). Internal: no RPC exposes it.
func (s *Server) SplitShard(ctx context.Context, collectionID int64, source string) (split.Request, error) {
	if s.splitManager == nil {
		return split.Request{}, merr.WrapErrServiceNotReadyMsg("the shard split manager of DataCoord is not started")
	}
	return s.splitManager.Split(ctx, collectionID, source)
}

// ShardSplitStore returns the records of the shard splits this cluster has
// consumed, nil while the metadata is not recovered. The later steps of a
// split (rewrite, adoption, retirement) read their split from it and move it
// forward with Store.Advance.
func (s *Server) ShardSplitStore() *split.Store {
	return s.splitStore
}

// newSplitManager builds the trigger over the server.
func (s *Server) newSplitManager(ctx context.Context) *split.Manager {
	return split.NewManager(ctx, s.splitDependencies())
}

// splitDependencies is what the trigger reads and calls of the server.
func (s *Server) splitDependencies() split.Dependencies {
	return split.Dependencies{
		Store:              s.splitStore,
		CheckParentRetired: s.checkSplitParentRetired,
		Collections: func() []split.Collection {
			collections := make([]split.Collection, 0)
			for _, collection := range s.meta.GetCollections() {
				if collection.IsCreated() {
					collections = append(collections, splitCollectionOf(collection))
				}
			}
			return collections
		},
		Collection: func(collectionID int64) (split.Collection, bool) {
			collection := s.meta.GetCollection(collectionID)
			if !collection.IsCreated() {
				return split.Collection{}, false
			}
			return splitCollectionOf(collection), true
		},
		ShardStats:     s.splitShardStats,
		CheckRewrite:   s.checkSplitRewriteCapacity,
		DataViewShards: s.dataViewShards,
		PChannels:      availablePChannels,
		AllocID:        s.allocator.AllocID,
		IsPrimary: func(ctx context.Context) (bool, error) {
			role, _, err := ddl.GetReplicationRole(ctx)
			return role == replicateutil.RolePrimary, err
		},
		IsReplicating: func(ctx context.Context) (bool, error) {
			_, replicating, err := ddl.GetReplicationRole(ctx)
			return replicating, err
		},
		NodeVersions: s.splitNodeVersions,
		HasPendingImport: func(ctx context.Context, collectionID int64) bool {
			return len(s.importMeta.GetJobBy(ctx, bulkimport.WithCollectionID(collectionID),
				bulkimport.WithoutJobStates(internalpb.ImportJobState_Completed, internalpb.ImportJobState_Failed))) > 0
		},
		HasPendingRestore: func(ctx context.Context, collectionID int64) bool {
			return s.copySegmentMeta.CountJobBy(ctx, copysegment.WithCopyJobCollectionID(collectionID),
				copysegment.WithoutCopyJobStates(datapb.CopySegmentJobState_CopySegmentJobCompleted, datapb.CopySegmentJobState_CopySegmentJobFailed)) > 0
		},
		FlushSource: s.flushSplitSource,
		Issue:       s.issueSplitShard,
	}
}

// splitNodeRoles are the roles whose sessions must know a shard split before
// one is planned: the StreamingNodes (the message, the fence, the genesis),
// the coordinators (the ack callbacks; mixcoord, and the separate
// coordinators of an older deployment), the proxies (residue routing), the
// QueryNodes (lineage reads) and the DataNodes (the rewrite compaction of a
// later layer).
var splitNodeRoles = []string{
	typeutil.StreamingNodeRole,
	typeutil.MixCoordRole,
	typeutil.RootCoordRole,
	typeutil.DataCoordRole,
	typeutil.QueryCoordRole,
	typeutil.StreamingCoordRole,
	typeutil.ProxyRole,
	typeutil.QueryNodeRole,
	typeutil.DataNodeRole,
}

// splitNodeVersions lists the version every session of those roles registers
// in the etcd of this cluster. It sees this cluster only: the nodes of
// another cluster of a replicate topology are not listed (see
// dataCoord.shardSplit.allowWithReplication). A node that has not registered
// yet, or whose session expired, is not listed either.
func (s *Server) splitNodeVersions(ctx context.Context) ([]split.NodeVersion, error) {
	if s.session == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("the session of DataCoord is not set")
	}
	var nodes []split.NodeVersion
	for _, role := range splitNodeRoles {
		sessions, _, err := s.session.GetSessions(ctx, role)
		if err != nil {
			return nil, merr.Wrapf(err, "list the %s sessions", role)
		}
		keys := lo.Keys(sessions)
		slices.Sort(keys)
		for _, key := range keys {
			session := sessions[key]
			nodes = append(nodes, split.NodeVersion{Role: role, ServerID: session.ServerID, Version: session.Version})
		}
	}
	return nodes, nil
}

func splitCollectionOf(collection *meta.CollectionInfo) split.Collection {
	return split.Collection{
		ID:               collection.ID,
		VChannels:        collection.VChannelNames,
		Properties:       collection.PropertyPairs,
		SchemaProperties: collection.Schema.GetProperties(),
		EnableNamespace:  collection.Schema.GetEnableNamespace(),
		External:         collection.IsExternal(),
	}
}

// splitShardStats returns the bytes and the rows of the healthy data segments
// of a vchannel.
func (s *Server) splitShardStats(vchannel string) (int64, int64) {
	var size, rows int64
	for _, segment := range s.meta.GetSegmentsByChannel(vchannel) {
		if segment.GetLevel() == datapb.SegmentLevel_L0 {
			continue
		}
		size += segment.GetSegmentSize()
		rows += segment.GetNumOfRows()
	}
	return size, rows
}

// dataViewShards returns the shards of the latest DataView of the collection.
func (s *Server) dataViewShards(ctx context.Context, collectionID int64) ([]string, error) {
	if s.dataViewManager == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("DataView manager is not initialized")
	}
	ref, err := s.dataViewManager.Latest(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	if ref == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("collection %d has no DataView", collectionID)
	}
	defer ref.Deref()
	shards := ref.DataView().GetShards()
	vchannels := make([]string, 0, len(shards))
	for _, shard := range shards {
		vchannels = append(vchannels, shard.GetVchannel())
	}
	return vchannels, nil
}

// availablePChannels returns ready, replication-eligible pchannels in the
// allocator's load order. Partial allocation samples them once, including
// during dynamic expansion when many discovered WALs have not opened yet.
func availablePChannels(ctx context.Context) ([]string, error) {
	count := len(channel.GetClusterChannels().Channels)
	if count == 0 {
		return nil, nil
	}
	vchannels, err := snmanager.StaticStreamingNodeManager.AllocVirtualChannels(ctx, balancer.AllocVChannelParam{Num: count, RequireReady: true, AllowPartial: true})
	if err != nil {
		return nil, err
	}
	pchannels := make([]string, 0, len(vchannels))
	for _, vchannel := range vchannels {
		pchannels = append(pchannels, funcutil.ToPhysicalChannel(vchannel))
	}
	return pchannels, nil
}

// flushSplitSource flushes one shard on this cluster and waits for the flush:
// a ManualFlush on that vchannel alone, unreplicable, acknowledged by the
// consuming side once the segments and the L0 data are durable.
func (s *Server) flushSplitSource(ctx context.Context, collectionID int64, vchannel string) error {
	bc, err := s.ddl.StartLocalFlushBroadcast(ctx, collectionID)
	if err != nil {
		return err
	}
	defer bc.Close()
	msg, err := message.NewManualFlushMessageBuilderV2().
		WithUnreplicable().
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: collectionID}).
		WithBody(&message.ManualFlushMessageBody{}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		BuildBroadcast()
	if err != nil {
		return merr.Wrap(err, "build the flush of a shard split source")
	}
	_, err = bc.Broadcast(ctx, msg)
	return err
}

// splitTransformPins merges the pins of the shard splits of the collection
// into the pending Transform bounds of a DataView publication: the frontier of
// a target stays at T_switch until the adoption.
func (s *Server) splitTransformPins(store *split.Store, view *viewpb.DataViewOfCollection, bounds map[string]uint64) map[string]uint64 {
	pins := store.TransformPins(view.GetCollectionId())
	if len(pins) == 0 {
		return bounds
	}
	if bounds == nil {
		bounds = make(map[string]uint64, len(pins))
	}
	for vchannel, pin := range pins {
		if old, ok := bounds[vchannel]; !ok || pin < old {
			bounds[vchannel] = pin
		}
	}
	return bounds
}

// checkSplitWindow refuses an operation this cluster originates on a
// collection that has a shard split in its window (spec 10.1): an import, or
// a snapshot. The targets do not hold the rows of the source yet, and the
// vchannel list of RootCoord, which these operations work on, no longer names
// the source.
//
// The request is valid and the state passes at the adoption, so this is a
// System error. It is not marked retriable: the window can last as long as
// the rewrite of a shard.
func (s *Server) checkSplitWindow(collectionID int64, operation string) error {
	if s.splitStore == nil {
		return nil
	}
	task := s.splitStore.WindowOf(collectionID)
	if task == nil {
		return nil
	}
	return merr.WrapErrOperationNotSupportedMsg(
		"%s is refused while shard split task %d of collection %d is in its window (state %s), try again after the split is adopted",
		operation, task.GetTaskId(), collectionID, task.GetState().String())
}

// checkFlushOfUndeclaredShard refuses the final flush of a segment on a
// vchannel that DataCoord cannot place yet: a target of a shard split whose
// ack callback has not recorded the split.
//
// The DataView would create the shard on the fly, or find it declared but not
// pinned, and take the flush certificate as its Transform frontier. That
// frontier would be above T_switch, and the segments of the source need the
// history of the target from T_switch on. Once the split is recorded the pin
// holds and the flush is accepted. The StreamingNode retries a refused flush.
//
// A vchannel is known if RootCoord lists it for the collection, if it is the
// source of a split record, or if it is a target of a split record that pins
// the frontier of its targets (split.Store.FlushableVChannels). RootCoord
// lists a target only after the callback has recorded the split. A pending
// record (the source acknowledged before the callback, as on a secondary)
// names its targets but pins nothing, so their flushes still wait. A
// collection DataCoord cannot read is not judged, as before.
//
// An L0 flush is never judged: SaveBinlogPaths handles it in another branch,
// and it does not touch the frontier of its shard.
func (s *Server) checkFlushOfUndeclaredShard(collectionID int64, vchannel string) error {
	if s.splitStore == nil {
		return nil
	}
	collection := s.meta.GetCollection(collectionID)
	if collection == nil || len(collection.VChannelNames) == 0 {
		return nil
	}
	for _, known := range collection.VChannelNames {
		if known == vchannel {
			return nil
		}
	}
	for _, known := range s.splitStore.FlushableVChannels(collectionID) {
		if known == vchannel {
			return nil
		}
	}
	return merr.WrapErrServiceNotReadyMsg(
		"vchannel %s of collection %d is not declared yet: the shard split that creates it is not recorded", vchannel, collectionID)
}

// dropSplitSourcesOfCollection drops the data of the split sources of a
// dropped collection, and its split records.
//
// DropCollection is broadcast to the shards RootCoord lists, so no
// DropVirtualChannel ever names a split source. This does for each source
// that is not retired what DropVirtualChannel does for a listed vchannel. It
// is idempotent and runs in the DropCollection ack callback, on every cluster.
func (s *Server) dropSplitSourcesOfCollection(ctx context.Context, collectionID int64) error {
	if s.splitStore == nil {
		return nil
	}
	channels := make(map[string]struct{})
	hasTask := false
	for _, task := range s.splitStore.List() {
		if task.GetCollectionId() == collectionID {
			hasTask = true
			channels[task.GetSourceVchannel()] = struct{}{}
		}
	}
	if hasTask {
		// A committed overlay may not yet have canonicalized the original S rows.
		// Persist Dropped for every effective owner before deleting that overlay.
		// This also makes a crash in the CChannel branch before the target branches
		// recoverable: no raw source row can reappear after its task is removed.
		for _, segment := range s.meta.SelectSegments(ctx, meta.WithCollection(collectionID)) {
			channels[segment.GetInsertChannel()] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(channels))
	for channel := range channels {
		ordered = append(ordered, channel)
	}
	slices.Sort(ordered)
	for _, channel := range ordered {
		if err := s.dropVirtualChannelData(ctx, channel); err != nil {
			return merr.Wrapf(err, "drop effective split channel %s of collection %d", channel, collectionID)
		}
		mlog.Info(ctx, "split channel dropped with its collection", mlog.FieldCollectionID(collectionID), mlog.String("vchannel", channel))
	}
	return s.splitStore.DropCollection(ctx, collectionID)
}

// dropVirtualChannelData marks every segment of the vchannel dropped and the
// channel checkpoint dropped: the body of DropVirtualChannel for a request
// without segments.
func (s *Server) dropVirtualChannelData(ctx context.Context, vchannel string) error {
	if err := s.meta.UpdateDropChannelSegmentInfo(ctx, vchannel, nil); err != nil {
		return err
	}
	s.compactionInspector.RemoveTasksByChannel(vchannel)
	metrics.DataCoordCheckpointUnixSeconds.DeleteLabelValues(paramtable.GetStringNodeID(), vchannel)
	return s.meta.MarkChannelCheckpointDropped(ctx, vchannel)
}

// truncateSplitSources drops every segment of the split sources of a
// truncated collection that are not adopted yet.
//
// Truncate is broadcast to the shards RootCoord lists, and its flush time
// ticks name those only. Every row of a split source is older than the fence
// of the source, which is below every time tick of the targets, so a truncate
// of the collection covers the source completely. The split callback keeps
// the resource key of the collection until the residual of the source has
// flushed, so no segment of the source is still growing when this runs.
func (s *Server) truncateSplitSources(ctx context.Context, collectionID int64, truncated map[string]uint64) error {
	if s.splitStore == nil {
		return nil
	}
	for _, source := range s.splitStore.Sources(collectionID, datapb.SplitShardTaskState_SplitShardTaskAdopted) {
		if _, ok := truncated[source]; ok {
			continue
		}
		if err := s.meta.TruncateChannelByTime(ctx, source, math.MaxUint64); err != nil {
			return merr.Wrapf(err, "truncate split source %s of collection %d", source, collectionID)
		}
	}
	return nil
}

// A secondary can persist target L0 before the SplitShard callback records
// the target. Unknown channels retain L0 from that first flush onward.
func splitL0CompactionSuspended(mt *meta.Meta, store *split.Store, channel string) bool {
	if store.IsL0CompactionSuspended(channel) {
		return true
	}
	collection := mt.GetCollection(funcutil.GetCollectionIDFromVChannel(channel))
	if collection == nil || len(collection.VChannelNames) == 0 {
		// Source and target copies can be consumed while coordinator callbacks
		// are still catching up. Absence is not proof of an ordinary channel.
		return funcutil.GetCollectionIDFromVChannel(channel) > 0
	}
	if slices.Contains(collection.VChannelNames, channel) {
		return false
	}
	for _, task := range store.List() {
		if task.GetSourceVchannel() == channel {
			return false
		}
		for _, target := range task.GetTargets() {
			if target.GetVchannel() == channel {
				return false
			}
		}
	}
	return true
}
