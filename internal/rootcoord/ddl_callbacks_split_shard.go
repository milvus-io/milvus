// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rootcoord

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/internal/util/shardsplit"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SplitShardAckHook is a step of the SplitShard ack callback that another
// coordinator owns. DataCoord registers one to declare the target shards and
// to record the split before RootCoord publishes the new routing.
//
// The hook runs on every cluster that consumes the message, and again after a
// coordinator restart or a failed attempt, so it must be idempotent. It runs
// before the collection metadata is written. An error makes the broadcaster
// run the whole callback again.
type SplitShardAckHook func(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error

var (
	splitShardAckHookMu sync.RWMutex
	splitShardAckHook   SplitShardAckHook
	// splitShardAppliedHook runs after the collection metadata is written.
	splitShardAppliedHook SplitShardAckHook
)

// RegisterSplitShardAckHook sets the hook the SplitShard ack callback runs
// before it writes the collection metadata. A nil hook removes it. Without a
// hook the callback writes the collection metadata only.
func RegisterSplitShardAckHook(hook SplitShardAckHook) {
	splitShardAckHookMu.Lock()
	defer splitShardAckHookMu.Unlock()
	splitShardAckHook = hook
}

// RegisterSplitShardAppliedHook sets the hook the SplitShard ack callback runs
// last, after the collection metadata is written and the proxy caches are
// invalidated. A nil hook removes it.
//
// The callback holds the broadcast resource key of the collection until it
// returns, so a hook that waits keeps every other DDL of the collection behind
// the split without delaying the write path, which already routes by the new
// metadata. DataCoord registers one that waits until the residual of the
// source has flushed. Like the first hook it runs on every cluster, possibly
// many times, and an error makes the broadcaster run the whole callback again.
func RegisterSplitShardAppliedHook(hook SplitShardAckHook) {
	splitShardAckHookMu.Lock()
	defer splitShardAckHookMu.Unlock()
	splitShardAppliedHook = hook
}

func getSplitShardAppliedHook() SplitShardAckHook {
	splitShardAckHookMu.RLock()
	defer splitShardAckHookMu.RUnlock()
	return splitShardAppliedHook
}

func getSplitShardAckHook() SplitShardAckHook {
	splitShardAckHookMu.RLock()
	defer splitShardAckHookMu.RUnlock()
	return splitShardAckHook
}

// splitShardV2AckCallback applies a shard split once every copy of its
// SplitShard message is appended: the source is fenced and the targets exist.
//
// It runs on the cluster that issued the split and on every cluster that
// received the message through replication. It may run more than once: after
// a failed attempt and after a coordinator restart. Every step is idempotent.
//
//  1. Check the message against the collection, without writing. A collection
//     that is gone or being dropped ends the callback: there is nothing left
//     to split.
//  2. Run the hook of the other coordinators.
//  3. Write the collection metadata, once: the source leaves the vchannel
//     list, the targets join it, and the lineage of the source is recorded. A
//     collection that already records the split is left as it is.
//  4. Invalidate the proxy caches of the collection, so that the proxies route
//     by the new routing.
//  5. Run the applied hook of the other coordinators. It may wait: the write
//     path is switched already.
//
// Every failure is returned and the broadcaster runs the callback again. The
// source is already fenced when the callback runs, so the split cannot be
// abandoned: a message that can never be applied keeps the resource key of
// the collection, and is logged as an error each time.
func (c *DDLCallback) splitShardV2AckCallback(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
	header := result.Message.Header()
	logger := mlog.With(
		mlog.FieldCollectionID(header.GetCollectionId()),
		mlog.Int64("splitTaskID", header.GetSplitTaskId()),
		mlog.String("source", header.GetSourceVchannel()),
		mlog.Strings("targets", header.GetTargetVchannels()))

	applied, err := c.meta.CheckShardSplit(ctx, result)
	if err != nil {
		if errors.IsAny(err, errAlterCollectionNotFound, errShardSplitCollectionUnavailable) {
			logger.Warn(ctx, "shard split of a collection that is gone or being dropped, ignore it", mlog.Err(err))
			return nil
		}
		logger.Error(ctx, "shard split cannot be applied to the collection; the source is fenced already and the callback is retried", mlog.Err(err))
		return merr.Wrap(err, "check the shard split against the collection")
	}

	if hook := getSplitShardAckHook(); hook != nil {
		if err := hook(ctx, result); err != nil {
			return merr.Wrap(err, "run the shard split hook")
		}
	}

	if !applied {
		switch err := c.meta.ApplyShardSplit(ctx, result); {
		case err == nil:
		case errors.Is(err, errShardSplitApplied):
			// Applied between the check and the write.
		case errors.IsAny(err, errAlterCollectionNotFound, errShardSplitCollectionUnavailable):
			logger.Warn(ctx, "collection is gone or being dropped while its shard split is applied, ignore it", mlog.Err(err))
			return nil
		default:
			return merr.Wrap(err, "apply the shard split to the collection")
		}
	} else {
		logger.Info(ctx, "shard split is already applied to the collection, only the proxy caches are invalidated")
	}

	// The name is read again by id: a rename takes another resource key, so on
	// a secondary it can be applied between the write above and this point.
	coll, err := c.meta.GetCollectionByID(ctx, "", header.GetCollectionId(), typeutil.MaxTimestamp, false)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			logger.Warn(ctx, "collection is gone after its shard split was applied, its caches are invalidated by the drop")
			return nil
		}
		return merr.Wrap(err, "read the collection to invalidate its proxy caches")
	}
	expirations, err := c.getCacheExpireForCollection(ctx, coll.DBName, coll.Name)
	if err != nil {
		if errors.Is(err, merr.ErrCollectionNotFound) {
			return nil
		}
		return merr.Wrap(err, "collect the proxy caches of the collection")
	}
	if err := c.ExpireCaches(ctx, expirations); err != nil {
		return err
	}
	if hook := getSplitShardAppliedHook(); hook != nil {
		if err := hook(ctx, result); err != nil {
			return merr.Wrap(err, "run the shard split applied hook")
		}
	}
	return nil
}

// splitShardAppliedResolver answers the broadcaster of a secondary cluster
// that holds no task for a SplitShard broadcast: the copy on the source
// vchannel was acknowledged on this cluster if the collection records the
// lineage of that vchannel, and its time tick is the one the record holds.
// The record is written by the ack callback and is never removed while the
// collection exists, so it outlives the broadcast task.
func (c *DDLCallback) splitShardAppliedResolver(ctx context.Context, vchannels []string) (uint64, bool, error) {
	var timeTick uint64
	for _, vchannel := range vchannels {
		coll, err := c.meta.GetCollectionByID(ctx, "", funcutil.GetCollectionIDFromVChannel(vchannel), typeutil.MaxTimestamp, true)
		if err != nil {
			if errors.Is(err, merr.ErrCollectionNotFound) {
				return 0, false, nil
			}
			return 0, false, err
		}
		lineage := coll.SplitLineageOf(vchannel)
		if lineage == nil {
			return 0, false, nil
		}
		timeTick = max(timeTick, lineage.SwitchTimeTick)
	}
	return timeTick, len(vchannels) > 0, nil
}

// SplitShardRequest is one shard split as its issuer plans it. It names the
// vchannels only; the residues are derived by routing.PlanSplit unless the
// issuer gives them.
type SplitShardRequest struct {
	CollectionID int64
	// SplitTaskID is allocated by the issuer. Never zero.
	SplitTaskID int64
	// Source is the vchannel to split.
	Source string
	// Targets are the two new vchannels. Each must be on a pchannel that holds
	// no vchannel of the collection.
	Targets [routing.SplitTargetCount]string
	// Split, if set, is the plan of the split: the residues of each target and
	// the modulus afterwards. If nil, the split is planned by routing.PlanSplit.
	Split *routing.Split
	// ShardBy is the routing key expression to record if the collection has
	// none yet. Ignored if Split is set.
	ShardBy string
	// Admit, if set, is the issuer's own admission of the split. It runs
	// first while the exclusive resource key of the collection is held,
	// before the collection is read and the message is built; an error ends
	// the broadcast with nothing appended. Every DDL of the collection whose
	// ack callback ran before is visible to it, and none starts until the
	// split's own callback returns.
	Admit func(ctx context.Context) error
}

// BroadcastSplitShard issues a shard split: it builds the SplitShard message
// from the collection metadata and broadcasts it.
//
// The collection is read while the exclusive resource key of the collection is
// held, so no other DDL of the collection runs between the read and the ack
// callback. The call returns after the ack callback has run. It fails on a
// cluster that is not the primary.
//
// Nothing calls it yet: the split manager of DataCoord decides when to split.
func (c *Core) BroadcastSplitShard(ctx context.Context, req *SplitShardRequest) (*types.BroadcastAppendResult, error) {
	// The resource key is the name of the collection. The name is read first
	// and checked again under the key: a rename in between is refused.
	coll, err := c.meta.GetCollectionByID(ctx, "", req.CollectionID, typeutil.MaxTimestamp, false)
	if err != nil {
		return nil, merr.Wrap(err, "read the collection to split a shard")
	}
	dbName, collectionName := coll.DBName, coll.Name
	return shardsplit.Broadcast(ctx, dbName, collectionName, func(ctx context.Context) (*shardsplit.Plan, error) {
		if req.Admit != nil {
			if err := req.Admit(ctx); err != nil {
				return nil, err
			}
		}
		coll, err := c.meta.GetCollectionByID(ctx, "", req.CollectionID, typeutil.MaxTimestamp, false)
		if err != nil {
			return nil, merr.Wrap(err, "read the collection to split a shard")
		}
		if coll.DBName != dbName || coll.Name != collectionName {
			return nil, merr.WrapErrServiceUnavailableMsg(
				"collection %d was renamed from %s.%s to %s.%s while its shard split was starting, try again",
				req.CollectionID, dbName, collectionName, coll.DBName, coll.Name)
		}
		return newSplitShardPlan(coll, req)
	})
}

// newSplitShardPlan builds the plan of a shard split from the collection
// metadata.
func newSplitShardPlan(coll *model.Collection, req *SplitShardRequest) (*shardsplit.Plan, error) {
	current := coll.Routing()
	var split routing.Split
	if req.Split != nil {
		split = *req.Split
	} else {
		planned, err := routing.PlanSplit(current, req.Source, req.Targets)
		if err != nil {
			return nil, err
		}
		if planned.ShardBy == "" {
			planned.ShardBy = req.ShardBy
		}
		split = planned
	}
	partitionIDs := make([]int64, 0, len(coll.Partitions))
	for _, partition := range coll.Partitions {
		if partition.Available() {
			partitionIDs = append(partitionIDs, partition.PartitionID)
		}
	}
	return &shardsplit.Plan{
		SplitTaskID:    req.SplitTaskID,
		CollectionID:   coll.CollectionID,
		CollectionName: coll.Name,
		DBID:           coll.DBID,
		DBName:         coll.DBName,
		Schema:         collectionSchemaPB(coll, coll.DBName),
		PartitionIDs:   partitionIDs,
		Current:        current,
		SplitSources:   coll.SplitSourceVChannels(),
		Split:          split,
	}, nil
}
