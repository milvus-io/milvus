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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/internal/util/shardsplit"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var (
	// errShardSplitApplied: the collection already records this shard split.
	errShardSplitApplied = errors.New("shard split is already applied")
	// errShardSplitCollectionUnavailable: the collection is being dropped.
	errShardSplitCollectionUnavailable = errors.New("collection is unavailable for a shard split")
)

// CheckShardSplit reports whether the shard split of a SplitShard message is
// already applied to its collection. It refuses a message that cannot be
// applied, with the errors ApplyShardSplit returns, and changes nothing.
func (mt *MetaTable) CheckShardSplit(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) (bool, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	_, _, err := mt.planShardSplitLocked(result)
	if errors.Is(err, errShardSplitApplied) {
		return true, nil
	}
	return false, err
}

// ApplyShardSplit records a shard split in the collection metadata, in one
// catalog write:
//
//   - the source shard leaves the vchannel list of the collection, so it gets
//     no DML and no DDL broadcast any more;
//   - the two target shards join it, with the residues they own;
//   - one lineage record names the source, the targets and the time tick of
//     the SplitShard message on the source vchannel. It is never changed again.
//
// It is called from the ack callback of the SplitShard message, on every
// cluster that consumes the message. The time ticks are those of the WAL of
// this cluster.
//
// It returns errShardSplitApplied and writes nothing if the collection already
// records this split, errAlterCollectionNotFound if the collection is gone and
// errShardSplitCollectionUnavailable if it is being dropped.
func (mt *MetaTable) ApplyShardSplit(ctx context.Context, result message.BroadcastResultSplitShardMessageV2) error {
	header := result.Message.Header()

	var oldView, newView *metacache.CollectionInfo
	defer func() {
		// After ddLock is released.
		if oldView.IsCreated() && newView.IsCreated() {
			mt.collHooks.NotifyAltered(oldView, newView)
		}
	}()
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	next, lineage, err := mt.planShardSplitLocked(result)
	if err != nil {
		return err
	}
	oldColl := mt.collID2Meta[header.GetCollectionId()].Clone()
	newColl := oldColl.Clone()
	newColl.ApplyShardSplit(next, lineage)
	newColl.UpdateTimestamp = result.GetMaxTimeTick()

	ctx1 := contextutil.WithTenantID(ctx, Params.CommonCfg.ClusterName.GetValue())
	if err := mt.catalog.AlterCollection(ctx1, oldColl, newColl, metastore.MODIFY, newColl.UpdateTimestamp, false); err != nil {
		return err
	}

	// The count of shards changed by one.
	mt.generalCnt += newColl.GetPartitionNum(true) * int(newColl.ShardsNum-oldColl.ShardsNum)
	stats := channel.StaticPChannelStatsManager.MustGet()
	stats.AddVChannel(lineage.TargetVChannels...)
	stats.RemoveVChannel(lineage.SourceVChannel)

	oldView, _ = mt.collViews.Get(newColl.CollectionID)
	mt.setCollectionLocked(newColl)
	newView, _ = mt.collViews.Get(newColl.CollectionID)
	mlog.Info(ctx, "shard split applied to the collection",
		mlog.FieldCollectionID(newColl.CollectionID),
		mlog.Int64("splitTaskID", lineage.SplitTaskID),
		mlog.String("source", lineage.SourceVChannel),
		mlog.Strings("targets", lineage.TargetVChannels),
		mlog.Uint64("switchTimeTick", lineage.SwitchTimeTick),
		mlog.Uint64("routingModulus", newColl.RoutingModulus),
		mlog.Strings("vchannels", newColl.VirtualChannelNames),
		mlog.Uint64("ts", newColl.UpdateTimestamp),
	)
	return nil
}

// planShardSplitLocked checks a SplitShard message against its collection and
// returns the routing after the split and the lineage record to write. The
// caller must hold ddLock.
func (mt *MetaTable) planShardSplitLocked(result message.BroadcastResultSplitShardMessageV2) (routing.Routing, *model.ShardSplitLineage, error) {
	header := result.Message.Header()
	body := result.Message.MustBody()

	coll, ok := mt.collID2Meta[header.GetCollectionId()]
	if !ok {
		return routing.Routing{}, nil, errAlterCollectionNotFound
	}
	if !coll.Available() {
		return routing.Routing{}, nil, errShardSplitCollectionUnavailable
	}
	source := header.GetSourceVchannel()
	if recorded := coll.SplitLineageOf(source); recorded != nil {
		if recorded.SplitTaskID == header.GetSplitTaskId() {
			return routing.Routing{}, nil, errShardSplitApplied
		}
		return routing.Routing{}, nil, merr.WrapErrServiceInternalMsg(
			"shard split task %d names source %s of collection %d, which shard split task %d already split",
			header.GetSplitTaskId(), source, coll.CollectionID, recorded.SplitTaskID)
	}
	if err := shardsplit.CheckMessage(header, body); err != nil {
		return routing.Routing{}, nil, err
	}
	if err := shardsplit.CheckAgainstCollection(coll.Routing(), coll.SplitSourceVChannels(), coll.Properties, header, body); err != nil {
		return routing.Routing{}, nil, err
	}
	next, err := shardsplit.RoutingOf(body)
	if err != nil {
		return routing.Routing{}, nil, err
	}

	// T_switch is the time tick of the copy on the source vchannel, in the WAL
	// of this cluster.
	sourceResult := result.Results[source]
	if sourceResult == nil || sourceResult.TimeTick == 0 {
		return routing.Routing{}, nil, merr.WrapErrServiceInternalMsg(
			"shard split task %d was acknowledged without the time tick of its source %s", header.GetSplitTaskId(), source)
	}
	lineage := &model.ShardSplitLineage{
		SourceVChannel:  source,
		SourcePChannel:  coll.ShardInfos[source].PChannelName,
		TargetVChannels: append([]string(nil), header.GetTargetVchannels()...),
		SwitchTimeTick:  sourceResult.TimeTick,
		SplitTaskID:     header.GetSplitTaskId(),
	}
	return next, lineage, nil
}
