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

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// broadcastCommitShardSplitRouting commits a shard-split routing change into the
// collection meta. It reuses the alter-collection DDL machinery: the routing
// topology rides in an AlterCollection message under the shard-split routing
// field mask, so the existing broadcast -> ack -> meta_table.AlterCollection ->
// Collection.ApplyUpdates path persists it atomically and invalidates the proxy
// caches. The broadcast reaches every shard of the new topology (existing shards
// plus the split targets) so the streamingnode shard managers converge on the
// new routing version.
func (c *Core) broadcastCommitShardSplitRouting(ctx context.Context, req *rootcoordpb.CommitShardSplitRoutingRequest) error {
	if req.GetCollectionName() == "" {
		return merr.WrapErrParameterInvalidMsg("commit shard split routing failed, collection name is empty")
	}
	vchannels := req.GetVirtualChannelNames()
	if len(vchannels) == 0 {
		return merr.WrapErrParameterInvalidMsg("commit shard split routing failed, no vchannels provided")
	}
	if len(vchannels) != len(req.GetPhysicalChannelNames()) || len(vchannels) != len(req.GetShardInfos()) {
		return merr.WrapErrParameterInvalidMsg("commit shard split routing failed, channel and shard-info arrays must be parallel")
	}

	// Refuse a topology that does not tile the key space. Deriving it here is the
	// last point at which a bad split plan is still only a rejected DDL: once
	// committed, a gap silently drops the writes of the residues nobody claims,
	// and an overlap sends one key to two shards. The same derivation the write
	// path will do is therefore done first, over the same writable-shard filter.
	writable, err := routing.ShardsFromMeta(vchannels, req.GetShardInfos())
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("commit shard split routing failed, %s", err.Error())
	}
	if _, err := routing.Derive(req.GetRoutingModulus(), vchannels, writable); err != nil {
		return merr.WrapErrParameterInvalidMsg("commit shard split routing failed, %s", err.Error())
	}

	// No collection lock here, deliberately. This is the last step of a split's
	// write switch, and its only caller — datacoord's split manager — already
	// holds the collection's exclusive resource key across the whole fence ->
	// create -> commit span, precisely so no DDL can change the collection
	// underneath the target vchannels it just created. Taking the same exclusive
	// key again from inside that span would block on the caller and deadlock:
	// the broadcaster's locker is one process-wide instance, and its acquire does
	// not honour a context.
	//
	// The routing commit is therefore protected by its caller, not by itself.
	// Reaching it from anywhere else would be a bug, and an unlocked one.
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx)
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp, false)
	if err != nil {
		return err
	}

	// Idempotent by shard state: if the collection already carries exactly the
	// requested vchannels each at the requested lifecycle state, the routing is
	// already committed and this is a no-op. Otherwise the whole topology is
	// (re)applied atomically below. There is no version counter — the source
	// fence plus per-shard state drive the write switch, so a routing change is
	// identified by the states it sets, not by a monotonic epoch.
	committed := len(coll.VirtualChannelNames) == len(vchannels)
	if committed {
		for i, vchannel := range vchannels {
			info, ok := coll.ShardInfos[vchannel]
			if !ok || info.State != req.GetShardInfos()[i].GetState() {
				committed = false
				break
			}
		}
	}
	if committed {
		return errIgnoredAlterCollection
	}

	cacheExpirations, err := c.getCacheExpireForCollection(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	header := &messagespb.AlterCollectionMessageHeader{
		DbId:         coll.DBID,
		CollectionId: coll.CollectionID,
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{message.FieldMaskCollectionShardSplitRouting},
		},
		CacheExpirations: cacheExpirations,
	}
	updates := &messagespb.AlterCollectionMessageUpdates{
		VirtualChannelNames:  vchannels,
		PhysicalChannelNames: req.GetPhysicalChannelNames(),
		ShardInfos:           req.GetShardInfos(),
		RoutingModulus:       req.GetRoutingModulus(),
		ShardBy:              req.GetShardBy(),
	}

	// Broadcast to every shard of the new topology plus the control channel, so
	// all streamingnode shard managers (including the new split targets) and the
	// proxy caches pick up the new routing version.
	channels := make([]string, 0, len(vchannels)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, vchannels...)
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(header).
		WithBody(&messagespb.AlterCollectionMessageBody{Updates: updates}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}
