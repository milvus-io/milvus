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
package shard

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

// handleSplitShard handles one copy of a SplitShard broadcast. What the copy
// does is decided by the place of its vchannel in the header. The copy on the
// control channel never reaches here: DoAppend skips it.
func (impl *shardInterceptor) handleSplitShard(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	splitMsg := message.MustAsMutableSplitShardMessageV2(msg)
	header := splitMsg.Header()
	if header.GetSplitTaskId() == 0 {
		return nil, status.NewUnrecoverableError("split shard message on vchannel %s carries no split task id", msg.VChannel())
	}
	switch messageutil.SplitShardRoleOf(header, msg.VChannel()) {
	case messageutil.SplitShardRoleSource:
		return impl.handleSplitShardOnSource(ctx, msg, splitMsg, appendOp)
	case messageutil.SplitShardRoleTarget:
		return impl.handleSplitShardOnTarget(ctx, msg, splitMsg, appendOp)
	default:
		// The broadcast reaches the source, the targets and the control channel
		// only. A copy on any other vchannel is a bug of the issuer.
		return nil, status.NewUnrecoverableError("split shard message of task %d landed on vchannel %s, which is neither its source nor a target",
			header.GetSplitTaskId(), msg.VChannel())
	}
}

// handleSplitShardOnSource installs the DML fence of the source vchannel.
//
// The fence is a DML fence, not the end of the vchannel. The registration of
// the collection is kept, so the vchannel still admits the messages that are
// local to a cluster after the split: a flush, a forwarded schema, and later
// its retirement. Only DML and the creation of new segments are refused, with
// an error that tells the caller to refresh its routing.
//
// The fence is installed before the append and is kept whatever the append
// returns. An append that reports an error may still have persisted the
// message; if the fence were installed only after a successful append, the
// vchannel could accept DML behind a persisted fence. The price of an append
// that really failed is that DML is refused until the broadcaster appends the
// copy again, which it does until it succeeds.
//
// A copy that arrives on a vchannel that is already fenced is appended again
// and changes nothing: nothing was written since the first fence.
func (impl *shardInterceptor) handleSplitShardOnSource(ctx context.Context, msg message.MutableMessage, splitMsg message.MutableSplitShardMessageV2, appendOp interceptors.Append) (message.MessageID, error) {
	header := splitMsg.Header()
	// The sealed segments are not recorded in the message: segment ids are
	// local to this cluster, and every consumer seals by the time tick of the
	// fence (the recovery module, the L0 materializer, the IDF oracle).
	if _, _, err := impl.shardManager.FenceForSplit(header.GetCollectionId(), msg.VChannel(), header.GetSplitTaskId(), msg.TimeTick()); err != nil {
		// The vchannel does not exist on this pchannel, or another split fenced
		// it. Refusing is safe: no state was changed. The issuer holds the
		// exclusive resource key of the collection, so neither case is reached
		// by a correct coordinator.
		return nil, status.NewUnrecoverableError("cannot fence vchannel %s for shard split task %d: %s",
			msg.VChannel(), header.GetSplitTaskId(), err.Error())
	}
	return appendOp(ctx, msg)
}

type splitGenesis struct {
	task   int64
	source string
	target string
}

// handleSplitShardOnTarget creates a target vchannel. It registers the
// collection on this pchannel for DML and segment assignment exactly as a
// CreateCollection message does, through the same shard manager entry.
func (impl *shardInterceptor) handleSplitShardOnTarget(ctx context.Context, msg message.MutableMessage, splitMsg message.MutableSplitShardMessageV2, appendOp interceptors.Append) (message.MessageID, error) {
	header := splitMsg.Header()
	body := splitMsg.MustBody()
	if body.GetGenesisHeader() == nil || body.GetGenesis().GetCollectionSchema() == nil {
		return nil, status.NewUnrecoverableError("split shard message of task %d carries no creation payload for target %s",
			header.GetSplitTaskId(), msg.VChannel())
	}
	if body.GetGenesisHeader().GetCollectionId() != header.GetCollectionId() {
		return nil, status.NewUnrecoverableError("split shard message of task %d creates collection %d on target %s but splits collection %d",
			header.GetSplitTaskId(), body.GetGenesisHeader().GetCollectionId(), msg.VChannel(), header.GetCollectionId())
	}
	holder, exists := impl.shardManager.CollectionVChannel(header.GetCollectionId())
	if exists && holder != msg.VChannel() {
		// This pchannel keeps one registration per collection. Appending the
		// genesis anyway would create a vchannel that can never be written.
		// The issuer must place a target on a pchannel that holds no vchannel
		// of the collection, see routing.CheckSplitChannels. The source is
		// fenced already, so this refusal leaves the key range of the source
		// unwritable until an operator steps in: log it loudly.
		impl.shardManager.Logger().Error(ctx, "target vchannel of a shard split is refused, its pchannel already holds a vchannel of the collection",
			mlog.FieldCollectionID(header.GetCollectionId()), mlog.FieldVChannel(msg.VChannel()),
			mlog.String("holder", holder), mlog.Int64("splitTaskID", header.GetSplitTaskId()))
		return nil, status.NewUnrecoverableError("cannot create target vchannel %s of shard split task %d: the pchannel already holds vchannel %s of collection %d",
			msg.VChannel(), header.GetSplitTaskId(), holder, header.GetCollectionId())
	}

	origin := splitGenesis{header.GetSplitTaskId(), header.GetSourceVchannel(), msg.VChannel()}
	// Reserve before append: an error response can follow a durable WAL write.
	// An already registered ordinary vchannel has no split origin.
	previous, found := impl.genesis.Load(header.GetCollectionId())
	if !exists {
		previous, _ = impl.genesis.LoadOrStore(header.GetCollectionId(), origin)
	}
	if (exists && !found) || previous != origin {
		return nil, status.NewUnrecoverableError("conflicting genesis for collection %d, target %s, source %s, split task %d",
			header.GetCollectionId(), msg.VChannel(), header.GetSourceVchannel(), header.GetSplitTaskId())
	}

	msgID, err := appendOp(ctx, msg)
	if err != nil {
		return msgID, err
	}
	if exists {
		// The same genesis, appended again by the broadcaster. The vchannel is
		// registered already, with its function runners; nothing is created.
		impl.shardManager.Logger().Info(ctx, "target vchannel of a shard split already exists, its genesis was appended again",
			mlog.FieldCollectionID(header.GetCollectionId()), mlog.FieldVChannel(msg.VChannel()))
		return msgID, nil
	}
	created := messageutil.SplitShardGenesisMessage(msg.IntoImmutableMessage(msgID))
	impl.shardManager.CreateCollection(created)
	impl.allocFunctionRunners(header.GetCollectionId(), msg.VChannel(), impl.shardManager.GetCollectionSchemaSnapshot(header.GetCollectionId()))
	return msgID, nil
}

// handleBeginTxn refuses to open a transaction on the source of a shard split.
//
// Every body of such a transaction would be refused (a body is DML), so the
// transaction could only expire. The refusal is the fence error, so the writer
// routes the whole transaction to the new shards at once.
//
// The time tick interceptor runs before this one and has already opened a
// session for the Begin. It is ended here, while the shared lock of the
// vchannel is held. Every session of a fenced vchannel is dead: the fence
// ended those that were open when it was appended, and every later Begin is
// refused here. So ending the sessions of the vchannel ends exactly the
// session of this Begin and of Begins refused concurrently.
func (impl *shardInterceptor) handleBeginTxn(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	if !shards.IsSplitFenced(impl.shardManager, msg.VChannel()) {
		return appendOp(ctx, msg)
	}
	if impl.txnManager != nil {
		impl.txnManager.FailTxnAtVChannel(msg.VChannel())
	}
	return nil, status.NewShardFenced(msg.VChannel())
}
