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

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// describeSplitCollection reads rootcoord's current record of a collection and
// its partition snapshot, as the model the split checks judge. A dropped
// collection is answered with merr.ErrCollectionNotFound.
func (s *Server) describeSplitCollection(ctx context.Context, collectionID int64) (*splitCollection, error) {
	resp, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	partitionIDs, err := s.broker.ShowPartitionsInternal(ctx, collectionID)
	if err != nil {
		return nil, err
	}
	return splitCollectionFromDescribe(resp, partitionIDs), nil
}

// startSplitCollectionBroadcast takes the collection's resource keys -- the
// shared database name and the exclusive collection name, which every DDL of
// the collection takes -- and returns the collection as rootcoord records it
// UNDER them.
//
// The keys are names, so the name is read first to take them and the record is
// read again once they are held: what a split is built and checked against must
// be the meta no other DDL of the collection can change until its broadcast is
// issued. A rename in between leaves the keys naming something else, and is
// refused as retriable; the next attempt takes the new name's keys.
func (s *Server) startSplitCollectionBroadcast(ctx context.Context, collectionID int64) (broadcaster.BroadcastAPI, *splitCollection, error) {
	named, err := s.broker.DescribeCollectionInternal(ctx, collectionID)
	if err != nil {
		return nil, nil, err
	}
	api, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(named.GetDbName()),
		message.NewExclusiveCollectionNameResourceKey(named.GetDbName(), named.GetCollectionName()))
	if err != nil {
		return nil, nil, err
	}
	coll, err := s.describeSplitCollection(ctx, collectionID)
	if err != nil {
		api.Close()
		return nil, nil, err
	}
	if coll.DBName != named.GetDbName() || coll.Name != named.GetCollectionName() {
		api.Close()
		return nil, nil, merr.WrapErrServiceUnavailableMsg(
			"collection %d was renamed from %s.%s to %s.%s while its resource keys were being taken",
			collectionID, named.GetDbName(), named.GetCollectionName(), coll.DBName, coll.Name)
	}
	return api, coll, nil
}

// issueShardSplit issues a planned split task's write switch: the one
// SplitShard broadcast that fences the source, creates the two targets and
// carries the routing post-image (design doc §6.1 step 2).
//
// Everything runs under the collection's resource keys, which the broadcast
// then holds until its ack callback returns, so the meta the message is built
// and checked against is the meta it is applied to:
//
//   - the task store is asked first. A record under this id that names another
//     collection or another source is two splits sharing an id, which every
//     check below would pass and DataCoord's CommitShardSplit would refuse only
//     after the fence. A record the ack callback has already fenced needs no
//     broadcast at all;
//   - streaming.NewSplitShardBroadcastMessage runs every message-only check
//     and refuses while dataCoord.shardSplit.enable is off;
//   - streaming.CheckSplitShardAgainstCollection makes, against the meta held
//     under the keys, every refusal the ack callback and the routing apply
//     would otherwise make only once the source is fenced, when a refusal can
//     only be retried forever.
//
// The broadcast is deduplicated by the task id, so a retry of one that landed
// is the same broadcast. Every error is returned for the next tick to retry;
// merr.ErrCollectionNotFound says the collection is gone.
func (s *Server) issueShardSplit(ctx context.Context, task *datapb.SplitShardTask, controlChannel string) error {
	api, coll, err := s.startSplitCollectionBroadcast(ctx, task.GetCollectionId())
	if err != nil {
		return err
	}
	defer api.Close()

	fenced, err := s.checkSplitTaskRecord(task)
	if err != nil {
		return err
	}
	if fenced {
		return nil
	}
	if reason := splitRefusalReason(coll.schema); reason != "" {
		// Checked again under the collection's keys: a schema change raced the
		// target allocation. The task is past its abort point, so it waits.
		return merr.WrapErrOperationNotSupportedMsg("refuse to issue shard split task %d: %s", task.GetTaskId(), reason)
	}
	param, err := buildSplitShardParam(task, coll, controlChannel)
	if err != nil {
		return err
	}
	msg, err := streaming.NewSplitShardBroadcastMessage(param)
	if err != nil {
		return err
	}
	typed := message.MustAsSpecializedBroadcastMessage[*message.SplitShardMessageHeader, *message.SplitShardMessageBody](msg)
	if err := streaming.CheckSplitShardAgainstCollection(coll.Collection, typed.Header(), typed.MustBody()); err != nil {
		return err
	}
	if _, err := api.Broadcast(ctx, msg); err != nil {
		return merr.Wrapf(err, "broadcast the write switch of shard split task %d", task.GetTaskId())
	}
	mlog.Info(ctx, "shard split write switch broadcast",
		mlog.FieldCollectionID(task.GetCollectionId()),
		mlog.Int64("splitTaskID", task.GetTaskId()),
		mlog.String("source", param.SourceVChannel),
		mlog.Strings("targets", param.TargetVChannels),
		mlog.Uint64("routingModulus", param.Routing.GetRoutingModulus()))
	return nil
}

// checkSplitTaskRecord compares the task about to be broadcast with what the
// store records under its id, and reports whether the ack callback has already
// recorded its fence (nothing is left to broadcast).
func (s *Server) checkSplitTaskRecord(task *datapb.SplitShardTask) (fenced bool, err error) {
	recorded, ok := s.shardSplitTasks.get(task.GetTaskId())
	if !ok {
		return false, merr.WrapErrServiceInternalMsg("shard split task %d is not recorded, refuse to broadcast it", task.GetTaskId())
	}
	if recorded.GetCollectionId() != task.GetCollectionId() {
		return false, merr.WrapErrServiceInternalMsg(
			"shard split task %d is recorded on collection %d, refuse to broadcast it for collection %d",
			task.GetTaskId(), recorded.GetCollectionId(), task.GetCollectionId())
	}
	if len(task.GetSources()) != 1 || splitTaskSource(recorded) != splitTaskSource(task) {
		return false, merr.WrapErrServiceInternalMsg(
			"shard split task %d is recorded with source %v, refuse to broadcast it with source %v",
			task.GetTaskId(), splitSourceVChannels(recorded), splitSourceVChannels(task))
	}
	return recorded.GetFenced(), nil
}
