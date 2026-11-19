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
// Package shardsplit builds and checks the SplitShard message, the single WAL
// message of an online shard split.
//
// The issuer of a split and the consumer of its message run the same checks:
// the issuer on the plan before the broadcast, the consumer on the message
// before it applies it. A message that passed the first passes the second
// unless the collection changed in between, which the exclusive resource key
// of the collection prevents on the cluster that issues the split.
package shardsplit

import (
	"context"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Plan is one shard split and everything its SplitShard message is built
// from. The issuer reads the collection fields while it holds the exclusive
// resource key of the collection.
type Plan struct {
	// SplitTaskID is the id of the split, allocated by the issuer. Never zero.
	SplitTaskID int64

	CollectionID   int64
	CollectionName string
	DBID           int64
	DBName         string
	// Schema is the complete schema of the collection, as
	// DescribeCollectionInternal returns it, with the collection properties.
	Schema *schemapb.CollectionSchema
	// PartitionIDs are the partitions of the collection that accept DML. Each
	// target vchannel is created with them.
	PartitionIDs []int64
	// Current is the routing of the collection before the split.
	Current routing.Routing
	// SplitSources are the vchannels of the collection that were split before.
	SplitSources []string

	// Split names the source, the two targets with the residues each owns,
	// and the routing modulus after the split. routing.PlanSplit builds one.
	Split routing.Split
}

// NewBroadcastMessage checks a plan and builds its SplitShard message.
//
// The message is broadcast to the source and the two targets; the broadcaster
// adds the control channel. A shard the split leaves alone gets no copy:
// nothing there would consume it. The source is named append-first, so that
// the genesis of each target is above the fence of the source. The message is
// replicable, so that a secondary cluster ends with the same shards.
func NewBroadcastMessage(plan *Plan) (message.BroadcastMutableMessage, error) {
	if plan.SplitTaskID == 0 {
		return nil, merr.WrapErrServiceInternal("a shard split needs a split task id")
	}
	if plan.Schema == nil {
		return nil, merr.WrapErrServiceInternalMsg("shard split task %d carries no collection schema", plan.SplitTaskID)
	}
	next, err := plan.Current.ApplySplit(plan.Split)
	if err != nil {
		return nil, err
	}
	targets := make([]string, 0, len(plan.Split.Targets))
	for _, target := range plan.Split.Targets {
		targets = append(targets, target.VChannel)
	}

	// The schema a target vchannel starts with. collection.on.truncating is
	// state of the coordinators and is never part of the schema of a vchannel.
	schema := proto.Clone(plan.Schema).(*schemapb.CollectionSchema)
	schema.Properties = slices.DeleteFunc(schema.Properties, func(property *commonpb.KeyValuePair) bool {
		return property.GetKey() == common.CollectionOnTruncatingKey
	})

	header := &message.SplitShardMessageHeader{
		CollectionId:    plan.CollectionID,
		SplitTaskId:     plan.SplitTaskID,
		SourceVchannel:  plan.Split.Source,
		TargetVchannels: targets,
	}
	body := &message.SplitShardMessageBody{
		GenesisHeader: &message.CreateCollectionMessageHeader{
			CollectionId: plan.CollectionID,
			PartitionIds: slices.Clone(plan.PartitionIDs),
			DbId:         plan.DBID,
		},
		Genesis: &message.CreateCollectionRequest{
			DbName:           plan.DBName,
			CollectionName:   plan.CollectionName,
			DbID:             plan.DBID,
			CollectionID:     plan.CollectionID,
			PartitionIDs:     slices.Clone(plan.PartitionIDs),
			CollectionSchema: schema,
		},
		Routing: &message.SplitShardRouting{
			RoutingModulus: next.Modulus,
			ShardBy:        next.ShardBy,
			Shards:         next.ShardInfos(),
		},
	}
	// The very checks the consumer runs, on the very header and body that go
	// into the WAL.
	if err := CheckMessage(header, body); err != nil {
		return nil, err
	}
	if err := CheckAgainstCollection(plan.Current, plan.SplitSources, plan.Schema.GetProperties(), header, body); err != nil {
		return nil, err
	}
	msg, err := message.NewSplitShardMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast(append([]string{plan.Split.Source}, targets...), message.OptBuildBroadcastAppendFirst(plan.Split.Source)).
		BuildBroadcast()
	if err != nil {
		return nil, merr.Wrap(err, "build the split shard message")
	}
	return msg, nil
}

// CheckMessage checks that a SplitShard message is complete and consistent in
// itself. Every refusal is a System error: the message is built by a
// coordinator.
func CheckMessage(header *message.SplitShardMessageHeader, body *message.SplitShardMessageBody) error {
	if header.GetSplitTaskId() == 0 {
		return merr.WrapErrServiceInternal("split shard message carries no split task id")
	}
	if header.GetSourceVchannel() == "" {
		return merr.WrapErrServiceInternalMsg("split shard message of task %d names no source vchannel", header.GetSplitTaskId())
	}
	if len(header.GetTargetVchannels()) != routing.SplitTargetCount {
		return merr.WrapErrServiceInternalMsg("split shard message of task %d names %d targets, a split creates exactly %d",
			header.GetSplitTaskId(), len(header.GetTargetVchannels()), routing.SplitTargetCount)
	}
	if body.GetGenesisHeader() == nil || body.GetGenesis().GetCollectionSchema() == nil {
		return merr.WrapErrServiceInternalMsg("split shard message of task %d carries no creation payload for its targets", header.GetSplitTaskId())
	}
	if body.GetGenesisHeader().GetCollectionId() != header.GetCollectionId() {
		return merr.WrapErrServiceInternalMsg("split shard message of task %d splits collection %d but its targets are created for collection %d",
			header.GetSplitTaskId(), header.GetCollectionId(), body.GetGenesisHeader().GetCollectionId())
	}
	next, err := RoutingOf(body)
	if err != nil {
		return err
	}
	if _, err := next.Table(); err != nil {
		return err
	}
	vchannels := next.VChannels()
	if slices.Contains(vchannels, header.GetSourceVchannel()) {
		return merr.WrapErrServiceInternalMsg("the routing after shard split task %d still lists its source %s",
			header.GetSplitTaskId(), header.GetSourceVchannel())
	}
	for _, target := range header.GetTargetVchannels() {
		if !slices.Contains(vchannels, target) {
			return merr.WrapErrServiceInternalMsg("the routing after shard split task %d does not list its target %s",
				header.GetSplitTaskId(), target)
		}
	}
	return nil
}

// CheckAgainstCollection checks a SplitShard message against the collection
// it splits: current is the routing of the collection before the split,
// splitSources are its vchannels that were split before, and properties are
// its properties.
func CheckAgainstCollection(
	current routing.Routing,
	splitSources []string,
	properties []*commonpb.KeyValuePair,
	header *message.SplitShardMessageHeader,
	body *message.SplitShardMessageBody,
) error {
	if err := routing.CheckSplitAdmission(properties); err != nil {
		return err
	}
	if err := routing.CheckSplitChannels(current.VChannels(), splitSources, header.GetSourceVchannel(), header.GetTargetVchannels()); err != nil {
		return err
	}
	next, err := RoutingOf(body)
	if err != nil {
		return err
	}
	return routing.CheckSplitPostImage(current, next, header.GetSourceVchannel(), header.GetTargetVchannels())
}

// RoutingOf returns the routing of the collection after the split, as the
// message carries it.
func RoutingOf(body *message.SplitShardMessageBody) (routing.Routing, error) {
	return routing.RoutingFromShardInfos(
		body.GetRouting().GetRoutingModulus(), body.GetRouting().GetShardBy(), body.GetRouting().GetShards())
}

// Broadcast issues a shard split of the collection dbName.collectionName.
//
// It takes the exclusive resource key of the collection, which keeps every
// other DDL of the collection out until the ack callback of the split has
// run. plan is called while the key is held: it must read the collection then,
// not before, and return the plan of the split. The message is built from the
// plan and broadcast. The call returns when every copy is appended and the ack
// callback has run.
//
// It fails on a cluster that is not the primary: a secondary receives the
// split through replication.
func Broadcast(ctx context.Context, dbName string, collectionName string, plan func(ctx context.Context) (*Plan, error)) (*types.BroadcastAppendResult, error) {
	api, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(dbName),
		message.NewExclusiveCollectionNameResourceKey(dbName, collectionName),
	)
	if err != nil {
		return nil, merr.Wrap(err, "start the broadcast of a shard split")
	}
	defer api.Close()

	p, err := plan(ctx)
	if err != nil {
		return nil, err
	}
	msg, err := NewBroadcastMessage(p)
	if err != nil {
		return nil, err
	}
	return api.Broadcast(ctx, msg)
}
