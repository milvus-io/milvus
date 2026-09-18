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
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// splitCollection is the collection meta a split is planned, issued and adopted
// against: rootcoord's current record as a model.Collection -- the type
// streaming.CheckSplitShardAgainstCollection and routing.JudgeCommit judge --
// together with the partition snapshot and the schema the SplitShard message
// carries.
type splitCollection struct {
	*model.Collection
	// schema is the collection's schema as DescribeCollection returns it, with
	// the collection properties on it (the genesis the targets start from, and
	// what namespace admission reads before the fence).
	schema *schemapb.CollectionSchema
	// partitionIDs is the current partition snapshot.
	partitionIDs []int64
}

// splitCollectionFromDescribe converts rootcoord's DescribeCollection answer
// into the model the split checks judge.
//
// DescribeCollection is what rootcoord's own convertModelToDesc produces from
// its model.Collection, and it carries everything the checks read: the vchannel
// and pchannel lists, every shard's state and residues, the routing modulus,
// shard_by, the properties and enable_namespace. The conversion goes through
// model.UnmarshalCollectionModel, the one decoder of a persisted collection, so
// the shard infos are read exactly as rootcoord reads them. The model is
// always marked available. That is not a claim about the collection:
// DescribeCollectionInternal also answers for a Dropping collection
// (allowUnavailable), and such a collection reaches here as if available. It
// is benign: nothing here applies anything. A write switch or an adoption
// broadcast of a Dropping collection is ignored by its ack callback, which
// loads the collection itself and stops on one that is not available, and a
// dropped collection is answered CollectionNotFound, which finishes the task.
func splitCollectionFromDescribe(resp *milvuspb.DescribeCollectionResponse, partitionIDs []int64) *splitCollection {
	coll := model.UnmarshalCollectionModel(&etcdpb.CollectionInfo{
		ID:                   resp.GetCollectionID(),
		DbId:                 resp.GetDbId(),
		Schema:               resp.GetSchema(),
		VirtualChannelNames:  resp.GetVirtualChannelNames(),
		PhysicalChannelNames: resp.GetPhysicalChannelNames(),
		ShardsNum:            resp.GetShardsNum(),
		ConsistencyLevel:     resp.GetConsistencyLevel(),
		CreateTime:           resp.GetCreatedTimestamp(),
		StartPositions:       resp.GetStartPositions(),
		State:                etcdpb.CollectionState_CollectionCreated,
		Properties:           resp.GetProperties(),
		ShardInfos:           resp.GetShardInfos(),
		RoutingModulus:       resp.GetRoutingModulus(),
		ShardBy:              resp.GetShardBy(),
	})
	coll.Name = resp.GetCollectionName()
	coll.DBName = resp.GetDbName()
	for _, partitionID := range partitionIDs {
		coll.Partitions = append(coll.Partitions, &model.Partition{PartitionID: partitionID, CollectionID: coll.CollectionID})
	}
	return &splitCollection{
		Collection:   coll,
		schema:       resp.GetSchema(),
		partitionIDs: slices.Clone(partitionIDs),
	}
}

// shardByOf is the routing-key expression a split commits. A collection that
// already declares one keeps it; one created before shard_by existed is split
// by its primary key, the only key this branch splits by (design doc §1.3), and
// its first split is what records it.
func shardByOf(coll *splitCollection) (string, error) {
	if coll.ShardBy != "" {
		return coll.ShardBy, nil
	}
	for _, field := range coll.schema.GetFields() {
		if field.GetIsPrimaryKey() {
			return "hash(" + field.GetName() + ")", nil
		}
	}
	return "", merr.WrapErrServiceInternalMsg("collection %d declares no primary key to route by", coll.CollectionID)
}

// splitTargetVChannels lists a task's target vchannel names.
func splitTaskTargetVChannels(task *datapb.SplitShardTask) []string {
	out := make([]string, 0, len(task.GetTargets()))
	for _, target := range task.GetTargets() {
		out = append(out, target.GetVchannel())
	}
	return out
}

// splitTaskSource is the one source vchannel of a split task, "" for a task
// that names none.
func splitTaskSource(task *datapb.SplitShardTask) string {
	if len(task.GetSources()) == 0 {
		return ""
	}
	return task.GetSources()[0].GetVchannel()
}

// pchannelAt is the pchannel of the i-th listed vchannel, as the collection
// records it.
func pchannelAt(coll *model.Collection, i int) string {
	if i < len(coll.PhysicalChannelNames) && coll.PhysicalChannelNames[i] != "" {
		return coll.PhysicalChannelNames[i]
	}
	return funcutil.ToPhysicalChannel(coll.VirtualChannelNames[i])
}

// listedShardInfo is the i-th listed shard's info as the collection records it,
// named by its vchannel. A legacy shard without one is Normal.
func listedShardInfo(coll *model.Collection, i int) *schemapb.CollectionShardInfo {
	vchannel := coll.VirtualChannelNames[i]
	info := &schemapb.CollectionShardInfo{VchannelName: vchannel}
	if shard, ok := coll.ShardInfos[vchannel]; ok && shard != nil {
		info = shard.ToPB()
		info.VchannelName = vchannel
	}
	return info
}

// buildSplitPostImage builds the routing post-image a split's write switch
// carries: the collection as it stands plus exactly the split's own delta --
// its source fenced (Splitting, owning nothing) and its two targets created
// (Creating, owning the residues the task planned). Every other shard keeps its
// state and last truncate tick, and its residues re-expressed at the task's
// modulus, which differs from the collection's only when the split doubled it.
//
// The modulus comes from the task, never re-derived: the targets' residues are
// taken against it, and a modulus computed from meta a split has changed would
// disagree with them.
func buildSplitPostImage(task *datapb.SplitShardTask, coll *splitCollection) (*messagespb.AlterCollectionMessageUpdates, error) {
	source := splitTaskSource(task)
	after := task.GetRoutingModulus()
	if after == 0 {
		return nil, merr.WrapErrServiceInternalMsg("shard split task %d carries no routing modulus", task.GetTaskId())
	}
	before, err := residuesOf(coll.Collection)
	if err != nil {
		return nil, err
	}
	shardBy, err := shardByOf(coll)
	if err != nil {
		return nil, err
	}
	updates := &messagespb.AlterCollectionMessageUpdates{RoutingModulus: after, ShardBy: shardBy}
	for i, vchannel := range coll.VirtualChannelNames {
		info := listedShardInfo(coll.Collection, i)
		switch {
		case vchannel == source:
			// Fenced: its key space belongs to the targets now.
			info.State = schemapb.ShardState_ShardSplitting
			info.Routing = nil
		case slices.Contains(splitTaskTargetVChannels(task), vchannel):
			// Already listed: the split has been committed; the target is
			// written below exactly as the task planned it.
			continue
		default:
			if own, ok := before.byVChannel[vchannel]; ok {
				rebased, err := rebaseResidues(own, before.modulus, after)
				if err != nil {
					return nil, merr.Wrapf(err, "re-express shard %s for shard split task %d", vchannel, task.GetTaskId())
				}
				info.Routing = &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: rebased}}
			}
		}
		updates.VirtualChannelNames = append(updates.VirtualChannelNames, vchannel)
		updates.PhysicalChannelNames = append(updates.PhysicalChannelNames, pchannelAt(coll.Collection, i))
		updates.ShardInfos = append(updates.ShardInfos, info)
	}
	for _, target := range task.GetTargets() {
		updates.VirtualChannelNames = append(updates.VirtualChannelNames, target.GetVchannel())
		updates.PhysicalChannelNames = append(updates.PhysicalChannelNames, funcutil.ToPhysicalChannel(target.GetVchannel()))
		updates.ShardInfos = append(updates.ShardInfos, &schemapb.CollectionShardInfo{
			State:        schemapb.ShardState_ShardCreating,
			VchannelName: target.GetVchannel(),
			Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: slices.Clone(target.GetBuckets())},
			},
		})
	}
	return updates, nil
}

// buildSplitShardParam builds a planned task's write switch against the
// collection as it stands.
func buildSplitShardParam(task *datapb.SplitShardTask, coll *splitCollection, controlChannel string) (streaming.SplitShardParam, error) {
	postImage, err := buildSplitPostImage(task, coll)
	if err != nil {
		return streaming.SplitShardParam{}, err
	}
	return streaming.SplitShardParam{
		CollectionID:    task.GetCollectionId(),
		SplitTaskID:     task.GetTaskId(),
		SourceVChannel:  splitTaskSource(task),
		TargetVChannels: splitTaskTargetVChannels(task),
		Schema:          coll.schema,
		PartitionIDs:    slices.Clone(coll.partitionIDs),
		Routing:         postImage,
		ControlChannel:  controlChannel,
	}, nil
}
