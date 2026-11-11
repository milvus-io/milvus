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
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// PrepareRestoreCollection allocates the complete creation recipe while the
// caller owns the target collection-name broadcast lock. It publishes nothing.
func (c *Core) PrepareRestoreCollection(ctx context.Context, req *milvuspb.CreateCollectionRequest, partitions []string) (*messagespb.RestoreSnapshotMessageBody, func(), error) {
	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(req.GetSchema(), schema); err != nil {
		return nil, nil, err
	}
	if req.GetShardsNum() <= 0 {
		req.ShardsNum = common.DefaultShardsNum
	}
	if _, err := typeutil.GetPartitionKeyFieldSchema(schema); err == nil {
		if req.GetNumPartitions() <= 0 {
			req.NumPartitions = common.DefaultPartitionsWithPartitionKey
		}
	} else {
		req.NumPartitions = 1
	}
	task := &createCollectionTask{
		Core: c, Req: req, preserveFieldID: true,
		header: &message.CreateCollectionMessageHeader{},
		body:   &message.CreateCollectionRequest{DbName: req.GetDbName(), CollectionName: req.GetCollectionName(), CollectionSchema: schema},
	}
	if err := task.Prepare(ctx); err != nil {
		task.releaseFileResources()
		return nil, nil, err
	}
	cleanup := task.releaseFileResources
	// Preparation already creates default/partition-key partitions. Include all
	// other snapshot partitions in the same message, with deterministic ordering.
	slices.Sort(partitions)
	for _, name := range partitions {
		if slices.Contains(task.body.PartitionNames, name) {
			continue
		}
		id, err := c.idAllocator.AllocOne()
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		task.body.PartitionNames = append(task.body.PartitionNames, name)
		task.body.PartitionIDs = append(task.body.PartitionIDs, id)
		task.header.PartitionIds = append(task.header.PartitionIds, id)
	}
	if len(task.body.PartitionIDs) > Params.RootCoordCfg.MaxPartitionNum.GetAsInt() {
		cleanup()
		return nil, nil, merr.WrapErrParameterInvalidMsg("snapshot partition count exceeds collection limit")
	}
	return &messagespb.RestoreSnapshotMessageBody{CollectionHeader: task.header, Collection: task.body}, cleanup, nil
}

// ApplyRestoreCollection reuses creation persistence without another broadcast.
func (c *Core) ApplyRestoreCollection(ctx context.Context, result message.BroadcastResultRestoreSnapshotMessageV2) error {
	body := result.Message.MustBody()
	creation := message.NewCreateCollectionMessageBuilderV1().WithHeader(body.GetCollectionHeader()).
		WithBody(body.GetCollection()).WithBroadcast(body.GetCollection().GetVirtualChannelNames()).MustBuildBroadcast()
	callback := &DDLCallback{Core: c}
	return callback.applyCollectionCreation(ctx, message.BroadcastResultCreateCollectionMessageV1{
		Message: message.MustAsBroadcastCreateCollectionMessageV1(creation), Results: result.Results,
	}, etcdpb.CollectionState_CollectionRestoring)
}

func (c *Core) CompleteRestoreCollection(ctx context.Context, collectionID int64) error {
	return c.meta.CompleteRestoreCollection(ctx, collectionID)
}

// Restore admission includes unavailable names, which remain reserved on retry.
func (c *Core) CheckRestoreTargetAbsent(ctx context.Context, db, name string) error {
	_, err := c.meta.GetCollectionByName(ctx, db, name, typeutil.MaxTimestamp, true)
	if err == nil {
		return merr.WrapErrParameterInvalidMsg("target collection %s already exists in database %s", name, db)
	}
	return err
}
