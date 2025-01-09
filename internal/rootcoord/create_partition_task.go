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
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/log"
)

type createPartitionTask struct {
	baseTask
	Req      *milvuspb.CreatePartitionRequest
	collMeta *model.Collection
}

func (t *createPartitionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_CreatePartition); err != nil {
		return err
	}
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.GetTs())
	if err != nil {
		return err
	}
	t.collMeta = collMeta
	return checkGeneralCapacity(ctx, 0, 1, 0, t.core)
}

func (t *createPartitionTask) Execute(ctx context.Context) error {
	for _, partition := range t.collMeta.Partitions {
		if partition.PartitionName == t.Req.GetPartitionName() {
			log.Ctx(ctx).Warn("add duplicate partition", zap.String("collection", t.Req.GetCollectionName()), zap.String("partition", t.Req.GetPartitionName()), zap.Uint64("ts", t.GetTs()))
			return nil
		}
	}

	cfgMaxPartitionNum := Params.RootCoordCfg.MaxPartitionNum.GetAsInt()
	if len(t.collMeta.Partitions) >= cfgMaxPartitionNum {
		return fmt.Errorf("partition number (%d) exceeds max configuration (%d), collection: %s",
			len(t.collMeta.Partitions), cfgMaxPartitionNum, t.collMeta.Name)
	}

	partID, err := t.core.idAllocator.AllocOne()
	if err != nil {
		return err
	}
	partition := &model.Partition{
		PartitionID:               partID,
		PartitionName:             t.Req.GetPartitionName(),
		PartitionCreatedTimestamp: t.GetTs(),
		Extra:                     nil,
		CollectionID:              t.collMeta.CollectionID,
		State:                     pb.PartitionState_PartitionCreating,
	}

	return executeCreatePartitionTaskSteps(ctx, t.core, partition, t.collMeta, t.Req.GetDbName(), t.GetTs())
}

func (t *createPartitionTask) GetLockerKey() LockerKey {
	collection := t.core.getCollectionIDStr(t.ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), 0)
	return NewLockerKeyChain(
		NewClusterLockerKey(false),
		NewDatabaseLockerKey(t.Req.GetDbName(), false),
		NewCollectionLockerKey(collection, true),
	)
}

func executeCreatePartitionTaskSteps(ctx context.Context,
	core *Core,
	partition *model.Partition,
	col *model.Collection,
	dbName string,
	ts Timestamp,
) error {
	undoTask := newBaseUndoTask(core.stepExecutor)
	partID := partition.PartitionID
	collectionID := partition.CollectionID
	undoTask.AddStep(&expireCacheStep{
		baseStep:        baseStep{core: core},
		dbName:          dbName,
		collectionNames: []string{col.Name},
		collectionID:    collectionID,
		partitionName:   partition.PartitionName,
		ts:              ts,
		opts:            []proxyutil.ExpireCacheOpt{proxyutil.SetMsgType(commonpb.MsgType_CreatePartition)},
	}, &nullStep{})

	undoTask.AddStep(&addPartitionMetaStep{
		baseStep:  baseStep{core: core},
		partition: partition,
	}, &removePartitionMetaStep{
		baseStep:     baseStep{core: core},
		dbID:         col.DBID,
		collectionID: partition.CollectionID,
		partitionID:  partition.PartitionID,
		ts:           ts,
	})

	if streamingutil.IsStreamingServiceEnabled() {
		undoTask.AddStep(&broadcastCreatePartitionMsgStep{
			baseStep:  baseStep{core: core},
			vchannels: col.VirtualChannelNames,
			partition: partition,
			ts:        ts,
		}, &nullStep{})
	}

	undoTask.AddStep(&nullStep{}, &releasePartitionsStep{
		baseStep:     baseStep{core: core},
		collectionID: col.CollectionID,
		partitionIDs: []int64{partID},
	})

	undoTask.AddStep(&changePartitionStateStep{
		baseStep:     baseStep{core: core},
		collectionID: col.CollectionID,
		partitionID:  partID,
		state:        pb.PartitionState_PartitionCreated,
		ts:           ts,
	}, &nullStep{})

	return undoTask.Execute(ctx)
}
