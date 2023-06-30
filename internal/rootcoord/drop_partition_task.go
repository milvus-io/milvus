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
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
)

type dropPartitionTask struct {
	baseTask
	Req      *milvuspb.DropPartitionRequest
	collMeta *model.Collection
}

func (t *dropPartitionTask) Prepare(ctx context.Context) error {
	if err := CheckMsgType(t.Req.GetBase().GetMsgType(), commonpb.MsgType_DropPartition); err != nil {
		return err
	}
	if t.Req.GetPartitionName() == Params.CommonCfg.DefaultPartitionName.GetValue() {
		return fmt.Errorf("default partition cannot be deleted")
	}
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.GetDbName(), t.Req.GetCollectionName(), t.GetTs())
	if err != nil {
		// Is this idempotent?
		return err
	}
	t.collMeta = collMeta
	return nil
}

func (t *dropPartitionTask) Execute(ctx context.Context) error {
	partID := common.InvalidPartitionID
	for _, partition := range t.collMeta.Partitions {
		if partition.PartitionName == t.Req.GetPartitionName() {
			partID = partition.PartitionID
			break
		}
	}
	if partID == common.InvalidPartitionID {
		log.Warn("drop an non-existent partition", zap.String("collection", t.Req.GetCollectionName()), zap.String("partition", t.Req.GetPartitionName()))
		// make dropping partition idempotent.
		return nil
	}

	redoTask := newBaseRedoTask(t.core.stepExecutor)

	redoTask.AddSyncStep(&expireCacheStep{
		baseStep:        baseStep{core: t.core},
		dbName:          t.Req.GetDbName(),
		collectionNames: []string{t.collMeta.Name},
		collectionID:    t.collMeta.CollectionID,
		ts:              t.GetTs(),
	})
	redoTask.AddSyncStep(&changePartitionStateStep{
		baseStep:     baseStep{core: t.core},
		collectionID: t.collMeta.CollectionID,
		partitionID:  partID,
		state:        pb.PartitionState_PartitionDropping,
		ts:           t.GetTs(),
	})

	redoTask.AddAsyncStep(&deletePartitionDataStep{
		baseStep: baseStep{core: t.core},
		pchans:   t.collMeta.PhysicalChannelNames,
		partition: &model.Partition{
			PartitionID:   partID,
			PartitionName: t.Req.GetPartitionName(),
			CollectionID:  t.collMeta.CollectionID,
		},
	})
	redoTask.AddAsyncStep(newConfirmGCStep(t.core, t.collMeta.CollectionID, partID))
	redoTask.AddAsyncStep(&removePartitionMetaStep{
		baseStep:     baseStep{core: t.core},
		dbID:         t.collMeta.DBID,
		collectionID: t.collMeta.CollectionID,
		partitionID:  partID,
		// This ts is less than the ts when we notify data nodes to drop partition, but it's OK since we have already
		// marked this partition as deleted. If we want to make this ts greater than the notification's ts, we should
		// wrap a step who will have these children and connect them with ts.
		ts: t.GetTs(),
	})

	return redoTask.Execute(ctx)
}
