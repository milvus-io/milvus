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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type markTempCollectionAsDeleteTask struct {
	tempCollectionTask
}

func (t *markTempCollectionAsDeleteTask) Execute(ctx context.Context) error {
	ts := t.GetTs()

	redoTask := newBaseRedoTask(t.core.stepExecutor)

	redoTask.AddSyncStep(&changeCollectionStateStep{
		baseStep:     baseStep{core: t.core},
		collectionID: t.collInfo.CollectionID,
		state:        pb.CollectionState_CollectionDropping,
		ts:           ts,
	})

	return redoTask.Execute(ctx)
}

type tempCollectionTask struct {
	baseTask
	CollectionIDs *indexpb.CollectionWithTempRequest
	TruncateReq   *rootcoordpb.TruncateCollectionRequest
	collInfo      *model.Collection
}

func (t *tempCollectionTask) validate(ctx context.Context) error {
	if t.TruncateReq != nil {
		if err := CheckMsgType(t.TruncateReq.GetBase().GetMsgType(), commonpb.MsgType_DropCollection); err != nil {
			return err
		}
	}
	return nil
}

func (t *tempCollectionTask) Prepare(ctx context.Context) error {
	err := t.validate(ctx)
	if err != nil {
		return err
	}
	if t.CollectionIDs == nil {
		collInfo, err := t.core.meta.GetCollectionByName(ctx, t.TruncateReq.GetDbName(), t.TruncateReq.GetCollectionName(), typeutil.MaxTimestamp)
		if err != nil && !errors.Is(err, merr.ErrCollectionNotFound) {
			return err
		}
		log.Debug("get the meta of target collection by name, may be dropped already", zap.String("collectionName", t.TruncateReq.CollectionName))
		t.collInfo, err = t.core.meta.GetCollectionByName(ctx, t.TruncateReq.GetDbName(), util.GenerateTempCollectionName(t.TruncateReq.GetCollectionName()), typeutil.MaxTimestamp)
		if err == nil {
			t.CollectionIDs = &indexpb.CollectionWithTempRequest{
				CollectionID:     t.collInfo.CollectionID,
				TempCollectionID: t.collInfo.CollectionID,
			}
			if collInfo != nil {
				t.CollectionIDs.CollectionID = collInfo.CollectionID
			}
			log.Debug("get the meta of temp collection by name", zap.String("collectionName", t.TruncateReq.CollectionName),
				zap.Any("tempCollectionInfo", collInfo), zap.Int64("collectionID", t.CollectionIDs.CollectionID), zap.Int64("tempCollectionID", t.CollectionIDs.TempCollectionID))
		}
	} else {
		t.collInfo, err = t.core.meta.GetCollectionByID(ctx, "", t.CollectionIDs.TempCollectionID, typeutil.MaxTimestamp, true)
		if err == nil {
			log.Debug("get the meta of temp collection by id",
				zap.Int64("collectionID", t.CollectionIDs.CollectionID),
				zap.Int64("tempCollectionID", t.CollectionIDs.TempCollectionID),
				zap.Any("tempCollectionInfo", t.collInfo))
			if t.collInfo.Name != util.GenerateTempCollectionName(t.TruncateReq.CollectionName) {
				err = fmt.Errorf("temp collection may be modified")
			} else if t.CollectionIDs.CollectionID == 0 {
				t.CollectionIDs.CollectionID = t.CollectionIDs.TempCollectionID
			}
		}
	}
	log.Debug("get the meta of temp collection done", zap.String("collectionName", t.TruncateReq.CollectionName), zap.Error(err))
	return err
}

type dropTempCollectionTask struct {
	tempCollectionTask
}

func (t *dropTempCollectionTask) Execute(ctx context.Context) error {
	ts := t.GetTs()

	redoTask := newBaseRedoTask(t.core.stepExecutor)

	redoTask.AddAsyncStep(&releaseCollectionStep{
		baseStep:     baseStep{core: t.core},
		collectionID: t.collInfo.CollectionID,
	})
	redoTask.AddAsyncStep(&dropIndexTempStep{
		baseStep:         baseStep{core: t.core},
		collectionID:     t.CollectionIDs.CollectionID,
		tempCollectionID: t.CollectionIDs.TempCollectionID,
	})
	redoTask.AddAsyncStep(&deleteCollectionDataStep{
		baseStep: baseStep{core: t.core},
		coll:     t.collInfo,
		// isSkip:   t.CollectionIDs.GetBase().GetReplicateInfo().GetIsReplicate(),
	})
	redoTask.AddAsyncStep(&removeDmlChannelsStep{
		baseStep:  baseStep{core: t.core},
		pChannels: t.collInfo.PhysicalChannelNames,
	})
	redoTask.AddAsyncStep(newConfirmGCStep(t.core, t.collInfo.CollectionID, allPartition))
	redoTask.AddAsyncStep(&deleteCollectionMetaStep{
		baseStep:     baseStep{core: t.core},
		collectionID: t.collInfo.CollectionID,
		// This ts is less than the ts when we notify data nodes to drop collection, but it's OK since we have already
		// marked this collection as deleted. If we want to make this ts greater than the notification's ts, we should
		// wrap a step who will have these three children and connect them with ts.
		ts: ts,
	})

	return redoTask.Execute(ctx)
}

type dropIndexTempStep struct {
	baseStep
	collectionID     UniqueID
	tempCollectionID UniqueID
	times            int64
}

func (s *dropIndexTempStep) Execute(ctx context.Context) ([]nestedStep, error) {
	err := s.core.broker.DropTempCollectionIndexes(ctx, s.collectionID, s.tempCollectionID)
	s.times++
	return nil, err
}

func (s *dropIndexTempStep) Desc() string {
	return fmt.Sprintf("drop temp collection's indexes, collectionID: %d, tempCollectionID: %d, times: %d", s.collectionID, s.tempCollectionID, s.times)
}

func (s *dropIndexTempStep) Weight() stepPriority {
	return stepPriorityNormal
}
