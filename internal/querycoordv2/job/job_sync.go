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

package job

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
)

type SyncNewCreatedPartitionJob struct {
	*BaseJob
	req            *querypb.SyncNewCreatedPartitionRequest
	meta           *meta.Meta
	cluster        session.Cluster
	broker         meta.Broker
	targetObserver *observers.TargetObserver
	targetMgr      meta.TargetManagerInterface
}

func NewSyncNewCreatedPartitionJob(
	ctx context.Context,
	req *querypb.SyncNewCreatedPartitionRequest,
	meta *meta.Meta,
	broker meta.Broker,
	targetObserver *observers.TargetObserver,
	targetMgr meta.TargetManagerInterface,
) *SyncNewCreatedPartitionJob {
	return &SyncNewCreatedPartitionJob{
		BaseJob:        NewBaseJob(ctx, req.Base.GetMsgID(), req.GetCollectionID()),
		req:            req,
		meta:           meta,
		broker:         broker,
		targetObserver: targetObserver,
		targetMgr:      targetMgr,
	}
}

func (job *SyncNewCreatedPartitionJob) PreExecute() error {
	return nil
}

func (job *SyncNewCreatedPartitionJob) Execute() error {
	req := job.req
	log := log.Ctx(job.ctx).With(
		zap.Int64("collectionID", req.GetCollectionID()),
		zap.Int64("partitionID", req.GetPartitionID()),
	)

	// check if collection not load or loadType is loadPartition
	collection := job.meta.GetCollection(job.ctx, job.req.GetCollectionID())
	if collection == nil || collection.GetLoadType() == querypb.LoadType_LoadPartition {
		return nil
	}

	// check if partition already existed
	if partition := job.meta.GetPartition(job.ctx, job.req.GetPartitionID()); partition != nil {
		return nil
	}

	partition := &meta.Partition{
		PartitionLoadInfo: &querypb.PartitionLoadInfo{
			CollectionID: req.GetCollectionID(),
			PartitionID:  req.GetPartitionID(),
			Status:       querypb.LoadStatus_Loaded,
		},
		LoadPercentage: 100,
		CreatedAt:      time.Now(),
	}
	err := job.meta.CollectionManager.PutPartition(job.ctx, partition)
	if err != nil {
		msg := "failed to store partitions"
		log.Warn(msg, zap.Error(err))
		return errors.Wrap(err, msg)
	}

	return waitCurrentTargetUpdated(job.ctx, job.targetObserver, job.req.GetCollectionID())
}
