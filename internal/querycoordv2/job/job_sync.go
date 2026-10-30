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

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

	// check if collection not load or loadType is loadPartition
	collection := job.meta.GetCollection(job.ctx, job.req.GetCollectionID())
	if collection == nil || collection.GetLoadType() == querypb.LoadType_LoadPartition {
		return nil
	}
	// The partition may already exist: the CreatePartition ack callback adds it
	// before the proxy request arrives.
	if partition := job.meta.GetPartition(job.ctx, job.req.GetPartitionID()); partition == nil {
		partition := &meta.Partition{
			PartitionLoadInfo: &querypb.PartitionLoadInfo{
				CollectionID: req.GetCollectionID(),
				PartitionID:  req.GetPartitionID(),
				Status:       querypb.LoadStatus_Loaded,
			},
			LoadPercentage: 100,
			CreatedAt:      time.Now(),
		}
		err := job.meta.PutPartition(job.ctx, partition)
		if err != nil {
			msg := "failed to store partitions"
			mlog.Warn(job.ctx, msg, mlog.Err(err))
			return merr.Wrapf(err, "%s", msg)
		}
	}

	if req.GetSkipWaitReady() {
		if _, err := job.targetObserver.UpdatePartition(job.ctx, req.GetCollectionID(), req.GetPartitionID()); err != nil {
			// A DropCollection already reached the channel level on this
			// cluster, so the target can never include the partition. The
			// release of the collection removes the partition from the load meta.
			if errors.Is(err, merr.ErrChannelDroppedSentinel) {
				mlog.Warn(job.ctx, "skip target update of new partition: collection channel is dropped sentinel",
					mlog.FieldCollectionID(req.GetCollectionID()),
					mlog.FieldPartitionID(req.GetPartitionID()),
					mlog.Err(err))
				return nil
			}
			return merr.Wrapf(err, "failed to update next target, collection=%d", req.GetCollectionID())
		}
		job.targetObserver.TriggerUpdateCurrentTarget(req.GetCollectionID())
		return nil
	}
	return WaitUpdatePartition(job.ctx, job.targetObserver, job.req.GetCollectionID(), job.req.GetPartitionID())
}
