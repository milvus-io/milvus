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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/observers"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type UpdateLoadConfigJob struct {
	*BaseJob
	collectionID       int64
	newReplicaNumber   int32
	newResourceGroups  []string
	meta               *meta.Meta
	targetMgr          meta.TargetManagerInterface
	targetObserver     *observers.TargetObserver
	collectionObserver *observers.CollectionObserver
}

func NewUpdateLoadConfigJob(ctx context.Context,
	req *querypb.UpdateLoadConfigRequest,
	meta *meta.Meta,
	targetMgr meta.TargetManagerInterface,
	targetObserver *observers.TargetObserver,
	collectionObserver *observers.CollectionObserver,
) *UpdateLoadConfigJob {
	collectionID := req.GetCollectionIDs()[0]
	return &UpdateLoadConfigJob{
		BaseJob:            NewBaseJob(ctx, req.Base.GetMsgID(), collectionID),
		meta:               meta,
		targetMgr:          targetMgr,
		targetObserver:     targetObserver,
		collectionObserver: collectionObserver,
		collectionID:       collectionID,
		newReplicaNumber:   req.GetReplicaNumber(),
		newResourceGroups:  req.GetResourceGroups(),
	}
}

func (job *UpdateLoadConfigJob) Execute() error {
	if !job.meta.CollectionManager.Exist(job.ctx, job.collectionID) {
		msg := "modify replica for unloaded collection is not supported"
		err := merr.WrapErrCollectionNotLoaded(msg)
		log.Warn(msg, zap.Error(err))
		return err
	}

	// 1. check replica parameters
	if job.newReplicaNumber == 0 {
		msg := "set replica number to 0 for loaded collection is not supported"
		err := merr.WrapErrParameterInvalidMsg(msg)
		log.Warn(msg, zap.Error(err))
		return err
	}

	if len(job.newResourceGroups) == 0 {
		job.newResourceGroups = []string{meta.DefaultResourceGroupName}
	}

	var err error
	// 2. reassign
	toSpawn, toTransfer, toRelease, err := utils.ReassignReplicaToRG(job.ctx, job.meta, job.collectionID, job.newReplicaNumber, job.newResourceGroups)
	if err != nil {
		log.Warn("failed to reassign replica", zap.Error(err))
		return err
	}

	log.Info("reassign replica",
		zap.Int64("collectionID", job.collectionID),
		zap.Int32("replicaNumber", job.newReplicaNumber),
		zap.Strings("resourceGroups", job.newResourceGroups),
		zap.Any("toSpawn", toSpawn),
		zap.Any("toTransfer", toTransfer),
		zap.Any("toRelease", toRelease))

	// 3. try to spawn new replica
	channels := job.targetMgr.GetDmChannelsByCollection(job.ctx, job.collectionID, meta.CurrentTargetFirst)
	newReplicas, spawnErr := job.meta.ReplicaManager.Spawn(job.ctx, job.collectionID, toSpawn, lo.Keys(channels))
	if spawnErr != nil {
		log.Warn("failed to spawn replica", zap.Error(spawnErr))
		err := spawnErr
		return err
	}
	defer func() {
		if err != nil {
			// roll back replica from meta
			replicaIDs := lo.Map(newReplicas, func(r *meta.Replica, _ int) int64 { return r.GetID() })
			err := job.meta.ReplicaManager.RemoveReplicas(job.ctx, job.collectionID, replicaIDs...)
			if err != nil {
				log.Warn("failed to remove replicas", zap.Int64s("replicaIDs", replicaIDs), zap.Error(err))
			}
		}
	}()

	// 4. try to transfer replicas
	replicaOldRG := make(map[int64]string)
	for rg, replicas := range toTransfer {
		collectionReplicas := lo.GroupBy(replicas, func(r *meta.Replica) int64 { return r.GetCollectionID() })
		for collectionID, replicas := range collectionReplicas {
			for _, replica := range replicas {
				replicaOldRG[replica.GetID()] = replica.GetResourceGroup()
			}

			if transferErr := job.meta.ReplicaManager.MoveReplica(job.ctx, rg, replicas); transferErr != nil {
				log.Warn("failed to transfer replica for collection", zap.Int64("collectionID", collectionID), zap.Error(transferErr))
				err = transferErr
				return err
			}
		}
	}
	defer func() {
		if err != nil {
			for _, replicas := range toTransfer {
				for _, replica := range replicas {
					oldRG := replicaOldRG[replica.GetID()]
					if replica.GetResourceGroup() != oldRG {
						if err := job.meta.ReplicaManager.TransferReplica(job.ctx, replica.GetID(), replica.GetResourceGroup(), oldRG, 1); err != nil {
							log.Warn("failed to roll back replicas", zap.Int64("replica", replica.GetID()), zap.Error(err))
						}
					}
				}
			}
		}
	}()

	// 5. remove replica from meta
	err = job.meta.ReplicaManager.RemoveReplicas(job.ctx, job.collectionID, toRelease...)
	if err != nil {
		log.Warn("failed to remove replicas", zap.Int64s("replicaIDs", toRelease), zap.Error(err))
		return err
	}

	// 6. recover node distribution among replicas
	utils.RecoverReplicaOfCollection(job.ctx, job.meta, job.collectionID)

	// 7. update replica number in meta
	err = job.meta.UpdateReplicaNumber(job.ctx, job.collectionID, job.newReplicaNumber)
	if err != nil {
		msg := "failed to update replica number"
		log.Warn(msg, zap.Error(err))
		return err
	}

	// 8. update next target, no need to rollback if pull target failed, target observer will pull target in periodically
	_, err = job.targetObserver.UpdateNextTarget(job.collectionID)
	if err != nil {
		msg := "failed to update next target"
		log.Warn(msg, zap.Error(err))
	}

	return nil
}
